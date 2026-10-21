#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>

#include "core/config.hpp"
#include "provider/provider_book.hpp"
#include "provider/stale_detector.hpp"
#include "rpc/backoff.hpp"
#include "rpc/classify.hpp"
#include "rpc/codec.hpp"
#include "rpc/http_rpc_client.hpp"
#include "rpc/ws_session.hpp"

namespace evml::provider {

/// @brief Network endpoint descriptor for a single RPC provider (WebSocket and HTTP URLs).
struct ProviderEndpoint {
    std::string name;
    std::string ws_url;    // empty for providers that are only used for getLogs
    std::string http_url;  // required for every provider that appears in an order list
};

/// @brief Configuration settings controlling provider pool failover, timeouts, backoff, and poll intervals.
struct PoolConfig {
    using ms = std::chrono::milliseconds;

    std::vector<ProviderEndpoint> providers;
    std::vector<std::string> ws_order;       // WS failover chain, best first
    std::vector<std::string> getlogs_order;  // HTTP failover chain, best first
    std::uint64_t chain_id{};
    std::string user_agent;
    bool subscribe_heads{true};
    std::optional<core::LogFilter> logs;  // nullopt => no logs subscription (then subscribe_heads must be true)

    ms connect_timeout{10000};
    ms handshake_timeout{10000};
    ms request_timeout{10000};
    ms subscribe_timeout{10000};
    ms idle_timeout{20000};
    ms http_timeout{10000};

    Policy policy{};
    StaleThresholds stale{};

    std::uint64_t seed{1};  // per-provider backoff RNG seeds derive from it
    ms backoff_base{250};
    ms backoff_cap{30000};
    ms backoff_min_live{30000};  // a session must stay Ready this long to count as healthy

    ms tick{250};                   // maintenance cadence
    ms standby_probe_period{60000};  // eth_blockNumber probe of Up providers that carry no traffic
    ms safe_poll{4000};              // getLogs provider: head + `safe`
    ms finalized_poll{12000};        // getLogs provider: `finalized`
    ms crosscheck_period{60000};     // WS provider's own `safe` (for the getLogs-lag check)
};

/// @brief Event callbacks fired by the provider pool for streaming blocks, logs, transitions, and connection events.
struct PoolHandlers {
    // All optional; called on the pool's executor, must not throw.
    std::function<void(const rpc::Head&, const std::string& provider)> on_head;
    std::function<void(const rpc::Log&, const std::string& provider)> on_log;
    std::function<void(const Transition&, std::string_view reason)> on_transition;
    // nullopt = no WS provider available. `reason`: initial | failover | switch_back | none_available.
    std::function<void(const std::optional<std::string>&, std::string_view reason)> on_ws_active_changed;
    std::function<void(const std::string& provider)> on_ws_ready;
    std::function<void(const rpc::WsClosed&, const std::string& provider)> on_ws_down;
};

/// @brief Block tag identifiers for chain progression queries.
enum class Tag { Safe, Finalized };

/// @brief Point-in-time snapshot of a resolved block tag (number and hash) from a specific provider.
struct TagSnapshot {
    std::string provider;  // the provider that answered: a snapshot is only meaningful against that provider
    std::uint64_t number{};
    core::Hash32 hash;
    TimePoint at;
};

using RequestBuilder = std::function<std::string(std::uint64_t id)>;

/// @brief Result of a JSON-RPC HTTP invocation with error classification and retry metadata.
struct RpcOutcome {
    bool ok{false};
    std::string provider;    // who answered (or who failed last)
    std::string result_raw;  // raw JSON `result` when ok
    rpc::FailKind kind{rpc::FailKind::Retry};  // when !ok
    std::optional<std::uint32_t> range_limit;  // RangeTooLarge: provider's limit when it states one
    std::optional<std::chrono::seconds> retry_after;
    std::string error;  // human-readable, no secrets
};

// Runs the provider chains (plan 2.5/2.6): one supervised WebSocket session on the best usable WS provider,
// HTTP requests with failover over the getLogs chain, health probes, `safe`/`finalized` tag polling and
// stale/lag detection, all driven by ProviderBook + StaleDetector. Single-threaded: create it, and use every
// member, from one executor (strand-free io_context thread). Create via create(); keep the shared_ptr alive
// until every awaitable returned by call()/read_tag() has completed.
/// @brief Multi-provider RPC orchestrator managing active WebSocket streaming and HTTP request failover.
/// Invariants & Architecture:
/// - Dual Paths: Keeps active WebSocket stream for real-time events while using distinct HTTP endpoints for getLogs.
/// - Self-Healing: Background tick probes dormant providers, polls safe/finalized block tags, and restarts dropped sessions.
/// - Concurrency: Thread-confined to a single Boost.Asio io_context executor.
class ProviderPool : public std::enable_shared_from_this<ProviderPool> {
public:
    // Throws std::invalid_argument for an inconsistent config (unknown/duplicate names, missing URLs, empty chains,
    // no subscription) and rpc::TransportError for a malformed URL.
    /// @brief Factory creating and validating a new ProviderPool instance.
    static std::shared_ptr<ProviderPool> create(boost::asio::any_io_executor executor, PoolConfig cfg,
                                                PoolHandlers handlers);

    /// @brief Starts background tick timer, initiates WebSocket session, and schedules periodic probes.
    void start();  // throws std::logic_error if called twice
    /// @brief Stops timers, terminates active WebSocket session, and suppresses pending callbacks.
    void stop();   // idempotent; no handler is called afterwards

    // One JSON-RPC request over the getLogs chain. `build` is called once per attempt with that attempt's id.
    // A provider failure falls through to the next provider; RangeTooLarge returns immediately (it is the
    // caller's range, not the provider's fault). All providers failing returns ok=false with the last error.
    /// @brief Coroutine executing JSON-RPC HTTP request with sequential failover across usable providers.
    boost::asio::awaitable<RpcOutcome> call(RequestBuilder build);

    // Fresh `safe`/`finalized` read from ONE provider (caches it). nullopt on any failure (counted against the
    // provider; a null block counts as a failure too).
    /// @brief Queries named provider for latest block matching Tag (Safe or Finalized) and caches the snapshot.
    boost::asio::awaitable<std::optional<TagSnapshot>> read_tag(std::string provider, Tag tag);
    // Last successful read_tag from that provider. Never mixes providers.
    /// @brief Returns cached block tag snapshot previously fetched from specified provider.
    std::optional<TagSnapshot> cached_tag(const std::string& provider, Tag tag) const;

    /// @brief Returns name of currently active WebSocket provider.
    const std::optional<std::string>& active_ws() const { return book_.active_ws(); }
    /// @brief Returns name of primary getLogs provider currently selected.
    std::optional<std::string> current_getlogs() const { return book_.pick_getlogs(Clock::now()); }
    /// @brief Returns current health state of named provider.
    State state(const std::string& provider) const { return book_.state(provider).state; }
    /// @brief Returns measured block height divergence between WebSocket and HTTP providers.
    std::optional<std::uint64_t> divergence() const { return detector_.divergence(); }

    ProviderPool(const ProviderPool&) = delete;
    ProviderPool& operator=(const ProviderPool&) = delete;

private:
    struct Private {};

public:
    ProviderPool(Private, boost::asio::any_io_executor executor, PoolConfig cfg, PoolHandlers handlers);

private:
    using Clock = std::chrono::steady_clock;
    using ms = std::chrono::milliseconds;

    struct Node {
        Node(ProviderEndpoint e, std::unique_ptr<rpc::HttpRpcClient> h, rpc::Backoff b)
            : ep(std::move(e)), http(std::move(h)), backoff(std::move(b)) {}
        ProviderEndpoint ep;
        std::unique_ptr<rpc::HttpRpcClient> http;
        rpc::Backoff backoff;
        std::array<std::optional<TagSnapshot>, 2> tags;
        TimePoint next_standby_probe{};
    };

    // WS supervisor
    void reconcile();
    void start_session(const std::string& name);
    void end_session_quiet();
    void end_session(rpc::WsClosed c);
    void schedule_restart(const std::string& name, ms delay);

    // maintenance
    void schedule_tick();
    void tick();
    void note(const std::string& provider, const RpcOutcome& o);
    void emit_transition(const Transition& t, std::string_view reason);

    // I/O (each keeps the pool alive through `self`)
    boost::asio::awaitable<RpcOutcome> call_on(std::string provider, std::string body, std::uint64_t id);
    boost::asio::awaitable<void> probe(std::shared_ptr<ProviderPool> self, std::string provider);
    boost::asio::awaitable<void> poll(std::shared_ptr<ProviderPool> self, std::string provider);
    boost::asio::awaitable<void> crosscheck(std::shared_ptr<ProviderPool> self, std::string provider);

    boost::asio::any_io_executor exec_;
    PoolConfig cfg_;
    PoolHandlers handlers_;
    ProviderBook book_;
    StaleDetector detector_;
    rpc::Decoder dec_;  // used only synchronously (never across a co_await)
    std::unordered_map<std::string, Node> nodes_;
    std::vector<std::string> names_;  // every provider, for probe iteration

    boost::asio::steady_timer tick_timer_;
    boost::asio::steady_timer restart_timer_;

    bool started_{false};
    bool running_{false};
    std::uint64_t next_id_{1};

    std::shared_ptr<rpc::WsSession> session_;
    std::string session_name_;
    std::uint64_t session_gen_{0};
    bool ws_ready_{false};
    bool ws_credited_{false};
    TimePoint ws_ready_at_{};

    std::optional<std::string> last_getlogs_;
    std::unordered_set<std::string> probing_;
    bool polling_{false};
    bool crosschecking_{false};
    TimePoint next_poll_{};
    TimePoint next_finalized_{};
    TimePoint next_crosscheck_{};
};

}  // namespace evml::provider
