#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "core/hex.hpp"

namespace evml::core {

enum class Finality { Safe, Finalized, Depth };
enum class StreamKind { Events, Registry };
enum class StartPolicy { ContractStart, Gate };

const char* to_string(Finality f);
const char* to_string(StreamKind s);

struct ShardCfg {
    std::string id;       // [a-z0-9_]+ : used inside the Redis hash tag {id}
    std::string host_id;  // toml value, else $EVML_HOST_ID, else hostname
};

struct ChainCfg {
    std::string name;
    std::uint64_t chain_id{};
    std::uint32_t block_time_ms{};
    Finality default_finality{Finality::Finalized};
    std::uint32_t depth{};  // only meaningful when some finality == Depth
    std::uint32_t hash_ring_size{};
    StartPolicy start_policy{StartPolicy::ContractStart};
    std::uint32_t safe_poll_ms{};
    std::uint32_t finalized_poll_ms{};
};

// A provider that is ENABLED (its env var(s) resolved non-empty). URLs contain secrets: never log them
// unredacted (see env::redact).
struct Provider {
    std::string name;
    std::string ws_url;    // empty => not a WS candidate
    std::string http_url;  // empty => not a getLogs/tags candidate
    std::uint32_t max_getlogs_range{};
};

struct RedundancyCfg {
    std::string mode;  // "failover" | "hedged"
    std::vector<std::string> ws_order;
    std::vector<std::string> getlogs_order;
    std::uint32_t failover_after{};
    std::uint32_t stale_after_blocks{};
    std::uint32_t prefer_primary_after_s{};
    std::uint32_t quota_cooldown_s{};
};

struct LeaseCfg {
    bool enabled{};
    std::uint32_t ttl_ms{};
    std::uint32_t renew_ms{};
};

struct ContractCfg {
    std::string name;
    Address20 address;                         // normalised (lowercase on output)
    std::optional<std::uint64_t> start_block;  // nullopt => "gate" (no deep history)
    Finality finality{Finality::Finalized};
    StreamKind stream{StreamKind::Events};
    std::vector<std::string> events;  // canonical signatures
    std::vector<Hash32> topics;       // topic0 per event, same order
};

struct BackfillCfg {
    std::uint32_t min_range{};
    std::uint32_t request_timeout_ms{};
};

struct WatchdogCfg {
    std::uint32_t head_stall_ms{};
    std::uint32_t subscribe_timeout_ms{};
    std::uint32_t progress_stall_ms{};
    std::uint32_t safe_gate_stuck_s{};
    std::uint32_t gate_lag_alert_s{};
    std::uint32_t process_stall_ms{};
};

// A consumer group the downstream bots created; the listener only watches it (plan 4.10).
struct ConsumerGroupCfg {
    StreamKind stream{StreamKind::Events};
    std::string group;
};

struct SinkCfg {
    std::string redis_url_env;
    std::uint64_t retention_ms{};
    std::uint32_t dedup_ttl_s{};
    std::uint32_t queue_capacity{};
    std::string spill_dir;
    std::uint64_t spill_max_bytes{};
    bool require_noeviction{};
    std::vector<ConsumerGroupCfg> consumer_groups;  // empty = lag probe disabled
    std::uint32_t consumer_lag_warn_s{43200};
    std::uint32_t consumer_lag_crit_s{86400};
};

struct OpsCfg {
    std::string http_listen;
    std::uint32_t heartbeat_s{};
    std::string alert_webhook_env;
    std::string alert_webhook_token_env;
    std::string alert_service;
    std::string deadman_ping_env;
    std::uint32_t deadman_ping_s{};
    std::string http_user_agent;  // may contain "{version}"
};

struct EventRoute {
    std::string contract;
    std::string event;  // event name (signature up to '(')
    StreamKind stream{StreamKind::Events};
    Finality finality{Finality::Finalized};
};

struct RouteKey {
    Address20 address;
    Hash32 topic0;
    friend bool operator==(const RouteKey&, const RouteKey&) = default;
};
struct RouteKeyHash {
    std::size_t operator()(const RouteKey& k) const noexcept {
        return std::hash<Address20>{}(k.address) * 1000003u ^ std::hash<Hash32>{}(k.topic0);
    }
};

// One eth_getLogs filter: {address:[...], topics:[[t0...]]}.
struct LogFilter {
    std::vector<Address20> addresses;
    std::vector<Hash32> topic0s;
};

using EnvLookup = std::function<std::string(const std::string&)>;

struct Config {
    ShardCfg shard;
    ChainCfg chain;
    RedundancyCfg redundancy;
    LeaseCfg lease;
    std::vector<ContractCfg> contracts;
    BackfillCfg backfill;
    WatchdogCfg watchdog;
    SinkCfg sink;
    OpsCfg ops;

    std::vector<Provider> ws_chain;       // enabled WS providers in ws_order
    std::vector<Provider> getlogs_chain;  // enabled getLogs providers in getlogs_order

    // INFO/WARN lines produced while resolving (skipped providers, disabled dead-man switch). No secrets.
    std::vector<std::string> notes;

    std::unordered_map<RouteKey, EventRoute, RouteKeyHash> routes;

    // nullptr if (address, topic0) is not a tracked pair (callers count it in a metric and drop it).
    const EventRoute* route(const Address20& address, const Hash32& topic0) const;

    LogFilter filter() const;                // every contract
    LogFilter filter(Finality only) const;   // contracts with this finality class

    // Throws ConfigError with a precise message. `origin` is used in error text (e.g. the file path).
    static Config parse(std::string_view toml_text, const std::string& origin, const EnvLookup& env);
    static Config parse(std::string_view toml_text, const std::string& origin);  // process environment
    static Config load_file(const std::filesystem::path& path);                  // process environment
};

}  // namespace evml::core
