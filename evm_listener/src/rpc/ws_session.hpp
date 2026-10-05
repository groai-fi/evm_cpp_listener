#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>

#include "core/config.hpp"
#include "rpc/classify.hpp"
#include "rpc/codec.hpp"
#include "rpc/url.hpp"

namespace evml::rpc {

struct WsSessionConfig {
    std::string name;  // provider name, used in logs/reasons
    std::string url;   // wss:// (prod) or ws:// (mock tests only)
    std::string user_agent;
    std::uint64_t expected_chain_id{};
    bool subscribe_heads{true};
    std::optional<core::LogFilter> logs;  // nullopt => no logs subscription
    std::chrono::milliseconds connect_timeout{10000};    // TCP connect and TLS handshake, each
    std::chrono::milliseconds handshake_timeout{10000};  // WebSocket upgrade
    std::chrono::milliseconds request_timeout{10000};    // eth_chainId response
    std::chrono::milliseconds subscribe_timeout{10000};  // each eth_subscribe ACK
    std::chrono::milliseconds idle_timeout{20000};       // no frame/pong at all => half-open => fail
};

// Why a session ended. Delivered exactly once, last, and never after stop().
struct WsClosed {
    FailKind kind{FailKind::Retry};
    std::string reason;  // human-readable, contains no secrets (URL is always redacted)
    std::optional<std::chrono::seconds> retry_after;  // RateLimited: provider hint (e.g. from the upgrade 429)
};

// Callbacks run on the session's executor. They must not throw: an exception from on_closed terminates the process
// (fail fast); an exception from the others ends the session with Retry and is reported through on_closed.
struct WsHandlers {
    std::function<void()> on_ready;  // chain id verified and every configured subscription ACKed
    std::function<void(const Head&)> on_head;
    std::function<void(const Log&)> on_log;
    std::function<void(const WsClosed&)> on_closed;
};

// One WebSocket connection to one provider: resolve -> TCP -> TLS (SNI + host verification) -> WS upgrade ->
// eth_chainId check -> subscribe(newHeads / logs) -> stream. ONE-SHOT: after on_closed the owner creates a new
// session (and applies Backoff). Single-threaded: use only from the executor it was created with.
//
// Failure detection:
//  - dead/half-open peers: Beast idle timeout with keep-alive pings (`idle_timeout`);
//  - missing chain-id reply or subscription ACK: per-request deadline that aborts the socket;
//  - upgrade rejections (401/403/429/...) are classified via classify().
// Subscription ids are tracked per session only (providers may reuse the same id across connections).
class WsSession : public std::enable_shared_from_this<WsSession> {
public:
    // Throws std::invalid_argument when no subscription is configured or a handler is missing.
    static std::shared_ptr<WsSession> create(boost::asio::any_io_executor executor, WsSessionConfig cfg,
                                             WsHandlers handlers);

    void start();  // throws std::logic_error if called twice
    void stop();   // idempotent; hard-closes the socket; suppresses all further callbacks

    bool closed() const { return closed_; }
    const std::string& name() const { return cfg_.name; }
    // How long the session was Ready before it ended (0 if it never became ready). For Backoff::report_live.
    std::chrono::milliseconds live_for() const { return live_for_; }
    // Notifications whose subscription id matched nothing this session subscribed to (dropped, WARN-logged).
    std::uint64_t unknown_subscription_msgs() const { return unknown_msgs_; }

    WsSession(const WsSession&) = delete;
    WsSession& operator=(const WsSession&) = delete;

private:
    struct Private {};

public:
    WsSession(Private, boost::asio::any_io_executor executor, WsSessionConfig cfg, WsHandlers handlers);

private:
    friend struct WsSessionAccess;  // cpp-local protocol code

    boost::asio::awaitable<void> run();
    void finish(WsClosed c);
    void close_socket();

    boost::asio::any_io_executor exec_;
    WsSessionConfig cfg_;
    WsHandlers handlers_;
    ParsedUrl url_;
    std::string redacted_;

    bool started_{false};
    bool closed_{false};
    std::function<void()> closer_;  // set while a socket exists
    std::optional<std::chrono::steady_clock::time_point> ready_at_;
    std::chrono::milliseconds live_for_{0};
    std::uint64_t unknown_msgs_{0};
};

}  // namespace evml::rpc
