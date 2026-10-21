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

/// @brief Configuration options for a single WebSocket RPC connection session.
struct WsSessionConfig {
    std::string name;                                    // Provider name for logs and error tracing
    std::string url;                                     // wss:// (production) or ws:// (local mock nodes)
    std::string user_agent;                              // User-Agent sent in WebSocket upgrade request
    std::uint64_t expected_chain_id{};                   // Required EVM chain ID (mismatch triggers ConfigFatal)
    bool subscribe_heads{true};                          // If true, subscribes to "newHeads" feed
    std::optional<core::LogFilter> logs;                 // Optional filter for log event subscription
    std::chrono::milliseconds connect_timeout{10000};    // TCP connect and TLS handshake deadline, each
    std::chrono::milliseconds handshake_timeout{10000};  // WebSocket HTTP upgrade deadline
    std::chrono::milliseconds request_timeout{10000};    // eth_chainId RPC response deadline
    std::chrono::milliseconds subscribe_timeout{10000};  // eth_subscribe ACK deadline per feed
    std::chrono::milliseconds idle_timeout{20000};       // Max silence before detecting half-open peer
};

/// @brief Terminal event explaining why a WebSocket session terminated.
/// Delivered exactly once to on_closed, guaranteed to contain no sensitive URL tokens.
struct WsClosed {
    FailKind kind{FailKind::Retry};                   // Actionable failure category for recovery/backoff
    std::string reason;                               // Human-readable diagnostic description (URL redacted)
    std::optional<std::chrono::seconds> retry_after;  // Parsed delta-seconds hint from HTTP upgrade 429
};

/// @brief Callbacks invoked on the session's Boost.Asio executor.
/// @warning Handlers must NOT throw exceptions. Any throw from on_closed halts the process immediately;
/// exceptions in other handlers abort the session with FailKind::Retry.
struct WsHandlers {
    std::function<void()> on_ready;                     // Invoked once chain ID verified and all feeds ACKed
    std::function<void(const Head&)> on_head;          // Invoked on new block header push notification
    std::function<void(const Log&)> on_log;            // Invoked on log event push notification
    std::function<void(const WsClosed&)> on_closed;    // Invoked once upon session termination
};

/// @brief One-shot WebSocket ingestion actor streaming block headers and log events from an EVM node.
/// Lifecycle & Invariants:
/// - One-Shot Actor: A session instance represents a single connection attempt. After on_closed, the owner
///   discards this instance and instantiates a new one following Backoff schedule.
/// - Concurrency: All member methods and callbacks execute strictly on the assigned Boost.Asio executor.
/// - Chain Safety: Rejects provider connection immediately (ConfigFatal) if eth_chainId does not match config.
/// - Half-Open Detection: Enforces Beast ping/pong keepalive and kernel-level TCP keepalive probes.
class WsSession : public std::enable_shared_from_this<WsSession> {
public:
    /// @brief Factory function creating an initialized WebSocket session.
    /// @warning Throws std::invalid_argument if no subscription is configured or required handlers are missing.
    static std::shared_ptr<WsSession> create(boost::asio::any_io_executor executor, WsSessionConfig cfg,
                                             WsHandlers handlers);

    /// @brief Spawns the session coroutine on the executor (throws std::logic_error if called twice).
    void start();
    /// @brief Idempotently hard-closes the underlying socket, suppressing further callbacks.
    void stop();

    /// @brief Returns true if the session has terminated or been stopped.
    bool closed() const { return closed_; }
    /// @brief Provider identifier name.
    const std::string& name() const { return cfg_.name; }
    /// @brief Total duration the session was in Ready state (used by Backoff::report_live to measure stability).
    std::chrono::milliseconds live_for() const { return live_for_; }
    /// @brief Number of dropped notifications whose subscription ID was unknown to this session.
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
