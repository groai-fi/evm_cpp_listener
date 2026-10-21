#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <string>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>

#include "rpc/classify.hpp"
#include "rpc/codec.hpp"
#include "rpc/url.hpp"

namespace evml::rpc {

namespace detail {
struct HttpConn;  // one live TCP or TLS connection (defined in the .cpp)
}

/// @brief Configuration options for HttpRpcClient.
struct HttpRpcClientConfig {
    std::string name;                          // Provider name (e.g., "primary_alchemy", "backup_infura")
    std::string url;                           // Endpoint URL (http:// for tests/local nodes, https:// for production)
    std::string user_agent;                    // HTTP User-Agent header
    std::chrono::milliseconds timeout{10000};  // Per-phase deadline: connect, TLS handshake, write, read
};

/// @brief Result payload of an HTTP exchange.
struct HttpResult {
    int status{};                                     // HTTP status code (200, 429, 500, etc.)
    std::string body;                                 // Response body string (up to 64 MiB)
    std::optional<std::chrono::seconds> retry_after;  // Parsed delta-seconds from Retry-After header
};

/// @brief Dissects an HTTP response into ErrorInfo suitable for the failure classifier.
/// @param r HTTP exchange result.
/// @param decoder JSON-RPC decoder used to probe body for error objects.
/// @return Extracted ErrorInfo.
ErrorInfo error_info_from_http(const HttpResult& r, Decoder& decoder);

/// @brief Asynchronous HTTP 1.1 client for a single JSON-RPC provider with connection pooling and FIFO serialization.
/// Concurrency & Threading:
/// - Single-threaded coroutine actor: all methods MUST execute on the configured executor.
/// - Requests are serialized FIFO (one request in flight on the persistent TCP/TLS stream).
/// Failure & Retry Invariants:
/// - Dead Connection Healing: If a previously established keep-alive connection was closed idle by the server/LB,
///   it is replaced and retried exactly once.
/// - Fail-Fast on Timeout: Phase timeouts are never retried locally; they propagate immediately to ProviderPool.
/// - Credential Protection: Error messages always redact tokens and secrets from URLs.
class HttpRpcClient {
public:
    /// @brief Constructs client bound to the specified executor and configuration.
    HttpRpcClient(boost::asio::any_io_executor executor, HttpRpcClientConfig cfg);
    ~HttpRpcClient();
    HttpRpcClient(const HttpRpcClient&) = delete;
    HttpRpcClient& operator=(const HttpRpcClient&) = delete;

    /// @brief Sends a serialized JSON-RPC payload asynchronously.
    /// @param json_body Complete request JSON string.
    /// @return HttpResult with status code and body.
    /// @warning Throws TransportError on unrecoverable transport or TLS errors.
    boost::asio::awaitable<HttpResult> call(std::string json_body);

    /// @brief Closes the active socket connection if open (safe to call at any time).
    void close();

    /// @brief Provider identifier name.
    const std::string& name() const { return cfg_.name; }

private:
    boost::asio::awaitable<std::unique_ptr<detail::HttpConn>> open();

    boost::asio::any_io_executor exec_;
    HttpRpcClientConfig cfg_;
    ParsedUrl url_;
    std::string redacted_;
    std::unique_ptr<detail::HttpConn> conn_;
    boost::asio::steady_timer lock_timer_;  // wake-up channel of the FIFO request lock
    bool locked_{false};
};

}  // namespace evml::rpc
