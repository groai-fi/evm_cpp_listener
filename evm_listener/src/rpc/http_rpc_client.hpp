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

struct HttpRpcClientConfig {
    std::string name;  // provider name, used in error messages
    std::string url;   // http:// (tests/local nodes) or https://
    std::string user_agent;
    std::chrono::milliseconds timeout{10000};  // applied to each phase: connect, TLS, write, read
};

struct HttpResult {
    int status{};
    std::string body;
    std::optional<std::chrono::seconds> retry_after;  // from a delta-seconds Retry-After header
};

// Builds the classifier input from a non-2xx (or JSON-RPC error) HTTP result.
ErrorInfo error_info_from_http(const HttpResult& r, Decoder& decoder);

// Async JSON-RPC-over-HTTP client for ONE provider. Single-threaded: every member must be used from the executor
// it was constructed with.
//  - One persistent keep-alive connection; requests are serialised (FIFO, one in flight).
//  - A reused connection that turns out to be dead (idle-closed by the server/LB) is replaced and the request is
//    retried exactly once; timeouts are never retried here (the pool decides).
//  - Sends `Accept-Encoding: identity` (Beast does not gunzip) and the configured User-Agent.
//  - Throws TransportError for network/TLS/timeout failures. Any HTTP status is returned as a result.
class HttpRpcClient {
public:
    HttpRpcClient(boost::asio::any_io_executor executor, HttpRpcClientConfig cfg);
    ~HttpRpcClient();
    HttpRpcClient(const HttpRpcClient&) = delete;
    HttpRpcClient& operator=(const HttpRpcClient&) = delete;

    boost::asio::awaitable<HttpResult> call(std::string json_body);

    // Drops the connection (next call reconnects). Safe to call at any time.
    void close();

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
