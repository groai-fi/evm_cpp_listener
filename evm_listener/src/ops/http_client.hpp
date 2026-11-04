#pragma once
// Async generic HTTP(S) client for the ops side (plan 60 §5.3, slice 5-B2): the n8n alert POST (custom X-API-Key header)
// and the dead-man GET. One request = one connection (`Connection: close`): alerts and pings are rare, so no pooling.
// HttpRpcClient is JSON-RPC only and the blocking client is forbidden on the io thread, hence this class.
//
// Credential protection: the URL and every header value may be a secret. Error messages carry only the redacted URL
// (core::env::redact), never header values, and request bodies are never echoed.
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>

#include "core/error.hpp"

namespace evml::ops {

struct HttpClientOptions {
    std::string user_agent{"evm-listener"};
    std::chrono::milliseconds timeout{10000};     // per phase: resolve is OS-bound, then connect, TLS handshake, write, read
    std::size_t max_response_bytes{1u << 20};     // 1 MiB: these endpoints answer with a few bytes
};

struct HttpResponse {
    int status{};
    std::string body;
};

class HttpClient {
public:
    using Headers = std::vector<std::pair<std::string, std::string>>;

    explicit HttpClient(boost::asio::any_io_executor executor, HttpClientOptions opts = {});

    [[nodiscard]] boost::asio::awaitable<core::Result<HttpResponse>> try_post_json(
        std::string url, std::string body, Headers headers = {});
    [[nodiscard]] boost::asio::awaitable<core::Result<HttpResponse>> try_get(
        std::string url, Headers headers = {});

    /// @brief POST `body` as application/json. Any status is returned to the caller (it decides what a non-2xx means).
    /// @throws rpc::TransportError on a bad URL, resolve/connect/TLS/read/write failure, timeout or an oversized response
    ///         (message has the redacted URL only); std::invalid_argument on a header that could break the request.
    [[nodiscard]] boost::asio::awaitable<HttpResponse> post_json(std::string url, std::string body,
                                                                 Headers headers = {});

    /// @brief GET. Same contract as post_json.
    [[nodiscard]] boost::asio::awaitable<HttpResponse> get(std::string url, Headers headers = {});

    /// @brief Cancels any in-flight resolve or socket operations on this client.
    void cancel();

    struct State;

private:
    enum class Method : std::uint8_t { Get, Post };
    static boost::asio::awaitable<core::Result<HttpResponse>> request(
        std::shared_ptr<State> st, Method method, std::string url, std::string body, Headers headers);

    std::shared_ptr<State> state_;
};

}  // namespace evml::ops
