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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>

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

    HttpClient(boost::asio::any_io_executor executor, HttpClientOptions opts = {});

    /// @brief POST `body` as application/json. Any status is returned to the caller (it decides what a non-2xx means).
    /// @throws rpc::TransportError on a bad URL, resolve/connect/TLS/read/write failure, timeout or an oversized response
    ///         (message has the redacted URL only); std::invalid_argument on a header that could break the request.
    boost::asio::awaitable<HttpResponse> post_json(const std::string& url, std::string body, const Headers& headers = {});

    /// @brief GET. Same contract as post_json.
    boost::asio::awaitable<HttpResponse> get(const std::string& url, const Headers& headers = {});

    /// @brief Cancels any in-flight resolve or socket operations on this client.
    void cancel();

private:
    enum class Method { Get, Post };
    boost::asio::awaitable<HttpResponse> request(Method method, const std::string& url, std::string body,
                                                 const Headers& headers);

    boost::asio::any_io_executor exec_;
    HttpClientOptions opts_;
    bool cancelled_{false};
    std::uint64_t next_closer_id_{1};
    std::unordered_map<std::uint64_t, std::function<void()>> closers_;
};

}  // namespace evml::ops
