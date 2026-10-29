#include "ops/http_client.hpp"

#include <cctype>
#include <memory>
#include <stdexcept>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include <openssl/ssl.h>

#include "core/env.hpp"
#include "rpc/blocking_http.hpp"
#include "rpc/url.hpp"

namespace evml::ops {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

namespace {

/// Runs one phase, turning Boost errors into a TransportError that names the phase and the REDACTED url only.
template <class Awaitable>
net::awaitable<void> phase(const std::string& who, const char* name, Awaitable&& a) {
    try {
        co_await std::forward<Awaitable>(a);
    } catch (const boost::system::system_error& e) {
        const bool timed_out = e.code() == beast::error::timeout;
        throw rpc::TransportError(std::string(name) + " failed for " + who + ": " +
                                  (timed_out ? "timeout" : e.code().message()));
    }
}

template <class Stream>
net::awaitable<http::response<http::string_body>> exchange(Stream& stream, http::request<http::string_body>& req,
                                                           std::chrono::milliseconds timeout, std::size_t max_bytes,
                                                           const std::string& who) {
    beast::get_lowest_layer(stream).expires_after(timeout);
    co_await phase(who, "write", http::async_write(stream, req, net::use_awaitable));

    beast::flat_buffer buf;
    http::response_parser<http::string_body> parser;
    parser.body_limit(max_bytes);
    beast::get_lowest_layer(stream).expires_after(timeout);
    co_await phase(who, "read", http::async_read(stream, buf, parser, net::use_awaitable));
    co_return parser.release();
}

bool iequals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i)
        if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(b[i]))) return false;
    return true;
}

/// Header values may be secrets (X-API-Key): the messages below never include them.
void validate_headers(const HttpClient::Headers& headers) {
    static constexpr std::string_view kOwned[] = {"host", "content-length", "connection", "transfer-encoding"};
    for (const auto& [name, value] : headers) {
        if (name.empty() || name.find_first_of(" \t:\r\n") != std::string::npos)
            throw std::invalid_argument("HttpClient: invalid header name");
        if (value.find_first_of("\r\n") != std::string::npos)
            throw std::invalid_argument("HttpClient: header \"" + name + "\" has a line break in its value");
        for (const auto owned : kOwned)
            if (iequals(name, owned)) throw std::invalid_argument("HttpClient: header \"" + name + "\" is set by the client");
    }
}

}  // namespace

HttpClient::HttpClient(net::any_io_executor executor, HttpClientOptions opts)
    : exec_(std::move(executor)), opts_(std::move(opts)) {
    if (opts_.timeout.count() <= 0) throw std::invalid_argument("HttpClient: timeout must be > 0");
    if (opts_.max_response_bytes == 0) throw std::invalid_argument("HttpClient: max_response_bytes must be > 0");
}

net::awaitable<HttpResponse> HttpClient::post_json(const std::string& url, std::string body, const Headers& headers) {
    co_return co_await request(Method::Post, url, std::move(body), headers);
}

net::awaitable<HttpResponse> HttpClient::get(const std::string& url, const Headers& headers) {
    co_return co_await request(Method::Get, url, {}, headers);
}

void HttpClient::cancel() {
    cancelled_ = true;
    auto active = closers_;
    for (auto& [_, fn] : active) {
        if (fn) fn();
    }
}

net::awaitable<HttpResponse> HttpClient::request(Method method, const std::string& url, std::string body,
                                                 const Headers& headers) {
    validate_headers(headers);
    const std::string who = core::env::redact(url);
    if (cancelled_) throw rpc::TransportError("request cancelled for " + who);
    const rpc::ParsedUrl u = rpc::parse_url(url, rpc::UrlKind::Http);  // throws TransportError with the redacted URL

    struct CloserGuard {
        std::unordered_map<std::uint64_t, std::function<void()>>& map;
        std::uint64_t id;
        CloserGuard(std::unordered_map<std::uint64_t, std::function<void()>>& m, std::uint64_t i, std::function<void()> fn)
            : map(m), id(i) {
            map[id] = std::move(fn);
        }
        ~CloserGuard() { map.erase(id); }
    };

    http::request<http::string_body> req{method == Method::Post ? http::verb::post : http::verb::get, u.target, 11};
    req.set(http::field::host, u.host_header());
    req.set(http::field::user_agent, opts_.user_agent);
    req.set(http::field::accept, "*/*");
    req.set(http::field::accept_encoding, "identity");  // no gunzip on our side
    req.set(http::field::connection, "close");
    if (method == Method::Post) {
        req.set(http::field::content_type, "application/json");
        req.body() = std::move(body);
    }
    for (const auto& [name, value] : headers) req.set(name, value);
    req.prepare_payload();

    tcp::resolver resolver(exec_);
    tcp::resolver::results_type endpoints;
    {
        CloserGuard rg(closers_, next_closer_id_++, [&resolver] { resolver.cancel(); });
        try {
            endpoints = co_await resolver.async_resolve(u.host, u.port, net::use_awaitable);
        } catch (const boost::system::system_error& e) {
            throw rpc::TransportError("resolve failed for " + who + ": " + e.code().message());
        }
    }
    if (cancelled_) throw rpc::TransportError("request cancelled for " + who);

    http::response<http::string_body> res;
    if (!u.tls) {
        auto stream = std::make_shared<beast::tcp_stream>(exec_);
        CloserGuard sg(closers_, next_closer_id_++, [stream] {
            beast::error_code ec;
            stream->socket().close(ec);
        });
        stream->expires_after(opts_.timeout);
        co_await phase(who, "connect", stream->async_connect(endpoints, net::use_awaitable));
        res = co_await exchange(*stream, req, opts_.timeout, opts_.max_response_bytes, who);
        beast::error_code ec;
        stream->socket().shutdown(tcp::socket::shutdown_both, ec);
        stream->close();
    } else {
        // TLS with the system trust store, peer verification and a hostname check. Hold ctx before stream in a
        // heap-allocated struct so ssl::context is guaranteed to outlive beast::ssl_stream across cancellation/teardown.
        struct TlsHttpConn {
            std::shared_ptr<ssl::context> ctx;
            beast::ssl_stream<beast::tcp_stream> stream;
            TlsHttpConn(const net::any_io_executor& ex, std::shared_ptr<ssl::context> c)
                : ctx(std::move(c)), stream(ex, *ctx) {}
        };
        auto ctx = std::make_shared<ssl::context>(ssl::context::tls_client);
        ctx->set_default_verify_paths();
        ctx->set_verify_mode(ssl::verify_peer);
        auto conn = std::make_shared<TlsHttpConn>(exec_, std::move(ctx));
        auto& stream = conn->stream;
        CloserGuard sg(closers_, next_closer_id_++, [conn] {
            beast::error_code ec;
            beast::get_lowest_layer(conn->stream).socket().close(ec);
        });
        if (!SSL_set_tlsext_host_name(stream.native_handle(), u.host.c_str()))
            throw rpc::TransportError("cannot set TLS SNI for " + who);
        stream.set_verify_callback(ssl::host_name_verification(u.host));
        beast::get_lowest_layer(stream).expires_after(opts_.timeout);
        co_await phase(who, "connect", beast::get_lowest_layer(stream).async_connect(endpoints, net::use_awaitable));
        beast::get_lowest_layer(stream).expires_after(opts_.timeout);
        co_await phase(who, "TLS handshake", stream.async_handshake(ssl::stream_base::client, net::use_awaitable));
        res = co_await exchange(stream, req, opts_.timeout, opts_.max_response_bytes, who);
        beast::error_code ec;
        beast::get_lowest_layer(stream).socket().shutdown(tcp::socket::shutdown_both, ec);
        beast::get_lowest_layer(stream).close();
    }
    co_return HttpResponse{static_cast<int>(res.result_int()), std::move(res.body())};
}

}  // namespace evml::ops
