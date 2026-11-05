#include "ops/http_client.hpp"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <cctype>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <unordered_map>

#include "core/env.hpp"
#include "rpc/blocking_http.hpp"
#include "rpc/tls_client.hpp"
#include "rpc/url.hpp"

namespace evml::ops {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

namespace {

core::Error format_phase_err(std::string_view who, const char* name, const boost::system::error_code& ec) {
    const bool timed_out = ec == beast::error::timeout;
    return core::Error{std::string(name) + " failed for " + std::string(who) + ": " +
                       (timed_out ? "timeout" : ec.message())};
}

template <class Stream>
net::awaitable<core::Result<http::response<http::string_body>>> exchange(std::shared_ptr<Stream> stream,
                                                                         http::request<http::string_body> req,
                                                                         std::chrono::milliseconds timeout,
                                                                         std::size_t max_bytes, std::string who) {
    boost::system::error_code ec;
    beast::get_lowest_layer(*stream).expires_after(timeout);
    co_await http::async_write(*stream, req, net::redirect_error(net::use_awaitable, ec));
    if (ec) co_return format_phase_err(who, "write", ec);

    beast::flat_buffer buf;
    http::response_parser<http::string_body> parser;
    parser.body_limit(max_bytes);
    beast::get_lowest_layer(*stream).expires_after(timeout);
    co_await http::async_read(*stream, buf, parser, net::redirect_error(net::use_awaitable, ec));
    if (ec) co_return format_phase_err(who, "read", ec);
    co_return parser.release();
}

bool iequals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i)
        if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(b[i])))
            return false;
    return true;
}

/// Header values may be secrets (X-API-Key): the messages below never include them.
std::optional<core::Error> validate_headers(const HttpClient::Headers& headers) {
    static constexpr std::string_view kOwned[] = {"host", "content-length", "connection", "transfer-encoding"};
    for (const auto& [name, value] : headers) {
        if (name.empty() || name.find_first_of(" \t:\r\n") != std::string::npos)
            return core::Error{"HttpClient: invalid header name"};
        if (value.find_first_of("\r\n") != std::string::npos)
            return core::Error{"HttpClient: header \"" + name + "\" has a line break in its value"};
        for (const auto owned : kOwned)
            if (iequals(name, owned)) return core::Error{"HttpClient: header \"" + name + "\" is set by the client"};
    }
    return std::nullopt;
}

}  // namespace

struct HttpClient::State {
    State(net::any_io_executor ex, HttpClientOptions o) : exec(std::move(ex)), opts(std::move(o)) {}
    net::any_io_executor exec;
    HttpClientOptions opts;
    bool cancelled{false};
    std::uint64_t next_closer_id{1};
    std::unordered_map<std::uint64_t, std::function<void()>> closers;
};

HttpClient::HttpClient(net::any_io_executor executor, HttpClientOptions opts) {
    if (opts.timeout.count() <= 0) throw std::invalid_argument("HttpClient: timeout must be > 0");
    if (opts.max_response_bytes == 0) throw std::invalid_argument("HttpClient: max_response_bytes must be > 0");
    state_ = std::make_shared<State>(std::move(executor), std::move(opts));
}

net::awaitable<core::Result<HttpResponse>> HttpClient::try_post_json(std::string url, std::string body,
                                                                     Headers headers) {
    return request(state_, Method::Post, std::move(url), std::move(body), std::move(headers));
}

net::awaitable<core::Result<HttpResponse>> HttpClient::try_get(std::string url, Headers headers) {
    return request(state_, Method::Get, std::move(url), {}, std::move(headers));
}

net::awaitable<HttpResponse> HttpClient::post_json(std::string url, std::string body, Headers headers) {
    return post_json_impl(state_, std::move(url), std::move(body), std::move(headers));
}

net::awaitable<HttpResponse> HttpClient::get(std::string url, Headers headers) {
    return get_impl(state_, std::move(url), std::move(headers));
}

net::awaitable<HttpResponse> HttpClient::post_json_impl(std::shared_ptr<State> st, std::string url, std::string body,
                                                        Headers headers) {
    if (const auto err = validate_headers(headers)) throw std::invalid_argument(err->what());
    auto r = co_await request(std::move(st), Method::Post, std::move(url), std::move(body), std::move(headers));
    if (!r) throw rpc::TransportError(r.error().what());
    co_return std::move(*r);
}

net::awaitable<HttpResponse> HttpClient::get_impl(std::shared_ptr<State> st, std::string url, Headers headers) {
    if (const auto err = validate_headers(headers)) throw std::invalid_argument(err->what());
    auto r = co_await request(std::move(st), Method::Get, std::move(url), {}, std::move(headers));
    if (!r) throw rpc::TransportError(r.error().what());
    co_return std::move(*r);
}

void HttpClient::cancel() {
    state_->cancelled = true;
    auto active = state_->closers;
    for (auto& [_, fn] : active) {
        if (fn) fn();
    }
}

net::awaitable<core::Result<HttpResponse>> HttpClient::request(std::shared_ptr<State> st, Method method,
                                                               std::string url, std::string body, Headers headers) {
    if (const auto err = validate_headers(headers)) co_return *err;
    const std::string who = core::env::redact(url);
    if (st->cancelled) co_return core::Error{"request cancelled for " + who};
    const auto parsed = rpc::try_parse_url(url, rpc::UrlKind::Http);
    if (!parsed) co_return parsed.error();
    const rpc::ParsedUrl& u = *parsed;

    struct CloserGuard {
        std::shared_ptr<State> st;
        std::uint64_t id;
        CloserGuard(std::shared_ptr<State> s, std::uint64_t i, std::function<void()> fn) : st(std::move(s)), id(i) {
            st->closers[id] = std::move(fn);
        }
        ~CloserGuard() { st->closers.erase(id); }
        CloserGuard(const CloserGuard&) = delete;
        CloserGuard& operator=(const CloserGuard&) = delete;
        CloserGuard(CloserGuard&&) = delete;
        CloserGuard& operator=(CloserGuard&&) = delete;
    };

    http::request<http::string_body> req{method == Method::Post ? http::verb::post : http::verb::get, u.target, 11};
    req.set(http::field::host, u.host_header());
    req.set(http::field::user_agent, st->opts.user_agent);
    req.set(http::field::accept, "*/*");
    req.set(http::field::accept_encoding, "identity");  // no gunzip on our side
    req.set(http::field::connection, "close");
    if (method == Method::Post) {
        req.set(http::field::content_type, "application/json");
        req.body() = std::move(body);
    }
    for (const auto& [name, value] : headers)
        req.set(name, value);
    req.prepare_payload();

    auto resolver = std::make_shared<tcp::resolver>(st->exec);
    tcp::resolver::results_type endpoints;
    {
        CloserGuard rg(st, st->next_closer_id++, [resolver] { resolver->cancel(); });
        boost::system::error_code ec;
        endpoints = co_await resolver->async_resolve(u.host, u.port, net::redirect_error(net::use_awaitable, ec));
        if (ec) co_return core::Error{"resolve failed for " + who + ": " + ec.message()};
    }
    if (st->cancelled) co_return core::Error{"request cancelled for " + who};

    http::response<http::string_body> res;
    if (!u.tls) {
        auto stream = std::make_shared<beast::tcp_stream>(st->exec);
        CloserGuard sg(st, st->next_closer_id++, [stream] {
            beast::error_code ec;
            stream->socket().close(ec);
        });
        boost::system::error_code ec;
        stream->expires_after(st->opts.timeout);
        co_await stream->async_connect(endpoints, net::redirect_error(net::use_awaitable, ec));
        if (ec) co_return format_phase_err(who, "connect", ec);
        auto ex = co_await exchange(stream, std::move(req), st->opts.timeout, st->opts.max_response_bytes, who);
        if (!ex) co_return ex.error();
        res = std::move(*ex);
        beast::error_code close_ec;
        stream->socket().shutdown(tcp::socket::shutdown_both, close_ec);
        stream->close();
    } else {
        // TLS with the system trust store, peer verification and a hostname check. Hold ctx before stream in a
        // heap-allocated struct so ssl::context is guaranteed to outlive beast::ssl_stream across
        // cancellation/teardown.
        struct TlsHttpConn {
            std::shared_ptr<ssl::context> ctx;
            beast::ssl_stream<beast::tcp_stream> stream;
            TlsHttpConn(const net::any_io_executor& ex, std::shared_ptr<ssl::context> c)
                : ctx(std::move(c)), stream(ex, *ctx) {}
        };
        auto conn = std::make_shared<TlsHttpConn>(st->exec, rpc::make_tls_client_context());
        auto stream_ptr = std::shared_ptr<beast::ssl_stream<beast::tcp_stream>>(conn, &conn->stream);
        CloserGuard sg(st, st->next_closer_id++, [conn] {
            beast::error_code ec;
            beast::get_lowest_layer(conn->stream).socket().close(ec);
        });
        if (!rpc::configure_tls_sni_and_hostname(*stream_ptr, u.host))
            co_return core::Error{"cannot set TLS SNI for " + who};
        boost::system::error_code ec;
        beast::get_lowest_layer(*stream_ptr).expires_after(st->opts.timeout);
        co_await beast::get_lowest_layer(*stream_ptr)
            .async_connect(endpoints, net::redirect_error(net::use_awaitable, ec));
        if (ec) co_return format_phase_err(who, "connect", ec);
        beast::get_lowest_layer(*stream_ptr).expires_after(st->opts.timeout);
        co_await stream_ptr->async_handshake(ssl::stream_base::client, net::redirect_error(net::use_awaitable, ec));
        if (ec) co_return format_phase_err(who, "TLS handshake", ec);
        auto ex = co_await exchange(stream_ptr, std::move(req), st->opts.timeout, st->opts.max_response_bytes, who);
        if (!ex) co_return ex.error();
        res = std::move(*ex);
        beast::error_code close_ec;
        beast::get_lowest_layer(*stream_ptr).socket().shutdown(tcp::socket::shutdown_both, close_ec);
        beast::get_lowest_layer(*stream_ptr).close();
    }
    co_return HttpResponse{static_cast<int>(res.result_int()), std::move(res.body())};
}

}  // namespace evml::ops
