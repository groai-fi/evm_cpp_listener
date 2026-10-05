#include "rpc/http_rpc_client.hpp"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include <openssl/ssl.h>

#include "core/env.hpp"
#include "rpc/blocking_http.hpp"

namespace evml::rpc {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

namespace {

constexpr std::size_t kMaxBodyBytes = 64u << 20;  // 64 MiB: a 10k-block getLogs page can be large

// TransportError that says whether it was a deadline (never retried) or a dead connection (retried once).
struct PhaseError : TransportError {
    PhaseError(const std::string& m, bool is_timeout) : TransportError(m), timeout(is_timeout) {}
    bool timeout;
};

template <class Awaitable>
net::awaitable<void> phase(const std::string& who, const char* name, Awaitable&& a) {
    try {
        co_await std::forward<Awaitable>(a);
    } catch (const boost::system::system_error& e) {
        const bool to = e.code() == beast::error::timeout;
        throw PhaseError(std::string(name) + " failed for " + who + ": " + (to ? "timeout" : e.code().message()), to);
    }
}

template <class Stream>
net::awaitable<http::response<http::string_body>> exchange(Stream& stream, http::request<http::string_body>& req,
                                                           std::chrono::milliseconds timeout, const std::string& who) {
    beast::get_lowest_layer(stream).expires_after(timeout);
    co_await phase(who, "write", http::async_write(stream, req, net::use_awaitable));

    beast::flat_buffer buf;
    http::response_parser<http::string_body> parser;
    parser.body_limit(kMaxBodyBytes);
    beast::get_lowest_layer(stream).expires_after(timeout);
    co_await phase(who, "read", http::async_read(stream, buf, parser, net::use_awaitable));
    co_return parser.release();
}

}  // namespace

namespace detail {

struct HttpConn {
    virtual ~HttpConn() = default;
    virtual net::awaitable<http::response<http::string_body>> roundtrip(http::request<http::string_body>& req,
                                                                        std::chrono::milliseconds timeout,
                                                                        const std::string& who) = 0;
    virtual void close() noexcept = 0;
};

namespace {

struct PlainConn final : HttpConn {
    explicit PlainConn(const net::any_io_executor& ex) : stream(ex) {}
    net::awaitable<http::response<http::string_body>> roundtrip(http::request<http::string_body>& req,
                                                                std::chrono::milliseconds timeout,
                                                                const std::string& who) override {
        co_return co_await exchange(stream, req, timeout, who);
    }
    void close() noexcept override {
        beast::error_code ec;
        stream.socket().shutdown(tcp::socket::shutdown_both, ec);
        stream.socket().close(ec);
    }
    beast::tcp_stream stream;
};

struct TlsConn final : HttpConn {
    TlsConn(const net::any_io_executor& ex, ssl::context& ctx) : stream(ex, ctx) {}
    net::awaitable<http::response<http::string_body>> roundtrip(http::request<http::string_body>& req,
                                                                std::chrono::milliseconds timeout,
                                                                const std::string& who) override {
        co_return co_await exchange(stream, req, timeout, who);
    }
    void close() noexcept override {
        // No async close_notify: this path is used for dead/failed connections or shutdown.
        beast::error_code ec;
        beast::get_lowest_layer(stream).socket().shutdown(tcp::socket::shutdown_both, ec);
        beast::get_lowest_layer(stream).socket().close(ec);
    }
    beast::ssl_stream<beast::tcp_stream> stream;
};

}  // namespace
}  // namespace detail

ErrorInfo error_info_from_http(const HttpResult& r, Decoder& decoder) {
    ErrorInfo e;
    e.http_status = r.status;
    e.retry_after = r.retry_after;
    if (const auto err = decoder.try_parse_error_body(r.body)) {
        e.rpc_code = err->code;
        e.message = err->message;
    } else {
        e.message = r.body.substr(0, 120);
    }
    return e;
}

HttpRpcClient::HttpRpcClient(net::any_io_executor executor, HttpRpcClientConfig cfg)
    : exec_(std::move(executor)),
      cfg_(std::move(cfg)),
      url_(parse_url(cfg_.url, UrlKind::Http)),
      redacted_(core::env::redact(cfg_.url)),
      lock_timer_(exec_, net::steady_timer::time_point::max()) {}

HttpRpcClient::~HttpRpcClient() { close(); }

void HttpRpcClient::close() {
    if (conn_) {
        conn_->close();
        conn_.reset();
    }
}

net::awaitable<std::unique_ptr<detail::HttpConn>> HttpRpcClient::open() {
    tcp::resolver resolver(exec_);
    tcp::resolver::results_type endpoints;
    try {
        endpoints = co_await resolver.async_resolve(url_.host, url_.port, net::use_awaitable);
    } catch (const boost::system::system_error& e) {
        throw TransportError(cfg_.name + ": resolve failed for " + redacted_ + ": " + e.code().message());
    }

    if (!url_.tls) {
        auto c = std::make_unique<detail::PlainConn>(exec_);
        c->stream.expires_after(cfg_.timeout);
        co_await phase(cfg_.name + " " + redacted_, "connect", c->stream.async_connect(endpoints, net::use_awaitable));
        co_return std::move(c);
    }

    // One context per connection: cheap relative to the handshake, and keeps this class free of shared state.
    auto ctx = std::make_shared<ssl::context>(ssl::context::tls_client);
    ctx->set_default_verify_paths();
    ctx->set_verify_mode(ssl::verify_peer);
    auto c = std::make_unique<detail::TlsConn>(exec_, *ctx);
    if (!SSL_set_tlsext_host_name(c->stream.native_handle(), url_.host.c_str())) {
        throw TransportError(cfg_.name + ": cannot set TLS SNI for " + redacted_);
    }
    c->stream.set_verify_callback(ssl::host_name_verification(url_.host));
    const std::string who = cfg_.name + " " + redacted_;
    beast::get_lowest_layer(c->stream).expires_after(cfg_.timeout);
    co_await phase(who, "connect", beast::get_lowest_layer(c->stream).async_connect(endpoints, net::use_awaitable));
    beast::get_lowest_layer(c->stream).expires_after(cfg_.timeout);
    co_await phase(who, "TLS handshake", c->stream.async_handshake(ssl::stream_base::client, net::use_awaitable));
    co_return std::move(c);
}

net::awaitable<HttpResult> HttpRpcClient::call(std::string json_body) {
    // FIFO lock: waiters park on lock_timer_ and are woken one at a time by cancel_one().
    while (locked_) {
        boost::system::error_code ignored;
        co_await lock_timer_.async_wait(net::redirect_error(net::use_awaitable, ignored));
    }
    locked_ = true;
    struct Unlock {
        HttpRpcClient& c;
        ~Unlock() {
            c.locked_ = false;
            c.lock_timer_.cancel_one();
        }
    } unlock{*this};

    http::request<http::string_body> req{http::verb::post, url_.target, 11};
    req.set(http::field::host, url_.host_header());
    req.set(http::field::user_agent, cfg_.user_agent);
    req.set(http::field::content_type, "application/json");
    req.set(http::field::accept, "application/json");
    req.set(http::field::accept_encoding, "identity");
    req.body() = std::move(json_body);
    req.prepare_payload();

    const std::string who = cfg_.name + " " + redacted_;
    for (int attempt = 0;; ++attempt) {
        const bool reused = conn_ != nullptr;
        try {
            if (!conn_) conn_ = co_await open();
            auto res = co_await conn_->roundtrip(req, cfg_.timeout, who);
            const bool keep = res.keep_alive();
            HttpResult out;
            out.status = static_cast<int>(res.result_int());
            if (const auto it = res.find(http::field::retry_after); it != res.end()) {
                out.retry_after = parse_retry_after(std::string_view(it->value().data(), it->value().size()));
            }
            out.body = std::move(res.body());
            if (!keep) close();
            co_return out;
        } catch (const PhaseError& e) {
            close();
            // A reused connection that died while idle is the only retryable case, and only once.
            if (attempt == 0 && reused && !e.timeout) continue;
            throw;
        } catch (const TransportError&) {
            close();
            throw;
        }
    }
}

}  // namespace evml::rpc
