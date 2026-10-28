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

/// @brief TransportError subtype capturing whether the phase failed on timeout vs socket disconnect.
struct PhaseError : TransportError {
    PhaseError(const std::string& m, bool is_timeout) : TransportError(m), timeout(is_timeout) {}
    bool timeout;  // True if phase failed due to timeout deadline
};

/// @brief Executes a single coroutine phase, converting Boost timeouts to PhaseError(..., true).
template <class Awaitable>
net::awaitable<void> phase(const std::string& who, const char* name, Awaitable&& a) {
    try {
        co_await std::forward<Awaitable>(a);
    } catch (const boost::system::system_error& e) {
        const bool to = e.code() == beast::error::timeout;
        // Include provider identifier and phase name in error description
        throw PhaseError(std::string(name) + " failed for " + who + ": " + (to ? "timeout" : e.code().message()), to);
    }
}

/// @brief Sends request and reads full HTTP response body over active socket stream.
template <class Stream>
net::awaitable<http::response<http::string_body>> exchange(Stream& stream, http::request<http::string_body>& req,
                                                           std::chrono::milliseconds timeout, const std::string& who) {
    // Set socket write timeout deadline
    beast::get_lowest_layer(stream).expires_after(timeout);
    co_await phase(who, "write", http::async_write(stream, req, net::use_awaitable));

    beast::flat_buffer buf;
    http::response_parser<http::string_body> parser;
    // Permit up to 64 MiB response bodies for heavy historical event queries
    parser.body_limit(kMaxBodyBytes);
    // Set socket read timeout deadline
    beast::get_lowest_layer(stream).expires_after(timeout);
    co_await phase(who, "read", http::async_read(stream, buf, parser, net::use_awaitable));
    co_return parser.release();
}

}  // namespace

namespace detail {

/// @brief Abstract connection handle representing either a plain TCP or TLS stream.
struct HttpConn {
    virtual ~HttpConn() = default;
    virtual net::awaitable<http::response<http::string_body>> roundtrip(http::request<http::string_body>& req,
                                                                        std::chrono::milliseconds timeout,
                                                                        const std::string& who) = 0;
    virtual void close() noexcept = 0;
};

namespace {

/// @brief Plain unencrypted TCP connection implementation for local test environments.
struct PlainConn final : HttpConn {
    explicit PlainConn(const net::any_io_executor& ex) : stream(ex) {}
    net::awaitable<http::response<http::string_body>> roundtrip(http::request<http::string_body>& req,
                                                                std::chrono::milliseconds timeout,
                                                                const std::string& who) override {
        co_return co_await exchange(stream, req, timeout, who);
    }
    void close() noexcept override {
        beast::error_code ec;
        // Graceful shutdown before close
        stream.socket().shutdown(tcp::socket::shutdown_both, ec);
        stream.close();
    }
    beast::tcp_stream stream;
};

/// @brief TLS-encrypted connection implementation for production RPC endpoints.
struct TlsConn final : HttpConn {
    TlsConn(const net::any_io_executor& ex, ssl::context& ctx) : stream(ex, ctx) {}
    net::awaitable<http::response<http::string_body>> roundtrip(http::request<http::string_body>& req,
                                                                std::chrono::milliseconds timeout,
                                                                const std::string& who) override {
        co_return co_await exchange(stream, req, timeout, who);
    }
    void close() noexcept override {
        // Direct socket shutdown without async close_notify (for dead connections or process exit)
        beast::error_code ec;
        beast::get_lowest_layer(stream).socket().shutdown(tcp::socket::shutdown_both, ec);
        beast::get_lowest_layer(stream).close();
    }
    beast::ssl_stream<beast::tcp_stream> stream;
};

}  // namespace
}  // namespace detail

/// @brief Dissects an HTTP response into ErrorInfo for failure classification.
ErrorInfo error_info_from_http(const HttpResult& r, Decoder& decoder) {
    ErrorInfo e;
    e.http_status = r.status;
    e.retry_after = r.retry_after;
    // Attempt parsing JSON-RPC error format from response body
    if (const auto err = decoder.try_parse_error_body(r.body)) {
        e.rpc_code = err->code;
        e.message = err->message;
    } else {
        // Truncate raw HTML/text error body to avoid unbounded memory in error structures
        e.message = r.body.substr(0, 120);
    }
    return e;
}

/// @brief Initializes client configuration, URL parsing, and FIFO concurrency lock timer.
HttpRpcClient::HttpRpcClient(net::any_io_executor executor, HttpRpcClientConfig cfg)
    : exec_(std::move(executor)),
      cfg_(std::move(cfg)),
      url_(parse_url(cfg_.url, UrlKind::Http)),
      redacted_(core::env::redact(cfg_.url)),
      lock_timer_(exec_, net::steady_timer::time_point::max()) {}

/// @brief Cleans up socket connections on destruction.
HttpRpcClient::~HttpRpcClient() { cancel(); }

/// @brief Explicitly shuts down and resets the active socket connection.
void HttpRpcClient::close() {
    if (conn_) {
        // Shutdown and close underlying socket
        conn_->close();
        conn_.reset();
    }
}

/// @brief Cancels any in-flight request or queued waiters and closes the active connection.
void HttpRpcClient::cancel() {
    cancelled_ = true;
    lock_timer_.cancel();
    if (active_resolver_) {
        active_resolver_->cancel();
    }
    if (opening_conn_) {
        opening_conn_->close();
    }
    close();
}

/// @brief Asynchronously resolves DNS and opens either plain TCP or verified TLS connection.
net::awaitable<std::shared_ptr<detail::HttpConn>> HttpRpcClient::open() {
    if (cancelled_) {
        throw TransportError(cfg_.name + ": client cancelled");
    }
    tcp::resolver resolver(exec_);
    struct ResolverGuard {
        HttpRpcClient& self;
        ResolverGuard(HttpRpcClient& s, tcp::resolver& r) : self(s) { self.active_resolver_ = &r; }
        ~ResolverGuard() { self.active_resolver_ = nullptr; }
    };
    struct OpeningGuard {
        HttpRpcClient& self;
        OpeningGuard(HttpRpcClient& s, detail::HttpConn* c) : self(s) { self.opening_conn_ = c; }
        ~OpeningGuard() { self.opening_conn_ = nullptr; }
    };
    tcp::resolver::results_type endpoints;
    try {
        ResolverGuard rg(*this, resolver);
        // Resolve host to IP endpoints
        endpoints = co_await resolver.async_resolve(url_.host, url_.port, net::use_awaitable);
    } catch (const boost::system::system_error& e) {
        throw TransportError(cfg_.name + ": resolve failed for " + redacted_ + ": " + e.code().message());
    }
    if (cancelled_) {
        throw TransportError(cfg_.name + ": client cancelled");
    }

    // Unencrypted HTTP path (for anvil / hardhat local testing)
    if (!url_.tls) {
        auto c = std::make_shared<detail::PlainConn>(exec_);
        OpeningGuard og(*this, c.get());
        c->stream.expires_after(cfg_.timeout);
        co_await phase(cfg_.name + " " + redacted_, "connect", c->stream.async_connect(endpoints, net::use_awaitable));
        co_return c;
    }

    // TLS 1.2/1.3 client context with system trust store and peer verification
    // One context per connection: cheap relative to the handshake, and keeps this class free of shared state.
    auto ctx = std::make_shared<ssl::context>(ssl::context::tls_client);
    ctx->set_default_verify_paths();
    ctx->set_verify_mode(ssl::verify_peer);
    auto c = std::make_shared<detail::TlsConn>(exec_, *ctx);
    OpeningGuard og(*this, c.get());
    // Configure SNI extension for cloud proxy / CDN routing
    if (!SSL_set_tlsext_host_name(c->stream.native_handle(), url_.host.c_str())) {
        throw TransportError(cfg_.name + ": cannot set TLS SNI for " + redacted_);
    }
    // Verify TLS certificate matches hostname
    c->stream.set_verify_callback(ssl::host_name_verification(url_.host));
    const std::string who = cfg_.name + " " + redacted_;
    // Connect TCP socket with timeout deadline
    beast::get_lowest_layer(c->stream).expires_after(cfg_.timeout);
    co_await phase(who, "connect", beast::get_lowest_layer(c->stream).async_connect(endpoints, net::use_awaitable));
    if (cancelled_) {
        throw TransportError(cfg_.name + ": client cancelled");
    }
    // Complete TLS handshake with timeout deadline
    beast::get_lowest_layer(c->stream).expires_after(cfg_.timeout);
    co_await phase(who, "TLS handshake", c->stream.async_handshake(ssl::stream_base::client, net::use_awaitable));
    co_return c;
}

/// @brief Sends a JSON-RPC request over the persistent connection, serializing callers FIFO.
net::awaitable<HttpResult> HttpRpcClient::call(std::string json_body) {
    if (cancelled_) {
        throw TransportError(cfg_.name + ": client cancelled");
    }
    // FIFO lock: waiters park on lock_timer_ and are woken one at a time by cancel_one().
    while (locked_) {
        boost::system::error_code ignored;
        co_await lock_timer_.async_wait(net::redirect_error(net::use_awaitable, ignored));
        if (cancelled_) {
            throw TransportError(cfg_.name + ": client cancelled");
        }
    }
    locked_ = true;
    // RAII guard ensuring lock is released and next waiter woken on exit
    struct Unlock {
        HttpRpcClient& c;
        ~Unlock() {
            c.locked_ = false;
            c.lock_timer_.cancel_one();
        }
    } unlock{*this};

    // Construct HTTP 1.1 POST request
    http::request<http::string_body> req{http::verb::post, url_.target, 11};
    req.set(http::field::host, url_.host_header());
    req.set(http::field::user_agent, cfg_.user_agent);
    req.set(http::field::content_type, "application/json");
    req.set(http::field::accept, "application/json");
    // Explicitly disable compression to avoid manual gunzip overhead
    req.set(http::field::accept_encoding, "identity");
    req.body() = std::move(json_body);
    req.prepare_payload();

    const std::string who = cfg_.name + " " + redacted_;
    for (int attempt = 0;; ++attempt) {
        if (cancelled_) {
            throw TransportError(cfg_.name + ": client cancelled");
        }
        const bool reused = conn_ != nullptr;
        try {
            // Lazy connect or reconnect if connection dropped
            if (!conn_) conn_ = co_await open();
            const auto conn = conn_;
            auto res = co_await conn->roundtrip(req, cfg_.timeout, who);
            const bool keep = res.keep_alive();
            HttpResult out;
            out.status = static_cast<int>(res.result_int());
            // Extract optional Retry-After header for rate limit backoff
            if (const auto it = res.find(http::field::retry_after); it != res.end()) {
                out.retry_after = parse_retry_after(std::string_view(it->value().data(), it->value().size()));
            }
            out.body = std::move(res.body());
            // If server indicated close, drop connection immediately
            if (!keep) close();
            co_return out;
        } catch (const PhaseError& e) {
            close();
            if (cancelled_) throw;
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
