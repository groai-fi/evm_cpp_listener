#include "rpc/http_rpc_client.hpp"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include "core/env.hpp"
#include "rpc/blocking_http.hpp"
#include "rpc/tls_client.hpp"

namespace evml::rpc {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

namespace {

constexpr std::size_t kMaxBodyBytes = 64u << 20;  // 64 MiB: a 10k-block getLogs page can be large

/// @brief Transport error details capturing whether the phase failed on timeout vs socket disconnect.
struct PhaseError {
    std::string message;
    bool timeout{false};  // True if phase failed due to timeout deadline
};

/// @brief Formats a phase error from a Boost error_code.
[[nodiscard]] PhaseError make_phase_error(std::string_view who, const char* name, const boost::system::error_code& ec) {
    const bool to = ec == beast::error::timeout;
    return PhaseError{std::string(name) + " failed for " + std::string(who) + ": " + (to ? "timeout" : ec.message()),
                      to};
}

/// @brief Sends request and reads full HTTP response body over active socket stream without throwing.
template <class Stream>
net::awaitable<core::Result<http::response<http::string_body>, PhaseError>> exchange(
    std::shared_ptr<Stream> stream, http::request<http::string_body> req, std::chrono::milliseconds timeout,
    std::string who) {
    boost::system::error_code ec;
    // Set socket write timeout deadline
    beast::get_lowest_layer(*stream).expires_after(timeout);
    co_await http::async_write(*stream, req, net::redirect_error(net::use_awaitable, ec));
    if (ec) {
        co_return make_phase_error(who, "write", ec);
    }

    beast::flat_buffer buf;
    http::response_parser<http::string_body> parser;
    // Permit up to 64 MiB response bodies for heavy historical event queries
    parser.body_limit(kMaxBodyBytes);
    // Set socket read timeout deadline
    beast::get_lowest_layer(*stream).expires_after(timeout);
    co_await http::async_read(*stream, buf, parser, net::redirect_error(net::use_awaitable, ec));
    if (ec) {
        co_return make_phase_error(who, "read", ec);
    }
    co_return parser.release();
}

}  // namespace

namespace detail {

/// @brief Abstract connection handle representing either a plain TCP or TLS stream.
struct HttpConn {
    HttpConn() = default;
    virtual ~HttpConn() = default;
    HttpConn(const HttpConn&) = delete;
    HttpConn& operator=(const HttpConn&) = delete;
    HttpConn(HttpConn&&) = delete;
    HttpConn& operator=(HttpConn&&) = delete;

    virtual net::awaitable<core::Result<http::response<http::string_body>, PhaseError>> roundtrip(
        http::request<http::string_body> req, std::chrono::milliseconds timeout, std::string who) = 0;
    virtual void close() noexcept = 0;
};

namespace {

/// @brief Plain unencrypted TCP connection implementation for local test environments.
struct PlainConn final : HttpConn, std::enable_shared_from_this<PlainConn> {
    explicit PlainConn(const net::any_io_executor& ex) : stream(ex) {}
    net::awaitable<core::Result<http::response<http::string_body>, PhaseError>> roundtrip(
        http::request<http::string_body> req, std::chrono::milliseconds timeout, std::string who) override {
        return roundtrip_impl(shared_from_this(), std::move(req), timeout, std::move(who));
    }
    static net::awaitable<core::Result<http::response<http::string_body>, PhaseError>> roundtrip_impl(
        std::shared_ptr<PlainConn> self, http::request<http::string_body> req, std::chrono::milliseconds timeout,
        std::string who) {
        auto s = std::shared_ptr<beast::tcp_stream>(self, &self->stream);
        co_return co_await exchange(std::move(s), std::move(req), timeout, std::move(who));
    }
    void close() noexcept override {
        beast::error_code ec;
        // Graceful shutdown before close
        stream.socket().shutdown(tcp::socket::shutdown_both, ec);
        stream.socket().close(ec);
    }
    beast::tcp_stream stream;
};

/// @brief TLS-encrypted connection implementation for production RPC endpoints.
struct TlsConn final : HttpConn, std::enable_shared_from_this<TlsConn> {
    explicit TlsConn(const net::any_io_executor& ex, std::shared_ptr<ssl::context> c)
        : ctx(std::move(c)), stream(ex, *ctx) {}
    net::awaitable<core::Result<http::response<http::string_body>, PhaseError>> roundtrip(
        http::request<http::string_body> req, std::chrono::milliseconds timeout, std::string who) override {
        return roundtrip_impl(shared_from_this(), std::move(req), timeout, std::move(who));
    }
    static net::awaitable<core::Result<http::response<http::string_body>, PhaseError>> roundtrip_impl(
        std::shared_ptr<TlsConn> self, http::request<http::string_body> req, std::chrono::milliseconds timeout,
        std::string who) {
        auto s = std::shared_ptr<beast::ssl_stream<beast::tcp_stream>>(self, &self->stream);
        co_return co_await exchange(std::move(s), std::move(req), timeout, std::move(who));
    }
    void close() noexcept override {
        // Direct socket shutdown without async close_notify (for dead connections or process exit)
        beast::error_code ec;
        beast::get_lowest_layer(stream).socket().shutdown(tcp::socket::shutdown_both, ec);
        beast::get_lowest_layer(stream).socket().close(ec);
    }
    std::shared_ptr<ssl::context> ctx;
    beast::ssl_stream<beast::tcp_stream> stream;
};

}  // namespace
}  // namespace detail

struct HttpRpcClient::State {
    net::any_io_executor exec;
    HttpRpcClientConfig cfg;
    ParsedUrl url;
    std::string redacted;
    std::shared_ptr<detail::HttpConn> conn;
    std::shared_ptr<tcp::resolver> active_resolver;
    std::shared_ptr<detail::HttpConn> opening_conn;
    net::steady_timer lock_timer;  // wake-up channel of the FIFO request lock
    bool locked{false};
    bool cancelled{false};

    State(net::any_io_executor executor, HttpRpcClientConfig c)
        : exec(std::move(executor)),
          cfg(std::move(c)),
          url(parse_url(cfg.url, UrlKind::Http)),
          redacted(core::env::redact(cfg.url)),
          lock_timer(exec, net::steady_timer::time_point::max()) {}

    void close() noexcept {
        if (conn) {
            conn->close();
            conn.reset();
        }
    }

    void cancel() noexcept {
        cancelled = true;
        try {
            lock_timer.cancel();
            if (active_resolver) {
                active_resolver->cancel();
            }
        } catch (...) {
        }
        if (opening_conn) {
            opening_conn->close();
        }
        close();
    }
};

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
    : state_(std::make_shared<State>(std::move(executor), std::move(cfg))) {}

/// @brief Cleans up socket connections on destruction.
HttpRpcClient::~HttpRpcClient() { cancel(); }

const std::string& HttpRpcClient::name() const noexcept { return state_->cfg.name; }

/// @brief Explicitly shuts down and resets the active socket connection.
void HttpRpcClient::close() noexcept { state_->close(); }

/// @brief Cancels any in-flight request or queued waiters and closes the active connection.
void HttpRpcClient::cancel() noexcept { state_->cancel(); }

/// @brief Asynchronously resolves DNS and opens either plain TCP or verified TLS connection.
net::awaitable<core::Result<std::shared_ptr<detail::HttpConn>>> HttpRpcClient::open(std::shared_ptr<State> st) {
    if (st->cancelled) {
        co_return core::Error{st->cfg.name + ": client cancelled"};
    }
    auto resolver = std::make_shared<tcp::resolver>(st->exec);
    struct ResolverGuard {
        std::shared_ptr<State> st;
        ResolverGuard(std::shared_ptr<State> s, std::shared_ptr<tcp::resolver> r) : st(std::move(s)) {
            st->active_resolver = std::move(r);
        }
        ~ResolverGuard() { st->active_resolver.reset(); }
        ResolverGuard(const ResolverGuard&) = delete;
        ResolverGuard& operator=(const ResolverGuard&) = delete;
        ResolverGuard(ResolverGuard&&) = delete;
        ResolverGuard& operator=(ResolverGuard&&) = delete;
    };
    struct OpeningGuard {
        std::shared_ptr<State> st;
        OpeningGuard(std::shared_ptr<State> s, std::shared_ptr<detail::HttpConn> c) : st(std::move(s)) {
            st->opening_conn = std::move(c);
        }
        ~OpeningGuard() { st->opening_conn.reset(); }
        OpeningGuard(const OpeningGuard&) = delete;
        OpeningGuard& operator=(const OpeningGuard&) = delete;
        OpeningGuard(OpeningGuard&&) = delete;
        OpeningGuard& operator=(OpeningGuard&&) = delete;
    };
    boost::system::error_code ec;
    tcp::resolver::results_type endpoints;
    {
        ResolverGuard rg(st, resolver);
        // Resolve host to IP endpoints
        endpoints = co_await resolver->async_resolve(st->url.host, st->url.port,
                                                     net::redirect_error(net::use_awaitable, ec));
    }
    if (ec) {
        co_return core::Error{st->cfg.name + ": resolve failed for " + st->redacted + ": " + ec.message()};
    }
    if (st->cancelled) {
        co_return core::Error{st->cfg.name + ": client cancelled"};
    }

    const std::string who = st->cfg.name + " " + st->redacted;
    // Unencrypted HTTP path (for anvil / hardhat local testing)
    if (!st->url.tls) {
        auto c = std::make_shared<detail::PlainConn>(st->exec);
        OpeningGuard og(st, c);
        c->stream.expires_after(st->cfg.timeout);
        co_await c->stream.async_connect(endpoints, net::redirect_error(net::use_awaitable, ec));
        if (ec) {
            co_return core::Error{make_phase_error(who, "connect", ec).message};
        }
        if (st->cancelled) {
            co_return core::Error{st->cfg.name + ": client cancelled"};
        }
        co_return std::shared_ptr<detail::HttpConn>(std::move(c));
    }

    // TLS 1.2/1.3 client context with system trust store and peer verification
    // One context per connection: cheap relative to the handshake, and keeps this class free of shared state.
    auto c = std::make_shared<detail::TlsConn>(st->exec, make_tls_client_context());
    OpeningGuard og(st, c);
    if (!configure_tls_sni_and_hostname(c->stream, st->url.host)) {
        co_return core::Error{st->cfg.name + ": cannot set TLS SNI for " + st->redacted};
    }
    // Connect TCP socket with timeout deadline
    beast::get_lowest_layer(c->stream).expires_after(st->cfg.timeout);
    co_await beast::get_lowest_layer(c->stream).async_connect(endpoints, net::redirect_error(net::use_awaitable, ec));
    if (ec) {
        co_return core::Error{make_phase_error(who, "connect", ec).message};
    }
    if (st->cancelled) {
        co_return core::Error{st->cfg.name + ": client cancelled"};
    }
    // Complete TLS handshake with timeout deadline
    beast::get_lowest_layer(c->stream).expires_after(st->cfg.timeout);
    co_await c->stream.async_handshake(ssl::stream_base::client, net::redirect_error(net::use_awaitable, ec));
    if (ec) {
        co_return core::Error{make_phase_error(who, "TLS handshake", ec).message};
    }
    if (st->cancelled) {
        co_return core::Error{st->cfg.name + ": client cancelled"};
    }
    co_return std::shared_ptr<detail::HttpConn>(std::move(c));
}

/// @brief Sends a JSON-RPC request over the persistent connection without throwing on transport failure.
net::awaitable<core::Result<HttpResult>> HttpRpcClient::try_call_impl(std::shared_ptr<State> st, std::string json_body) {
    if (st->cancelled) {
        co_return core::Error{st->cfg.name + ": client cancelled"};
    }
    // FIFO lock: waiters park on lock_timer and are woken one at a time by cancel_one().
    while (st->locked) {
        boost::system::error_code ignored;
        co_await st->lock_timer.async_wait(net::redirect_error(net::use_awaitable, ignored));
        if (st->cancelled) {
            co_return core::Error{st->cfg.name + ": client cancelled"};
        }
    }
    st->locked = true;
    // RAII guard ensuring lock is released and next waiter woken on exit
    struct Unlock {
        std::shared_ptr<State> st;
        explicit Unlock(std::shared_ptr<State> s) : st(std::move(s)) {}
        ~Unlock() noexcept {
            st->locked = false;
            try {
                st->lock_timer.cancel_one();
            } catch (...) {
            }
        }
        Unlock(const Unlock&) = delete;
        Unlock& operator=(const Unlock&) = delete;
        Unlock(Unlock&&) = delete;
        Unlock& operator=(Unlock&&) = delete;
    } unlock{st};

    // Construct HTTP 1.1 POST request
    http::request<http::string_body> req{http::verb::post, st->url.target, 11};
    req.set(http::field::host, st->url.host_header());
    req.set(http::field::user_agent, st->cfg.user_agent);
    req.set(http::field::content_type, "application/json");
    req.set(http::field::accept, "application/json");
    // Explicitly disable compression to avoid manual gunzip overhead
    req.set(http::field::accept_encoding, "identity");
    req.body() = std::move(json_body);
    req.prepare_payload();

    const std::string who = st->cfg.name + " " + st->redacted;
    for (int attempt = 0;; ++attempt) {
        if (st->cancelled) {
            co_return core::Error{st->cfg.name + ": client cancelled"};
        }
        const bool reused = st->conn != nullptr;
        // Lazy connect or reconnect if connection dropped
        if (!st->conn) {
            auto opened = co_await open(st);
            if (opened.has_error()) {
                st->close();
                co_return opened.error();
            }
            st->conn = std::move(*opened);
        }
        const auto conn = st->conn;
        auto res_r = co_await conn->roundtrip(req, st->cfg.timeout, who);
        if (res_r.has_error()) {
            st->close();
            if (st->cancelled) {
                co_return core::Error{res_r.error().message};
            }
            // A reused connection that died while idle is the only retryable case, and only once.
            if (attempt == 0 && reused && !res_r.error().timeout) continue;
            co_return core::Error{res_r.error().message};
        }
        auto& res = *res_r;
        const bool keep = res.keep_alive();
        HttpResult out;
        out.status = static_cast<int>(res.result_int());
        // Extract optional Retry-After header for rate limit backoff
        if (const auto it = res.find(http::field::retry_after); it != res.end()) {
            out.retry_after = parse_retry_after(std::string_view(it->value().data(), it->value().size()));
        }
        out.body = std::move(res.body());
        // If server indicated close, drop connection immediately
        if (!keep) st->close();
        co_return out;
    }
}

net::awaitable<core::Result<HttpResult>> HttpRpcClient::try_call(std::string json_body) {
    return try_call_impl(state_, std::move(json_body));
}

net::awaitable<HttpResult> HttpRpcClient::call_impl(std::shared_ptr<State> st, std::string json_body) {
    auto r = co_await try_call_impl(std::move(st), std::move(json_body));
    if (r.has_error()) throw TransportError(r.error().message);
    co_return std::move(*r);
}

/// @brief Sends a JSON-RPC request over the persistent connection, serializing callers FIFO.
net::awaitable<HttpResult> HttpRpcClient::call(std::string json_body) {
    return call_impl(state_, std::move(json_body));
}

}  // namespace evml::rpc
