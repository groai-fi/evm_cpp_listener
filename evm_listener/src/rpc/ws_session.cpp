#include "rpc/ws_session.hpp"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <stdexcept>
#include <variant>

#ifdef __linux__
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#endif

#include "core/env.hpp"
#include "core/log.hpp"
#include "rpc/blocking_http.hpp"
#include "rpc/tls_client.hpp"

namespace evml::rpc {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

namespace {

constexpr std::size_t kMaxMessageBytes = 16u << 20;  // 16 MiB max incoming frame size

/// @brief Translates common Boost.Beast and Boost.Asio error codes to human-readable text.
[[nodiscard]] std::string describe(const boost::system::error_code& ec) {
    if (ec == websocket::error::closed) return "server closed the connection";
    if (ec == beast::error::timeout) return "idle timeout: no frames or pongs (half-open peer?)";
    return ec.message();
}

/// @brief Per-request deadline timer that invokes on_timeout when the deadline expires.
/// Threading & Safety:
/// - Uses shared active_ flag to guard against stale timer cancellations firing after operation succeeded.
class Deadline {
public:
    Deadline(const net::any_io_executor& ex, std::chrono::milliseconds d, std::function<void()> on_timeout)
        : timer_(ex), active_(std::make_shared<bool>(true)), hit_(std::make_shared<bool>(false)) {
        timer_.expires_after(d);
        // Asynchronously wait for deadline expiration
        timer_.async_wait(
            [active = active_, hit = hit_, on_timeout = std::move(on_timeout)](const boost::system::error_code& ec) {
                // Bail if canceled or operation completed in time
                if (ec || !*active) return;
                *hit = true;
                if (on_timeout) on_timeout();
            });
    }
    ~Deadline() noexcept {
        // Disarm active flag before cancelling timer
        *active_ = false;
        try {
            timer_.cancel();
        } catch (...) {
        }
    }
    Deadline(const Deadline&) = delete;
    Deadline& operator=(const Deadline&) = delete;
    Deadline(Deadline&&) = delete;
    Deadline& operator=(Deadline&&) = delete;
    [[nodiscard]] bool hit() const noexcept { return *hit_; }

private:
    net::steady_timer timer_;
    std::shared_ptr<bool> active_;
    std::shared_ptr<bool> hit_;
};

/// @brief RAII guard installing and tearing down a socket closer closure on a target slot.
struct CloserGuard {
    CloserGuard(std::function<void()>& slot, std::function<void()> f) : slot_(slot) { slot_ = std::move(f); }
    ~CloserGuard() { slot_ = nullptr; }
    CloserGuard(const CloserGuard&) = delete;
    CloserGuard& operator=(const CloserGuard&) = delete;
    CloserGuard(CloserGuard&&) = delete;
    CloserGuard& operator=(CloserGuard&&) = delete;
    std::function<void()>& slot_;
};

/// @brief Tunes low-level TCP socket options for low latency and aggressive half-open peer detection.
void tune_socket(tcp::socket& sock, std::string_view who) {
    const auto check = [&](const boost::system::error_code& ec, const char* what) {
        if (ec) {
            core::log::warn("ws", "socket option not applied",
                            {{"provider", std::string(who)}, {"option", what}, {"error", ec.message()}});
        }
    };
    boost::system::error_code ec;
    // Disable Nagle's algorithm for immediate transmission of RPC requests
    sock.set_option(tcp::no_delay(true), ec);
    check(ec, "TCP_NODELAY");
    // Enable SO_KEEPALIVE to detect dead remote hosts
    sock.set_option(net::socket_base::keep_alive(true), ec);
    check(ec, "SO_KEEPALIVE");
#ifdef __linux__
    // Linux-specific TCP keepalive tuning: 10s idle, 5s interval, 3 probes, 25s user timeout
    const auto apply = [&](int opt, int val, const char* name) {
        if (::setsockopt(sock.native_handle(), IPPROTO_TCP, opt, &val, sizeof(val)) != 0) {
            check(boost::system::error_code(errno, boost::system::system_category()), name);
        }
    };
    apply(TCP_KEEPIDLE, 10, "TCP_KEEPIDLE");
    apply(TCP_KEEPINTVL, 5, "TCP_KEEPINTVL");
    apply(TCP_KEEPCNT, 3, "TCP_KEEPCNT");
    apply(TCP_USER_TIMEOUT, 25000, "TCP_USER_TIMEOUT");
#endif
}

/// @brief Ephemeral per-session protocol context tracking subscriptions and decoder state.
struct Proto {
    std::shared_ptr<WsSession> owner;
    const WsSessionConfig& cfg;
    const WsHandlers& handlers;
    const net::any_io_executor& exec;
    std::function<void()> on_timeout;
    std::uint64_t& unknown_msgs;
    Decoder dec{};
    beast::flat_buffer buf{};
    std::uint64_t next_id{1};
    std::optional<std::string> heads_sub{};
    std::optional<std::string> logs_sub{};
};

/// @brief Classifies JSON-RPC error payload and builds WsClosed with appropriate category.
[[nodiscard]] WsClosed make_rpc_error(const RpcError& e, std::string_view what) {
    ErrorInfo info;
    info.rpc_code = e.code;
    info.message = e.message;
    const auto c = classify(info);
    return WsClosed{c.kind,
                    std::string(what) + ": rpc error " + std::to_string(e.code) + ": " + e.message.substr(0, 120),
                    c.retry_after, 0, e.code};
}

/// @brief Reads single WebSocket frame into protocol buffer with optional deadline.
template <class Ws>
net::awaitable<std::optional<WsClosed>> read_frame(std::shared_ptr<Ws> ws, std::shared_ptr<Proto> p,
                                                   std::shared_ptr<const Deadline> dl, std::string what) {
    // Clear buffer from previously consumed frame
    p->buf.consume(p->buf.size());
    boost::system::error_code ec;
    co_await ws->async_read(p->buf, net::redirect_error(net::use_awaitable, ec));
    if (ec) {
        // Differentiate explicit deadline expiration from socket disconnect
        if (dl && dl->hit()) {
            co_return WsClosed{FailKind::Retry, "timeout waiting for " + what, std::nullopt, 0, std::nullopt};
        }
        co_return WsClosed{FailKind::Retry, "read failed while waiting for " + what + ": " + describe(ec), std::nullopt,
                           0, std::nullopt};
    }
    co_return std::nullopt;
}

/// @brief Provides zero-copy string_view over current buffer bytes.
[[nodiscard]] std::string_view frame_view(const Proto& p) noexcept {
    return std::string_view(static_cast<const char*>(p.buf.data().data()), p.buf.size());
}

/// @brief Routes decoded notification to on_head or on_log handler based on subscription ID.
void dispatch(Proto& p, Notification&& n) {
    // Route 1: newHeads notification
    if (const auto* head = std::get_if<Head>(&n.payload); head && p.heads_sub && n.subscription == *p.heads_sub) {
        p.handlers.on_head(*head);
    } else if (const auto* lg = std::get_if<Log>(&n.payload); lg && p.logs_sub && n.subscription == *p.logs_sub) {
        // Route 2: filtered logs notification
        p.handlers.on_log(*lg);
    } else {
        // Drop notifications for subscription IDs not recognized by this session (e.g. stale server frames)
        ++p.unknown_msgs;
        core::log::warn("ws", "notification for an unknown subscription dropped",
                        {{"provider", p.cfg.name}, {"subscription", n.subscription}});
    }
}

/// @brief Asynchronously writes text frame over WebSocket.
template <class Ws>
net::awaitable<std::optional<WsClosed>> send_text(std::shared_ptr<Ws> ws, std::string msg) {
    boost::system::error_code ec;
    co_await ws->async_write(net::buffer(msg), net::redirect_error(net::use_awaitable, ec));
    if (ec) {
        co_return WsClosed{FailKind::Retry, "write failed: " + describe(ec), std::nullopt, 0, std::nullopt};
    }
    co_return std::nullopt;
}

/// @brief Sends request and awaits matching Response; interleaving notifications are dispatched inline.
template <class Ws>
net::awaitable<core::Result<Response, WsClosed>> request(std::shared_ptr<Ws> ws, std::shared_ptr<Proto> p,
                                                         std::uint64_t id, std::string req,
                                                         std::chrono::milliseconds timeout, std::string what) {
    // Send request frame
    if (auto err = co_await send_text(ws, std::move(req))) co_return std::move(*err);
    // Install request deadline timer
    auto dl = std::make_shared<Deadline>(p->exec, timeout, p->on_timeout);
    for (;;) {
        if (auto err = co_await read_frame(ws, p, dl, what)) co_return std::move(*err);
        auto msg_r = p->dec.try_decode(frame_view(*p));
        if (msg_r.has_error()) {
            co_return WsClosed{FailKind::Retry, msg_r.error().message, std::nullopt, 0, std::nullopt};
        }
        auto& msg = msg_r.value();
        // Notifications arriving while awaiting response are dispatched immediately
        if (auto* n = std::get_if<Notification>(&msg)) {
            dispatch(*p, std::move(*n));
            continue;
        }
        auto& r = std::get<Response>(msg);
        // Match response ID against pending request
        if (r.id && *r.id == id) co_return std::move(r);
        co_return WsClosed{FailKind::Retry, "unexpected response id while waiting for " + what, std::nullopt, 0,
                           std::nullopt};
    }
}

/// @brief Issues an eth_subscribe request and extracts subscription ID string.
template <class Ws>
net::awaitable<core::Result<std::string, WsClosed>> subscribe(std::shared_ptr<Ws> ws, std::shared_ptr<Proto> p,
                                                              std::string req, std::uint64_t id, std::string what) {
    auto r = co_await request(ws, p, id, std::move(req), p->cfg.subscribe_timeout, "subscribe ACK (" + what + ")");
    if (r.has_error()) co_return r.error();
    if (!r->ok) co_return make_rpc_error(r->error, "subscribe " + what);
    auto str_r = p->dec.try_parse_string(r->result_raw);
    if (str_r.has_error()) {
        co_return WsClosed{FailKind::Retry, str_r.error().message, std::nullopt, 0, std::nullopt};
    }
    co_return std::move(*str_r);
}

}  // namespace

/// @brief Drives WebSocket session lifecycle: upgrade, chain ID check, subscriptions, and stream loop.
template <class Ws>
net::awaitable<WsClosed> WsSession::serve(std::shared_ptr<WsSession> self, std::shared_ptr<Ws> ws) {
    auto weak = self->weak_from_this();
    auto p = std::make_shared<Proto>(Proto{self, self->cfg_, self->handlers_, self->exec_,
                                           [weak] {
                                               if (auto s = weak.lock()) s->close_socket();
                                           },
                                           self->unknown_msgs_});

    // Configure maximum frame size limit
    ws->read_message_max(kMaxMessageBytes);
    // Set custom User-Agent in HTTP upgrade handshake
    ws->set_option(websocket::stream_base::decorator(
        [ua = self->cfg_.user_agent](websocket::request_type& req) { req.set(http::field::user_agent, ua); }));
    beast::get_lowest_layer(*ws).expires_never();  // WS-level timeouts take over from here
    // Enable automated keepalive ping frames and idle detection timeout
    ws->set_option(websocket::stream_base::timeout{self->cfg_.handshake_timeout, self->cfg_.idle_timeout,
                                                   /*keep_alive_pings=*/true});

    websocket::response_type res;
    boost::system::error_code ec;
    // Perform WebSocket upgrade handshake
    co_await ws->async_handshake(res, self->url_.host_header(), self->url_.target,
                                 net::redirect_error(net::use_awaitable, ec));
    if (ec) {
        // Inspect HTTP upgrade rejection (401, 403, 429) and classify failure
        if (ec == websocket::error::upgrade_declined) {
            ErrorInfo info;
            info.http_status = static_cast<int>(res.result_int());
            info.message = res.body().substr(0, 120);
            if (const auto it = res.find(http::field::retry_after); it != res.end()) {
                info.retry_after = parse_retry_after(std::string_view(it->value().data(), it->value().size()));
            }
            const auto c = classify(info);
            co_return WsClosed{c.kind,
                               "WS upgrade rejected: HTTP " + std::to_string(info.http_status) + " " + info.message,
                               c.retry_after, info.http_status, std::nullopt};
        }
        co_return WsClosed{FailKind::Retry, "WS upgrade failed: " + describe(ec), std::nullopt, 0, std::nullopt};
    }
    // Set WebSocket stream to text mode
    ws->text(true);

    // Phase 1: Verify chain ID (FailKind::ConfigFatal if mismatched)
    {
        const auto id = p->next_id++;
        auto r = co_await request(ws, p, id, build_chain_id(id), self->cfg_.request_timeout, "eth_chainId response");
        if (r.has_error()) co_return r.error();
        if (!r->ok) co_return make_rpc_error(r->error, "eth_chainId");
        const auto got_r = p->dec.try_parse_quantity(r->result_raw);
        if (got_r.has_error()) {
            co_return WsClosed{FailKind::Retry, got_r.error().message, std::nullopt, 0, std::nullopt};
        }
        const auto got = *got_r;
        if (got != self->cfg_.expected_chain_id) {
            co_return WsClosed{FailKind::ConfigFatal,
                               "chain id mismatch: provider reports " + std::to_string(got) + ", config expects " +
                                   std::to_string(self->cfg_.expected_chain_id),
                               std::nullopt, 0, std::nullopt};
        }
    }
    // Phase 2: Establish subscriptions
    if (self->cfg_.subscribe_heads) {
        const auto id = p->next_id++;
        auto sub_r = co_await subscribe(ws, p, build_subscribe(id, "newHeads"), id, "newHeads");
        if (sub_r.has_error()) co_return sub_r.error();
        p->heads_sub = std::move(*sub_r);
    }
    if (self->cfg_.logs) {
        const auto id = p->next_id++;
        auto sub_r = co_await subscribe(ws, p, build_subscribe_logs(id, *self->cfg_.logs), id, "logs");
        if (sub_r.has_error()) co_return sub_r.error();
        p->logs_sub = std::move(*sub_r);
    }

    // Phase 3: Transition to Ready state and enter infinite event consumption loop
    self->ready_at_ = std::chrono::steady_clock::now();
    self->handlers_.on_ready();
    for (;;) {
        if (auto err = co_await read_frame(ws, p, nullptr, "stream data")) co_return std::move(*err);
        auto msg_r = p->dec.try_decode(frame_view(*p));
        if (msg_r.has_error()) {
            co_return WsClosed{FailKind::Retry, msg_r.error().message, std::nullopt, 0, std::nullopt};
        }
        auto& msg = msg_r.value();
        if (auto* n = std::get_if<Notification>(&msg)) {
            dispatch(*p, std::move(*n));
        } else {
            co_return WsClosed{FailKind::Retry, "unexpected JSON-RPC response on a live stream", std::nullopt, 0,
                               std::nullopt};
        }
    }
}

/// @brief Establishes underlying TCP/TLS connection, socket tuning, and hands off to serve().
net::awaitable<WsClosed> WsSession::session_main(std::shared_ptr<WsSession> self) {
    const std::string who = self->cfg_.name + " " + self->redacted_;

    // Resolve hostname to IP endpoints
    auto resolver = std::make_shared<tcp::resolver>(self->exec_);
    boost::system::error_code ec;
    tcp::resolver::results_type endpoints;
    {
        CloserGuard resolve_guard(self->closer_, [resolver] { resolver->cancel(); });
        endpoints = co_await resolver->async_resolve(self->url_.host, self->url_.port,
                                                     net::redirect_error(net::use_awaitable, ec));
    }
    if (ec) {
        co_return WsClosed{FailKind::Retry, "resolve failed for " + who + ": " + ec.message(), std::nullopt, 0,
                           std::nullopt};
    }

    // Branch 1: Plain unencrypted WebSocket stream (for mock test servers)
    if (!self->url_.tls) {
        auto ws = std::make_shared<websocket::stream<beast::tcp_stream>>(self->exec_);
        CloserGuard guard(self->closer_, [ws] {
            boost::system::error_code close_ec;
            beast::get_lowest_layer(*ws).socket().close(close_ec);
        });
        beast::get_lowest_layer(*ws).expires_after(self->cfg_.connect_timeout);
        co_await beast::get_lowest_layer(*ws).async_connect(endpoints, net::redirect_error(net::use_awaitable, ec));
        if (ec) {
            co_return WsClosed{FailKind::Retry, "connect failed for " + who + ": " + describe(ec), std::nullopt, 0,
                               std::nullopt};
        }
        tune_socket(beast::get_lowest_layer(*ws).socket(), who);
        co_return co_await serve(self, std::move(ws));
    }

    // Branch 2: TLS encrypted WebSocket stream (wss:// for production providers).
    // Hold ssl::context before ssl_stream inside a heap-allocated struct so ctx always outlives the stream.
    struct TlsWsConn {
        std::shared_ptr<ssl::context> ctx;
        websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws;
        TlsWsConn(const net::any_io_executor& ex, std::shared_ptr<ssl::context> c) : ctx(std::move(c)), ws(ex, *ctx) {}
    };
    auto conn = std::make_shared<TlsWsConn>(self->exec_, make_tls_client_context());
    auto ws_ptr = std::shared_ptr<websocket::stream<beast::ssl_stream<beast::tcp_stream>>>(conn, &conn->ws);
    CloserGuard guard(self->closer_, [conn] {
        boost::system::error_code close_ec;
        beast::get_lowest_layer(conn->ws).socket().close(close_ec);
    });
    if (!configure_tls_sni_and_hostname(ws_ptr->next_layer(), self->url_.host)) {
        co_return WsClosed{FailKind::Retry, "cannot set TLS SNI for " + who, std::nullopt, 0, std::nullopt};
    }
    beast::get_lowest_layer(*ws_ptr).expires_after(self->cfg_.connect_timeout);
    co_await beast::get_lowest_layer(*ws_ptr).async_connect(endpoints, net::redirect_error(net::use_awaitable, ec));
    if (ec) {
        co_return WsClosed{FailKind::Retry, "connect failed for " + who + ": " + describe(ec), std::nullopt, 0,
                           std::nullopt};
    }
    tune_socket(beast::get_lowest_layer(*ws_ptr).socket(), who);
    beast::get_lowest_layer(*ws_ptr).expires_after(self->cfg_.connect_timeout);
    co_await ws_ptr->next_layer().async_handshake(ssl::stream_base::client,
                                                  net::redirect_error(net::use_awaitable, ec));
    if (ec) {
        co_return WsClosed{FailKind::Retry, "TLS handshake failed for " + who + ": " + describe(ec), std::nullopt, 0,
                           std::nullopt};
    }
    co_return co_await serve(self, std::move(ws_ptr));
}

/// @brief Private constructor called by factory method create().
WsSession::WsSession(Private, net::any_io_executor executor, WsSessionConfig cfg, WsHandlers handlers)
    : exec_(std::move(executor)),
      cfg_(std::move(cfg)),
      handlers_(std::move(handlers)),
      url_(parse_url(cfg_.url, UrlKind::Ws)),
      redacted_(core::env::redact(cfg_.url)) {}

/// @brief Validates options and instantiates a new WsSession.
std::shared_ptr<WsSession> WsSession::create(net::any_io_executor executor, WsSessionConfig cfg, WsHandlers handlers) {
    if (!cfg.subscribe_heads && !cfg.logs) throw std::invalid_argument("WsSession: no subscription configured");
    if (cfg.logs && (cfg.logs->addresses.empty() || cfg.logs->topic0s.empty())) {
        throw std::invalid_argument("WsSession: logs filter needs at least one address and one topic0");
    }
    if (!handlers.on_ready || !handlers.on_closed)
        throw std::invalid_argument("WsSession: on_ready/on_closed required");
    if (cfg.subscribe_heads && !handlers.on_head) throw std::invalid_argument("WsSession: on_head required");
    if (cfg.logs && !handlers.on_log) throw std::invalid_argument("WsSession: on_log required");
    return std::make_shared<WsSession>(Private{}, std::move(executor), std::move(cfg), std::move(handlers));
}

/// @brief Spawns asynchronous session runner coroutine.
void WsSession::start() {
    if (started_) throw std::logic_error("WsSession::start called twice");
    started_ = true;
    net::co_spawn(exec_, run(), net::detached);
}

/// @brief Invokes installed closer closure to terminate active socket.
void WsSession::close_socket() {
    if (closer_) closer_();
}

/// @brief Stops the session and suppresses further callbacks.
void WsSession::stop() {
    closed_ = true;  // suppresses finish()
    close_socket();
}

/// @brief Finalizes session state, computes live duration, and delivers on_closed event.
void WsSession::finish(WsClosed c) {
    // Idempotent completion check
    if (closed_) return;
    closed_ = true;
    // Calculate total continuous duration in Ready state for backoff decay evaluation
    if (ready_at_) {
        live_for_ =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - *ready_at_);
    }
    // Deliver terminal event to subscriber
    handlers_.on_closed(c);
}

/// @brief Top-level coroutine orchestrating session execution.
net::awaitable<void> WsSession::run() {
    return run_impl(shared_from_this());
}

net::awaitable<void> WsSession::run_impl(std::shared_ptr<WsSession> self) {
    WsClosed out = co_await session_main(self);
    self->finish(std::move(out));
}

}  // namespace evml::rpc
