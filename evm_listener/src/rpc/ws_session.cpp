#include "rpc/ws_session.hpp"

#include <stdexcept>
#include <variant>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <openssl/ssl.h>
#ifdef __linux__
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#endif

#include "core/env.hpp"
#include "core/log.hpp"
#include "rpc/blocking_http.hpp"

namespace evml::rpc {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

// Friend of WsSession: the only door through which the cpp-local protocol code touches session state.
struct WsSessionAccess {
    static const WsSessionConfig& cfg(const WsSession& s) { return s.cfg_; }
    static const WsHandlers& handlers(const WsSession& s) { return s.handlers_; }
    static const ParsedUrl& url(const WsSession& s) { return s.url_; }
    static const std::string& redacted(const WsSession& s) { return s.redacted_; }
    static const net::any_io_executor& exec(const WsSession& s) { return s.exec_; }
    static void set_closer(WsSession& s, std::function<void()> f) { s.closer_ = std::move(f); }
    static void close_socket(WsSession& s) { s.close_socket(); }
    static void mark_ready(WsSession& s) { s.ready_at_ = std::chrono::steady_clock::now(); }
    static void bump_unknown(WsSession& s) { ++s.unknown_msgs_; }
};

namespace {

constexpr std::size_t kMaxMessageBytes = 16u << 20;  // 16 MiB

struct SessionFail : std::runtime_error {
    SessionFail(FailKind k, const std::string& msg, std::optional<std::chrono::seconds> ra = std::nullopt)
        : std::runtime_error(msg), kind(k), retry_after(ra) {}
    FailKind kind;
    std::optional<std::chrono::seconds> retry_after;
};

std::string describe(const boost::system::error_code& ec) {
    if (ec == websocket::error::closed) return "server closed the connection";
    if (ec == beast::error::timeout) return "idle timeout: no frames or pongs (half-open peer?)";
    return ec.message();
}

// Per-request deadline that aborts the socket. `active` guards against a timer callback that was already queued
// when the awaited operation completed: such a late callback must not kill a healthy connection.
class Deadline {
public:
    Deadline(const net::any_io_executor& ex, std::chrono::milliseconds d, std::weak_ptr<WsSession> w)
        : timer_(ex), active_(std::make_shared<bool>(true)), hit_(std::make_shared<bool>(false)) {
        timer_.expires_after(d);
        timer_.async_wait([active = active_, hit = hit_, w](const boost::system::error_code& ec) {
            if (ec || !*active) return;
            *hit = true;
            if (auto s = w.lock()) WsSessionAccess::close_socket(*s);
        });
    }
    ~Deadline() {
        *active_ = false;
        timer_.cancel();
    }
    Deadline(const Deadline&) = delete;
    Deadline& operator=(const Deadline&) = delete;
    bool hit() const { return *hit_; }

private:
    net::steady_timer timer_;
    std::shared_ptr<bool> active_;
    std::shared_ptr<bool> hit_;
};

struct CloserGuard {
    CloserGuard(WsSession& s, std::function<void()> f) : s_(s) { WsSessionAccess::set_closer(s_, std::move(f)); }
    ~CloserGuard() { WsSessionAccess::set_closer(s_, nullptr); }
    WsSession& s_;
};

template <class Awaitable>
net::awaitable<void> phase(const std::string& who, const char* name, Awaitable&& a) {
    try {
        co_await std::forward<Awaitable>(a);
    } catch (const boost::system::system_error& e) {
        throw SessionFail(FailKind::Retry, std::string(name) + " failed for " + who + ": " + describe(e.code()));
    }
}

void tune_socket(tcp::socket& sock, const std::string& who) {
    const auto check = [&](const boost::system::error_code& ec, const char* what) {
        if (ec) {
            core::log::warn("ws", "socket option not applied",
                            {{"provider", who}, {"option", what}, {"error", ec.message()}});
        }
    };
    boost::system::error_code ec;
    sock.set_option(tcp::no_delay(true), ec);
    check(ec, "TCP_NODELAY");
    sock.set_option(net::socket_base::keep_alive(true), ec);
    check(ec, "SO_KEEPALIVE");
#ifdef __linux__
    // Half-open detection below the WS layer: probe after 10 s idle, every 5 s, 3 misses; hard cap 25 s unacked.
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

struct Proto {
    WsSession& s;
    std::weak_ptr<WsSession> weak;
    Decoder dec;
    beast::flat_buffer buf;
    std::uint64_t next_id{1};
    std::optional<std::string> heads_sub;
    std::optional<std::string> logs_sub;
};

[[noreturn]] void throw_rpc_error(const RpcError& e, const std::string& what) {
    ErrorInfo info;
    info.rpc_code = e.code;
    info.message = e.message;
    const auto c = classify(info);
    throw SessionFail(c.kind, what + ": rpc error " + std::to_string(e.code) + ": " + e.message.substr(0, 120),
                      c.retry_after);
}

template <class Ws>
net::awaitable<void> read_frame(Ws& ws, Proto& p, const Deadline* dl, const std::string& what) {
    p.buf.consume(p.buf.size());
    try {
        co_await ws.async_read(p.buf, net::use_awaitable);
    } catch (const boost::system::system_error& e) {
        if (dl && dl->hit()) throw SessionFail(FailKind::Retry, "timeout waiting for " + what);
        throw SessionFail(FailKind::Retry, "read failed while waiting for " + what + ": " + describe(e.code()));
    }
}

std::string_view frame_view(const Proto& p) {
    return std::string_view(static_cast<const char*>(p.buf.data().data()), p.buf.size());
}

void dispatch(Proto& p, Notification&& n) {
    const auto& h = WsSessionAccess::handlers(p.s);
    if (const auto* head = std::get_if<Head>(&n.payload); head && p.heads_sub && n.subscription == *p.heads_sub) {
        h.on_head(*head);
    } else if (const auto* lg = std::get_if<Log>(&n.payload); lg && p.logs_sub && n.subscription == *p.logs_sub) {
        h.on_log(*lg);
    } else {
        WsSessionAccess::bump_unknown(p.s);
        core::log::warn("ws", "notification for an unknown subscription dropped",
                        {{"provider", p.s.name()}, {"subscription", n.subscription}});
    }
}

template <class Ws>
net::awaitable<void> send_text(Ws& ws, const std::string& msg) {
    try {
        co_await ws.async_write(net::buffer(msg), net::use_awaitable);
    } catch (const boost::system::system_error& e) {
        throw SessionFail(FailKind::Retry, "write failed: " + describe(e.code()));
    }
}

// Sends `req` and returns the matching Response; notifications arriving meanwhile are dispatched.
template <class Ws>
net::awaitable<Response> request(Ws& ws, Proto& p, std::uint64_t id, const std::string& req,
                                 std::chrono::milliseconds timeout, const std::string& what) {
    co_await send_text(ws, req);
    Deadline dl(WsSessionAccess::exec(p.s), timeout, p.weak);
    for (;;) {
        co_await read_frame(ws, p, &dl, what);
        auto msg = p.dec.decode(frame_view(p));
        if (auto* n = std::get_if<Notification>(&msg)) {
            dispatch(p, std::move(*n));
            continue;
        }
        auto& r = std::get<Response>(msg);
        if (r.id && *r.id == id) co_return std::move(r);
        throw SessionFail(FailKind::Retry, "unexpected response id while waiting for " + what);
    }
}

template <class Ws>
net::awaitable<std::string> subscribe(Ws& ws, Proto& p, const std::string& req, std::uint64_t id, const std::string& what) {
    const auto& cfg = WsSessionAccess::cfg(p.s);
    auto r = co_await request(ws, p, id, req, cfg.subscribe_timeout, "subscribe ACK (" + what + ")");
    if (!r.ok) throw_rpc_error(r.error, "subscribe " + what);
    co_return p.dec.parse_string(r.result_raw);
}

template <class Ws>
net::awaitable<void> serve(WsSession& s, Ws& ws) {
    const auto& cfg = WsSessionAccess::cfg(s);
    const auto& url = WsSessionAccess::url(s);
    Proto p{s, s.weak_from_this(), {}, {}, 1, std::nullopt, std::nullopt};

    ws.read_message_max(kMaxMessageBytes);
    ws.set_option(websocket::stream_base::decorator(
        [ua = cfg.user_agent](websocket::request_type& req) { req.set(http::field::user_agent, ua); }));
    beast::get_lowest_layer(ws).expires_never();  // WS-level timeouts take over from here
    ws.set_option(websocket::stream_base::timeout{cfg.handshake_timeout, cfg.idle_timeout, /*keep_alive_pings=*/true});

    websocket::response_type res;
    try {
        co_await ws.async_handshake(res, url.host_header(), url.target, net::use_awaitable);
    } catch (const boost::system::system_error& e) {
        if (e.code() == websocket::error::upgrade_declined) {
            ErrorInfo info;
            info.http_status = static_cast<int>(res.result_int());
            info.message = res.body().substr(0, 120);
            if (const auto it = res.find(http::field::retry_after); it != res.end()) {
                info.retry_after = parse_retry_after(std::string_view(it->value().data(), it->value().size()));
            }
            const auto c = classify(info);
            throw SessionFail(c.kind, "WS upgrade rejected: HTTP " + std::to_string(info.http_status) + " " + info.message,
                              c.retry_after);
        }
        throw SessionFail(FailKind::Retry, "WS upgrade failed: " + describe(e.code()));
    }
    ws.text(true);

    // 1. chain id
    {
        const auto id = p.next_id++;
        auto r = co_await request(ws, p, id, build_chain_id(id), cfg.request_timeout, "eth_chainId response");
        if (!r.ok) throw_rpc_error(r.error, "eth_chainId");
        const auto got = p.dec.parse_quantity(r.result_raw);
        if (got != cfg.expected_chain_id) {
            throw SessionFail(FailKind::ConfigFatal, "chain id mismatch: provider reports " + std::to_string(got) +
                                                         ", config expects " + std::to_string(cfg.expected_chain_id));
        }
    }
    // 2. subscriptions (ids are per-session; never compared across sessions)
    if (cfg.subscribe_heads) {
        const auto id = p.next_id++;
        p.heads_sub = co_await subscribe(ws, p, build_subscribe(id, "newHeads"), id, "newHeads");
    }
    if (cfg.logs) {
        const auto id = p.next_id++;
        p.logs_sub = co_await subscribe(ws, p, build_subscribe_logs(id, *cfg.logs), id, "logs");
    }

    // 3. live
    WsSessionAccess::mark_ready(s);
    WsSessionAccess::handlers(s).on_ready();
    for (;;) {
        co_await read_frame(ws, p, nullptr, "stream data");
        auto msg = p.dec.decode(frame_view(p));
        if (auto* n = std::get_if<Notification>(&msg)) {
            dispatch(p, std::move(*n));
        } else {
            throw SessionFail(FailKind::Retry, "unexpected JSON-RPC response on a live stream");
        }
    }
}

net::awaitable<void> session_main(std::shared_ptr<WsSession> self) {
    WsSession& s = *self;
    const auto& cfg = WsSessionAccess::cfg(s);
    const auto& url = WsSessionAccess::url(s);
    const auto& ex = WsSessionAccess::exec(s);
    const std::string who = cfg.name + " " + WsSessionAccess::redacted(s);

    tcp::resolver resolver(ex);
    tcp::resolver::results_type endpoints;
    try {
        endpoints = co_await resolver.async_resolve(url.host, url.port, net::use_awaitable);
    } catch (const boost::system::system_error& e) {
        throw SessionFail(FailKind::Retry, "resolve failed for " + who + ": " + e.code().message());
    }

    if (!url.tls) {
        websocket::stream<beast::tcp_stream> ws(ex);
        CloserGuard guard(s, [&ws] {
            boost::system::error_code ec;
            beast::get_lowest_layer(ws).socket().close(ec);
        });
        beast::get_lowest_layer(ws).expires_after(cfg.connect_timeout);
        co_await phase(who, "connect", beast::get_lowest_layer(ws).async_connect(endpoints, net::use_awaitable));
        tune_socket(beast::get_lowest_layer(ws).socket(), who);
        co_await serve(s, ws);
        co_return;
    }

    ssl::context ctx(ssl::context::tls_client);
    ctx.set_default_verify_paths();
    ctx.set_verify_mode(ssl::verify_peer);
    websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws(ex, ctx);
    CloserGuard guard(s, [&ws] {
        boost::system::error_code ec;
        beast::get_lowest_layer(ws).socket().close(ec);
    });
    if (!SSL_set_tlsext_host_name(ws.next_layer().native_handle(), url.host.c_str())) {
        throw SessionFail(FailKind::Retry, "cannot set TLS SNI for " + who);
    }
    ws.next_layer().set_verify_callback(ssl::host_name_verification(url.host));
    beast::get_lowest_layer(ws).expires_after(cfg.connect_timeout);
    co_await phase(who, "connect", beast::get_lowest_layer(ws).async_connect(endpoints, net::use_awaitable));
    tune_socket(beast::get_lowest_layer(ws).socket(), who);
    beast::get_lowest_layer(ws).expires_after(cfg.connect_timeout);
    co_await phase(who, "TLS handshake", ws.next_layer().async_handshake(ssl::stream_base::client, net::use_awaitable));
    co_await serve(s, ws);
}

}  // namespace

WsSession::WsSession(Private, net::any_io_executor executor, WsSessionConfig cfg, WsHandlers handlers)
    : exec_(std::move(executor)),
      cfg_(std::move(cfg)),
      handlers_(std::move(handlers)),
      url_(parse_url(cfg_.url, UrlKind::Ws)),
      redacted_(core::env::redact(cfg_.url)) {}

std::shared_ptr<WsSession> WsSession::create(net::any_io_executor executor, WsSessionConfig cfg, WsHandlers handlers) {
    if (!cfg.subscribe_heads && !cfg.logs) throw std::invalid_argument("WsSession: no subscription configured");
    if (cfg.logs && (cfg.logs->addresses.empty() || cfg.logs->topic0s.empty())) {
        throw std::invalid_argument("WsSession: logs filter needs at least one address and one topic0");
    }
    if (!handlers.on_ready || !handlers.on_closed) throw std::invalid_argument("WsSession: on_ready/on_closed required");
    if (cfg.subscribe_heads && !handlers.on_head) throw std::invalid_argument("WsSession: on_head required");
    if (cfg.logs && !handlers.on_log) throw std::invalid_argument("WsSession: on_log required");
    return std::make_shared<WsSession>(Private{}, std::move(executor), std::move(cfg), std::move(handlers));
}

void WsSession::start() {
    if (started_) throw std::logic_error("WsSession::start called twice");
    started_ = true;
    net::co_spawn(exec_, run(), net::detached);
}

void WsSession::close_socket() {
    if (closer_) closer_();
}

void WsSession::stop() {
    closed_ = true;  // suppresses finish()
    close_socket();
}

void WsSession::finish(WsClosed c) {
    if (closed_) return;
    closed_ = true;
    if (ready_at_) {
        live_for_ = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - *ready_at_);
    }
    handlers_.on_closed(c);
}

net::awaitable<void> WsSession::run() {
    auto self = shared_from_this();  // keeps the session alive for the whole coroutine
    WsClosed out;
    try {
        co_await session_main(self);
        out = {FailKind::Retry, "session ended unexpectedly", std::nullopt};
    } catch (const SessionFail& f) {
        out = {f.kind, f.what(), f.retry_after};
    } catch (const std::exception& e) {
        out = {FailKind::Retry, std::string("error: ") + e.what(), std::nullopt};
    }
    finish(std::move(out));
}

}  // namespace evml::rpc
