#include "mock_ws_server.hpp"

#include <atomic>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

namespace evml::test {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;

struct MockWsServer::State {
    State(net::io_context& ioc, Script s, MockWsOptions o)
        : script(std::move(s)), opts(std::move(o)), acceptor(ioc, tcp::endpoint(net::ip::make_address("127.0.0.1"), 0)) {}
    Script script;
    MockWsOptions opts;
    tcp::acceptor acceptor;
    std::atomic<int> conns{0};
    std::atomic<int> upgrades{0};
};

net::awaitable<MockWsConn::Req> MockWsConn::read_req() {
    beast::flat_buffer b;
    co_await ws_.async_read(b, net::use_awaitable);
    const auto text = beast::buffers_to_string(b.data());
    const auto obj = boost::json::parse(text).as_object();
    Req r;
    r.id = static_cast<std::uint64_t>(obj.at("id").as_int64());
    r.method = std::string(obj.at("method").as_string());
    r.params = obj.at("params");
    co_return r;
}

net::awaitable<void> MockWsConn::send(const std::string& text) {
    ws_.text(true);
    co_await ws_.async_write(net::buffer(text), net::use_awaitable);
}

net::awaitable<void> MockWsConn::close(websocket::close_code code) {
    co_await ws_.async_close(websocket::close_reason(code), net::use_awaitable);
}

void MockWsConn::drop() {
    beast::error_code ec;
    ws_.next_layer().shutdown(tcp::socket::shutdown_both, ec);
    ws_.next_layer().close(ec);
}

net::awaitable<void> MockWsConn::go_silent() { co_await sleep(std::chrono::hours(24)); }

net::awaitable<void> MockWsConn::sleep(std::chrono::milliseconds d) {
    net::steady_timer t(co_await net::this_coro::executor, d);
    co_await t.async_wait(net::use_awaitable);
}

namespace {

net::awaitable<void> session(std::shared_ptr<MockWsServer::State> st, tcp::socket sock) {
    try {
        beast::flat_buffer buf;
        http::request<http::string_body> req;
        co_await http::async_read(sock, buf, req, net::use_awaitable);

        if (st->opts.reject_status != 0) {
            http::response<http::string_body> res{static_cast<http::status>(st->opts.reject_status), req.version()};
            for (const auto& [k, v] : st->opts.reject_headers) res.set(k, v);
            res.set(http::field::content_type, "text/plain");
            res.body() = st->opts.reject_body;
            res.keep_alive(false);
            res.prepare_payload();
            co_await http::async_write(sock, res, net::use_awaitable);
            beast::error_code ec;
            sock.shutdown(tcp::socket::shutdown_both, ec);
            co_return;
        }

        const std::string ua(req[http::field::user_agent]);
        websocket::stream<tcp::socket> ws(std::move(sock));
        co_await ws.async_accept(req, net::use_awaitable);
        ++st->upgrades;
        MockWsConn conn(ws, ua);
        co_await st->script(conn);
    } catch (const boost::system::system_error&) {
        // Peer went away or the script dropped the socket: end of this connection (normal in tests).
    }
}

net::awaitable<void> accept_loop(std::shared_ptr<MockWsServer::State> st) {
    auto ex = co_await net::this_coro::executor;
    for (;;) {
        tcp::socket sock(ex);
        co_await st->acceptor.async_accept(sock, net::use_awaitable);
        ++st->conns;
        net::co_spawn(ex, session(st, std::move(sock)), net::detached);
    }
}

}  // namespace

MockWsServer::MockWsServer(net::io_context& ioc, Script script, MockWsOptions options)
    : st_(std::make_shared<State>(ioc, std::move(script), std::move(options))) {
    net::co_spawn(ioc, accept_loop(st_), net::detached);
}

std::string MockWsServer::url() const {
    return "ws://127.0.0.1:" + std::to_string(st_->acceptor.local_endpoint().port()) + "/";
}
int MockWsServer::connections() const { return st_->conns; }
int MockWsServer::upgrades() const { return st_->upgrades; }

}  // namespace evml::test
