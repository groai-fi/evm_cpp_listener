#include "mock_http_server.hpp"

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

namespace evml::test {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = net::ip::tcp;

struct MockHttpServer::State {
    State(net::io_context& ioc, Handler h) : handler(std::move(h)), acceptor(ioc, tcp::endpoint(net::ip::make_address("127.0.0.1"), 0)) {}
    Handler handler;
    tcp::acceptor acceptor;
    std::atomic<int> conns{0};
    std::atomic<int> reqs{0};
};

namespace {

net::awaitable<void> session(std::shared_ptr<MockHttpServer::State> st, tcp::socket sock);

net::awaitable<void> accept_loop(std::shared_ptr<MockHttpServer::State> st) {
    auto ex = co_await net::this_coro::executor;
    for (;;) {
        tcp::socket sock(ex);
        co_await st->acceptor.async_accept(sock, net::use_awaitable);
        ++st->conns;
        net::co_spawn(ex, session(st, std::move(sock)), net::detached);
    }
}

net::awaitable<void> session(std::shared_ptr<MockHttpServer::State> st, tcp::socket sock) {
    beast::flat_buffer buf;
    try {
        for (;;) {
            http::request<http::string_body> req;
            co_await http::async_read(sock, buf, req, net::use_awaitable);
            ++st->reqs;

            MockRequest mr;
            mr.target = std::string(req.target());
            mr.user_agent = std::string(req[http::field::user_agent]);
            mr.accept_encoding = std::string(req[http::field::accept_encoding]);
            mr.body = req.body();
            const MockReply rep = st->handler(mr);

            if (rep.hang) {
                net::steady_timer forever(co_await net::this_coro::executor, std::chrono::hours(24));
                co_await forever.async_wait(net::use_awaitable);
                co_return;
            }
            if (rep.delay.count() > 0) {
                net::steady_timer t(co_await net::this_coro::executor, rep.delay);
                co_await t.async_wait(net::use_awaitable);
            }

            http::response<http::string_body> res{static_cast<http::status>(rep.status), req.version()};
            res.set(http::field::content_type, "application/json");
            for (const auto& [k, v] : rep.headers) res.set(k, v);
            res.body() = rep.body;
            res.keep_alive(!rep.connection_close);
            res.prepare_payload();
            co_await http::async_write(sock, res, net::use_awaitable);

            if (rep.connection_close || rep.close_silently) break;
        }
    } catch (const boost::system::system_error&) {
        // Peer closed or reset: end of this session (normal in tests).
    }
    beast::error_code ec;
    sock.shutdown(tcp::socket::shutdown_both, ec);
    sock.close(ec);
}

}  // namespace

MockHttpServer::MockHttpServer(net::io_context& ioc, Handler handler) : st_(std::make_shared<State>(ioc, std::move(handler))) {
    net::co_spawn(ioc, accept_loop(st_), net::detached);
}

std::string MockHttpServer::url() const {
    return "http://127.0.0.1:" + std::to_string(st_->acceptor.local_endpoint().port()) + "/";
}
int MockHttpServer::connections() const { return st_->conns; }
int MockHttpServer::requests() const { return st_->reqs; }

}  // namespace evml::test
