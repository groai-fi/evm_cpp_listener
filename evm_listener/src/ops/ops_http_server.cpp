#include "ops/ops_http_server.hpp"

#include <stdexcept>

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include "core/log.hpp"

namespace evml::ops {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = net::ip::tcp;

struct OpsHttpServer::State {
    State(net::io_context& ioc, const tcp::endpoint& ep, Routes r, std::chrono::milliseconds t)
        : routes(std::move(r)), timeout(t), acceptor(ioc) {
        acceptor.open(ep.protocol());
        acceptor.set_option(net::socket_base::reuse_address(true));
        // `[::]` must also accept IPv4 (127.0.0.1 in local tests, the Docker HEALTHCHECK) whatever the host's
        // net.ipv6.bindv6only default is (plan 60 §5.2). The option only exists on IPv6 sockets.
        if (ep.protocol() == tcp::v6()) acceptor.set_option(net::ip::v6_only(false));
        acceptor.bind(ep);
        acceptor.listen();
    }
    Routes routes;
    std::chrono::milliseconds timeout;
    tcp::acceptor acceptor;
};

namespace {

tcp::endpoint parse_listen(const std::string& listen) {
    const auto colon = listen.rfind(':');
    if (colon == std::string::npos || colon + 1 >= listen.size())
        throw std::invalid_argument("ops http listen address must be host:port, got \"" + listen + "\"");
    std::string host = listen.substr(0, colon);
    const std::string port = listen.substr(colon + 1);
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']') host = host.substr(1, host.size() - 2);
    if (port.size() > 5 || port.find_first_not_of("0123456789") != std::string::npos)
        throw std::invalid_argument("ops http listen port is not a number: \"" + listen + "\"");
    const unsigned long p = std::stoul(port);
    if (p > 65535) throw std::invalid_argument("ops http listen port out of range: \"" + listen + "\"");
    boost::system::error_code ec;
    const auto addr = net::ip::make_address(host, ec);
    if (ec) throw std::invalid_argument("ops http listen host is not an IP literal: \"" + listen + "\"");
    return tcp::endpoint(addr, static_cast<unsigned short>(p));
}

http::response<http::string_body> make_response(unsigned version, bool keep_alive, int status, std::string body,
                                                const std::string& content_type) {
    http::response<http::string_body> res{static_cast<http::status>(status), version};
    res.set(http::field::content_type, content_type);
    res.set(http::field::cache_control, "no-store");
    res.keep_alive(keep_alive);
    res.body() = std::move(body);
    res.prepare_payload();
    return res;
}

const OpsHttpServer::Handler* route(const OpsHttpServer::Routes& r, std::string_view path) {
    if (path == "/healthz") return &r.healthz;
    if (path == "/readyz") return &r.readyz;
    if (path == "/metrics") return &r.metrics;
    return nullptr;
}

net::awaitable<void> session(std::shared_ptr<OpsHttpServer::State> st, tcp::socket raw) {
    beast::tcp_stream stream(std::move(raw));
    beast::flat_buffer buf;
    try {
        for (;;) {
            stream.expires_after(st->timeout);
            http::request<http::empty_body> req;
            co_await http::async_read(stream, buf, req, net::use_awaitable);

            std::string_view path = req.target();
            path = path.substr(0, path.find('?'));
            const bool keep = req.keep_alive();

            http::response<http::string_body> res = make_response(req.version(), keep, 404, "not found\n", "text/plain");
            if (req.method() != http::verb::get) {
                res = make_response(req.version(), keep, 405, "method not allowed\n", "text/plain");
                res.set(http::field::allow, "GET");
            } else if (const auto* handler = route(st->routes, path)) {
                try {
                    OpsResponse out = (*handler)();
                    res = make_response(req.version(), keep, out.status, std::move(out.body), out.content_type);
                } catch (const std::exception& e) {
                    core::log::error("ops_http", "handler failed", {{"path", std::string(path)}, {"what", e.what()}});
                    res = make_response(req.version(), keep, 500, "internal error\n", "text/plain");
                }
            }
            co_await http::async_write(stream, res, net::use_awaitable);
            if (!res.keep_alive()) break;
        }
    } catch (const boost::system::system_error&) {
        // Peer closed, malformed request, or the slow-client deadline fired: end this session.
    }
    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_both, ec);
    stream.close();
}

net::awaitable<void> accept_loop(std::shared_ptr<OpsHttpServer::State> st) {
    auto ex = co_await net::this_coro::executor;
    try {
        for (;;) {
            tcp::socket sock(ex);
            co_await st->acceptor.async_accept(sock, net::use_awaitable);
            net::co_spawn(ex, session(st, std::move(sock)), net::detached);
        }
    } catch (const boost::system::system_error&) {
        // acceptor closed by stop(): normal end
    }
}

}  // namespace

OpsHttpServer::OpsHttpServer(net::io_context& ioc, const std::string& listen, Routes routes,
                             std::chrono::milliseconds request_timeout) {
    if (!routes.healthz || !routes.readyz || !routes.metrics)
        throw std::invalid_argument("OpsHttpServer: every route needs a handler");
    if (request_timeout.count() <= 0) throw std::invalid_argument("OpsHttpServer: request_timeout must be > 0");
    st_ = std::make_shared<State>(ioc, parse_listen(listen), std::move(routes), request_timeout);
    net::co_spawn(ioc, accept_loop(st_), net::detached);
}

OpsHttpServer::~OpsHttpServer() { stop(); }

std::uint16_t OpsHttpServer::port() const { return st_->acceptor.local_endpoint().port(); }

void OpsHttpServer::stop() {
    boost::system::error_code ec;
    st_->acceptor.close(ec);
}

}  // namespace evml::ops
