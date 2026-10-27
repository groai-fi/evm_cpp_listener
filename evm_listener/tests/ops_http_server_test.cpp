#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <chrono>
#include <future>
#include <stdexcept>
#include <string>

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include "core/log.hpp"
#include "ops/ops_http_server.hpp"

using evml::ops::OpsHttpServer;
using evml::ops::OpsResponse;
using Catch::Matchers::ContainsSubstring;

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = net::ip::tcp;

namespace {

struct Reply {
    int status{};
    std::string body;
    std::string content_type;
    std::string allow;
};

net::awaitable<Reply> request_on(tcp::socket& s, std::string target, http::verb verb) {
    http::request<http::empty_body> req{verb, target, 11};
    req.set(http::field::host, "localhost");
    co_await http::async_write(s, req, net::use_awaitable);
    beast::flat_buffer buf;
    http::response<http::string_body> res;
    co_await http::async_read(s, buf, res, net::use_awaitable);
    co_return Reply{static_cast<int>(res.result_int()), res.body(), std::string(res[http::field::content_type]),
                    std::string(res[http::field::allow])};
}

net::awaitable<Reply> fetch(tcp::endpoint ep, std::string target, http::verb verb) {
    tcp::socket s(co_await net::this_coro::executor);
    co_await s.async_connect(ep, net::use_awaitable);
    co_return co_await request_on(s, std::move(target), verb);
}

net::awaitable<std::pair<Reply, Reply>> two_on_one_connection(tcp::endpoint ep) {
    tcp::socket s(co_await net::this_coro::executor);
    co_await s.async_connect(ep, net::use_awaitable);
    Reply a = co_await request_on(s, "/healthz", http::verb::get);
    Reply b = co_await request_on(s, "/metrics", http::verb::get);
    co_return std::make_pair(std::move(a), std::move(b));
}

// True when the server closed an idle connection (read hit EOF / reset) instead of waiting forever.
net::awaitable<bool> server_closes_idle(tcp::endpoint ep) {
    tcp::socket s(co_await net::this_coro::executor);
    co_await s.async_connect(ep, net::use_awaitable);
    char c;
    boost::system::error_code ec;
    co_await s.async_read_some(net::buffer(&c, 1), net::redirect_error(net::use_awaitable, ec));
    co_return static_cast<bool>(ec);
}

// Drives the single-threaded io_context until the coroutine finishes; fails the test instead of hanging.
template <class T>
T run(net::io_context& ioc, net::awaitable<T> aw) {
    auto fut = net::co_spawn(ioc, std::move(aw), net::use_future);
    ioc.restart();  // a previous poll() that ran out of work leaves the context stopped; run_one_for would then do nothing
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (fut.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
        if (std::chrono::steady_clock::now() > deadline) FAIL("ops http test timed out");
        ioc.run_one_for(std::chrono::milliseconds(10));
    }
    return fut.get();
}

OpsHttpServer::Routes basic_routes() {
    OpsHttpServer::Routes r;
    r.healthz = [] { return OpsResponse{200, "ok\n", "text/plain"}; };
    r.readyz = [] { return OpsResponse{503, "{\"state\":\"catching_up\"}", "application/json"}; };
    r.metrics = [] { return OpsResponse{200, "# TYPE x counter\nx 1\n", "text/plain; version=0.0.4"}; };
    return r;
}

tcp::endpoint loopback4(const OpsHttpServer& s) { return {net::ip::make_address("127.0.0.1"), s.port()}; }

}  // namespace

TEST_CASE("ops http: each route returns the handler's status, body and content type", "[ops_http][integration]") {
    evml::core::log::init("info");
    net::io_context ioc;
    OpsHttpServer srv(ioc, "127.0.0.1:0", basic_routes());
    const auto ep = loopback4(srv);

    const auto h = run(ioc, fetch(ep, "/healthz", http::verb::get));
    CHECK(h.status == 200);
    CHECK(h.body == "ok\n");

    const auto r = run(ioc, fetch(ep, "/readyz", http::verb::get));
    CHECK(r.status == 503);
    CHECK(r.content_type == "application/json");
    CHECK(r.body == "{\"state\":\"catching_up\"}");

    const auto m = run(ioc, fetch(ep, "/metrics?x=1", http::verb::get));  // query string is ignored
    CHECK(m.status == 200);
    CHECK_THAT(m.body, ContainsSubstring("x 1"));
    CHECK(m.content_type == "text/plain; version=0.0.4");
}

TEST_CASE("ops http: unknown path is 404 and a non-GET method is 405", "[ops_http][integration]") {
    evml::core::log::init("info");
    net::io_context ioc;
    OpsHttpServer srv(ioc, "127.0.0.1:0", basic_routes());
    const auto ep = loopback4(srv);

    CHECK(run(ioc, fetch(ep, "/nope", http::verb::get)).status == 404);
    CHECK(run(ioc, fetch(ep, "/", http::verb::get)).status == 404);
    const auto post = run(ioc, fetch(ep, "/healthz", http::verb::post));
    CHECK(post.status == 405);
    CHECK(post.allow == "GET");
}

TEST_CASE("ops http: a throwing handler is a 500 that does not leak the exception text", "[ops_http][integration]") {
    evml::core::log::init("info");
    net::io_context ioc;
    auto routes = basic_routes();
    routes.readyz = []() -> OpsResponse { throw std::runtime_error("secret-token-123"); };
    OpsHttpServer srv(ioc, "127.0.0.1:0", std::move(routes));
    const auto res = run(ioc, fetch(loopback4(srv), "/readyz", http::verb::get));
    CHECK(res.status == 500);
    CHECK(res.body.find("secret-token-123") == std::string::npos);
    CHECK(run(ioc, fetch(loopback4(srv), "/healthz", http::verb::get)).status == 200);  // server still serves
}

TEST_CASE("ops http: [::] is dual-stack (IPv4 and IPv6 loopback both answer)", "[ops_http][integration]") {
    evml::core::log::init("info");
    net::io_context ioc;
    OpsHttpServer srv(ioc, "[::]:0", basic_routes());
    const tcp::endpoint v4(net::ip::make_address("127.0.0.1"), srv.port());
    const tcp::endpoint v6(net::ip::make_address("::1"), srv.port());
    CHECK(run(ioc, fetch(v4, "/healthz", http::verb::get)).status == 200);
    CHECK(run(ioc, fetch(v6, "/healthz", http::verb::get)).status == 200);
}

TEST_CASE("ops http: keep-alive serves two requests on one connection", "[ops_http][integration]") {
    evml::core::log::init("info");
    net::io_context ioc;
    OpsHttpServer srv(ioc, "127.0.0.1:0", basic_routes());
    const auto both = run(ioc, two_on_one_connection(loopback4(srv)));
    CHECK(both.first.status == 200);
    CHECK(both.second.status == 200);
}

TEST_CASE("ops http: a client that sends nothing is closed after the request timeout", "[ops_http][integration]") {
    evml::core::log::init("info");
    net::io_context ioc;
    OpsHttpServer srv(ioc, "127.0.0.1:0", basic_routes(), std::chrono::milliseconds(100));
    CHECK(run(ioc, server_closes_idle(loopback4(srv))));
}

TEST_CASE("ops http: stop() refuses new connections", "[ops_http][integration]") {
    evml::core::log::init("info");
    net::io_context ioc;
    OpsHttpServer srv(ioc, "127.0.0.1:0", basic_routes());
    const auto ep = loopback4(srv);
    CHECK(run(ioc, fetch(ep, "/healthz", http::verb::get)).status == 200);
    srv.stop();
    ioc.poll();
    CHECK_THROWS(run(ioc, fetch(ep, "/healthz", http::verb::get)));
}

TEST_CASE("ops http: bad construction arguments are rejected", "[ops_http]") {
    net::io_context ioc;
    CHECK_THROWS_AS(OpsHttpServer(ioc, "9464", basic_routes()), std::invalid_argument);
    CHECK_THROWS_AS(OpsHttpServer(ioc, "localhost:9464", basic_routes()), std::invalid_argument);
    CHECK_THROWS_AS(OpsHttpServer(ioc, "127.0.0.1:99999", basic_routes()), std::invalid_argument);
    CHECK_THROWS_AS(OpsHttpServer(ioc, "127.0.0.1:12x", basic_routes()), std::invalid_argument);
    CHECK_THROWS_AS(OpsHttpServer(ioc, "127.0.0.1:", basic_routes()), std::invalid_argument);
    auto missing = basic_routes();
    missing.metrics = nullptr;
    CHECK_THROWS_AS(OpsHttpServer(ioc, "127.0.0.1:0", std::move(missing)), std::invalid_argument);
    CHECK_THROWS_AS(OpsHttpServer(ioc, "127.0.0.1:0", basic_routes(), std::chrono::milliseconds(0)), std::invalid_argument);
}
