#include <catch2/catch_test_macros.hpp>

#include <fstream>
#include <sstream>

#include "mock_http_server.hpp"
#include "rpc/blocking_http.hpp"
#include "rpc/http_rpc_client.hpp"
#include "test_util.hpp"

using namespace evml::rpc;
using namespace evml::test;
using namespace std::chrono_literals;
namespace net = boost::asio;

namespace {

constexpr const char* kUa = "evm-listener/0.1.0-test";

std::string fixture(const char* name) {
    std::ifstream f(std::string(EVML_FIXTURE_DIR) + "/" + name);
    REQUIRE(f.good());
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

HttpRpcClientConfig cfg(const MockHttpServer& s, std::chrono::milliseconds timeout = 2000ms, const char* ua = kUa) {
    return HttpRpcClientConfig{"mock", s.url(), ua, timeout};
}

const std::string kReq = build_block_number(1);
const std::string kOk = R"({"jsonrpc":"2.0","id":1,"result":"0x10"})";

}  // namespace

TEST_CASE("http client: round trip and keep-alive reuse", "[integration]") {
    net::io_context ioc;
    MockHttpServer srv(ioc, [](const MockRequest&) { return MockReply{.body = kOk}; });
    HttpRpcClient c(ioc.get_executor(), cfg(srv));

    for (int i = 0; i < 3; ++i) {
        const auto r = run_sync(ioc, c.call(kReq));
        CHECK(r.status == 200);
        CHECK(r.body == kOk);
        CHECK_FALSE(r.retry_after.has_value());
    }
    CHECK(srv.requests() == 3);
    CHECK(srv.connections() == 1);  // persistent connection
}

TEST_CASE("http client: sends identity encoding and the configured User-Agent (403 without it)", "[integration]") {
    net::io_context ioc;
    MockHttpServer srv(ioc, [](const MockRequest& r) {
        if (r.user_agent != kUa) return MockReply{.status = 403, .body = "error code: 1010"};
        if (r.accept_encoding != "identity") return MockReply{.status = 400, .body = "bad encoding"};
        return MockReply{.body = kOk};
    });

    HttpRpcClient good(ioc.get_executor(), cfg(srv));
    CHECK(run_sync(ioc, good.call(kReq)).status == 200);

    HttpRpcClient bad(ioc.get_executor(), cfg(srv, 2000ms, "python-urllib/3.12"));
    const auto r = run_sync(ioc, bad.call(kReq));
    CHECK(r.status == 403);
    Decoder dec;
    CHECK(classify(error_info_from_http(r, dec)).kind == FailKind::AuthFatal);
}

TEST_CASE("http client: QuickNode 413/-32615 body classifies as RangeTooLarge(5)", "[integration]") {
    net::io_context ioc;
    const auto body = fixture("err_quicknode_32615.json");
    MockHttpServer srv(ioc, [&](const MockRequest&) { return MockReply{.status = 413, .body = body}; });
    HttpRpcClient c(ioc.get_executor(), cfg(srv));

    const auto r = run_sync(ioc, c.call(kReq));
    CHECK(r.status == 413);
    Decoder dec;
    const auto k = classify(error_info_from_http(r, dec));
    CHECK(k.kind == FailKind::RangeTooLarge);
    CHECK(k.range_limit == 5u);
    CHECK(srv.connections() == 1);  // a provider error is not a connection failure
}

TEST_CASE("http client: 429 with Retry-After carries the hint", "[integration]") {
    net::io_context ioc;
    MockHttpServer srv(ioc, [](const MockRequest&) {
        return MockReply{.status = 429, .body = R"({"code":429,"message":"too many requests"})", .headers = {{"Retry-After", "3"}}};
    });
    HttpRpcClient c(ioc.get_executor(), cfg(srv));

    const auto r = run_sync(ioc, c.call(kReq));
    CHECK(r.status == 429);
    CHECK(r.retry_after == 3s);
    Decoder dec;
    const auto k = classify(error_info_from_http(r, dec));
    CHECK(k.kind == FailKind::RateLimited);
    CHECK(k.retry_after == 3s);
}

TEST_CASE("http client: Alchemy monthly quota body is QuotaExhausted", "[integration]") {
    net::io_context ioc;
    const auto body = fixture("err_alchemy_429.json");
    MockHttpServer srv(ioc, [&](const MockRequest&) { return MockReply{.status = 429, .body = body}; });
    HttpRpcClient c(ioc.get_executor(), cfg(srv));

    const auto r = run_sync(ioc, c.call(kReq));
    Decoder dec;
    CHECK(classify(error_info_from_http(r, dec)).kind == FailKind::QuotaExhausted);
}

TEST_CASE("http client: Connection: close is honoured (reconnects per request)", "[integration]") {
    net::io_context ioc;
    MockHttpServer srv(ioc, [](const MockRequest&) { return MockReply{.body = kOk, .connection_close = true}; });
    HttpRpcClient c(ioc.get_executor(), cfg(srv));

    for (int i = 0; i < 3; ++i) CHECK(run_sync(ioc, c.call(kReq)).status == 200);
    CHECK(srv.connections() == 3);
}

TEST_CASE("http client: connection killed while idle is replaced and the request retried once", "[integration]") {
    net::io_context ioc;
    int n = 0;
    MockHttpServer srv(ioc, [&](const MockRequest&) { return MockReply{.body = kOk, .close_silently = (++n == 1)}; });
    HttpRpcClient c(ioc.get_executor(), cfg(srv));

    CHECK(run_sync(ioc, c.call(kReq)).status == 200);  // server drops the socket after this reply
    const auto r = run_sync(ioc, c.call(kReq));        // reused dead connection -> reconnect + retry
    CHECK(r.status == 200);
    CHECK(srv.connections() == 2);
}

TEST_CASE("http client: unresponsive server -> timeout within the deadline, never retried, then recovers", "[integration]") {
    net::io_context ioc;
    bool hang = true;
    MockHttpServer srv(ioc, [&](const MockRequest&) { return MockReply{.body = kOk, .hang = hang}; });
    HttpRpcClient c(ioc.get_executor(), cfg(srv, 300ms));

    const auto t0 = std::chrono::steady_clock::now();
    try {
        run_sync(ioc, c.call(kReq));
        FAIL("expected TransportError");
    } catch (const TransportError& e) {
        CHECK(std::string(e.what()).find("timeout") != std::string::npos);
    }
    const auto dt = std::chrono::steady_clock::now() - t0;
    CHECK(dt >= 250ms);
    CHECK(dt < 1500ms);
    CHECK(srv.requests() == 1);  // exactly one attempt: timeouts are not retried

    hang = false;
    CHECK(run_sync(ioc, c.call(kReq)).status == 200);  // fresh connection after the timeout
}

TEST_CASE("http client: concurrent calls are serialised over one connection", "[integration]") {
    net::io_context ioc;
    MockHttpServer srv(ioc, [](const MockRequest&) { return MockReply{.body = kOk, .delay = 50ms}; });
    HttpRpcClient c(ioc.get_executor(), cfg(srv));

    int done = 0;
    std::vector<int> statuses;
    for (int i = 0; i < 4; ++i) {
        net::co_spawn(ioc, c.call(kReq), [&](std::exception_ptr e, HttpResult r) {
            REQUIRE_FALSE(e);
            statuses.push_back(r.status);
            if (++done == 4) ioc.stop();
        });
    }
    ioc.restart();
    ioc.run_for(10s);
    REQUIRE(done == 4);
    CHECK(statuses == std::vector<int>(4, 200));
    CHECK(srv.connections() == 1);
    CHECK(srv.requests() == 4);
}

TEST_CASE("http client: connection refused is a TransportError", "[integration]") {
    net::io_context ioc;
    HttpRpcClient c(ioc.get_executor(), HttpRpcClientConfig{"mock", "http://127.0.0.1:1/", kUa, 1000ms});
    CHECK_THROWS_AS(run_sync(ioc, c.call(kReq)), TransportError);
}

TEST_CASE("http client: invalid URL schemes are rejected at construction", "[integration]") {
    net::io_context ioc;
    CHECK_THROWS_AS(HttpRpcClient(ioc.get_executor(), HttpRpcClientConfig{"x", "ftp://h/", kUa, 1000ms}), TransportError);
    CHECK_THROWS_AS(HttpRpcClient(ioc.get_executor(), HttpRpcClientConfig{"x", "wss://h/", kUa, 1000ms}), TransportError);
}

TEST_CASE("parse_url: host header and kinds") {
    const auto a = parse_url("https://rpc.example.com/v2/KEY", UrlKind::Http);
    CHECK(a.tls);
    CHECK(a.host_header() == "rpc.example.com");
    CHECK(a.target == "/v2/KEY");
    const auto b = parse_url("http://127.0.0.1:8080", UrlKind::Http);
    CHECK_FALSE(b.tls);
    CHECK(b.host_header() == "127.0.0.1:8080");
    CHECK(b.target == "/");
    const auto w = parse_url("wss://node.example/ws/v3/KEY", UrlKind::Ws);
    CHECK(w.tls);
    CHECK(w.port == "443");
    CHECK_THROWS_AS(parse_url("ws://h/", UrlKind::Http), TransportError);
    CHECK_THROWS_AS(parse_url("https://u:p@h/", UrlKind::Http), TransportError);
}
