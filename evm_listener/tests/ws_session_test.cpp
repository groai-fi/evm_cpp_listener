#include <catch2/catch_test_macros.hpp>

#include <fstream>
#include <sstream>

#include "core/log.hpp"
#include "mock_ws_server.hpp"
#include "rpc/blocking_http.hpp"
#include "rpc/ws_session.hpp"
#include "test_util.hpp"

using namespace evml::rpc;
using namespace evml::test;
using namespace std::chrono_literals;
namespace net = boost::asio;

namespace {

constexpr const char* kUa = "evm-listener/0.1.0-test";
// Subscription ids baked into the captured notification fixtures.
constexpr const char* kHeadsSub = "0x9ce59a13059e417087c02d3236a0b1cc";
constexpr const char* kLogsSub = "0x4a8a4c0d8a3b4e5f9d7c1b2a3e4f5a6b";
constexpr const char* kPoolAddr = "0xe846373c1a92b167b4e9cd5d8e4d6b1db9e90ec7";
constexpr const char* kSwapTopic = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

std::string fixture(const char* name) {
    std::ifstream f(std::string(EVML_FIXTURE_DIR) + "/" + name);
    REQUIRE(f.good());
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

std::string reply(std::uint64_t id, const std::string& result_json) {
    return R"({"jsonrpc":"2.0","id":)" + std::to_string(id) + R"(,"result":)" + result_json + "}";
}

evml::core::LogFilter pool_filter() {
    evml::core::LogFilter f;
    f.addresses.push_back(evml::core::Address20::from_hex(kPoolAddr));
    f.topic0s.push_back(evml::core::Hash32::from_hex(kSwapTopic));
    return f;
}

struct Rec {
    int ready{0};
    std::vector<Head> heads;
    std::vector<Log> logs;
    std::vector<WsClosed> closed;
};

WsHandlers handlers(Rec& r) {
    WsHandlers h;
    h.on_ready = [&r] { ++r.ready; };
    h.on_head = [&r](const Head& x) { r.heads.push_back(x); };
    h.on_log = [&r](const Log& x) { r.logs.push_back(x); };
    h.on_closed = [&r](const WsClosed& c) { r.closed.push_back(c); };
    return h;
}

WsSessionConfig cfg(const MockWsServer& s) {
    WsSessionConfig c;
    c.name = "mock";
    c.url = s.url();
    c.user_agent = kUa;
    c.expected_chain_id = 8453;
    c.subscribe_heads = true;
    c.logs = pool_filter();
    c.connect_timeout = 2000ms;
    c.handshake_timeout = 2000ms;
    c.request_timeout = 2000ms;
    c.subscribe_timeout = 2000ms;
    c.idle_timeout = 5000ms;
    return c;
}

using Seen = std::vector<MockWsConn::Req>;

// chainId -> ACK(newHeads) -> ACK(logs), recording the requests the client sent.
net::awaitable<void> serve_setup(MockWsConn& c, Seen* seen, const std::string& chain_hex = "0x2105") {
    auto r1 = co_await c.read_req();
    if (seen) seen->push_back(r1);
    co_await c.send(reply(r1.id, "\"" + chain_hex + "\""));
    auto r2 = co_await c.read_req();
    if (seen) seen->push_back(r2);
    co_await c.send(reply(r2.id, std::string("\"") + kHeadsSub + "\""));
    auto r3 = co_await c.read_req();
    if (seen) seen->push_back(r3);
    co_await c.send(reply(r3.id, std::string("\"") + kLogsSub + "\""));
}

}  // namespace

TEST_CASE("ws session: clean connect, ACKs, heads and logs flow, stop() is silent", "[integration]") {
    net::io_context ioc;
    Rec rec;
    Seen seen;
    std::string server_ua;
    MockWsServer srv(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        server_ua = c.user_agent();
        co_await serve_setup(c, &seen);
        co_await c.send(fixture("ws_new_head.json"));
        co_await c.send(fixture("ws_log.json"));
        co_await c.sleep(30s);
    });
    auto s = WsSession::create(ioc.get_executor(), cfg(srv), handlers(rec));
    s->start();

    REQUIRE(run_until(ioc, [&] { return rec.heads.size() == 1 && rec.logs.size() == 1; }, 5s));
    CHECK(rec.ready == 1);
    CHECK(rec.heads[0].number == 0x31cbb7fULL);
    CHECK(rec.logs[0].address.to_hex() == kPoolAddr);
    CHECK(rec.logs[0].log_index == 0x107ULL);
    CHECK_FALSE(rec.logs[0].removed);
    CHECK(server_ua == kUa);

    REQUIRE(seen.size() == 3);
    CHECK(seen[0].method == "eth_chainId");
    CHECK(seen[1].method == "eth_subscribe");
    CHECK(seen[1].params.as_array().at(0).as_string() == "newHeads");
    CHECK(seen[2].params.as_array().at(0).as_string() == "logs");
    const auto& f = seen[2].params.as_array().at(1).as_object();
    CHECK(f.at("address").as_array().at(0).as_string() == kPoolAddr);
    CHECK(f.at("topics").as_array().at(0).as_array().at(0).as_string() == kSwapTopic);

    s->stop();
    run_until(ioc, [] { return false; }, 150ms);  // let the aborted read unwind
    CHECK(rec.closed.empty());                    // stop() never produces on_closed
    CHECK(s->closed());
    s->stop();                                    // idempotent
}

TEST_CASE("ws session: server drops the TCP connection mid-stream -> Retry", "[integration]") {
    net::io_context ioc;
    Rec rec;
    MockWsServer srv(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c, nullptr);
        co_await c.send(fixture("ws_new_head.json"));
        c.drop();
    });
    auto s = WsSession::create(ioc.get_executor(), cfg(srv), handlers(rec));
    s->start();

    REQUIRE(run_until(ioc, [&] { return !rec.closed.empty(); }, 5s));
    CHECK(rec.ready == 1);
    CHECK(rec.heads.size() == 1);
    REQUIRE(rec.closed.size() == 1);
    CHECK(rec.closed[0].kind == FailKind::Retry);
    CHECK_FALSE(rec.closed[0].reason.empty());
    CHECK(s->closed());
}

TEST_CASE("ws session: server close frame -> Retry naming the close", "[integration]") {
    net::io_context ioc;
    Rec rec;
    MockWsServer srv(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c, nullptr);
        co_await c.close();
    });
    auto s = WsSession::create(ioc.get_executor(), cfg(srv), handlers(rec));
    s->start();

    REQUIRE(run_until(ioc, [&] { return !rec.closed.empty(); }, 5s));
    CHECK(rec.closed[0].kind == FailKind::Retry);
    CHECK(rec.closed[0].reason.find("server closed") != std::string::npos);
}

TEST_CASE("ws session: half-open peer (no frames, no pongs) is detected by the idle timeout", "[integration]") {
    net::io_context ioc;
    Rec rec;
    MockWsServer srv(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c, nullptr);
        co_await c.go_silent();
    });
    auto conf = cfg(srv);
    conf.idle_timeout = 600ms;
    auto s = WsSession::create(ioc.get_executor(), conf, handlers(rec));
    s->start();

    const auto t0 = std::chrono::steady_clock::now();
    REQUIRE(run_until(ioc, [&] { return !rec.closed.empty(); }, 5s));
    const auto dt = std::chrono::steady_clock::now() - t0;
    CHECK(rec.ready == 1);
    CHECK(rec.closed[0].kind == FailKind::Retry);
    CHECK(rec.closed[0].reason.find("idle timeout") != std::string::npos);
    CHECK(dt >= 500ms);
    CHECK(dt < 3s);
    CHECK(s->live_for() >= 400ms);  // it was Ready for most of that time (feeds Backoff::report_live)
}

TEST_CASE("ws session: subscription ACK never arrives -> subscribe timeout", "[integration]") {
    net::io_context ioc;
    Rec rec;
    MockWsServer srv(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        auto r1 = co_await c.read_req();
        co_await c.send(reply(r1.id, "\"0x2105\""));
        co_await c.read_req();  // eth_subscribe newHeads: never answered
        co_await c.go_silent();
    });
    auto conf = cfg(srv);
    conf.subscribe_timeout = 400ms;
    auto s = WsSession::create(ioc.get_executor(), conf, handlers(rec));
    s->start();

    const auto t0 = std::chrono::steady_clock::now();
    REQUIRE(run_until(ioc, [&] { return !rec.closed.empty(); }, 5s));
    const auto dt = std::chrono::steady_clock::now() - t0;
    CHECK(rec.ready == 0);
    CHECK(rec.closed[0].kind == FailKind::Retry);
    CHECK(rec.closed[0].reason.find("timeout waiting for subscribe ACK (newHeads)") != std::string::npos);
    CHECK(dt >= 350ms);
    CHECK(dt < 2s);
    CHECK(s->live_for() == 0ms);
}

TEST_CASE("ws session: wrong chain id -> ConfigFatal", "[integration]") {
    net::io_context ioc;
    Rec rec;
    MockWsServer srv(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        auto r1 = co_await c.read_req();
        co_await c.send(reply(r1.id, "\"0x1\""));
        co_await c.go_silent();
    });
    auto s = WsSession::create(ioc.get_executor(), cfg(srv), handlers(rec));
    s->start();

    REQUIRE(run_until(ioc, [&] { return !rec.closed.empty(); }, 5s));
    CHECK(rec.ready == 0);
    CHECK(rec.closed[0].kind == FailKind::ConfigFatal);
    CHECK(rec.closed[0].reason.find("chain id mismatch") != std::string::npos);
}

TEST_CASE("ws session: upgrade rejections are classified", "[integration]") {
    const auto run = [](MockWsOptions opts) {
        net::io_context ioc;
        Rec rec;
        MockWsServer srv(ioc, [](MockWsConn&) -> net::awaitable<void> { co_return; }, std::move(opts));
        auto s = WsSession::create(ioc.get_executor(), cfg(srv), handlers(rec));
        s->start();
        REQUIRE(run_until(ioc, [&] { return !rec.closed.empty(); }, 5s));
        CHECK(rec.ready == 0);
        return rec.closed[0];
    };

    SECTION("401 -> AuthFatal") {
        const auto c = run(MockWsOptions{.reject_status = 401, .reject_body = "invalid project id"});
        CHECK(c.kind == FailKind::AuthFatal);
        CHECK(c.reason.find("HTTP 401") != std::string::npos);
    }
    SECTION("429 + Retry-After: 3 -> RateLimited carrying the hint") {
        const auto c = run(MockWsOptions{.reject_status = 429, .reject_body = "too many requests",
                                         .reject_headers = {{"Retry-After", "3"}}});
        CHECK(c.kind == FailKind::RateLimited);
        CHECK(c.retry_after == 3s);
    }
    SECTION("500 -> Retry") {
        const auto c = run(MockWsOptions{.reject_status = 500, .reject_body = "oops"});
        CHECK(c.kind == FailKind::Retry);
    }
}

TEST_CASE("ws session: provider quota error on subscribe -> QuotaExhausted", "[integration]") {
    net::io_context ioc;
    Rec rec;
    MockWsServer srv(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        auto r1 = co_await c.read_req();
        co_await c.send(reply(r1.id, "\"0x2105\""));
        auto r2 = co_await c.read_req();
        co_await c.send(R"({"jsonrpc":"2.0","id":)" + std::to_string(r2.id) +
                        R"(,"error":{"code":-32005,"message":"Monthly capacity limit exceeded."}})");
        co_await c.go_silent();
    });
    auto s = WsSession::create(ioc.get_executor(), cfg(srv), handlers(rec));
    s->start();

    REQUIRE(run_until(ioc, [&] { return !rec.closed.empty(); }, 5s));
    CHECK(rec.closed[0].kind == FailKind::QuotaExhausted);
}

TEST_CASE("ws session: unknown subscription ids are dropped and counted, not delivered", "[integration]") {
    evml::core::log::init("info");  // the session WARN-logs dropped notifications; log::warn requires init()
    net::io_context ioc;
    Rec rec;
    MockWsServer srv(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c, nullptr);
        auto stray = fixture("ws_new_head.json");
        stray.replace(stray.find(kHeadsSub), std::string(kHeadsSub).size(), "0xdeadbeefdeadbeefdeadbeefdeadbeef");
        co_await c.send(stray);
        co_await c.send(fixture("ws_new_head.json"));
        co_await c.sleep(30s);
    });
    auto s = WsSession::create(ioc.get_executor(), cfg(srv), handlers(rec));
    s->start();

    const bool got = run_until(ioc, [&] { return rec.heads.size() == 1; }, 5s);
    INFO("ready=" << rec.ready << " closed=" << rec.closed.size() << " reason="
                  << (rec.closed.empty() ? "<none>" : rec.closed[0].reason)
                  << " unknown=" << s->unknown_subscription_msgs());
    REQUIRE(got);
    CHECK(s->unknown_subscription_msgs() == 1);
    s->stop();
}

TEST_CASE("ws session: malformed frame ends the session with Retry", "[integration]") {
    net::io_context ioc;
    Rec rec;
    MockWsServer srv(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c, nullptr);
        co_await c.send("this is not json");
        co_await c.sleep(30s);
    });
    auto s = WsSession::create(ioc.get_executor(), cfg(srv), handlers(rec));
    s->start();

    REQUIRE(run_until(ioc, [&] { return !rec.closed.empty(); }, 5s));
    CHECK(rec.ready == 1);
    CHECK(rec.closed[0].kind == FailKind::Retry);
    CHECK_FALSE(rec.closed[0].reason.empty());
}

TEST_CASE("ws session: connection refused -> Retry", "[integration]") {
    net::io_context ioc;
    Rec rec;
    WsSessionConfig c;
    c.name = "mock";
    c.url = "ws://127.0.0.1:1/";
    c.user_agent = kUa;
    c.expected_chain_id = 8453;
    c.connect_timeout = 1000ms;
    auto s = WsSession::create(ioc.get_executor(), c, handlers(rec));
    s->start();

    REQUIRE(run_until(ioc, [&] { return !rec.closed.empty(); }, 5s));
    CHECK(rec.closed[0].kind == FailKind::Retry);
    CHECK(rec.closed[0].reason.find("connect failed") != std::string::npos);
}

TEST_CASE("ws session: invalid configuration is rejected at create()", "[integration]") {
    net::io_context ioc;
    Rec rec;
    WsSessionConfig base;
    base.name = "x";
    base.url = "ws://127.0.0.1:1/";
    base.user_agent = kUa;
    base.expected_chain_id = 8453;

    auto none = base;
    none.subscribe_heads = false;
    CHECK_THROWS_AS(WsSession::create(ioc.get_executor(), none, handlers(rec)), std::invalid_argument);

    auto empty_filter = base;
    empty_filter.logs = evml::core::LogFilter{};
    CHECK_THROWS_AS(WsSession::create(ioc.get_executor(), empty_filter, handlers(rec)), std::invalid_argument);

    auto no_closed = handlers(rec);
    no_closed.on_closed = nullptr;
    CHECK_THROWS_AS(WsSession::create(ioc.get_executor(), base, no_closed), std::invalid_argument);

    auto bad_url = base;
    bad_url.url = "https://example.com/";
    CHECK_THROWS_AS(WsSession::create(ioc.get_executor(), bad_url, handlers(rec)), TransportError);
}
