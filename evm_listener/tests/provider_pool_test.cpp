#include <catch2/catch_test_macros.hpp>

#include <fstream>
#include <sstream>

#include "core/hex.hpp"
#include "mock_http_server.hpp"
#include "mock_ws_server.hpp"
#include "provider/provider_pool.hpp"
#include "test_util.hpp"

using namespace evml::provider;
using namespace evml::test;
using namespace std::chrono_literals;
namespace net = boost::asio;

namespace {

constexpr const char* kUa = "evm-listener/0.1.0-test";
constexpr const char* kHeadsSub = "0x9ce59a13059e417087c02d3236a0b1cc";
constexpr const char* kLogsSub = "0x4a8a4c0d8a3b4e5f9d7c1b2a3e4f5a6b";
constexpr const char* kPoolAddr = "0xe846373c1a92b167b4e9cd5d8e4d6b1db9e90ec7";
constexpr const char* kSwapTopic = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
constexpr std::uint64_t kHead = 0x31cbb7f;  // block number inside fixtures/ws_new_head.json
constexpr const char* kDeadWs = "ws://127.0.0.1:9/";  // validated but never connected to (pool not started)

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

std::string rpc_error(std::uint64_t id, int code, const std::string& message) {
    return R"({"jsonrpc":"2.0","id":)" + std::to_string(id) + R"(,"error":{"code":)" + std::to_string(code) +
           R"(,"message":")" + message + R"("}})";
}

std::string block_json(std::uint64_t n) {
    return R"({"number":")" + evml::core::to_hex_u64(n) + R"(","hash":"0x)" + std::string(64, 'a') +
           R"(","parentHash":"0x)" + std::string(64, 'b') + R"(","timestamp":"0x1"})";
}

evml::core::LogFilter pool_filter() {
    evml::core::LogFilter f;
    f.addresses.push_back(evml::core::Address20::from_hex(kPoolAddr));
    f.topic0s.push_back(evml::core::Hash32::from_hex(kSwapTopic));
    return f;
}

// chainId -> ACK(newHeads) -> ACK(logs)
net::awaitable<void> serve_setup(MockWsConn& c, const std::string& chain_hex = "0x2105") {
    auto r1 = co_await c.read_req();
    co_await c.send(reply(r1.id, "\"" + chain_hex + "\""));
    auto r2 = co_await c.read_req();
    co_await c.send(reply(r2.id, std::string("\"") + kHeadsSub + "\""));
    auto r3 = co_await c.read_req();
    co_await c.send(reply(r3.id, std::string("\"") + kLogsSub + "\""));
}

struct Rpc {
    std::string method;
    std::uint64_t id{};
    boost::json::array params;
};

Rpc parse_req(const MockRequest& r) {
    const auto v = boost::json::parse(r.body).as_object();
    return {std::string(v.at("method").as_string()), static_cast<std::uint64_t>(v.at("id").as_int64()),
            v.at("params").as_array()};
}

MockReply ok_reply(std::uint64_t id, const std::string& result_json) { return MockReply{.body = reply(id, result_json)}; }

// What a healthy node answers; unknown methods fail loudly (HTTP 500) instead of being papered over.
struct Chain {
    std::uint64_t head{kHead};
    std::uint64_t safe{kHead - 100};
    std::uint64_t finalized{kHead - 200};
};

MockHttpServer::Handler healthy(Chain& ch) {
    return [&ch](const MockRequest& r) {
        const auto q = parse_req(r);
        if (q.method == "eth_blockNumber") return ok_reply(q.id, "\"" + evml::core::to_hex_u64(ch.head) + "\"");
        if (q.method == "eth_getBlockByNumber") {
            const auto tag = std::string(q.params.at(0).as_string());
            const auto n = tag == "safe" ? ch.safe : tag == "finalized" ? ch.finalized : ch.head;
            return ok_reply(q.id, block_json(n));
        }
        if (q.method == "eth_getLogs") return ok_reply(q.id, "[]");
        return MockReply{.status = 500, .body = "unexpected method " + q.method};
    };
}

PoolConfig base_cfg(std::vector<ProviderEndpoint> eps, std::vector<std::string> ws_order,
                    std::vector<std::string> getlogs_order) {
    PoolConfig c;
    c.providers = std::move(eps);
    c.ws_order = std::move(ws_order);
    c.getlogs_order = std::move(getlogs_order);
    c.chain_id = 8453;
    c.user_agent = kUa;
    c.subscribe_heads = true;
    c.logs = pool_filter();
    c.connect_timeout = c.handshake_timeout = c.request_timeout = c.subscribe_timeout = 2000ms;
    c.idle_timeout = 5000ms;
    c.http_timeout = 2000ms;
    c.policy.failover_after = 3;
    c.policy.probe_interval = 3600s;  // Down providers stay Down unless a test wants the probe
    c.policy.prefer_primary_after = 3600s;
    c.policy.min_voluntary_gap = 0s;
    c.backoff_base = 5ms;
    c.backoff_cap = 20ms;
    c.backoff_min_live = 10s;  // a session never counts as "live long enough" inside a test
    c.tick = 20ms;
    c.standby_probe_period = 3600s;
    c.safe_poll = 3600s;
    c.finalized_poll = 3600s;
    c.crosscheck_period = 3600s;
    return c;
}

struct Rec {
    std::vector<std::pair<std::uint64_t, std::string>> heads;
    std::vector<std::string> transitions;  // "A:up->down (reason)"
    std::vector<std::string> active;       // history of the active WS provider ("-" = none)
    std::vector<std::string> down_reasons;
};

PoolHandlers handlers(Rec& r) {
    PoolHandlers h;
    h.on_head = [&r](const evml::rpc::Head& x, const std::string& p) { r.heads.emplace_back(x.number, p); };
    h.on_transition = [&r](const Transition& t, std::string_view why) {
        r.transitions.push_back(t.provider + ":" + to_string(t.from) + "->" + to_string(t.to) + " (" + std::string(why) + ")");
    };
    h.on_ws_active_changed = [&r](const std::optional<std::string>& p, std::string_view) { r.active.push_back(p ? *p : "-"); };
    h.on_ws_down = [&r](const evml::rpc::WsClosed& c, const std::string&) { r.down_reasons.push_back(c.reason); };
    return h;
}

void shutdown(net::io_context& ioc, const std::shared_ptr<ProviderPool>& p) {
    p->stop();
    run_until(ioc, [] { return false; }, 150ms);  // let aborted I/O unwind
}

RequestBuilder get_logs_request() {
    return [](std::uint64_t id) { return evml::rpc::build_get_logs(id, 100, 104, pool_filter()); };
}

bool contains(const std::vector<std::string>& v, std::string_view needle) {
    for (const auto& s : v) {
        if (s.find(needle) != std::string::npos) return true;
    }
    return false;
}

}  // namespace

// ---------------------------------------------------------------------------------------------------------------
// Config validation (pure unit: nothing connects)
// ---------------------------------------------------------------------------------------------------------------

TEST_CASE("pool: create() rejects inconsistent configs") {
    net::io_context ioc;
    const std::vector<ProviderEndpoint> eps{{"A", "ws://127.0.0.1:9/", "http://127.0.0.1:9/"},
                                            {"B", "", "http://127.0.0.1:9/"}};
    auto make = [&](PoolConfig c) { return ProviderPool::create(ioc.get_executor(), std::move(c), {}); };

    CHECK_NOTHROW(make(base_cfg(eps, {"A"}, {"B", "A"})));
    CHECK_THROWS_AS(make(base_cfg({}, {"A"}, {"A"})), std::invalid_argument);
    CHECK_THROWS_AS(make(base_cfg(eps, {}, {"A"})), std::invalid_argument);
    CHECK_THROWS_AS(make(base_cfg(eps, {"A"}, {})), std::invalid_argument);
    CHECK_THROWS_AS(make(base_cfg(eps, {"Z"}, {"A"})), std::invalid_argument);   // unknown name
    CHECK_THROWS_AS(make(base_cfg(eps, {"B"}, {"A"})), std::invalid_argument);   // B has no ws_url
    CHECK_THROWS_AS(make(base_cfg(eps, {"A"}, {"Z"})), std::invalid_argument);
    CHECK_THROWS_AS(make(base_cfg({eps[0], eps[0]}, {"A"}, {"A"})), std::invalid_argument);  // duplicate
    CHECK_THROWS_AS(make(base_cfg({{"A", "ws://127.0.0.1:9/", ""}}, {"A"}, {"A"})), std::invalid_argument);  // no http_url

    auto no_chain = base_cfg(eps, {"A"}, {"A"});
    no_chain.chain_id = 0;
    CHECK_THROWS_AS(make(no_chain), std::invalid_argument);

    auto no_sub = base_cfg(eps, {"A"}, {"A"});
    no_sub.subscribe_heads = false;
    no_sub.logs.reset();
    CHECK_THROWS_AS(make(no_sub), std::invalid_argument);

    auto empty_filter = base_cfg(eps, {"A"}, {"A"});
    empty_filter.logs = evml::core::LogFilter{};
    CHECK_THROWS_AS(make(empty_filter), std::invalid_argument);

    auto p = make(base_cfg(eps, {"A"}, {"A"}));
    p->stop();  // never started: stop() is harmless
}

// ---------------------------------------------------------------------------------------------------------------
// WebSocket supervisor
// ---------------------------------------------------------------------------------------------------------------

TEST_CASE("pool: three session failures on A fail over to B and heads flow", "[integration]") {
    net::io_context ioc;
    int a_conns = 0;
    Chain chain;
    MockWsServer wsA(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        ++a_conns;
        co_await serve_setup(c);
        c.drop();
    });
    MockWsServer wsB(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c);
        co_await c.send(fixture("ws_new_head.json"));
        co_await c.sleep(30s);
    });
    MockHttpServer httpA(ioc, healthy(chain)), httpB(ioc, healthy(chain));

    Rec rec;
    auto pool = ProviderPool::create(
        ioc.get_executor(),
        base_cfg({{"A", wsA.url(), httpA.url()}, {"B", wsB.url(), httpB.url()}}, {"A", "B"}, {"B", "A"}), handlers(rec));
    pool->start();

    REQUIRE(run_until(ioc, [&] { return pool->active_ws() == "B" && !rec.heads.empty(); }, 8s));
    CHECK(a_conns >= 3);
    CHECK(pool->state("A") == State::Down);
    CHECK(rec.heads[0].first == kHead);
    CHECK(rec.heads[0].second == "B");
    CHECK(rec.active == std::vector<std::string>{"A", "B"});
    CHECK(contains(rec.transitions, "A:up->down"));
    shutdown(ioc, pool);
}

TEST_CASE("pool: quota error on subscribe fails over immediately and A is not retried", "[integration]") {
    net::io_context ioc;
    int a_conns = 0;
    Chain chain;
    MockWsServer wsA(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        ++a_conns;
        auto r1 = co_await c.read_req();
        co_await c.send(reply(r1.id, "\"0x2105\""));
        auto r2 = co_await c.read_req();
        co_await c.send(rpc_error(r2.id, -32005, "Monthly capacity limit exceeded."));
        co_await c.sleep(30s);
    });
    MockWsServer wsB(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c);
        co_await c.sleep(30s);
    });
    MockHttpServer httpA(ioc, healthy(chain)), httpB(ioc, healthy(chain));

    Rec rec;
    auto pool = ProviderPool::create(
        ioc.get_executor(),
        base_cfg({{"A", wsA.url(), httpA.url()}, {"B", wsB.url(), httpB.url()}}, {"A", "B"}, {"B", "A"}), handlers(rec));
    pool->start();

    REQUIRE(run_until(ioc, [&] { return pool->active_ws() == "B" && wsB.upgrades() == 1; }, 5s));
    run_until(ioc, [] { return false; }, 300ms);
    CHECK(a_conns == 1);
    REQUIRE(run_until(ioc, [&] { return pool->state("A") == State::Cooldown; }, 5s));
    CHECK(pool->state("A") == State::Cooldown);
    CHECK(contains(rec.transitions, "A:up->cooldown"));
    shutdown(ioc, pool);
}

TEST_CASE("pool: wrong chain on A makes A Fatal and B becomes active", "[integration]") {
    net::io_context ioc;
    Chain chain;
    MockWsServer wsA(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c, "0x1");
        co_await c.sleep(30s);
    });
    MockWsServer wsB(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c);
        co_await c.sleep(30s);
    });
    MockHttpServer httpA(ioc, healthy(chain)), httpB(ioc, healthy(chain));

    Rec rec;
    auto pool = ProviderPool::create(
        ioc.get_executor(),
        base_cfg({{"A", wsA.url(), httpA.url()}, {"B", wsB.url(), httpB.url()}}, {"A", "B"}, {"B", "A"}), handlers(rec));
    pool->start();

    REQUIRE(run_until(ioc, [&] { return pool->active_ws() == "B"; }, 5s));
    CHECK(pool->state("A") == State::Fatal);
    CHECK(wsA.connections() == 1);
    shutdown(ioc, pool);
}

TEST_CASE("pool: A recovers (probe) and takes over again after prefer_primary_after", "[integration]") {
    net::io_context ioc;
    int a_conns = 0;
    Chain chain;
    MockWsServer wsA(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        const bool bad = ++a_conns <= 3;
        co_await serve_setup(c);
        if (bad) {
            c.drop();
            co_return;
        }
        co_await c.send(fixture("ws_new_head.json"));
        co_await c.sleep(30s);
    });
    MockWsServer wsB(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c);
        co_await c.sleep(30s);
    });
    MockHttpServer httpA(ioc, healthy(chain)), httpB(ioc, healthy(chain));

    auto cfg = base_cfg({{"A", wsA.url(), httpA.url()}, {"B", wsB.url(), httpB.url()}}, {"A", "B"}, {"B", "A"});
    cfg.policy.probe_interval = 0s;
    cfg.policy.prefer_primary_after = 1s;
    Rec rec;
    auto pool = ProviderPool::create(ioc.get_executor(), std::move(cfg), handlers(rec));
    pool->start();

    REQUIRE(run_until(ioc, [&] { return rec.active.size() == 3 && !rec.heads.empty(); }, 10s));
    CHECK(rec.active == std::vector<std::string>{"A", "B", "A"});
    CHECK(pool->active_ws() == "A");
    CHECK(pool->state("A") == State::Up);
    CHECK(rec.heads.back().second == "A");
    CHECK(a_conns == 4);
    shutdown(ioc, pool);
}

TEST_CASE("pool: a stalled newHeads stream restarts the session", "[integration]") {
    net::io_context ioc;
    Chain chain;
    MockWsServer wsA(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c);  // ready, then silence
        co_await c.sleep(30s);
    });
    MockWsServer wsB(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c);
        co_await c.sleep(30s);
    });
    MockHttpServer httpA(ioc, healthy(chain)), httpB(ioc, healthy(chain));

    auto cfg = base_cfg({{"A", wsA.url(), httpA.url()}, {"B", wsB.url(), httpB.url()}}, {"A", "B"}, {"B", "A"});
    cfg.stale.head_stall = 1s;
    Rec rec;
    auto pool = ProviderPool::create(ioc.get_executor(), std::move(cfg), handlers(rec));
    pool->start();

    REQUIRE(run_until(ioc, [&] { return wsA.connections() >= 2; }, 6s));
    CHECK(contains(rec.down_reasons, "stalled"));
    CHECK(pool->active_ws() == "A");  // one stall is a single failure, not yet a failover
    shutdown(ioc, pool);
}

TEST_CASE("pool: WS head lagging the getLogs provider's head fails over to the other WS provider", "[integration]") {
    net::io_context ioc;
    Chain chain_a, chain_b;
    chain_b.head = kHead + 20;  // the getLogs provider (B while A is active) is 20 blocks ahead of A's stream
    MockWsServer wsA(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c);
        co_await c.send(fixture("ws_new_head.json"));
        co_await c.sleep(30s);
    });
    MockWsServer wsB(ioc, [&](MockWsConn& c) -> net::awaitable<void> {
        co_await serve_setup(c);
        co_await c.send(fixture("ws_new_head.json"));
        co_await c.sleep(30s);
    });
    MockHttpServer httpA(ioc, healthy(chain_a)), httpB(ioc, healthy(chain_b));

    auto cfg = base_cfg({{"A", wsA.url(), httpA.url()}, {"B", wsB.url(), httpB.url()}}, {"A", "B"}, {"B", "A"});
    cfg.stale.ws_lag_blocks = 5;
    cfg.stale.ws_lag_for = 1s;
    Rec rec;
    auto pool = ProviderPool::create(ioc.get_executor(), std::move(cfg), handlers(rec));
    pool->start();

    REQUIRE(run_until(ioc, [&] { return pool->active_ws() == "B"; }, 8s));
    REQUIRE_FALSE(rec.transitions.empty());
    CHECK(rec.transitions[0].find("A:up->down") == 0);
    CHECK(rec.transitions[0].find("lagging") != std::string::npos);
    shutdown(ioc, pool);
}

// ---------------------------------------------------------------------------------------------------------------
// HTTP failover (pool not started: no WS, no background traffic)
// ---------------------------------------------------------------------------------------------------------------

TEST_CASE("pool: call() falls through a quota-exhausted provider to the next", "[integration]") {
    net::io_context ioc;
    Chain chain;
    MockHttpServer h1(ioc, [](const MockRequest& r) {
        const auto q = parse_req(r);
        return MockReply{.status = 429, .body = rpc_error(q.id, 429, "Monthly capacity limit exceeded.")};
    });
    MockHttpServer h2(ioc, healthy(chain));
    auto pool = ProviderPool::create(
        ioc.get_executor(),
        base_cfg({{"H1", kDeadWs, h1.url()}, {"H2", kDeadWs, h2.url()}}, {"H1"}, {"H1", "H2"}), {});

    const auto o = run_sync(ioc, pool->call(get_logs_request()));
    CHECK(o.ok);
    CHECK(o.provider == "H2");
    CHECK(o.result_raw == "[]");
    CHECK(pool->state("H1") == State::Cooldown);
    CHECK(h1.requests() == 1);
    CHECK(h2.requests() == 1);

    // H1 is now cooling down: the next request goes straight to H2.
    const auto o2 = run_sync(ioc, pool->call(get_logs_request()));
    CHECK(o2.ok);
    CHECK(h1.requests() == 1);
    CHECK(h2.requests() == 2);
    shutdown(ioc, pool);
}

TEST_CASE("pool: RangeTooLarge returns immediately and does not penalise or skip to another provider",
          "[integration]") {
    net::io_context ioc;
    Chain chain;
    MockHttpServer h1(ioc, [](const MockRequest& r) {
        const auto q = parse_req(r);
        return MockReply{.status = 413, .body = rpc_error(q.id, -32615, "eth_getLogs is limited to a 5 range")};
    });
    MockHttpServer h2(ioc, healthy(chain));
    auto pool = ProviderPool::create(
        ioc.get_executor(),
        base_cfg({{"H1", kDeadWs, h1.url()}, {"H2", kDeadWs, h2.url()}}, {"H1"}, {"H1", "H2"}), {});

    const auto o = run_sync(ioc, pool->call(get_logs_request()));
    CHECK_FALSE(o.ok);
    CHECK(o.kind == evml::rpc::FailKind::RangeTooLarge);
    CHECK(o.range_limit == 5u);
    CHECK(o.provider == "H1");
    CHECK(h2.requests() == 0);
    CHECK(pool->state("H1") == State::Up);
    shutdown(ioc, pool);
}

TEST_CASE("pool: call() returns the last error when every provider fails", "[integration]") {
    net::io_context ioc;
    MockHttpServer h1(ioc, [](const MockRequest&) { return MockReply{.status = 500, .body = "boom-1"}; });
    MockHttpServer h2(ioc, [](const MockRequest&) { return MockReply{.status = 500, .body = "boom-2"}; });
    auto pool = ProviderPool::create(
        ioc.get_executor(),
        base_cfg({{"H1", kDeadWs, h1.url()}, {"H2", kDeadWs, h2.url()}}, {"H1"}, {"H1", "H2"}), {});

    const auto o = run_sync(ioc, pool->call(get_logs_request()));
    CHECK_FALSE(o.ok);
    CHECK(o.kind == evml::rpc::FailKind::Retry);
    CHECK(o.provider == "H2");
    CHECK(o.error.find("boom-2") != std::string::npos);
    CHECK(h1.requests() == 1);
    CHECK(h2.requests() == 1);
    shutdown(ioc, pool);
}

TEST_CASE("pool: tags are cached per answering provider and never mixed after a failover", "[integration]") {
    net::io_context ioc;
    Chain chain_a, chain_b;
    chain_a.safe = 100;
    chain_b.safe = 200;
    bool b_exhausted = false;
    MockHttpServer ha(ioc, healthy(chain_a));
    auto serve_b = healthy(chain_b);
    MockHttpServer hb(ioc, [&](const MockRequest& r) {
        if (b_exhausted) {
            const auto q = parse_req(r);
            return MockReply{.status = 429, .body = rpc_error(q.id, 429, "Monthly capacity limit exceeded.")};
        }
        return serve_b(r);
    });
    auto pool = ProviderPool::create(
        ioc.get_executor(),
        base_cfg({{"A", kDeadWs, ha.url()}, {"B", kDeadWs, hb.url()}}, {"A"}, {"B", "A"}), {});

    const auto s1 = run_sync(ioc, pool->read_tag("B", Tag::Safe));
    REQUIRE(s1.has_value());
    CHECK(s1->provider == "B");
    CHECK(s1->number == 200);

    b_exhausted = true;
    const auto o = run_sync(ioc, pool->call(get_logs_request()));
    REQUIRE(o.ok);
    CHECK(o.provider == "A");  // failed over

    const auto s2 = run_sync(ioc, pool->read_tag(o.provider, Tag::Safe));
    REQUIRE(s2.has_value());
    CHECK(s2->provider == "A");
    CHECK(s2->number == 100);

    CHECK(pool->cached_tag("A", Tag::Safe)->number == 100);
    CHECK(pool->cached_tag("B", Tag::Safe)->number == 200);  // B's own snapshot is untouched, not overwritten by A's
    CHECK_FALSE(pool->cached_tag("A", Tag::Finalized).has_value());
    shutdown(ioc, pool);
}
