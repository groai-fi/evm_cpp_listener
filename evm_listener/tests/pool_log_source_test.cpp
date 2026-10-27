#include <catch2/catch_test_macros.hpp>

#include <boost/json.hpp>

#include "app/pool_log_source.hpp"
#include "core/hex.hpp"
#include "mock_http_server.hpp"
#include "test_util.hpp"

using namespace evml;
using namespace evml::test;
using namespace std::chrono_literals;
namespace net = boost::asio;

namespace {

constexpr const char* kAddr = "0xe846373c1a92b167b4e9cd5d8e4d6b1db9e90ec7";
constexpr const char* kTopic = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
constexpr const char* kDeadWs = "ws://127.0.0.1:9/";
constexpr std::uint64_t kBlock = 1000;

std::string hash_str(char c) { return "0x" + std::string(64, c); }
core::Hash32 hash_of(char c) { return core::Hash32::from_hex(hash_str(c)); }

core::LogFilter filter() {
    core::LogFilter f;
    f.addresses.push_back(core::Address20::from_hex(kAddr));
    f.topic0s.push_back(core::Hash32::from_hex(kTopic));
    return f;
}

std::string rpc_ok(std::uint64_t id, const std::string& result) {
    return R"({"jsonrpc":"2.0","id":)" + std::to_string(id) + R"(,"result":)" + result + "}";
}

std::string log_json(std::uint64_t block, char hash_char) {
    return std::string(R"({"address":")") + kAddr + R"(","topics":[")" + kTopic + R"("],"data":"0x","blockNumber":")" +
           core::to_hex_u64(block) + R"(","blockHash":")" + hash_str(hash_char) + R"(","transactionHash":")" +
           hash_str('c') + R"(","transactionIndex":"0x0","logIndex":"0x0","removed":false})";
}

std::string block_json(std::uint64_t n, char hash_char, std::uint64_t ts) {
    return R"({"number":")" + core::to_hex_u64(n) + R"(","hash":")" + hash_str(hash_char) + R"(","parentHash":")" +
           hash_str('b') + R"(","timestamp":")" + core::to_hex_u64(ts) + R"("})";
}

// One mock provider: what it answers and how often it was asked.
struct Node {
    int logs_calls{0};
    int block_calls{0};
    int status{200};                    // != 200 fails every request
    std::string logs_result{"[]"};
    char block_hash{'a'};
    std::optional<std::uint64_t> block_number_override;  // answers a different block than asked
    bool null_block{false};
};

MockHttpServer::Handler serve(Node& n) {
    return [&n](const MockRequest& r) {
        const auto v = boost::json::parse(r.body).as_object();
        const auto method = std::string(v.at("method").as_string());
        const auto id = static_cast<std::uint64_t>(v.at("id").as_int64());
        if (n.status != 200) {
            const char* body = n.status == 413
                                   ? R"({"jsonrpc":"2.0","id":1,"error":{"code":-32615,"message":"eth_getLogs is limited to a 5 range"}})"
                                   : "boom";
            return MockReply{.status = n.status, .body = body};
        }
        if (method == "eth_getLogs") {
            ++n.logs_calls;
            return MockReply{.body = rpc_ok(id, n.logs_result)};
        }
        if (method == "eth_getBlockByNumber") {
            ++n.block_calls;
            if (n.null_block) return MockReply{.body = rpc_ok(id, "null")};
            const auto asked = std::stoull(std::string(v.at("params").as_array().at(0).as_string()), nullptr, 16);
            return MockReply{.body = rpc_ok(id, block_json(n.block_number_override.value_or(asked), n.block_hash, 1234))};
        }
        return MockReply{.status = 500, .body = "unexpected method " + method};
    };
}

provider::PoolConfig cfg_for(std::vector<provider::ProviderEndpoint> eps) {
    provider::PoolConfig c;
    std::vector<std::string> order;
    for (const auto& e : eps) order.push_back(e.name);
    c.providers = std::move(eps);
    c.ws_order = {order.front()};
    c.getlogs_order = order;
    c.chain_id = 8453;
    c.user_agent = "evm-listener/test";
    c.subscribe_heads = true;
    c.logs = filter();
    c.http_timeout = 2000ms;
    c.policy.failover_after = 3;
    c.policy.probe_interval = 3600s;
    c.policy.prefer_primary_after = 3600s;
    c.policy.min_voluntary_gap = 0s;
    c.reconcile_rps = 1000;  // pacing is covered in provider_pool_test
    c.reconcile_burst = 1000;
    return c;
}

// Three providers P1 > P2 > P3 on the getLogs chain; the pool is never started (no WS, no background traffic).
struct Fixture {
    net::io_context ioc;
    Node n1, n2, n3;
    MockHttpServer s1{ioc, serve(n1)};
    MockHttpServer s2{ioc, serve(n2)};
    MockHttpServer s3{ioc, serve(n3)};
    std::shared_ptr<provider::ProviderPool> pool = provider::ProviderPool::create(
        ioc.get_executor(),
        cfg_for({{"P1", kDeadWs, s1.url()}, {"P2", kDeadWs, s2.url()}, {"P3", kDeadWs, s3.url()}}), {});
};

pipeline::BlockRange range() { return {kBlock - 4, kBlock}; }

}  // namespace

TEST_CASE("pool log source: get_logs falls through to the next provider", "[integration][pool_log_source]") {
    Fixture f;
    f.n1.status = 500;
    f.n2.logs_result = "[" + log_json(kBlock, 'a') + "]";
    app::PoolLogSource src(f.pool, nullptr);

    const auto r = run_sync(f.ioc, src.get_logs(range(), filter()));
    REQUIRE(r.ok);
    CHECK(r.provider == "P2");
    CHECK(r.logs.size() == 1);
    CHECK(r.logs[0].block_number == kBlock);
    CHECK(f.n3.logs_calls == 0);
}

TEST_CASE("pool log source: RangeTooLarge returns at once without trying another provider",
          "[integration][pool_log_source]") {
    Fixture f;
    f.n1.status = 413;
    app::PoolLogSource src(f.pool, nullptr);

    const auto r = run_sync(f.ioc, src.get_logs(range(), filter()));
    CHECK_FALSE(r.ok);
    CHECK(r.kind == rpc::FailKind::RangeTooLarge);
    CHECK(r.range_limit == 5u);
    CHECK(r.provider == "P1");
    CHECK(f.n2.logs_calls == 0);
}

TEST_CASE("pool log source: every provider failing returns the last failure", "[integration][pool_log_source]") {
    Fixture f;
    f.n1.status = f.n2.status = f.n3.status = 500;
    app::PoolLogSource src(f.pool, nullptr);

    const auto r = run_sync(f.ioc, src.get_logs(range(), filter()));
    CHECK_FALSE(r.ok);
    CHECK(r.kind == rpc::FailKind::Retry);
    CHECK(r.provider == "P3");
}

TEST_CASE("pool log source: an undecodable getLogs answer is reported and the next provider is asked",
          "[integration][pool_log_source]") {
    Fixture f;
    f.n1.logs_result = R"([{"x":1}])";
    app::PoolLogSource src(f.pool, nullptr);

    const auto r = run_sync(f.ioc, src.get_logs(range(), filter()));
    REQUIRE(r.ok);
    CHECK(r.provider == "P2");
    CHECK(f.pool->inconsistency_total("P1") == 1);
}

TEST_CASE("pool log source: report_bad steers the next get_logs away, once", "[integration][pool_log_source]") {
    Fixture f;
    app::PoolLogSource src(f.pool, nullptr);
    CHECK(src.current_provider() == std::optional<std::string>("P1"));

    src.report_bad("P1", "bad page");
    CHECK(f.pool->inconsistency_total("P1") == 1);
    CHECK(f.pool->state("P1") == provider::State::Up);  // failover_after = 3
    CHECK(src.current_provider() == std::optional<std::string>("P2"));

    CHECK(run_sync(f.ioc, src.get_logs(range(), filter())).provider == "P2");
    CHECK(f.n1.logs_calls == 0);
    // the steering is spent: the next page goes back to the best provider
    CHECK(src.current_provider() == std::optional<std::string>("P1"));
    CHECK(run_sync(f.ioc, src.get_logs(range(), filter())).provider == "P1");
}

TEST_CASE("pool log source: three bad reports take a provider Down", "[integration][pool_log_source]") {
    Fixture f;
    app::PoolLogSource src(f.pool, nullptr);
    for (int i = 0; i < 3; ++i) src.report_bad("P1", "bad page");
    CHECK(f.pool->state("P1") == provider::State::Down);
    CHECK(src.current_provider() == std::optional<std::string>("P2"));
}

TEST_CASE("pool log source: the only provider is asked again after report_bad", "[integration][pool_log_source]") {
    net::io_context ioc;
    Node n;
    MockHttpServer s(ioc, serve(n));
    auto pool = provider::ProviderPool::create(ioc.get_executor(), cfg_for({{"P1", kDeadWs, s.url()}}), {});
    app::PoolLogSource src(pool, nullptr);

    src.report_bad("P1", "bad page");
    CHECK(src.current_provider() == std::optional<std::string>("P1"));
    const auto r = run_sync(ioc, src.get_logs(range(), filter()));
    CHECK(r.ok);
    CHECK(r.provider == "P1");
}

TEST_CASE("pool log source: block() never asks the provider that returned the logs", "[integration][pool_log_source]") {
    Fixture f;
    app::PoolLogSource src(f.pool, nullptr);
    REQUIRE(run_sync(f.ioc, src.get_logs(range(), filter())).provider == "P1");

    const auto b = run_sync(f.ioc, src.block(kBlock));
    REQUIRE(b.has_value());
    CHECK(b->hash == hash_of('a'));
    CHECK(b->timestamp == 1234);
    CHECK(f.n1.block_calls == 0);
    CHECK(f.n2.block_calls == 1);
}

TEST_CASE("pool log source: block() answers from the ring without any RPC", "[integration][pool_log_source]") {
    Fixture f;
    chain::ReorgDetector ring(16);
    ring.on_head(chain::BlockRef{kBlock, hash_of('a'), hash_of('b'), 777});
    app::PoolLogSource src(f.pool, &ring);

    const auto b = run_sync(f.ioc, src.block(kBlock));
    REQUIRE(b.has_value());
    CHECK(b->hash == hash_of('a'));
    CHECK(b->timestamp == 777);
    CHECK(f.n1.block_calls + f.n2.block_calls + f.n3.block_calls == 0);
}

TEST_CASE("pool log source: a ring entry without a timestamp is cross-checked by an RPC", "[integration][pool_log_source]") {
    Fixture f;
    chain::ReorgDetector ring(16);
    ring.on_head(chain::BlockRef{kBlock, hash_of('a'), hash_of('b'), 0});
    app::PoolLogSource src(f.pool, &ring);

    const auto b = run_sync(f.ioc, src.block(kBlock));
    REQUIRE(b.has_value());
    CHECK(b->timestamp == 1234);  // from the provider
    CHECK(f.n1.block_calls == 1);
}

TEST_CASE("pool log source: block() hash disagreeing with the ring is reported and returns nullopt",
          "[integration][pool_log_source]") {
    Fixture f;
    chain::ReorgDetector ring(16);
    ring.on_head(chain::BlockRef{kBlock, hash_of('a'), hash_of('b'), 0});
    f.n1.block_hash = 'd';  // the provider disagrees with the ring
    app::PoolLogSource src(f.pool, &ring);

    CHECK_FALSE(run_sync(f.ioc, src.block(kBlock)).has_value());
    CHECK(f.pool->inconsistency_total("P1") == 1);
}

TEST_CASE("pool log source: block() skips providers without the block or with the wrong one",
          "[integration][pool_log_source]") {
    Fixture f;
    f.n1.null_block = true;                      // does not have it yet: not reported
    f.n2.block_number_override = kBlock + 1;     // answers another block: reported
    app::PoolLogSource src(f.pool, nullptr);

    const auto b = run_sync(f.ioc, src.block(kBlock));
    REQUIRE(b.has_value());
    CHECK(f.n3.block_calls == 1);
    CHECK(f.pool->inconsistency_total("P1") == 0);
    CHECK(f.pool->inconsistency_total("P2") == 1);
}

TEST_CASE("pool log source: no independent provider means nullopt, never the logs provider",
          "[integration][pool_log_source]") {
    net::io_context ioc;
    Node n;
    MockHttpServer s(ioc, serve(n));
    auto pool = provider::ProviderPool::create(ioc.get_executor(), cfg_for({{"P1", kDeadWs, s.url()}}), {});
    app::PoolLogSource src(pool, nullptr);
    REQUIRE(run_sync(ioc, src.get_logs(range(), filter())).ok);

    CHECK_FALSE(run_sync(ioc, src.block(kBlock)).has_value());
    CHECK(n.block_calls == 0);
}

TEST_CASE("pool log source: a null pool is rejected", "[unit][pool_log_source]") {
    CHECK_THROWS_AS(app::PoolLogSource(nullptr, nullptr), std::invalid_argument);
}
