#include <catch2/catch_test_macros.hpp>

#include <boost/json.hpp>
#include <map>

#include "app/pool_log_source.hpp"
#include "core/hex.hpp"
#include "mock_http_server.hpp"
#include "pipeline/sweeper.hpp"
#include "sweep_fakes.hpp"
#include "test_util.hpp"

// PoolLogSource under the real Sweeper (plan 60 §Tests, LogSource adapter): the integration the unit tests of the
// adapter alone cannot show.

namespace {

using namespace std::chrono_literals;
namespace net = boost::asio;
namespace json = boost::json;

constexpr const char* kDeadWs = "ws://127.0.0.1:9/";

struct Counter {
    int logs{0};
    int blocks{0};
};

// One log of `contract` in `block` (block hash `hash_char`).
std::string log_json(const evml::core::ContractCfg& contract, std::uint64_t block, char hash_char, std::uint64_t index) {
    return std::string(R"({"address":")") + contract.address.to_hex() + R"(","topics":[")" + contract.topics.at(0).to_hex() +
           R"("],"data":"0x","blockNumber":")" + evml::core::to_hex_u64(block) + R"(","blockHash":"0x)" +
           std::string(64, hash_char) + R"(","transactionHash":"0x)" + std::string(64, 'c') +
           R"(","transactionIndex":"0x0","logIndex":")" + evml::core::to_hex_u64(index) + R"(","removed":false})";
}

std::string rpc_ok(std::uint64_t id, const std::string& result) {
    return R"({"jsonrpc":"2.0","id":)" + std::to_string(id) + R"(,"result":)" + result + "}";
}

// `logs_result` for eth_getLogs; eth_getBlockByNumber answers from `chain` (number -> hash char).
evml::test::MockHttpServer::Handler serve(Counter& n, const std::string& logs_result,
                                          const std::map<std::uint64_t, char>& chain) {
    return [&n, &logs_result, &chain](const evml::test::MockRequest& r) {
        const auto v = json::parse(r.body).as_object();
        const auto method = std::string(v.at("method").as_string());
        const auto id = static_cast<std::uint64_t>(v.at("id").as_int64());
        if (method == "eth_getLogs") {
            ++n.logs;
            return evml::test::MockReply{.body = rpc_ok(id, logs_result)};
        }
        if (method == "eth_getBlockByNumber") {
            ++n.blocks;
            const auto asked = std::stoull(std::string(v.at("params").as_array().at(0).as_string()), nullptr, 16);
            return evml::test::MockReply{
                .body = rpc_ok(id, R"({"number":")" + evml::core::to_hex_u64(asked) + R"(","hash":"0x)" +
                                       std::string(64, chain.at(asked)) + R"(","parentHash":"0x)" + std::string(64, 'b') +
                                       R"(","timestamp":"0x64"})")};
        }
        return evml::test::MockReply{.status = 500, .body = "unexpected method " + method};
    };
}

}  // namespace

TEST_CASE("pool log source under the sweeper: one block() RPC per distinct block, never on the logs provider",
          "[integration][pool_log_source]") {
    const auto cfg = evml::test::load_base_config();
    const auto& contract = cfg.contracts.at(0);  // a safe-class contract (kSafeA in sweeper_test)
    const std::map<std::uint64_t, char> chain{{103, 'b'}, {105, 'a'}};
    // three logs in two distinct blocks
    const std::string logs = "[" + log_json(contract, 103, 'b', 0) + "," + log_json(contract, 105, 'a', 0) + "," +
                             log_json(contract, 105, 'a', 1) + "]";
    const std::string no_logs = "[]";

    net::io_context ioc;
    // The Sweeper keys its pagers by the config's getLogs provider names, so the mock providers must use them.
    REQUIRE(cfg.getlogs_chain.size() >= 3);
    const std::string p1 = cfg.getlogs_chain[0].name, p2 = cfg.getlogs_chain[1].name, p3 = cfg.getlogs_chain[2].name;
    Counter c1, c2, c3;
    evml::test::MockHttpServer s1(ioc, serve(c1, logs, chain));
    evml::test::MockHttpServer s2(ioc, serve(c2, no_logs, chain));
    evml::test::MockHttpServer s3(ioc, serve(c3, no_logs, chain));

    evml::provider::PoolConfig pc;
    pc.providers = {{p1, kDeadWs, s1.url()}, {p2, kDeadWs, s2.url()}, {p3, kDeadWs, s3.url()}};
    pc.ws_order = {p1};
    pc.getlogs_order = {p1, p2, p3};
    pc.chain_id = 8453;
    pc.user_agent = "evm-listener/test";
    pc.logs = cfg.filter();
    pc.http_timeout = 2000ms;
    pc.reconcile_rps = 1000;
    pc.reconcile_burst = 1000;
    auto pool = evml::provider::ProviderPool::create(ioc.get_executor(), std::move(pc), {});

    evml::app::PoolLogSource source(pool, nullptr);
    evml::test::MemorySink sink;
    evml::pipeline::Sweeper sweeper(cfg, evml::pipeline::EventContext{"base_eurc_usdc", "test-host", "test"}, source, sink,
                                    [] { return std::uint64_t{1000}; });
    sweeper.set_cursors(100, 100);

    const auto res = evml::test::run_sync(ioc, sweeper.safe_step(110));
    REQUIRE(res.status == evml::pipeline::StepStatus::Advanced);
    CHECK(sink.emitted.size() == 3);
    CHECK(c1.logs == 1);
    CHECK(c1.blocks == 0);                // the logs provider is never asked for block hashes
    CHECK(c2.blocks + c3.blocks == 2);    // blocks 103 and 105: one call each, not one per log
    CHECK(c3.blocks == 0);                // the first independent provider answered both
    CHECK(sweeper.c_s() == 110);
}
