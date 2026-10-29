#include "app/tools.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/json.hpp>
#include <catch2/catch_test_macros.hpp>
#include <sw/redis++/redis++.h>

#include "base_config.hpp"
#include "core/env.hpp"
#include "core/hex.hpp"
#include "mock_http_server.hpp"
#include "pipeline/event.hpp"
#include "sink/keys.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;
namespace net = boost::asio;
namespace json = boost::json;

namespace {

std::string random_tag() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    return std::to_string(rng());
}

sw::redis::Redis make_admin() {
    const std::string url = evml::core::env::require("REDIS_URL");
    sw::redis::ConnectionOptions opts = sw::redis::Uri(url).connection_options();
    opts.connect_timeout = 2s;
    opts.socket_timeout = 2s;
    return sw::redis::Redis(opts);
}

struct ShardCleanup {
    std::string shard;
    ~ShardCleanup() {
        try {
            auto r = make_admin();
            const evml::sink::Keys k{shard};
            r.del({k.stream(evml::core::StreamKind::Events), k.stream(evml::core::StreamKind::Registry), k.ckpt(),
                   k.released(), k.released_data(), k.leader(), k.epoch()});
        } catch (...) {
        }
    }
};

std::string rpc_ok(std::uint64_t id, const std::string& result_json) {
    return R"({"jsonrpc":"2.0","id":)" + std::to_string(id) + R"(,"result":)" + result_json + "}";
}

evml::rpc::Log make_rpc_log(const evml::core::ContractCfg& c, std::uint64_t block, char hash_ch,
                            std::uint64_t log_index, std::string data = "0x1234") {
    evml::rpc::Log l;
    l.address = c.address;
    l.topics = {c.topics.at(0)};
    l.data = std::move(data);
    l.block_number = block;
    l.block_hash = evml::core::Hash32::from_hex("0x" + std::string(64, hash_ch));
    l.tx_hash = evml::core::Hash32::from_hex("0x" + std::string(64, 'c'));
    l.tx_index = 0;
    l.log_index = log_index;
    l.removed = false;
    return l;
}

std::string log_to_json(const evml::rpc::Log& l) {
    std::string topics = "[";
    for (std::size_t i = 0; i < l.topics.size(); ++i) {
        if (i != 0) topics += ",";
        topics += "\"" + l.topics[i].to_hex() + "\"";
    }
    topics += "]";
    return R"({"address":")" + l.address.to_hex() + R"(","topics":)" + topics + R"(,"data":")" + l.data +
           R"(","blockNumber":")" + evml::core::to_hex_u64(l.block_number) + R"(","blockHash":")" +
           l.block_hash.to_hex() + R"(","transactionHash":")" + l.tx_hash.to_hex() +
           R"(","transactionIndex":")" + evml::core::to_hex_u64(l.tx_index) + R"(","logIndex":")" +
           evml::core::to_hex_u64(l.log_index) + R"(","removed":false})";
}

void xadd_event(sw::redis::Redis& r, const evml::sink::Keys& keys, const evml::pipeline::Event& ev) {
    const auto fields = evml::pipeline::to_fields(ev);
    r.xadd(keys.stream(ev.stream), "*", fields.begin(), fields.end());
}

evml::core::Config make_audit_cfg(const std::string& shard, const std::string& infura_url,
                                  const std::string& ankr_url) {
    auto cfg = evml::test::load_base_config();
    cfg.shard.id = shard;
    cfg.shard.host_id = "audit-test";
    cfg.getlogs_chain = {
        evml::core::Provider{"infura", "", infura_url, 500},
        evml::core::Provider{"ankr", "", ankr_url, 10000},
    };
    cfg.redundancy.getlogs_order = {"infura", "ankr"};
    return cfg;
}

}  // namespace

TEST_CASE("tools: --audit-stream exits 0 on a clean stream and picks an independent provider", "[integration][tools]") {
    const std::string shard = "audit_clean_" + random_tag();
    ShardCleanup cleanup{shard};
    auto admin = make_admin();
    const evml::sink::Keys keys{shard};

    const auto base_cfg = evml::test::load_base_config();
    const auto& safe_pool = base_cfg.contracts.at(0);     // eurc_usdc_cl1_pool (safe, events)
    const auto& final_gauge = base_cfg.contracts.at(2);   // eurc_usdc_cl50_gauge (finalized, events)
    const auto& reg_factory = base_cfg.contracts.at(3);   // aero_cl_factory (finalized, registry)

    // Canonical logs in [52060000, 52060100]:
    //  - log_a at 52060010 (safe_pool)
    //  - log_b at 52060020 (safe_pool, emitted as log -> revert -> log flip, so net-live once)
    //  - log_c at 52060030 (final_gauge)
    //  - log_d at 52060040 (reg_factory on registry stream)
    // Plus a retracted orphan log_retracted at 52060015 (emitted as log -> revert, so not net-live and not canonical).
    const auto log_a = make_rpc_log(safe_pool, 52060010, 'a', 0);
    const auto log_retracted = make_rpc_log(safe_pool, 52060015, '9', 0);
    const auto log_b = make_rpc_log(safe_pool, 52060020, 'b', 1);
    const auto log_c = make_rpc_log(final_gauge, 52060030, 'd', 0);
    const auto log_d = make_rpc_log(reg_factory, 52060040, 'e', 2);

    const std::string canonical_json =
        "[" + log_to_json(log_a) + "," + log_to_json(log_b) + "," + log_to_json(log_c) + "," + log_to_json(log_d) + "]";

    net::io_context mock_ioc;
    std::size_t infura_getlogs = 0;
    std::size_t ankr_getlogs = 0;
    evml::test::MockHttpServer infura_srv(mock_ioc, [&](const evml::test::MockRequest& r) {
        const auto v = json::parse(r.body).as_object();
        const auto id = static_cast<std::uint64_t>(v.at("id").as_int64());
        ++infura_getlogs;
        return evml::test::MockReply{.body = rpc_ok(id, canonical_json)};
    });
    evml::test::MockHttpServer ankr_srv(mock_ioc, [&](const evml::test::MockRequest& r) {
        const auto v = json::parse(r.body).as_object();
        const auto id = static_cast<std::uint64_t>(v.at("id").as_int64());
        ++ankr_getlogs;
        return evml::test::MockReply{.body = rpc_ok(id, canonical_json)};
    });

    const auto cfg = make_audit_cfg(shard, infura_srv.url(), ankr_srv.url());
    const evml::pipeline::EventContext ectx{shard, "audit-test", "0.1.1"};

    admin.hset(keys.ckpt(), std::make_pair("safe", "52060100"));
    admin.hset(keys.ckpt(), std::make_pair("finalized", "52060100"));

    // Populate Redis streams with dominant first_source = "infura".
    xadd_event(admin, keys, evml::pipeline::make_event(log_a, cfg, ectx, evml::pipeline::EventType::Log, 1000, "infura", 1000, 1000));
    xadd_event(admin, keys, evml::pipeline::make_event(log_retracted, cfg, ectx, evml::pipeline::EventType::Log, 1005, "infura", 1005, 1005));
    xadd_event(admin, keys, evml::pipeline::make_event(log_retracted, cfg, ectx, evml::pipeline::EventType::Revert, 1005, "infura", 1005, 1006));
    xadd_event(admin, keys, evml::pipeline::make_event(log_b, cfg, ectx, evml::pipeline::EventType::Log, 1010, "infura", 1010, 1010));
    xadd_event(admin, keys, evml::pipeline::make_event(log_b, cfg, ectx, evml::pipeline::EventType::Revert, 1010, "infura", 1010, 1011));
    xadd_event(admin, keys, evml::pipeline::make_event(log_b, cfg, ectx, evml::pipeline::EventType::Log, 1010, "infura", 1010, 1012));
    xadd_event(admin, keys, evml::pipeline::make_event(log_c, cfg, ectx, evml::pipeline::EventType::Log, 1020, "infura", 1020, 1020));
    xadd_event(admin, keys, evml::pipeline::make_event(log_d, cfg, ectx, evml::pipeline::EventType::Log, 1030, "infura", 1030, 1030));

    evml::app::AuditReport rep;
    std::atomic<int> rc{-1};
    std::thread t([&] { rc = evml::app::audit_stream(cfg, 52060000, 52060100, &rep); });
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return rc.load() != -1; }, 10s));
    t.join();

    CHECK(rc.load() == evml::app::kExitOk);
    CHECK(rep.canonical_logs == 4);
    CHECK(rep.live_stream_entries == 4);
    CHECK(rep.missing == 0);
    CHECK(rep.phantom == 0);
    CHECK(rep.dup_live == 0);
    CHECK(rep.field_mismatch == 0);
    CHECK(rep.order_violation == 0);
    CHECK(rep.dominant_source == "infura");
    CHECK(rep.audit_provider == "ankr");
    CHECK(infura_getlogs == 0);
    CHECK(ankr_getlogs == 1);
}

TEST_CASE("tools: --audit-stream detects missing, phantom, dup_live, field_mismatch, and order_violation defects and exits 1",
          "[integration][tools]") {
    const std::string shard = "audit_defects_" + random_tag();
    ShardCleanup cleanup{shard};
    auto admin = make_admin();
    const evml::sink::Keys keys{shard};

    const auto base_cfg = evml::test::load_base_config();
    const auto& safe_pool = base_cfg.contracts.at(0);

    // Canonical logs in [52060000, 52060100]:
    //  - log_missing at 52060010 (never written to Redis -> missing = 1)
    //  - log_dup     at 52060030 (written twice as type=log without revert -> dup_live = 1)
    //  - log_mismatch at 52060040 (written to Redis with altered data -> field_mismatch = 1)
    //  - log_late_order at 52060025 (written to Redis after 52060040 -> order_violation = 1)
    // Plus in Redis only:
    //  - log_phantom at 52060020 (not in canonical_logs -> phantom = 1)
    const auto log_missing = make_rpc_log(safe_pool, 52060010, '1', 0);
    const auto log_phantom = make_rpc_log(safe_pool, 52060020, '2', 0);
    const auto log_late_order = make_rpc_log(safe_pool, 52060025, '3', 0);
    const auto log_dup = make_rpc_log(safe_pool, 52060030, '4', 0);
    const auto log_mismatch = make_rpc_log(safe_pool, 52060040, '5', 0, "0xaaaa");

    const std::string canonical_json = "[" + log_to_json(log_missing) + "," + log_to_json(log_late_order) + "," +
                                       log_to_json(log_dup) + "," + log_to_json(log_mismatch) + "]";

    net::io_context mock_ioc;
    evml::test::MockHttpServer srv(mock_ioc, [&](const evml::test::MockRequest& r) {
        const auto v = json::parse(r.body).as_object();
        const auto id = static_cast<std::uint64_t>(v.at("id").as_int64());
        return evml::test::MockReply{.body = rpc_ok(id, canonical_json)};
    });

    const auto cfg = make_audit_cfg(shard, srv.url(), srv.url());
    const evml::pipeline::EventContext ectx{shard, "audit-test", "0.1.1"};

    admin.hset(keys.ckpt(), std::make_pair("safe", "52060100"));
    admin.hset(keys.ckpt(), std::make_pair("finalized", "52060100"));

    // 1. Phantom entry (in Redis, not canonical).
    xadd_event(admin, keys, evml::pipeline::make_event(log_phantom, cfg, ectx, evml::pipeline::EventType::Log, 1000, "infura", 1000, 1000));
    // 2. Duplicate live entry (log -> log without revert).
    const auto ev_dup = evml::pipeline::make_event(log_dup, cfg, ectx, evml::pipeline::EventType::Log, 1010, "infura", 1010, 1010);
    xadd_event(admin, keys, ev_dup);
    xadd_event(admin, keys, ev_dup);
    // 3. Field mismatch (corrupted data payload in Redis).
    auto ev_mismatch = evml::pipeline::make_event(log_mismatch, cfg, ectx, evml::pipeline::EventType::Log, 1020, "infura", 1020, 1020);
    ev_mismatch.data = "0xbbbb";
    xadd_event(admin, keys, ev_mismatch);
    // 4. Out-of-order normal-path safe log (block 52060025 appended after block 52060040).
    xadd_event(admin, keys, evml::pipeline::make_event(log_late_order, cfg, ectx, evml::pipeline::EventType::Log, 1015, "infura", 1015, 1025));

    evml::app::AuditReport rep;
    std::atomic<int> rc{-1};
    std::thread t([&] { rc = evml::app::audit_stream(cfg, 52060000, 52060100, &rep); });
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return rc.load() != -1; }, 10s));
    t.join();

    CHECK(rc.load() == evml::app::kExitFailure);
    CHECK(rep.missing == 1);
    CHECK(rep.phantom == 1);
    CHECK(rep.dup_live == 1);
    CHECK(rep.field_mismatch == 1);
    CHECK(rep.order_violation == 1);
}

TEST_CASE("tools: --audit-stream exits 2 when from_block > to_block or to_block > ckpt.finalized",
          "[integration][tools]") {
    const std::string shard = "audit_bounds_" + random_tag();
    ShardCleanup cleanup{shard};
    auto admin = make_admin();
    const evml::sink::Keys keys{shard};

    const auto cfg = make_audit_cfg(shard, "http://127.0.0.1:9", "http://127.0.0.1:9");

    // 1. Invalid range (from_block > to_block) -> exit 2 before touching Redis.
    CHECK(evml::app::audit_stream(cfg, 52060100, 52060000) == evml::app::kExitUsage);

    // 2. Missing checkpoint in Redis -> exit 2.
    CHECK(evml::app::audit_stream(cfg, 52060000, 52060100) == evml::app::kExitUsage);

    // 3. to_block > ckpt.finalized -> exit 2.
    admin.hset(keys.ckpt(), std::make_pair("safe", "52060200"));
    admin.hset(keys.ckpt(), std::make_pair("finalized", "52060100"));
    CHECK(evml::app::audit_stream(cfg, 52060000, 52060101) == evml::app::kExitUsage);
}
