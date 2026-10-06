#include <catch2/catch_test_macros.hpp>

#include "base_config.hpp"
#include "pipeline/event.hpp"
#include "pipeline/log_validation.hpp"

using namespace evml::pipeline;
using evml::core::Finality;
using evml::core::StreamKind;
using evml::test::hash_of;
using evml::test::load_base_config;

namespace {

const EventContext kCtx{"base_eurc_usdc", "host-1", "0.1.0-test"};

const evml::core::ContractCfg& contract(const evml::core::Config& c, const std::string& name) {
    for (const auto& k : c.contracts) {
        if (k.name == name) return k;
    }
    FAIL("no contract " << name);
    throw std::logic_error("unreachable");
}

evml::rpc::Log log_of(const evml::core::ContractCfg& k, std::size_t event_idx, std::uint64_t block, std::uint64_t idx,
                      std::uint8_t block_hash_byte = 0xaa) {
    evml::rpc::Log l;
    l.address = k.address;
    l.topics = {k.topics.at(event_idx)};
    l.data = "0x00";
    l.block_number = block;
    l.block_hash = hash_of(block_hash_byte);
    l.tx_hash = hash_of(0xcc);
    l.tx_index = 3;
    l.log_index = idx;
    return l;
}

}  // namespace

TEST_CASE("event id and dedup key", "[unit][event]") {
    const auto id = make_event_id(8453, hash_of(0xab), 7);
    std::string hex_ab;
    for (int i = 0; i < 32; ++i) hex_ab += "ab";
    CHECK(id == "8453:0x" + hex_ab + ":7");
    CHECK(make_event_id(8453, hash_of(0xab), 7) != make_event_id(8453, hash_of(0xac), 7));  // reorg => new id
    CHECK(make_event_id(8453, hash_of(0xab), 7) != make_event_id(8453, hash_of(0xab), 8));
}

TEST_CASE("make_event: pool log is safe-class on the events stream with the full signature", "[unit][event]") {
    const auto cfg = load_base_config();
    const auto& pool = contract(cfg, "eurc_usdc_cl50_pool");
    const auto e = make_event(log_of(pool, 0, 52060500, 4), cfg, kCtx, EventType::Log, 1760000000, "infura", 111, 222);

    CHECK(e.type == EventType::Log);
    CHECK(e.finality == Finality::Safe);
    CHECK(e.stream == StreamKind::Events);
    CHECK(e.contract == "eurc_usdc_cl50_pool");
    CHECK(e.event == "Swap");
    CHECK(e.signature == "Swap(address,address,int256,int256,uint160,uint128,int24)");
    CHECK(e.chain_id == 8453);
    CHECK(e.event_id == make_event_id(8453, hash_of(0xaa), 4));
    CHECK(e.block_timestamp == 1760000000);
    CHECK(e.first_source == "infura");
    CHECK(e.observed_ms == 111);
    CHECK(e.emitted_ms == 222);
    CHECK(e.host == "host-1");
    CHECK(e.shard == "base_eurc_usdc");
    CHECK(e.listener_version == "0.1.0-test");
    CHECK(dedup_key(e) == "dedup:" + e.event_id);
}

TEST_CASE("make_event: gauge is finalized-class, factory goes to the registry stream, a revert shares its log's dedup key",
          "[unit][event]") {
    const auto cfg = load_base_config();
    const auto gauge = make_event(log_of(contract(cfg, "eurc_usdc_cl50_gauge"), 1, 52060600, 0), cfg, kCtx,
                                  EventType::Log, 1, "infura", 0, 0);
    CHECK(gauge.finality == Finality::Finalized);
    CHECK(gauge.event == "Withdraw");

    const auto reg = make_event(log_of(contract(cfg, "aero_cl_factory"), 0, 52060700, 1), cfg, kCtx, EventType::Log, 1,
                                "infura", 0, 0);
    CHECK(reg.stream == StreamKind::Registry);
    CHECK(reg.event == "PoolCreated");

    const auto rev = make_event(log_of(contract(cfg, "eurc_usdc_cl50_gauge"), 1, 52060600, 0), cfg, kCtx,
                                EventType::Revert, 1, "infura", 0, 0);
    CHECK(rev.event_id == gauge.event_id);  // same identity...
    CHECK(dedup_key(rev) == dedup_key(gauge));  // ...and one dedup key per event: its value is the last type (rev 5)
    CHECK(dedup_key(rev) == "dedup:" + rev.event_id);
}

TEST_CASE("make_event: untracked or topic-less logs are a contract violation", "[unit][event]") {
    const auto cfg = load_base_config();
    auto l = log_of(contract(cfg, "eurc_usdc_cl50_pool"), 0, 52060500, 4);
    l.topics.clear();
    CHECK_THROWS_AS(make_event(l, cfg, kCtx, EventType::Log, 1, "x", 0, 0), std::invalid_argument);
    l.topics = {hash_of(0x11)};
    CHECK_THROWS_AS(make_event(l, cfg, kCtx, EventType::Log, 1, "x", 0, 0), std::invalid_argument);
}

TEST_CASE("to_fields: schema v=1 order and values", "[unit][event]") {
    const auto cfg = load_base_config();
    const auto& pool = contract(cfg, "eurc_usdc_cl50_pool");
    auto l = log_of(pool, 0, 52060500, 4);
    l.topics.push_back(hash_of(0x22));
    const auto e = make_event(l, cfg, kCtx, EventType::Log, 1760000000, "infura", 111, 222);
    const auto f = to_fields(e);

    const std::vector<std::string> expected_keys{
        "v", "type", "finality", "event_id", "chain_id", "contract", "address", "event", "signature", "block_number",
        "block_hash", "block_timestamp", "tx_hash", "tx_index", "log_index", "topics", "data", "first_source",
        "observed_ms", "emitted_ms", "host", "shard", "listener_version"};
    REQUIRE(f.size() == expected_keys.size());
    for (std::size_t i = 0; i < f.size(); ++i) CHECK(f[i].first == expected_keys[i]);

    const auto get = [&](const std::string& k) {
        for (const auto& kv : f) {
            if (kv.first == k) return kv.second;
        }
        FAIL("missing field " << k);
        return std::string();
    };
    CHECK(get("v") == "1");
    CHECK(get("type") == "log");
    CHECK(get("finality") == "safe");
    CHECK(get("chain_id") == "8453");
    CHECK(get("block_number") == "52060500");
    CHECK(get("log_index") == "4");
    CHECK(get("address") == pool.address.to_hex());
    CHECK(get("topics") == "[\"" + pool.topics[0].to_hex() + "\",\"" + hash_of(0x22).to_hex() + "\"]");
    CHECK(get("data") == "0x00");
}

TEST_CASE("validate_logs: accepts a sound response (including an empty one)", "[unit][validation]") {
    const auto cfg = load_base_config();
    const auto& pool = contract(cfg, "eurc_usdc_cl50_pool");
    CHECK_FALSE(validate_logs({}, 100, 200, cfg, std::nullopt).has_value());
    const std::vector<evml::rpc::Log> logs{log_of(pool, 0, 100, 0), log_of(pool, 1, 150, 1), log_of(pool, 0, 200, 2)};
    CHECK_FALSE(validate_logs(logs, 100, 200, cfg, Finality::Safe).has_value());
    CHECK_FALSE(validate_logs(logs, 100, 200, cfg, std::nullopt).has_value());
}

TEST_CASE("validate_logs: rejects each class of provider fault with a precise message", "[unit][validation]") {
    const auto cfg = load_base_config();
    const auto& pool = contract(cfg, "eurc_usdc_cl50_pool");
    const auto& gauge = contract(cfg, "eurc_usdc_cl50_gauge");
    const auto msg = [&](std::vector<evml::rpc::Log> logs, std::optional<Finality> only = std::nullopt) {
        return validate_logs(logs, 100, 200, cfg, only).value_or("");
    };

    auto removed = log_of(pool, 0, 150, 0);
    removed.removed = true;
    CHECK(msg({removed}).find("removed=true") != std::string::npos);

    CHECK(msg({log_of(pool, 0, 99, 0)}).find("outside requested range 100..200") != std::string::npos);
    CHECK(msg({log_of(pool, 0, 201, 0)}).find("outside requested range") != std::string::npos);

    auto no_topic = log_of(pool, 0, 150, 0);
    no_topic.topics.clear();
    CHECK(msg({no_topic}).find("no topic0") != std::string::npos);

    auto foreign = log_of(pool, 0, 150, 0);
    foreign.topics = {hash_of(0x11)};
    CHECK(msg({foreign}).find("untracked") != std::string::npos);

    CHECK(msg({log_of(gauge, 0, 150, 0)}, Finality::Safe).find("not requested") != std::string::npos);

    const auto dup = msg({log_of(pool, 0, 150, 5), log_of(pool, 1, 150, 5)});  // same (blockHash, logIndex)
    CHECK(dup.find("duplicate") != std::string::npos);
    CHECK(dup.find("log #1") != std::string::npos);

    // The same logIndex under a different blockHash is legitimate (a reorg inside the window).
    CHECK(msg({log_of(pool, 0, 150, 5, 0xaa), log_of(pool, 0, 151, 5, 0xbb)}).empty());
}
