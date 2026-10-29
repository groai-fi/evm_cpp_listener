#include <catch2/catch_test_macros.hpp>

#include <fstream>
#include <sstream>
#include <stdexcept>

#include "app/ws_reconciler.hpp"
#include "base_config.hpp"
#include "core/log.hpp"
#include "pipeline/pending_buffer.hpp"

using namespace evml::pipeline;
using evml::test::hash_of;

namespace {

evml::rpc::Log mk(std::uint64_t block, std::uint8_t hash, std::uint64_t idx, std::uint8_t addr = 1) {
    evml::rpc::Log l;
    l.block_number = block;
    l.block_hash = hash_of(hash);
    l.log_index = idx;
    l.address.bytes.fill(addr);
    return l;
}

const auto all = [](const evml::rpc::Log&) { return true; };

}  // namespace

TEST_CASE("pending: stage is idempotent by (blockHash, logIndex)", "[unit][pending]") {
    PendingBuffer p(100);
    CHECK(p.stage(mk(10, 1, 0), "A") == StageResult::Added);
    CHECK(p.stage(mk(10, 1, 0), "B") == StageResult::Duplicate);
    CHECK(p.stage(mk(10, 1, 1), "A") == StageResult::Added);
    CHECK(p.stage(mk(10, 2, 0), "A") == StageResult::Added);  // same height, different hash: kept separately
    CHECK(p.size() == 3);
    CHECK(p.bucket_count() == 1);
}

TEST_CASE("pending: removed erases the staged entry and prunes empty buckets", "[unit][pending]") {
    PendingBuffer p(100);
    p.stage(mk(10, 1, 0), "A");
    p.stage(mk(10, 1, 1), "A");

    auto rm = mk(10, 1, 0);
    rm.removed = true;
    CHECK(p.stage(rm, "A") == StageResult::Removed);
    CHECK(p.size() == 1);
    CHECK(p.stage(rm, "A") == StageResult::RemovedUnknown);  // already gone
    auto rm_other_hash = mk(10, 9, 1);
    rm_other_hash.removed = true;
    CHECK(p.stage(rm_other_hash, "A") == StageResult::RemovedUnknown);  // other blockHash is untouched
    auto rm_unknown_block = mk(99, 1, 0);
    rm_unknown_block.removed = true;
    CHECK(p.stage(rm_unknown_block, "A") == StageResult::RemovedUnknown);
    CHECK(p.size() == 1);

    auto rm2 = mk(10, 1, 1);
    rm2.removed = true;
    CHECK(p.stage(rm2, "A") == StageResult::Removed);
    CHECK(p.size() == 0);
    CHECK(p.bucket_count() == 0);
}

TEST_CASE("pending: reconcile counts misses and extras, then drops the compared range", "[unit][pending]") {
    PendingBuffer p(100);
    p.stage(mk(10, 1, 0), "A");  // in both
    p.stage(mk(10, 1, 1), "A");  // WS only
    p.stage(mk(20, 2, 0), "A");  // outside the swept range: stays

    const std::vector<evml::rpc::Log> sweep{mk(10, 1, 0), mk(10, 1, 2), mk(11, 3, 0)};  // (10,1,2) and (11,3,0): WS missed
    const auto r = p.reconcile(10, 15, sweep, all);
    CHECK(r.ws_missed == 2);
    CHECK(r.ws_extra == 1);
    CHECK(p.size() == 1);
    CHECK(p.bucket_count() == 1);
}

TEST_CASE("pending: a WS log under another blockHash is extra and the sweep log is missed", "[unit][pending]") {
    PendingBuffer p(100);
    p.stage(mk(10, 1, 0), "A");  // WS saw the orphaned block
    const auto r = p.reconcile(10, 10, {mk(10, 2, 0)}, all);  // canonical block has another hash
    CHECK(r.ws_extra == 1);
    CHECK(r.ws_missed == 1);
    CHECK(p.size() == 0);
}

TEST_CASE("pending: reconcile leaves out-of-scope staged logs untouched", "[unit][pending]") {
    PendingBuffer p(100);
    p.stage(mk(10, 1, 0, 0xaa), "A");  // in scope
    p.stage(mk(10, 1, 1, 0xbb), "A");  // another class: this sweep did not cover it
    const auto only_aa = [](const evml::rpc::Log& l) { return l.address.bytes[0] == 0xaa; };

    const auto r = p.reconcile(10, 10, {mk(10, 1, 0, 0xaa)}, only_aa);
    CHECK(r.ws_missed == 0);
    CHECK(r.ws_extra == 0);
    CHECK(p.size() == 1);  // the 0xbb log waits for the sweep that covers it
}

TEST_CASE("pending: reconcile on an empty buffer only reports misses", "[unit][pending]") {
    PendingBuffer p(10);
    const auto r = p.reconcile(1, 100, {mk(5, 1, 0), mk(6, 1, 0)}, all);
    CHECK(r.ws_missed == 2);
    CHECK(r.ws_extra == 0);
}

TEST_CASE("pending: sweep logs outside the swept range are ignored", "[unit][pending]") {
    PendingBuffer p(10);
    const auto r = p.reconcile(10, 12, {mk(9, 1, 0), mk(13, 1, 0)}, all);
    CHECK(r.ws_missed == 0);
}

TEST_CASE("pending: the cap evicts the oldest buckets first", "[unit][pending]") {
    PendingBuffer p(4);
    for (std::uint64_t b = 1; b <= 3; ++b) {
        p.stage(mk(b, 1, 0), "A");
        p.stage(mk(b, 1, 1), "A");
    }
    // 6 staged against a cap of 4: block 1 (2 logs) is evicted, leaving blocks 2 and 3.
    CHECK(p.size() == 4);
    CHECK(p.bucket_count() == 2);
    CHECK(p.evicted_total() == 2);
    CHECK(p.stage(mk(2, 1, 0), "A") == StageResult::Duplicate);  // block 2 survived
    CHECK(p.stage(mk(1, 1, 0), "A") == StageResult::Added);      // block 1 was evicted (and now evicts itself again)
    CHECK(p.size() <= 4);
}

TEST_CASE("pending: eviction keeps size consistent when the newest bucket exceeds the cap", "[unit][pending]") {
    PendingBuffer p(2);
    for (std::uint64_t i = 0; i < 5; ++i) p.stage(mk(7, 1, i), "A");
    CHECK(p.size() <= 2);
    CHECK(p.evicted_total() + p.size() == 5);
}

TEST_CASE("pending: purge_from drops buckets at and above the fork block", "[unit][pending]") {
    PendingBuffer p(100);
    p.stage(mk(10, 1, 0), "A");
    p.stage(mk(11, 1, 0), "A");
    p.stage(mk(11, 2, 0), "A");
    p.stage(mk(12, 1, 0), "A");
    CHECK(p.purge_from(11) == 3);
    CHECK(p.size() == 1);
    CHECK(p.bucket_count() == 1);
    CHECK(p.purge_from(100) == 0);
}

TEST_CASE("pending: zero cap is rejected", "[unit][pending]") {
    CHECK_THROWS_AS(PendingBuffer(0), std::invalid_argument);
}

TEST_CASE("pending: memory ceiling at 10,000 logs stays well under 50 MB", "[unit][pending]") {
#if defined(__linux__)
    const auto read_rss_kb = []() -> std::size_t {
        std::ifstream in("/proc/self/status");
        std::string line;
        while (std::getline(in, line)) {
            if (line.rfind("VmRSS:", 0) == 0) {
                std::istringstream iss(line.substr(6));
                std::size_t kb = 0;
                iss >> kb;
                return kb;
            }
        }
        return 0;
    };
    const auto rss_before_kb = read_rss_kb();
    REQUIRE(rss_before_kb > 0);
#endif

    PendingBuffer p(10000);
    // Realistic Swap log: 160-byte ABI data payload (5 x 32-byte words = 320 hex chars) + 3 indexed topics.
    const std::string swap_data = "0x" + std::string(320, 'a');
    const std::vector<evml::core::Hash32> swap_topics{hash_of(0x11), hash_of(0x22), hash_of(0x33)};

    for (std::uint64_t i = 0; i < 10000; ++i) {
        const std::uint64_t block = 1000 + (i / 10);
        const std::uint64_t log_idx = i % 10;
        auto l = mk(block, static_cast<std::uint8_t>((block % 250) + 1), log_idx);
        l.topics = swap_topics;
        l.data = swap_data;
        l.tx_hash = hash_of(static_cast<std::uint8_t>((i % 250) + 1));
        l.tx_index = log_idx;
        REQUIRE(p.stage(l, "quicknode") == StageResult::Added);
    }

    CHECK(p.size() == 10000);
    CHECK(p.bucket_count() == 1000);
    CHECK(p.evicted_total() == 0);

#if defined(__linux__)
    const auto rss_after_kb = read_rss_kb();
    REQUIRE(rss_after_kb > 0);
    const auto rss_delta_kb = rss_after_kb > rss_before_kb ? (rss_after_kb - rss_before_kb) : 0;
    INFO("rss_before_kb=" << rss_before_kb << " rss_after_kb=" << rss_after_kb << " rss_delta_kb=" << rss_delta_kb);
    CHECK(rss_delta_kb < 50 * 1024);
#endif

    // Stage 10,001st log at block 2000: evict_to_cap() evicts the oldest block bucket (block 1000, 10 logs).
    auto extra = mk(2000, 0xfe, 0);
    extra.topics = swap_topics;
    extra.data = swap_data;
    CHECK(p.stage(extra, "quicknode") == StageResult::Added);
    CHECK(p.size() == 9991);
    CHECK(p.bucket_count() == 1000);
    CHECK(p.evicted_total() == 10);
}

TEST_CASE("ws reconciler: disconnect during catch-up waits for first live head before cross-checking", "[unit][pipeline]") {
    evml::core::log::init("info");
    const auto cfg = evml::test::load_base_config();
    evml::app::WsReconciler rec(cfg, 100);

    // WS disconnects during catch-up; pipeline then transitions to Live at c_s = 200 while a WS reconnect is in flight.
    rec.on_ws_provider_changed(false, std::nullopt);
    rec.on_live_tick(200, true);
    CHECK_FALSE(rec.live_from_block().has_value());
    CHECK(rec.reseed_pending());

    // First head on the reconnected WS arrives at block 250 while Live c_s = 200.
    rec.on_head(250, 200);
    REQUIRE(rec.live_from_block().has_value());
    CHECK(*rec.live_from_block() == 250);
    CHECK_FALSE(rec.reseed_pending());

    // Sweeping blocks [201, 249] below live_from_block must not count missed WS logs.
    const auto cc_pre = rec.on_sweep_page(evml::core::Finality::Safe, BlockRange{201, 249}, {mk(220, 1, 0)});
    CHECK(cc_pre.ws_missed == 0);
    CHECK(cc_pre.ws_extra == 0);

    // A log at block 250 (>= live_from_block) missed by WS is counted.
    const auto cc_live = rec.on_sweep_page(evml::core::Finality::Safe, BlockRange{250, 250}, {mk(250, 2, 0)});
    CHECK(cc_live.ws_missed == 1);
    CHECK(cc_live.ws_extra == 0);
}
