#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <memory>

#include "pipeline/sweeper.hpp"
#include "sweep_fakes.hpp"

using namespace evml::pipeline;
using namespace evml::test;
using evml::core::Finality;

namespace {

constexpr std::size_t kSafeA = 0;   // cl1 pool    (safe)
constexpr std::size_t kSafeB = 1;   // cl50 pool   (safe)
constexpr std::size_t kGauge = 2;   // cl50 gauge  (finalized)

struct Rig {
    evml::core::Config cfg;
    FakeSource src;
    MemorySink sink;
    std::unique_ptr<Sweeper> sw;

    explicit Rig(evml::core::Config c = load_base_config(), SweeperOptions opt = {}) : cfg(std::move(c)) {
        sw = std::make_unique<Sweeper>(cfg, EventContext{"base_eurc_usdc", "test-host", "test"}, src, sink,
                                       [] { return std::uint64_t{1000}; }, opt);
        sw->set_cursors(100, 100);
    }
    void add(const evml::rpc::Log& l, std::uint8_t canonical_hash) {
        src.logs.push_back(l);
        src.blocks[l.block_number] = binfo(canonical_hash);
    }
};

}  // namespace

TEST_CASE("sweep: safe step emits sorted safe logs, tracks them as released and checkpoints", "[unit][sweep]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 1), 1);
    r.add(mklog(r.cfg, kSafeB, 103, 2, 0), 2);
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    r.add(mklog(r.cfg, kGauge, 104, 3, 0), 3);  // finalized-class: not part of the safe sweep

    const auto res = run(r.sw->safe_step(110));
    REQUIRE(res.status == StepStatus::Advanced);
    CHECK(*res.range == BlockRange{101, 110});
    REQUIRE(r.sink.emitted.size() == 3);
    CHECK(field(r.sink.emitted[0], "block_number") == "103");
    CHECK(field(r.sink.emitted[1], "block_number") == "105");
    CHECK(field(r.sink.emitted[1], "log_index") == "0");
    CHECK(field(r.sink.emitted[2], "log_index") == "1");
    for (const auto& m : r.sink.emitted) {
        CHECK(field(m, "finality") == "safe");
        CHECK(m.released_block.has_value());
    }
    CHECK(r.sink.released.size() == 3);
    REQUIRE(r.sink.checkpoints.size() == 1);
    CHECK(r.sink.checkpoints[0].first == CheckpointKind::Safe);
    CHECK(r.sink.checkpoints[0].second == 110);
    CHECK(r.sw->c_s() == 110);
    CHECK(r.sw->stats().emitted_safe == 3);
}

TEST_CASE("sweep: safe step pages by the provider's range", "[unit][sweep]") {
    Rig r;  // infura: 500 blocks
    const auto res = run(r.sw->safe_step(2000));
    REQUIRE(res.status == StepStatus::Advanced);
    CHECK(*res.range == BlockRange{101, 600});
    CHECK(r.sw->c_s() == 600);
}

TEST_CASE("sweep: safe step has no work at the gate and pauses when the sink is saturated", "[unit][sweep]") {
    Rig r;
    CHECK(run(r.sw->safe_step(100)).status == StepStatus::NoWork);
    CHECK(run(r.sw->safe_step(50)).status == StepStatus::NoWork);

    r.sink.saturated_flag = true;
    CHECK(run(r.sw->safe_step(110)).status == StepStatus::Paused);
    CHECK(run(r.sw->final_step(110)).status == StepStatus::Paused);
    CHECK(r.src.calls.empty());
    CHECK(r.sw->c_s() == 100);
}

TEST_CASE("sweep: a RangeTooLarge with a stated limit shrinks the page and retries", "[unit][sweep]") {
    Rig r;
    r.src.limit = 100;
    const auto res = run(r.sw->safe_step(2000));
    REQUIRE(res.status == StepStatus::Advanced);
    CHECK(*res.range == BlockRange{101, 200});
    REQUIRE(r.src.calls.size() == 2);
    CHECK(r.src.calls[0].range == BlockRange{101, 600});
    CHECK(r.src.calls[1].range == BlockRange{101, 200});
    CHECK(r.sw->stats().range_shrinks == 1);

    // the pager remembers: the next page starts at the shrunk size
    r.src.calls.clear();
    CHECK(run(r.sw->safe_step(2000)).status == StepStatus::Advanced);
    CHECK(r.src.calls[0].range == BlockRange{201, 300});
}

TEST_CASE("sweep: at the minimum range a too-large request is split by address", "[unit][sweep]") {
    Rig r;
    r.src.reject_multi_address = true;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    r.add(mklog(r.cfg, kSafeB, 106, 2, 0), 2);

    const auto res = run(r.sw->safe_step(2000));
    REQUIRE(res.status == StepStatus::Advanced);
    CHECK(*res.range == BlockRange{101, 110});  // min_range = 10
    CHECK(r.sw->stats().address_splits == 1);
    CHECK(r.src.calls.size() >= 3);
    CHECK(r.src.calls[r.src.calls.size() - 1].addresses == 1);
    CHECK(r.src.calls[r.src.calls.size() - 2].addresses == 1);
    CHECK(r.sink.emitted.size() == 2);
}

TEST_CASE("sweep: still too large for a single address stalls without moving the cursor", "[unit][sweep]") {
    Rig r;
    r.src.reject_all = true;
    const auto res = run(r.sw->safe_step(2000));
    CHECK(res.status == StepStatus::Stalled);
    CHECK(res.detail.find("single address") != std::string::npos);
    CHECK(r.sink.emitted.empty());
    CHECK(r.sink.checkpoints.empty());
    CHECK(r.sw->c_s() == 100);
}

TEST_CASE("sweep: a provider failure is a retry that leaves everything untouched", "[unit][sweep]") {
    Rig r;
    LogResult bad;
    bad.provider = "infura";
    bad.kind = evml::rpc::FailKind::Retry;
    bad.error = "boom";
    r.src.overrides.push_back(bad);

    const auto res = run(r.sw->safe_step(110));
    CHECK(res.status == StepStatus::Retry);
    CHECK(res.detail.find("boom") != std::string::npos);
    CHECK(r.sink.emitted.empty());
    CHECK(r.sw->c_s() == 100);

    CHECK(run(r.sw->safe_step(110)).status == StepStatus::Advanced);  // the next attempt succeeds
}

TEST_CASE("sweep: no usable getLogs provider is a retry", "[unit][sweep]") {
    Rig r;
    r.src.no_provider = true;
    CHECK(run(r.sw->safe_step(110)).status == StepStatus::Retry);
    CHECK(r.src.calls.empty());
}

TEST_CASE("sweep: a log outside the requested range is reported and the page is retried", "[unit][sweep]") {
    Rig r;
    LogResult out;
    out.ok = true;
    out.provider = "infura";
    out.logs = {mklog(r.cfg, kSafeA, 999, 1, 0)};
    r.src.overrides.push_back(out);  // only the first answer is bad

    const auto res = run(r.sw->safe_step(110));
    CHECK(res.status == StepStatus::Advanced);  // the retry got a sound answer
    CHECK(r.sw->stats().inconsistent == 1);
    REQUIRE(r.src.bad.size() == 1);
    CHECK(r.src.bad[0].first == "infura");
    CHECK(r.src.bad[0].second.find("outside requested range") != std::string::npos);
    CHECK(r.src.calls.size() == 2);
    CHECK(r.sw->c_s() == 110);
}

TEST_CASE("sweep: a provider that keeps answering badly stalls the sweep after max_bad_attempts", "[unit][sweep]") {
    Rig r;
    LogResult out;
    out.ok = true;
    out.provider = "infura";
    out.logs = {mklog(r.cfg, kSafeA, 999, 1, 0)};
    for (int i = 0; i < 3; ++i) r.src.overrides.push_back(out);

    const auto res = run(r.sw->safe_step(110));
    CHECK(res.status == StepStatus::Stalled);
    CHECK(res.detail.find("no agreement after 3 attempts") != std::string::npos);
    CHECK(r.sw->stats().inconsistent == 3);
    CHECK(r.src.bad.size() == 3);
    CHECK(r.sink.emitted.empty());
    CHECK(r.sink.checkpoints.empty());
    CHECK(r.sw->c_s() == 100);
}

TEST_CASE("sweep: a log whose blockHash is not the canonical hash is never released", "[unit][sweep]") {
    Rig r;
    r.src.logs.push_back(mklog(r.cfg, kSafeA, 105, 1, 0));
    r.src.blocks[105] = binfo(9);  // canonical block 105 has another hash: the two sources never agree

    const auto res = run(r.sw->safe_step(110));
    CHECK(res.status == StepStatus::Stalled);
    CHECK(res.detail.find("blockHash") != std::string::npos);
    CHECK(r.sw->stats().inconsistent == 3);
    CHECK(r.src.bad.size() == 3);
    CHECK(r.sink.emitted.empty());
    CHECK(r.sw->c_s() == 100);
}

TEST_CASE("sweep: a stale block hash that resolves on the retry is released once", "[unit][sweep]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    LogResult stale;
    stale.ok = true;
    stale.provider = "infura";
    stale.logs = {mklog(r.cfg, kSafeA, 105, 7, 0)};  // a lagging replica's pre-reorg view
    r.src.overrides.push_back(stale);

    const auto res = run(r.sw->safe_step(110));
    REQUIRE(res.status == StepStatus::Advanced);
    CHECK(r.sw->stats().inconsistent == 1);
    REQUIRE(r.sink.emitted.size() == 1);
    CHECK(field(r.sink.emitted[0], "block_hash") == hash_of(1).to_hex());
}

TEST_CASE("sweep: a rate limit on a wide span halves the page once, none on a narrow one", "[unit][sweep]") {
    Rig r;  // infura: 500 blocks
    LogResult limited;
    limited.provider = "infura";
    limited.kind = evml::rpc::FailKind::RateLimited;
    limited.error = "429";
    r.src.overrides.push_back(limited);

    CHECK(run(r.sw->safe_step(2000)).status == StepStatus::Retry);
    CHECK(r.sw->stats().failure_shrinks == 1);
    CHECK(run(r.sw->safe_step(2000)).status == StepStatus::Advanced);
    CHECK(r.src.calls[0].range == BlockRange{101, 600});
    CHECK(r.src.calls[1].range == BlockRange{101, 350});  // 500 / 2

    // a span within min_range (10) is not a range problem
    Rig n;
    n.src.overrides.push_back(limited);
    CHECK(run(n.sw->safe_step(105)).status == StepStatus::Retry);
    CHECK(n.sw->stats().failure_shrinks == 0);
}

TEST_CASE("sweep: a timeout shrinks too, a quota error does not", "[unit][sweep]") {
    Rig r;
    LogResult slow;
    slow.provider = "infura";
    slow.kind = evml::rpc::FailKind::Retry;
    slow.error = "timeout";
    r.src.overrides.push_back(slow);
    CHECK(run(r.sw->safe_step(2000)).status == StepStatus::Retry);
    CHECK(r.sw->stats().failure_shrinks == 1);

    LogResult quota;
    quota.provider = "infura";
    quota.kind = evml::rpc::FailKind::QuotaExhausted;
    quota.error = "monthly capacity";
    r.src.overrides.push_back(quota);
    CHECK(run(r.sw->safe_step(2000)).status == StepStatus::Retry);
    CHECK(r.sw->stats().failure_shrinks == 1);
}

TEST_CASE("sweep: a page above the soft event cap is kept whole and the next page is smaller", "[unit][sweep]") {
    SweeperOptions opt;
    opt.max_page_logs = 2;
    Rig r(load_base_config(), opt);
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    r.add(mklog(r.cfg, kSafeA, 106, 2, 0), 2);
    r.add(mklog(r.cfg, kSafeA, 107, 3, 0), 3);

    const auto res = run(r.sw->safe_step(2000));
    REQUIRE(res.status == StepStatus::Advanced);
    CHECK(r.sink.emitted.size() == 3);  // nothing dropped
    CHECK(r.sw->stats().volume_shrinks == 1);
    CHECK(run(r.sw->safe_step(2000)).status == StepStatus::Advanced);
    CHECK(r.src.calls.back().range == BlockRange{601, 850});  // 500 / 2
}

TEST_CASE("sweep: max_bad_attempts must be at least 1", "[unit][sweep]") {
    SweeperOptions opt;
    opt.max_bad_attempts = 0;
    CHECK_THROWS_AS(Rig(load_base_config(), opt), std::invalid_argument);
}

TEST_CASE("sweep: an unreadable block is a retry", "[unit][sweep]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    r.src.unreadable_blocks.insert(105);
    const auto res = run(r.sw->safe_step(110));
    CHECK(res.status == StepStatus::Retry);
    CHECK(res.detail.find("105") != std::string::npos);
    CHECK(r.sink.emitted.empty());
}

TEST_CASE("sweep: with no contract of the class the cursor moves without asking the provider", "[unit][sweep]") {
    auto cfg = load_base_config();
    cfg.contracts.erase(std::remove_if(cfg.contracts.begin(), cfg.contracts.end(),
                                       [](const evml::core::ContractCfg& c) { return c.finality == Finality::Safe; }),
                        cfg.contracts.end());
    Rig r(std::move(cfg));
    const auto res = run(r.sw->safe_step(110));
    REQUIRE(res.status == StepStatus::Advanced);
    CHECK(*res.range == BlockRange{101, 110});
    CHECK(r.src.calls.empty());  // an empty address list would have meant "every address"
}

TEST_CASE("sweep: overlapping safe sweeps are absorbed by the in-process dedup", "[unit][sweep]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    r.sw->rewind_safe(100);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    CHECK(r.sink.emitted.size() == 1);
    CHECK(r.sw->stats().deduped == 1);
}

TEST_CASE("sweep: final step never passes the safe cursor and has no work before it", "[unit][sweep]") {
    Rig r;
    CHECK(run(r.sw->final_step(500)).status == StepStatus::NoWork);  // C_s == C_f

    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    const auto res = run(r.sw->final_step(500));
    REQUIRE(res.status == StepStatus::Advanced);
    CHECK(*res.range == BlockRange{101, 110});  // capped at C_s
    CHECK(r.sw->c_f() == 110);
}

TEST_CASE("sweep: final step emits finalized-class logs and leaves matched safe logs alone", "[unit][sweep]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    r.add(mklog(r.cfg, kGauge, 106, 2, 0), 2);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    REQUIRE(r.sink.emitted.size() == 1);  // only the safe-class log

    const auto res = run(r.sw->final_step(108));
    REQUIRE(res.status == StepStatus::Advanced);
    CHECK(*res.range == BlockRange{101, 108});
    CHECK_FALSE(res.safe_reorg);
    REQUIRE(r.sink.emitted.size() == 2);  // + the gauge log, the safe log was matched
    CHECK(field(r.sink.emitted[1], "finality") == "finalized");
    CHECK(field(r.sink.emitted[1], "contract") == "eurc_usdc_cl50_gauge");
    CHECK_FALSE(r.sink.emitted[1].released_block.has_value());
    CHECK(r.sw->stats().matched == 1);
    CHECK(r.sink.trims == std::vector<std::uint64_t>{108});
    CHECK(r.sink.stored.finalized == 108);
    CHECK(r.sink.released.empty());
}

TEST_CASE("sweep: a safe-class log the safe sweep missed is emitted late as finalized", "[unit][sweep]") {
    Rig r;
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);  // nothing there yet
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);                          // appears only for the finalized sweep

    const auto res = run(r.sw->final_step(110));
    REQUIRE(res.status == StepStatus::Advanced);
    CHECK_FALSE(res.safe_reorg);
    REQUIRE(r.sink.emitted.size() == 1);
    CHECK(field(r.sink.emitted[0], "finality") == "finalized");
    CHECK(field(r.sink.emitted[0], "type") == "log");
    CHECK_FALSE(r.sink.emitted[0].released_block.has_value());
    CHECK(r.sw->stats().late == 1);
    CHECK(r.sw->stats().last_late_block == 105);
}

TEST_CASE("sweep: a released safe log that is no longer canonical is reverted and replaced", "[unit][sweep]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    REQUIRE(r.sink.emitted.size() == 1);
    const auto original_id = r.sink.emitted[0].event_id;

    // L1 reorg below the safe tag: block 105 is re-mined with another hash
    r.src.logs.clear();
    r.add(mklog(r.cfg, kSafeA, 105, 2, 0), 2);

    const auto res = run(r.sw->final_step(110));
    REQUIRE(res.status == StepStatus::Advanced);
    CHECK(res.safe_reorg);
    REQUIRE(r.sink.emitted.size() == 3);
    CHECK(r.sink.emitted[1].type == EventType::Revert);
    CHECK(r.sink.emitted[1].event_id == original_id);
    CHECK(field(r.sink.emitted[1], "type") == "revert");
    CHECK(field(r.sink.emitted[1], "block_hash") == hash_of(1).to_hex());  // the retracted log's own fields
    CHECK(field(r.sink.emitted[1], "emitted_ms") == "1000");
    CHECK(r.sink.emitted[1].dedup_key == "dedup:" + original_id);
    CHECK(r.sink.emitted[2].type == EventType::Log);
    CHECK(field(r.sink.emitted[2], "block_hash") == hash_of(2).to_hex());
    CHECK(field(r.sink.emitted[2], "finality") == "finalized");
    CHECK(r.sw->stats().reverts == 1);
    CHECK(r.sw->stats().late == 1);
    CHECK(r.sink.released.empty());
}

TEST_CASE("sweep: a log that is canonical again after its revert is emitted again (I6)", "[unit][sweep]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    const auto id_a = r.sink.emitted[0].event_id;

    // block 105 is re-mined (hash 2): the final sweep retracts A and emits B late
    r.src.logs.clear();
    r.add(mklog(r.cfg, kSafeA, 105, 2, 0), 2);
    REQUIRE(run(r.sw->final_step(110)).status == StepStatus::Advanced);
    REQUIRE(r.sink.emitted.size() == 3);  // log A, revert A, log B

    // ...and flips back to hash 1: A is canonical again, so the old dedup state must not swallow it
    r.src.logs.clear();
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    r.sw->rewind_safe(100);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);

    REQUIRE(r.sink.emitted.size() == 4);
    CHECK(r.sink.emitted[3].event_id == id_a);
    CHECK(r.sink.emitted[3].type == EventType::Log);
    CHECK(r.sink.emitted[3].released_block.has_value());
    CHECK(r.sw->stats().deduped == 0);

    std::vector<EventType> seq_a;
    for (const auto& m : r.sink.emitted)
        if (m.event_id == id_a) seq_a.push_back(m.type);
    CHECK(seq_a == std::vector<EventType>{EventType::Log, EventType::Revert, EventType::Log});
}

TEST_CASE("sweep: a revert is not repeated when the same vanished entry is seen again", "[unit][sweep]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    const auto released_a = r.sink.released.begin()->second;  // the sink's record of A
    r.src.logs.clear();
    r.add(mklog(r.cfg, kSafeA, 105, 2, 0), 2);
    REQUIRE(run(r.sw->final_step(110)).status == StepStatus::Advanced);
    REQUIRE(r.sink.count(EventType::Revert) == 1);

    // a stale `released` record (e.g. a replayed sink) surfaces A again: the second revert is absorbed
    r.sink.released[released_a.event_id] = released_a;
    r.sw->rewind_safe(100);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    REQUIRE(run(r.sw->final_step(110)).status == StepStatus::Advanced);
    CHECK(r.sink.count(EventType::Revert) == 1);
    CHECK(r.sw->stats().reverts == 1);
    CHECK(r.sw->stats().deduped >= 1);
}

TEST_CASE("sweep: rewinding the safe cursor drags the final cursor along", "[unit][sweep]") {
    Rig r;
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    REQUIRE(run(r.sw->final_step(110)).status == StepStatus::Advanced);
    r.sw->rewind_safe(105);
    CHECK(r.sw->c_s() == 105);
    CHECK(r.sw->c_f() == 105);
    r.sw->rewind_safe(200);  // never moves forward
    CHECK(r.sw->c_s() == 105);
}

TEST_CASE("sweep: cursors violating C_f <= C_s are rejected", "[unit][sweep]") {
    Rig r;
    CHECK_THROWS_AS(r.sw->set_cursors(5, 10), std::invalid_argument);
}

TEST_CASE("reverify: nothing released above C_f is no work and asks nothing", "[unit][reverify]") {
    Rig r;  // C_s == C_f
    const auto res = run(r.sw->reverify_released());
    CHECK(res.status == StepStatus::NoWork);
    CHECK_FALSE(res.reorg);
    CHECK(r.src.calls.empty());
}

TEST_CASE("reverify: a window that is still canonical changes nothing", "[unit][reverify]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);

    const auto res = run(r.sw->reverify_released());
    CHECK(res.status == StepStatus::Advanced);
    CHECK_FALSE(res.reorg);
    CHECK_FALSE(res.fork_point.has_value());
    CHECK(r.sink.emitted.size() == 1);
    CHECK(r.sw->c_s() == 110);
    CHECK(r.sw->stats().matched == 1);
}

TEST_CASE("reverify: a reorg below the safe tag is reverted and replaced at once, C_s rewinds, C_f stays", "[unit][reverify]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);

    r.src.logs.clear();  // block 105 is re-mined with another hash
    r.add(mklog(r.cfg, kSafeA, 105, 2, 0), 2);

    const auto res = run(r.sw->reverify_released());
    REQUIRE(res.status == StepStatus::Advanced);
    CHECK(res.reorg);
    CHECK(res.fork_point == 104);
    CHECK(r.sw->c_s() == 104);
    CHECK(r.sw->c_f() == 100);

    REQUIRE(r.sink.emitted.size() == 3);
    CHECK(r.sink.emitted[1].type == EventType::Revert);
    CHECK(field(r.sink.emitted[1], "block_hash") == hash_of(1).to_hex());  // the retracted log's own fields
    CHECK(r.sink.emitted[2].type == EventType::Log);
    CHECK(field(r.sink.emitted[2], "block_hash") == hash_of(2).to_hex());
    CHECK(field(r.sink.emitted[2], "finality") == "safe");  // not final yet
    CHECK(r.sink.emitted[2].released_block.has_value());    // tracked for the final-sweep diff
    REQUIRE(r.sink.released.size() == 1);
    CHECK(r.sink.released.count(r.sink.emitted[2].event_id) == 1);
    CHECK(r.sw->stats().reverts == 1);
    CHECK(r.sw->stats().reverified_new == 1);
}

TEST_CASE("reverify: a block that flips A to B and back to A keeps alternating log and revert (I6)", "[unit][reverify]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    const auto id_a = r.sink.emitted[0].event_id;

    r.src.logs.clear();  // A -> B
    r.add(mklog(r.cfg, kSafeA, 105, 2, 0), 2);
    REQUIRE(run(r.sw->reverify_released()).reorg);
    const auto id_b = r.sink.emitted[2].event_id;
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);  // re-sweep after the rewind: B is absorbed
    CHECK(r.sink.emitted.size() == 3);

    r.src.logs.clear();  // B -> A
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    const auto res = run(r.sw->reverify_released());
    REQUIRE(res.status == StepStatus::Advanced);
    CHECK(res.reorg);
    CHECK(res.fork_point == 104);

    const auto seq = [&](const std::string& id) {
        std::vector<EventType> s;
        for (const auto& m : r.sink.emitted)
            if (m.event_id == id) s.push_back(m.type);
        return s;
    };
    CHECK(seq(id_a) == std::vector<EventType>{EventType::Log, EventType::Revert, EventType::Log});
    CHECK(seq(id_b) == std::vector<EventType>{EventType::Log, EventType::Revert});
    REQUIRE(r.sink.released.size() == 1);
    CHECK(r.sink.released.count(id_a) == 1);
}

TEST_CASE("reverify: a page that fails midway keeps the fork point for the next run", "[unit][reverify]") {
    Rig r;
    r.src.limit = 100;  // pages of 100 blocks
    r.add(mklog(r.cfg, kSafeA, 150, 1, 0), 1);
    r.add(mklog(r.cfg, kSafeA, 250, 3, 0), 3);
    for (int i = 0; i < 3; ++i) REQUIRE(run(r.sw->safe_step(400)).status == StepStatus::Advanced);
    REQUIRE(r.sw->c_s() == 400);
    REQUIRE(r.sink.released.size() == 2);

    r.src.logs.clear();  // block 150 is re-mined; block 250 is untouched
    r.add(mklog(r.cfg, kSafeA, 150, 2, 0), 2);
    r.add(mklog(r.cfg, kSafeA, 250, 3, 0), 3);
    r.src.unreadable_blocks.insert(250);  // the second page cannot be verified yet

    const auto first = run(r.sw->reverify_released());
    CHECK(first.status == StepStatus::Retry);
    CHECK(first.detail.find("250") != std::string::npos);
    CHECK_FALSE(first.reorg);
    CHECK(r.sw->c_s() == 400);  // not rewound until the window is complete
    CHECK(r.sink.count(EventType::Revert) == 1);

    r.src.unreadable_blocks.clear();
    const auto second = run(r.sw->reverify_released());
    REQUIRE(second.status == StepStatus::Advanced);
    CHECK(second.reorg);  // page 1 is already corrected, but the reorg is still reported
    CHECK(second.fork_point == 149);
    CHECK(r.sw->c_s() == 149);
    CHECK(r.sink.count(EventType::Revert) == 1);  // not repeated
}

TEST_CASE("reverify: a saturated sink pauses it without asking the provider", "[unit][reverify]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    r.src.calls.clear();
    r.sink.saturated_flag = true;
    CHECK(run(r.sw->reverify_released()).status == StepStatus::Paused);
    CHECK(r.src.calls.empty());
}

TEST_CASE("sweep: final step pauses without emitting or moving when the released set is unavailable", "[unit][sweep]") {
    Rig r;
    r.add(mklog(r.cfg, kGauge, 106, 2, 0), 2);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    const auto emitted = r.sink.emitted.size();
    const auto checkpoints = r.sink.checkpoints.size();

    r.sink.unreadable = true;
    const auto res = run(r.sw->final_step(108));
    CHECK(res.status == StepStatus::Paused);
    CHECK(r.sink.emitted.size() == emitted);
    CHECK(r.sink.checkpoints.size() == checkpoints);
    CHECK(r.sink.trims.empty());
    CHECK(r.sw->c_f() == 100);
    CHECK(r.sw->c_s() == 110);

    r.sink.unreadable = false;
    REQUIRE(run(r.sw->final_step(108)).status == StepStatus::Advanced);
    CHECK(r.sw->c_f() == 108);
    CHECK(r.sink.emitted.size() == emitted + 1);  // the gauge log
}

TEST_CASE("reverify: an unavailable released set is a pause, not a reorg", "[unit][reverify]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);

    r.sink.unreadable = true;
    const auto res = run(r.sw->reverify_released());
    CHECK(res.status == StepStatus::Paused);
    CHECK_FALSE(res.reorg);
    CHECK_FALSE(res.fork_point.has_value());
    CHECK(r.sw->c_s() == 110);

    r.sink.unreadable = false;
    const auto again = run(r.sw->reverify_released());
    CHECK(again.status == StepStatus::Advanced);
    CHECK_FALSE(again.reorg);
}

TEST_CASE("sweep: a sink that is not readable pauses the final step without fetching", "[unit][sweep]") {
    Rig r;
    r.add(mklog(r.cfg, kGauge, 106, 2, 0), 2);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    r.src.calls.clear();

    r.sink.readable_flag = false;
    for (int i = 0; i < 3; ++i) CHECK(run(r.sw->final_step(108)).status == StepStatus::Paused);
    CHECK(r.src.calls.empty());  // no getLogs while the released set could not be read anyway
    CHECK(r.sw->c_f() == 100);

    r.sink.readable_flag = true;
    CHECK(run(r.sw->final_step(108)).status == StepStatus::Advanced);
    CHECK(r.sw->c_f() == 108);
}

TEST_CASE("sweep: with nothing to do, a sink that is not readable is not reported as a pause", "[unit][sweep]") {
    Rig r;
    r.sink.readable_flag = false;
    CHECK(run(r.sw->final_step(100)).status == StepStatus::NoWork);  // C_f already at the ceiling
}

TEST_CASE("reverify: a sink that is not readable pauses it without fetching", "[unit][reverify]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    r.src.calls.clear();

    r.sink.readable_flag = false;
    const auto res = run(r.sw->reverify_released());
    CHECK(res.status == StepStatus::Paused);
    CHECK_FALSE(res.reorg);
    CHECK(r.src.calls.empty());

    r.sink.readable_flag = true;
    CHECK(run(r.sw->reverify_released()).status == StepStatus::Advanced);
}

TEST_CASE("sweep: on_page fires once per advanced page with the in-scope logs only", "[unit][sweep]") {
    struct Page {
        Finality kind;
        BlockRange range;
        std::vector<std::uint64_t> blocks;
    };
    std::vector<Page> pages;
    SweeperOptions opt;
    opt.on_page = [&pages](Finality kind, const BlockRange& range, const std::vector<evml::rpc::Log>& logs) {
        Page p{kind, range, {}};
        for (const auto& l : logs) p.blocks.push_back(l.block_number);
        pages.push_back(std::move(p));
    };
    Rig r(load_base_config(), opt);
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    r.add(mklog(r.cfg, kGauge, 106, 2, 0), 2);

    // NoWork and Paused: no call
    CHECK(run(r.sw->safe_step(100)).status == StepStatus::NoWork);
    r.sink.saturated_flag = true;
    CHECK(run(r.sw->safe_step(110)).status == StepStatus::Paused);
    r.sink.saturated_flag = false;
    CHECK(pages.empty());

    // safe page: the safe-class log only, after the checkpoint
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    REQUIRE(pages.size() == 1);
    CHECK(pages[0].kind == Finality::Safe);
    CHECK(pages[0].range == BlockRange{101, 110});
    CHECK(pages[0].blocks == std::vector<std::uint64_t>{105});

    // final page: the finalized-class log only (the matched safe log is not in scope)
    REQUIRE(run(r.sw->final_step(108)).status == StepStatus::Advanced);
    REQUIRE(pages.size() == 2);
    CHECK(pages[1].kind == Finality::Finalized);
    CHECK(pages[1].range == BlockRange{101, 108});
    CHECK(pages[1].blocks == std::vector<std::uint64_t>{106});

    // a retry does not call it
    LogResult bad;
    bad.provider = "infura";
    bad.kind = evml::rpc::FailKind::Retry;
    bad.error = "boom";
    r.src.overrides.push_back(bad);
    CHECK(run(r.sw->safe_step(120)).status == StepStatus::Retry);
    CHECK(pages.size() == 2);
}

TEST_CASE("sweep: a failed or paused step does not pollute recent dedup state before retry", "[unit][sweep]") {
    Rig r;
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    r.add(mklog(r.cfg, kSafeA, 106, 2, 0), 2);
    r.add(mklog(r.cfg, kGauge, 107, 3, 0), 3);

    // Block 106 is unreadable on the first safe_step attempt: neither 105 nor 106 may be admitted to recent_.
    r.src.unreadable_blocks.insert(106);
    CHECK(run(r.sw->safe_step(110)).status == StepStatus::Retry);
    CHECK(r.sink.emitted.empty());

    // Retry after block 106 becomes readable: both 105 and 106 must be emitted with zero false dedups.
    r.src.unreadable_blocks.clear();
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    CHECK(r.sink.emitted.size() == 2);
    CHECK(r.sw->stats().deduped == 0);

    // In final_step, if the sink's released_range() is unreadable after fetch_page() succeeded, nothing is admitted.
    r.sink.readable_flag = false;
    CHECK(run(r.sw->final_step(110)).status == StepStatus::Paused);
    CHECK(r.sink.emitted.size() == 2);

    r.sink.readable_flag = true;
    REQUIRE(run(r.sw->final_step(110)).status == StepStatus::Advanced);
    CHECK(r.sink.emitted.size() == 3);
    CHECK(r.sw->stats().deduped == 0);
}

TEST_CASE("sweep: backfill_step emits without touching checkpoints or final_trim or other contracts' released entries",
          "[unit][sweep]") {
    Rig r;
    // Pre-populate `released` with another contract's log at block 105 so a final_step diff would falsely retract it.
    r.add(mklog(r.cfg, kSafeB, 105, 1, 0), 1);
    REQUIRE(run(r.sw->safe_step(110)).status == StepStatus::Advanced);
    REQUIRE(r.sink.released.size() == 1);
    r.sink.emitted.clear();
    r.sink.checkpoints.clear();
    r.sink.trims.clear();

    // Now sweep (100, 110] via backfill_step for finalized (kGauge) and safe (kSafeA).
    r.src.logs.clear();
    r.add(mklog(r.cfg, kGauge, 104, 2, 0), 2);
    r.add(mklog(r.cfg, kSafeA, 108, 3, 0), 3);

    r.sw->set_cursors(100, 100);
    REQUIRE(run(r.sw->backfill_step(110, Finality::Finalized)).status == StepStatus::Advanced);
    CHECK(r.sw->c_s() == 110);
    CHECK(r.sink.checkpoints.empty());
    CHECK(r.sink.trims.empty());
    CHECK(r.sink.count(EventType::Revert) == 0);
    CHECK(r.sink.released.size() == 1);  // kSafeB's released entry untouched and no finalized log added to released
    REQUIRE(r.sink.emitted.size() == 1);
    CHECK(field(r.sink.emitted[0], "finality") == "finalized");

    r.sw->set_cursors(100, 100);
    REQUIRE(run(r.sw->backfill_step(110, Finality::Safe)).status == StepStatus::Advanced);
    CHECK(r.sink.checkpoints.empty());
    CHECK(r.sink.trims.empty());
    CHECK(r.sink.count(EventType::Revert) == 0);
    CHECK(r.sink.released.size() == 2);  // kSafeB + newly backfilled kSafeA
    REQUIRE(r.sink.emitted.size() == 2);
    CHECK(field(r.sink.emitted[1], "finality") == "safe");
}
