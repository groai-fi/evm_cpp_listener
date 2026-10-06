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

    explicit Rig(evml::core::Config c = load_base_config()) : cfg(std::move(c)) {
        sw = std::make_unique<Sweeper>(cfg, EventContext{"base_eurc_usdc", "test-host", "test"}, src, sink,
                                       [] { return std::uint64_t{1000}; });
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

TEST_CASE("sweep: a log outside the requested range discards the response", "[unit][sweep]") {
    Rig r;
    LogResult out;
    out.ok = true;
    out.provider = "infura";
    out.logs = {mklog(r.cfg, kSafeA, 999, 1, 0)};
    r.src.overrides.push_back(out);

    const auto res = run(r.sw->safe_step(110));
    CHECK(res.status == StepStatus::Retry);
    CHECK(r.sw->stats().inconsistent == 1);
    CHECK(r.sink.emitted.empty());
    CHECK(r.sw->c_s() == 100);
}

TEST_CASE("sweep: a log whose blockHash is not the canonical hash discards the response", "[unit][sweep]") {
    Rig r;
    r.src.logs.push_back(mklog(r.cfg, kSafeA, 105, 1, 0));
    r.src.blocks[105] = binfo(9);  // canonical block 105 has another hash

    const auto res = run(r.sw->safe_step(110));
    CHECK(res.status == StepStatus::Retry);
    CHECK(res.detail.find("blockHash") != std::string::npos);
    CHECK(r.sw->stats().inconsistent == 1);
    CHECK(r.sink.emitted.empty());
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
