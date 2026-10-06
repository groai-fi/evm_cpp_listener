#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <memory>

#include "pipeline/coverage.hpp"
#include "pipeline/pipeline_driver.hpp"
#include "sweep_fakes.hpp"

using namespace evml::pipeline;
using namespace evml::test;

namespace {

constexpr std::size_t kSafeA = 0;
constexpr std::uint64_t kAfterStart = 52061000;

struct Rig {
    evml::core::Config cfg;
    FakeSource src;
    MemorySink sink;
    std::uint64_t now{0};
    std::unique_ptr<Sweeper> sw;
    std::unique_ptr<PipelineDriver> drv;

    explicit Rig(evml::core::Config c = load_base_config()) : cfg(std::move(c)) {
        sw = std::make_unique<Sweeper>(cfg, EventContext{"base_eurc_usdc", "test-host", "test"}, src, sink,
                                       [this] { return now; });
        drv = std::make_unique<PipelineDriver>(cfg, *sw, sink, src, [this] { return now; });
    }

    // A previous run that stopped at (safe, finalized) with the current manifest.
    void seed(std::uint64_t safe, std::uint64_t fin) {
        sink.stored.safe = safe;
        sink.stored.finalized = fin;
        sink.stored.contracts = contract_manifest(cfg);
    }
    TickResult tick() { return run(drv->tick()); }
    // Seeds, then runs the recovery tick.
    void start(std::uint64_t safe, std::uint64_t fin) {
        seed(safe, fin);
        REQUIRE(tick().wait.count() == 0);
        REQUIRE(drv->status().state == PipelineState::CatchingUp);
    }
    void add(const evml::rpc::Log& l, std::uint8_t canonical_hash) {
        src.logs.push_back(l);
        src.blocks[l.block_number] = binfo(canonical_hash);
    }
};

std::vector<std::string> manifest_without(const evml::core::Config& cfg, std::size_t i) {
    auto m = contract_manifest(cfg);
    const auto prefix = cfg.contracts.at(i).address.to_hex() + ":";
    m.erase(std::remove_if(m.begin(), m.end(), [&](const std::string& e) { return e.rfind(prefix, 0) == 0; }), m.end());
    return m;
}

LogResult retry_failure() {
    LogResult bad;
    bad.provider = "infura";
    bad.kind = evml::rpc::FailKind::Retry;
    bad.error = "boom";
    return bad;
}

}  // namespace

TEST_CASE("driver: invalid options are rejected", "[unit][driver]") {
    Rig r;
    DriverOptions opt;
    opt.final_every = 0;
    CHECK_THROWS_AS(PipelineDriver(r.cfg, *r.sw, r.sink, r.src, [] { return std::uint64_t{0}; }, opt),
                    std::invalid_argument);
    auto cfg = load_base_config();
    cfg.watchdog.progress_stall_ms = 0;
    CHECK_THROWS_AS(PipelineDriver(cfg, *r.sw, r.sink, r.src, [] { return std::uint64_t{0}; }), std::invalid_argument);
}

TEST_CASE("driver: a first run waits for the safe gate then writes cursors and manifest", "[unit][driver]") {
    Rig r;
    const auto waiting = r.tick();
    CHECK(waiting.wait.count() == 1000);
    CHECK_FALSE(waiting.done);
    CHECK(r.drv->status().state == PipelineState::Recovering);
    CHECK(r.sink.checkpoints.empty());
    CHECK(r.sink.contract_writes == 0);

    r.drv->set_gates(52060100, 52060000);
    CHECK(r.tick().wait.count() == 0);
    CHECK(r.drv->status().state == PipelineState::CatchingUp);
    CHECK(r.sw->c_s() == 52059999);
    CHECK(r.sw->c_f() == 52059999);
    REQUIRE(r.sink.checkpoints.size() == 2);
    CHECK(r.sink.stored.safe == 52059999);
    CHECK(r.sink.stored.finalized == 52059999);
    CHECK(r.sink.contract_writes == 1);
    CHECK(r.sink.stored.contracts == contract_manifest(r.cfg));
}

TEST_CASE("driver: a restart restores the cursors and rewrites nothing", "[unit][driver]") {
    Rig r;
    r.start(kAfterStart, kAfterStart - 50);
    CHECK(r.sw->c_s() == kAfterStart);
    CHECK(r.sw->c_f() == kAfterStart - 50);
    CHECK(r.sink.checkpoints.empty());
    CHECK(r.sink.contract_writes == 0);
}

TEST_CASE("driver: a partial checkpoint is fatal and terminal", "[unit][driver]") {
    Rig r;
    r.sink.stored.safe = 100;
    const auto t = r.tick();
    CHECK(t.done);
    const auto st = r.drv->status();
    CHECK(st.state == PipelineState::Failed);
    CHECK(st.fatal.find("partial checkpoint") != std::string::npos);
    CHECK(r.tick().done);
    CHECK(r.sink.checkpoints.empty());
}

TEST_CASE("driver: an uncovered contract is fatal and named", "[unit][driver]") {
    Rig r;
    r.seed(kAfterStart, kAfterStart);
    r.sink.stored.contracts = manifest_without(r.cfg, 1);
    CHECK(r.tick().done);
    const auto st = r.drv->status();
    CHECK(st.state == PipelineState::Failed);
    CHECK(st.fatal.find(r.cfg.contracts[1].name) != std::string::npos);
    CHECK(st.fatal.find("--backfill-contract") != std::string::npos);
}

TEST_CASE("driver: a covered manifest difference is rewritten once", "[unit][driver]") {
    Rig r;
    r.seed(kAfterStart, kAfterStart);
    r.cfg.contracts[0].start_block = 52060500;  // later than stored: covered
    REQUIRE(r.sink.stored.contracts != contract_manifest(r.cfg));
    CHECK(r.tick().wait.count() == 0);
    CHECK(r.drv->status().state == PipelineState::CatchingUp);
    CHECK(r.sink.contract_writes == 1);
    CHECK(r.sink.stored.contracts == contract_manifest(r.cfg));
}

TEST_CASE("driver: catch-up runs safe first, never finalizes past a safe checkpoint, then goes live", "[unit][driver]") {
    Rig r;
    r.start(100, 100);
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    r.drv->set_gates(110, 108);

    CHECK(r.drv->status().state == PipelineState::CatchingUp);
    for (int i = 0; i < 2; ++i) CHECK(r.tick().wait.count() == 0);
    CHECK(r.sw->c_s() == 110);
    CHECK(r.sw->c_f() == 108);

    const auto idle = r.tick();
    CHECK(idle.wait.count() == 1000);
    CHECK(r.drv->status().state == PipelineState::Live);

    std::uint64_t max_safe = 0;
    for (const auto& [kind, block] : r.sink.checkpoints) {
        if (kind == CheckpointKind::Safe) max_safe = std::max(max_safe, block);
        else CHECK(block <= max_safe);
    }
    CHECK(r.sink.emitted.size() == 1);
}

TEST_CASE("driver: a retry backs off with the cursor unchanged", "[unit][driver]") {
    Rig r;
    r.start(100, 100);
    r.drv->set_gates(110, 100);
    r.src.overrides.push_back(retry_failure());
    const auto t = r.tick();
    CHECK(t.wait.count() <= 250);
    CHECK_FALSE(t.done);
    CHECK(r.sw->c_s() == 100);
    CHECK(r.sink.emitted.empty());

    CHECK(r.tick().wait.count() == 0);  // next tick succeeds
    CHECK(r.sw->c_s() == 110);
}

TEST_CASE("driver: a stalled sweep is reported until progress clears it", "[unit][driver]") {
    Rig r;
    r.start(100, 100);
    r.drv->set_gates(2000, 100);
    r.src.reject_all = true;
    const auto t = r.tick();
    CHECK(t.wait.count() <= 250);
    auto st = r.drv->status();
    CHECK(st.stalled);
    CHECK(st.stall_detail.find("single address") != std::string::npos);
    CHECK(r.sw->c_s() == 100);

    r.src.reject_all = false;
    CHECK(r.tick().wait.count() == 0);
    st = r.drv->status();
    CHECK_FALSE(st.stalled);
    CHECK(st.stall_detail.empty());
    CHECK(r.sw->c_s() > 100);
}

TEST_CASE("driver: missed progress deadlines report the provider and reset on progress", "[unit][driver]") {
    Rig r;
    r.start(100, 100);
    r.drv->set_gates(110, 100);
    const std::uint64_t deadline = r.cfg.watchdog.progress_stall_ms;
    for (int i = 0; i < 4; ++i) r.src.overrides.push_back(retry_failure());

    for (int i = 0; i < 4; ++i) {
        r.now = static_cast<std::uint64_t>(i) * deadline;
        r.tick();
    }
    CHECK(r.drv->status().safe_missed_deadlines == 3);
    REQUIRE(r.src.bad.size() == 3);
    CHECK(r.src.bad[0].first == "infura");
    CHECK(r.src.bad[0].second.find("no safe sweep progress") != std::string::npos);
    CHECK(r.drv->status().final_missed_deadlines == 0);

    r.now += 1;
    r.tick();  // succeeds: C_s moves
    CHECK(r.sw->c_s() == 110);
    r.tick();  // observes the new cursor
    CHECK(r.drv->status().safe_missed_deadlines == 0);
}

TEST_CASE("driver: a safe reorg signal reverifies before anything else", "[unit][driver]") {
    Rig r;
    r.start(100, 100);
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    r.drv->set_gates(110, 100);
    REQUIRE(r.tick().wait.count() == 0);
    REQUIRE(r.sw->c_s() == 110);

    r.src.logs.clear();  // block 105 is re-mined with another hash
    r.add(mklog(r.cfg, kSafeA, 105, 2, 0), 2);
    r.drv->on_safe_reorg_signal("safe tag moved back");
    CHECK(r.drv->status().reverify_pending);

    CHECK(r.tick().wait.count() == 0);
    const auto st = r.drv->status();
    CHECK_FALSE(st.reverify_pending);
    CHECK(st.safe_reorgs == 1);
    CHECK(st.last_fork_point == 104);
    CHECK(r.sink.count(EventType::Revert) == 1);
    CHECK(r.sw->c_s() == 104);
}

TEST_CASE("driver: a final sweep that finds a vanished safe log triggers a reverify that clears the flag",
          "[unit][driver]") {
    Rig r;
    r.start(100, 100);
    r.add(mklog(r.cfg, kSafeA, 105, 1, 0), 1);
    r.drv->set_gates(110, 100);
    REQUIRE(r.tick().wait.count() == 0);  // safe sweep releases the log

    r.src.logs.clear();
    r.add(mklog(r.cfg, kSafeA, 105, 2, 0), 2);
    r.drv->set_gates(110, 110);
    REQUIRE(r.tick().wait.count() == 0);  // final sweep sees the difference
    REQUIRE(r.drv->status().reverify_pending);

    r.tick();  // reverify runs whatever it finds
    CHECK_FALSE(r.drv->status().reverify_pending);
    CHECK(r.sink.count(EventType::Revert) == 1);
}

TEST_CASE("driver: draining stops all work", "[unit][driver]") {
    Rig r;
    r.start(100, 100);
    r.drv->set_gates(110, 110);
    r.drv->request_drain();
    CHECK(r.tick().done);
    CHECK(r.drv->status().state == PipelineState::Draining);
    CHECK(r.src.calls.empty());
    CHECK(r.sink.emitted.empty());
}

TEST_CASE("driver: a frozen gate makes no source calls", "[unit][driver]") {
    Rig r;
    r.start(100, 100);
    r.drv->set_gates(110, 110);
    r.drv->on_gate_frozen("finalized went backwards");
    const auto t = r.tick();
    CHECK(t.wait.count() == 1000);
    CHECK_FALSE(t.done);
    CHECK(r.src.calls.empty());
    const auto st = r.drv->status();
    CHECK(st.frozen);
    CHECK(st.frozen_reason == "finalized went backwards");
}

TEST_CASE("driver: a saturated sink makes the driver wait the pause poll", "[unit][driver]") {
    Rig r;
    r.start(100, 100);
    r.drv->set_gates(110, 100);
    r.sink.saturated_flag = true;
    const auto t = r.tick();
    CHECK(t.wait.count() == 200);
    CHECK(r.src.calls.empty());
    CHECK(r.sw->c_s() == 100);
}
