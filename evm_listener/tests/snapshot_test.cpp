#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>

#include "ops/alerts.hpp"
#include "ops/snapshot.hpp"

using evml::ops::AlertClock;
using evml::ops::AlertEngine;
using evml::ops::AlertEvent;
using evml::ops::AlertThresholds;
using evml::ops::SnapshotBuilder;
using evml::ops::SnapshotInputs;
using namespace std::chrono_literals;

namespace {

const AlertClock::time_point t0 = AlertClock::time_point{} + 1000s;

SnapshotInputs leader_inputs(AlertClock::time_point now = t0) {
    SnapshotInputs in;
    in.now = now;
    in.wall_ms = 1'000'000;
    in.leader = true;
    in.chain = "base";
    in.head = 5000;
    in.driver.state = evml::pipeline::PipelineState::Live;
    in.driver.c_s = 4990;
    in.driver.c_f = 4900;
    in.driver.g_s = 4995;
    in.driver.g_f = 4950;
    return in;
}

const AlertEvent* find(const evml::ops::OpsSnapshot& s, AlertEvent::Kind k) {
    for (const auto& e : s.events)
        if (e.kind == k) return &e;
    return nullptr;
}

}  // namespace

TEST_CASE("snapshot: a leader's driver, sink and ring facts are copied", "[snapshot]") {
    SnapshotBuilder b;
    auto in = leader_inputs();
    in.sink.spill_bytes = 123;
    in.spill_max_bytes = 1000;
    in.sink_saturated = true;
    in.safe_late_total = 7;
    in.driver.frozen = true;
    in.driver.frozen_reason = "finalized regressed";
    const auto s = b.build(in);

    CHECK(s.leader);
    CHECK(s.chain == "base");
    CHECK(s.head == 5000);
    CHECK(s.g_s == 4995);
    CHECK(s.g_f == 4950);
    CHECK(s.c_s == 4990);
    CHECK(s.c_f == 4900);
    CHECK(s.frozen);
    CHECK(s.frozen_reason == "finalized regressed");
    CHECK(s.sink_saturated);
    CHECK(s.spill_bytes == 123);
    CHECK(s.spill_max_bytes == 1000);
    CHECK(s.safe_late_total == 7);
    CHECK(s.stalled_detail.empty());
    CHECK(s.events.empty());
}

TEST_CASE("snapshot: stalled_detail is set only while the driver reports a stalled sweep", "[snapshot]") {
    SnapshotBuilder b;
    auto in = leader_inputs();
    in.driver.stall_detail = "old detail";
    CHECK(b.build(in).stalled_detail.empty());  // not stalled: a leftover detail is not reported
    in.driver.stalled = true;
    CHECK(b.build(in).stalled_detail == "old detail");
}

TEST_CASE("snapshot: a standby carries no leader facts but still delivers queued events once", "[snapshot]") {
    SnapshotBuilder b;
    AlertEvent fenced;
    fenced.kind = AlertEvent::Kind::SinkFenced;
    fenced.message = "fenced";
    b.push_event(fenced);

    SnapshotInputs in;
    in.now = t0;
    in.leader = false;
    in.driver.c_s = 99;  // must be ignored
    const auto s = b.build(in);
    CHECK_FALSE(s.leader);
    CHECK(s.c_s == 0);
    CHECK_FALSE(s.head.has_value());
    REQUIRE(s.events.size() == 1);
    CHECK(s.events[0].kind == AlertEvent::Kind::SinkFenced);
    CHECK(b.build(in).events.empty());  // delivered once
}

TEST_CASE("snapshot: counter increases become one event each; the first leader build only sets the baseline",
          "[snapshot]") {
    SnapshotBuilder b;
    auto in = leader_inputs();
    in.driver.safe_reorgs = 2;  // already non-zero at the first build: not an event
    in.sink.below_finalized_total = 1;
    in.ws_missed_total = 5;
    CHECK(b.build(in).events.empty());
    CHECK(b.build(in).events.empty());  // unchanged

    in.driver.safe_reorgs = 3;
    in.driver.last_fork_point = 4980;
    in.sink.below_finalized_total = 4;
    in.ws_missed_total = 6;
    const auto s = b.build(in);
    REQUIRE(s.events.size() == 3);
    const auto* reorg = find(s, AlertEvent::Kind::SafeReorg);
    REQUIRE(reorg);
    CHECK(reorg->details.at("new_reorgs").as_uint64() == 1);
    CHECK(reorg->details.at("fork_point").as_uint64() == 4980);
    const auto* inv = find(s, AlertEvent::Kind::SinkInvariant);
    REQUIRE(inv);
    CHECK(inv->details.at("new_refusals").as_uint64() == 3);
    const auto* miss = find(s, AlertEvent::Kind::WsMissedLogs);
    REQUIRE(miss);
    CHECK(miss->details.at("new_missed").as_uint64() == 1);

    CHECK(b.build(in).events.empty());  // the baseline moved up
}

TEST_CASE("snapshot: a new leadership term restarts the counter baseline", "[snapshot]") {
    SnapshotBuilder b;
    auto in = leader_inputs();
    in.driver.safe_reorgs = 5;
    b.build(in);

    SnapshotInputs standby;
    standby.now = t0 + 10s;
    standby.leader = false;
    b.build(standby);  // the term ended

    in = leader_inputs(t0 + 20s);
    in.driver.safe_reorgs = 0;  // a fresh driver
    CHECK(b.build(in).events.empty());
    in.driver.safe_reorgs = 1;
    CHECK(find(b.build(in), AlertEvent::Kind::SafeReorg) != nullptr);
}

TEST_CASE("snapshot: consumer probes are converted; a never-probed group is skipped", "[snapshot]") {
    SnapshotBuilder b;
    auto in = leader_inputs();
    in.retention_s = 604800;

    evml::sink::ConsumerLag probed;
    probed.stream = evml::core::StreamKind::Events;
    probed.group = "indexer";
    probed.undelivered_age_s = 30;
    probed.pending_age_s = 5;
    probed.probed_at_ms = in.wall_ms - 20'000;  // probed 20 s ago
    probed.trimmed_unread = true;               // lag stays nullopt: Redis could not compute it

    evml::sink::ConsumerLag missing;
    missing.stream = evml::core::StreamKind::Registry;
    missing.group = "registry-reader";
    missing.missing = true;
    missing.probed_at_ms = in.wall_ms;
    missing.lag = 12;

    evml::sink::ConsumerLag never;  // probed_at_ms == 0
    never.group = "never";

    in.consumers = {probed, missing, never};
    const auto s = b.build(in);
    REQUIRE(s.consumers.size() == 2);

    CHECK(s.consumers[0].stream == "events");
    CHECK(s.consumers[0].group == "indexer");
    CHECK(s.consumers[0].found);
    CHECK(s.consumers[0].lag == -1);
    CHECK(s.consumers[0].trimmed_unread);
    CHECK(s.consumers[0].retention_s == 604800);
    CHECK(s.consumers[0].undelivered_age_s == 30.0);
    CHECK(s.consumers[0].sampled_at == in.now - 20s);

    CHECK(s.consumers[1].stream == "registry");
    CHECK_FALSE(s.consumers[1].found);
    CHECK(s.consumers[1].lag == 12);
    CHECK(s.consumers[1].sampled_at == in.now);
}

TEST_CASE("snapshot: feeding the engine turns a frozen release and a counter jump into alerts", "[snapshot]") {
    SnapshotBuilder b;
    AlertEngine engine{AlertThresholds{}};

    auto in = leader_inputs();
    CHECK(engine.evaluate(b.build(in)).empty());  // a healthy leader is silent

    in = leader_inputs(t0 + 10s);
    in.driver.frozen = true;
    in.driver.frozen_reason = "finalized regressed";
    in.driver.safe_reorgs = 1;
    const auto alerts = engine.evaluate(b.build(in));
    bool gate = false, reorg = false;
    for (const auto& a : alerts) {
        gate |= a.title.find("Gate Regressed") != std::string::npos;
        reorg |= a.title == "Safe Reorg";
    }
    CHECK(gate);
    CHECK(reorg);
}
