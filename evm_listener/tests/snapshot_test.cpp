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

TEST_CASE("snapshot: provider health becomes quota / auth facts and any_healthy_provider", "[snapshot]") {
    using evml::provider::ProviderHealth;
    using evml::provider::State;
    SnapshotBuilder b;
    auto in = leader_inputs();
    ProviderHealth up{"infura", State::Up, 0, std::nullopt, "", std::nullopt};
    ProviderHealth quota{"alchemy", State::Cooldown, 200, -32005, "rpc error -32005: Monthly capacity limit exceeded", 90'000ms};
    ProviderHealth auth{"ankr", State::Disabled, 401, std::nullopt, "http 401: bad key", 30'000ms};
    in.providers = {up, quota, auth};
    auto s = b.build(in);

    REQUIRE(s.providers.size() == 3);
    CHECK(s.any_healthy_provider);
    CHECK_FALSE(s.providers[0].quota_exhausted);
    CHECK_FALSE(s.providers[0].auth_http_status);
    CHECK(s.providers[1].quota_exhausted);
    CHECK(s.providers[1].quota_error_code == "-32005");
    CHECK(s.providers[1].quota_message == "rpc error -32005: Monthly capacity limit exceeded");
    CHECK(s.providers[1].next_probe_ts == 1000 + 90);  // wall_ms 1'000'000 -> 1000 s, plus 90 s
    CHECK(s.providers[2].auth_http_status == 401);
    CHECK_FALSE(s.providers[2].quota_exhausted);

    in.providers = {quota, auth};
    CHECK_FALSE(b.build(in).any_healthy_provider);
}

TEST_CASE("snapshot: an increase of a provider's inconsistency total is one event naming that provider", "[snapshot]") {
    SnapshotBuilder b;
    auto in = leader_inputs();
    in.provider_inconsistent_total = {{"infura", 2}, {"alchemy", 0}};
    CHECK(b.build(in).events.empty());  // the first build of a term only sets the baseline

    in = leader_inputs(t0 + 10s);
    in.provider_inconsistent_total = {{"infura", 2}, {"alchemy", 1}};
    auto s = b.build(in);
    REQUIRE(s.events.size() == 1);
    CHECK(s.events[0].kind == AlertEvent::Kind::ProviderInconsistent);
    CHECK(s.events[0].subject == "alchemy");

    in = leader_inputs(t0 + 20s);
    in.provider_inconsistent_total = {{"infura", 2}, {"alchemy", 1}};
    CHECK(b.build(in).events.empty());  // no increase, no event
}

TEST_CASE("snapshot: head silence and the two safe tags reach the engine; a stalled WS and a lagging getLogs alert", "[snapshot]") {
    SnapshotBuilder b;
    AlertEngine engine{AlertThresholds{}};  // head_stall_ms 30000, getlogs lag 30 blocks for 120 s
    auto in = leader_inputs();
    in.ms_since_head = 45'000;
    in.ws_provider = "alchemy";
    in.getlogs_provider = "infura";
    in.ws_safe = 5000;
    in.getlogs_safe = 4900;
    const auto s0 = b.build(in);
    CHECK(s0.ms_since_head == 45'000);
    CHECK(s0.ws_provider == "alchemy");
    CHECK(s0.getlogs_provider == "infura");
    CHECK(s0.ws_safe == 5000);
    CHECK(s0.getlogs_safe == 4900);

    SnapshotBuilder b2;
    AlertEngine e2{AlertThresholds{}};
    bool stalled = false, lagging = false;
    for (int i = 0; i <= 13; ++i) {  // 130 s of the same lag
        auto x = in;
        x.now = t0 + std::chrono::seconds(10 * i);
        for (const auto& a : e2.evaluate(b2.build(x))) {
            stalled |= a.title == "WebSocket Stalled";
            lagging |= a.title == "getLogs Provider Lagging";
        }
    }
    CHECK(stalled);
    CHECK(lagging);

    // Unset facts stay silent.
    auto quiet = leader_inputs(t0 + 500s);
    CHECK(engine.evaluate(b.build(quiet)).empty());
}

TEST_CASE("snapshot: host-local Redis facts are copied in any role, and raise Redis alerts for a standby", "[snapshot]") {
    SnapshotBuilder b;
    AlertEngine engine{AlertThresholds{}};  // redis_unreachable_s = 60
    SnapshotInputs in;  // not leader
    in.now = t0;
    in.chain = "base";
    in.redis_unreachable_s = 95;
    in.addr_family = "ipv6";
    in.redis_misconfigured = evml::ops::RedisMisconfig{"allkeys-lru", "no", "8.4.7"};
    const auto s = b.build(in);
    CHECK_FALSE(s.leader);
    CHECK(s.redis_unreachable_s == 95);
    CHECK(s.addr_family == "ipv6");
    REQUIRE(s.redis_misconfigured);
    CHECK(s.redis_misconfigured->maxmemory_policy == "allkeys-lru");

    bool unreachable = false, misconfigured = false;
    for (const auto& a : engine.evaluate(s)) {
        unreachable |= a.title == "Redis Unreachable";
        misconfigured |= a.title == "Redis Misconfigured";
    }
    CHECK(unreachable);
    CHECK(misconfigured);

    in.redis_unreachable_s = 30;  // below the 60 s threshold
    in.redis_misconfigured.reset();
    SnapshotBuilder b2;
    AlertEngine e2{AlertThresholds{}};
    CHECK(e2.evaluate(b2.build(in)).empty());
}

TEST_CASE("snapshot: the sink's oldest pending write age becomes oldest_record_age_s", "[snapshot]") {
    SnapshotBuilder b;
    auto in = leader_inputs();
    in.sink.oldest_record_age_s = 420;
    CHECK(b.build(in).oldest_record_age_s == 420);
}
