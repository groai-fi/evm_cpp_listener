// The §5.3.3 alert table as a contract (plan 60, slice 5-E4c). One row per table row. Each row is driven through the
// real producers (AlertEngine, and SnapshotBuilder / the event helpers for the event rows) and checked for:
//   - title and severity,
//   - kind: a condition fires once and resolves once ("RESOLVED: <title>"), an event fires once and never resolves,
//   - every `details` key the table names.
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <functional>
#include <string>
#include <vector>

#include "ops/alerts.hpp"
#include "ops/snapshot.hpp"

using namespace evml::ops;
using namespace std::chrono_literals;

namespace {

const AlertClock::time_point kT0 = AlertClock::time_point{} + 1000s;

// A healthy leader snapshot at kT0 + at: the gates advance with `at`, so no stuck rule fires unless a row freezes them.
OpsSnapshot snap(std::chrono::seconds at) {
    OpsSnapshot s;
    s.now = kT0 + at;
    s.leader = true;
    s.chain = "base";
    s.head = 1000 + at.count();
    s.g_s = 1000 + at.count();
    s.g_f = 5000 + at.count();
    s.c_s = *s.g_s;
    s.c_f = *s.g_f;
    return s;
}

// The same, as the host feeds the builder.
SnapshotInputs leader_in(std::chrono::seconds at) {
    SnapshotInputs in;
    in.now = kT0 + at;
    in.wall_ms = 1'000'000;
    in.leader = true;
    in.chain = "base";
    in.head = 1000 + static_cast<std::uint64_t>(at.count());
    in.driver.state = evml::pipeline::PipelineState::Live;
    in.driver.g_s = 1000 + static_cast<std::uint64_t>(at.count());
    in.driver.g_f = 5000 + static_cast<std::uint64_t>(at.count());
    in.driver.c_s = *in.driver.g_s;
    in.driver.c_f = *in.driver.g_f;
    return in;
}

ConsumerFact consumer(const OpsSnapshot& s) {
    ConsumerFact f;
    f.stream = "events";
    f.group = "bot";
    f.sampled_at = s.now;  // a fresh probe
    f.found = true;
    f.retention_s = 604800;
    f.lag = 5;
    return f;
}

struct Run {
    std::vector<Alert> fire;  // what the engine returned when the row's condition / event appeared
    std::vector<Alert> next;  // what it returned on the following snapshot (condition cleared; event not repeated)
};

struct Row {
    std::string title;
    Severity severity;
    bool event;
    std::vector<std::string> spec_keys;  // the `details` column of §5.3.3
    std::function<Run()> run;
};

// A condition row: `bad` is applied at each listed time, then `good` at +10 s after the last one.
Run condition(const std::vector<int>& bad_at, const std::function<void(OpsSnapshot&)>& bad,
              const std::function<OpsSnapshot(std::chrono::seconds)>& base = snap) {
    AlertEngine e{AlertThresholds{}};
    e.evaluate(base(0s));  // baseline: trackers start from a healthy snapshot
    Run r;
    for (const int t : bad_at) {
        auto s = base(std::chrono::seconds(t));
        bad(s);
        r.fire = e.evaluate(s);
    }
    r.next = e.evaluate(snap(std::chrono::seconds(bad_at.back() + 10)));
    return r;
}

// A gate that stays where it was at 0 s (the "stuck" rows).
OpsSnapshot frozen(std::chrono::seconds at) {
    auto s = snap(0s);
    s.now = kT0 + at;
    return s;
}

// An event row driven through the builder: `before` sets the baseline, `after` the occurrence 10 s later.
Run builder_event(const std::function<void(SnapshotInputs&)>& before, const std::function<void(SnapshotInputs&)>& after) {
    AlertEngine e{AlertThresholds{}};
    SnapshotBuilder b;
    auto in0 = leader_in(0s);
    before(in0);
    e.evaluate(b.build(in0));
    auto in1 = leader_in(10s);
    after(in1);
    Run r;
    r.fire = e.evaluate(b.build(in1));
    auto in2 = leader_in(20s);
    after(in2);  // the same totals again: no new occurrence
    r.next = e.evaluate(b.build(in2));
    return r;
}

// An event the host pushes.
Run pushed_event(const AlertEvent& ev) {
    AlertEngine e{AlertThresholds{}};
    SnapshotBuilder b;
    e.evaluate(b.build(leader_in(0s)));
    b.push_event(ev);
    Run r;
    r.fire = e.evaluate(b.build(leader_in(10s)));
    r.next = e.evaluate(b.build(leader_in(20s)));
    return r;
}

std::vector<Row> rows() {
    const std::vector<std::string> consumer_keys{"stream", "group", "undelivered_age_s", "pending_age_s", "lag", "retention_s"};
    std::vector<Row> t;

    t.push_back({"WebSocket Stalled", Severity::Critical, false,
                 {"seconds_since_last_head", "liveness_mode", "head_lag_blocks", "last_processed_block", "provider"}, [] {
                     return condition({10}, [](OpsSnapshot& s) {
                         s.ms_since_head = 45000;
                         s.ws_provider = "alchemy";
                     });
                 }});
    t.push_back({"Provider Failover", Severity::Warning, true, {"from", "to", "reason"},
                 [] { return pushed_event(provider_failover_event("quicknode", "infura", "failover")); }});
    t.push_back({"Provider Quota Exhausted", Severity::Warning, false,
                 {"provider", "chain", "error_code", "message", "next_probe_ts"}, [] {
                     return condition({10}, [](OpsSnapshot& s) {
                         ProviderFact q;
                         q.name = "alchemy";
                         q.quota_exhausted = true;
                         q.quota_error_code = "429";
                         q.quota_message = "Monthly capacity limit exceeded";
                         q.next_probe_ts = 2000;
                         s.providers = {q};
                     });
                 }});
    t.push_back({"getLogs Provider Lagging", Severity::Warning, false, {"provider", "its_safe", "ws_safe"}, [] {
                     return condition({10, 140}, [](OpsSnapshot& s) {  // 130 s above 30 blocks: past the 2 min
                         s.getlogs_provider = "infura";
                         s.getlogs_safe = 100;
                         s.ws_safe = 200;
                     });
                 }});
    t.push_back({"Provider Auth Failure", Severity::Critical, false, {"provider", "http_status"}, [] {
                     return condition({10}, [](OpsSnapshot& s) {
                         ProviderFact a;
                         a.name = "infura";
                         a.auth_http_status = 401;
                         s.providers = {a};
                     });
                 }});
    t.push_back({"Safe Gate Stuck", Severity::Critical, false, {"G_s", "head", "seconds_since_advance"},
                 [] { return condition({650}, [](OpsSnapshot&) {}, frozen); }});
    t.push_back({"Finalized Gate Stuck", Severity::Critical, false, {"G_f", "head"},
                 [] { return condition({3700}, [](OpsSnapshot&) {}, frozen); }});
    t.push_back({"Checkpoint Lag", Severity::Critical, false, {"class", "gate", "ckpt", "lag_blocks"}, [] {
                     return condition({10, 920}, [](OpsSnapshot& s) {  // 910 s above 300 blocks: past the 15 min
                         s.g_s = 2000;
                         s.c_s = 1000;
                     });
                 }});
    t.push_back({"Safe Reorg", Severity::Critical, true, {"new_reorgs", "total", "fork_point"}, [] {
                     return builder_event([](SnapshotInputs&) {}, [](SnapshotInputs& in) {
                         in.driver.safe_reorgs = 1;
                         in.driver.last_fork_point = 123;
                     });
                 }});
    t.push_back({"Sink Degraded", Severity::Critical, false, {"spill_bytes", "oldest_record_age_s"}, [] {
                     return condition({10, 320}, [](OpsSnapshot& s) { s.spill_bytes = 100; });  // 310 s: past the 5 min
                 }});
    t.push_back({"Sink Saturated", Severity::Critical, false, {"spill_bytes", "spill_max_bytes"}, [] {
                     return condition({10}, [](OpsSnapshot& s) {
                         s.sink_saturated = true;
                         s.spill_bytes = 100;
                         s.spill_max_bytes = 100;
                     });
                 }});
    t.push_back({"Redis Unreachable", Severity::Critical, false, {"since_s", "addr_family"}, [] {
                     return condition({10}, [](OpsSnapshot& s) {
                         s.redis_unreachable_s = 95;
                         s.addr_family = "ipv6";
                     });
                 }});
    t.push_back({"Sink Fenced", Severity::Warning, true, {"discarded_records"},
                 [] { return pushed_event(sink_fenced_event("sink fenced: another term holds the lease", 12)); }});
    t.push_back({"Sink Invariant", Severity::Critical, true, {"new_refusals", "total"}, [] {
                     return builder_event([](SnapshotInputs&) {}, [](SnapshotInputs& in) { in.sink.below_finalized_total = 2; });
                 }});
    t.push_back({"WS Missed Logs", Severity::Warning, true, {"new_missed", "total", "provider"}, [] {
                     return builder_event([](SnapshotInputs&) {}, [](SnapshotInputs& in) {
                         in.ws_missed_total = 3;
                         in.ws_provider = "alchemy";
                     });
                 }});
    t.push_back({"Provider Inconsistency / Gate Regressed (finalized)", Severity::Critical, false, {"reason"}, [] {
                     return condition({10}, [](OpsSnapshot& s) {
                         s.frozen = true;
                         s.frozen_reason = "finalized regressed";
                     });
                 }});
    t.push_back({"Provider Inconsistent", Severity::Warning, true, {"provider", "new_inconsistent", "total"}, [] {
                     return builder_event([](SnapshotInputs& in) { in.provider_inconsistent_total = {{"infura", 0}}; },
                                          [](SnapshotInputs& in) { in.provider_inconsistent_total = {{"infura", 1}}; });
                 }});
    t.push_back({"Safe Sweep Unreliable", Severity::Warning, false, {"count_1h", "provider", "last_block"}, [] {
                     return condition({10}, [](OpsSnapshot& s) {  // 4 late logs against a baseline of 0
                         s.safe_late_total = 4;
                         s.safe_late_provider = "ankr";
                     });
                 }});
    t.push_back({"Pipeline Stalled", Severity::Critical, false, {"class", "cursor", "gate", "provider", "detail"}, [] {
                     return condition({10}, [](OpsSnapshot& s) {
                         s.stalled_detail = "no agreement after max_bad_attempts";
                         s.stalled_provider = "infura";
                     });
                 }});
    t.push_back({"Leader Changed", Severity::Warning, true, {"old_host", "new_host"},
                 [] { return pushed_event(leader_changed_event("hostB", "hostA")); }});
    t.push_back({"Redis Misconfigured", Severity::Critical, false, {"maxmemory_policy", "appendonly", "redis_version"}, [] {
                     return condition({10}, [](OpsSnapshot& s) {
                         s.redis_misconfigured = RedisMisconfig{"allkeys-lru", "no", "8.4.7"};
                     });
                 }});
    t.push_back({"Consumer Lagging", Severity::Warning, false, consumer_keys, [] {
                     return condition({10}, [](OpsSnapshot& s) {
                         auto f = consumer(s);
                         f.undelivered_age_s = 50000;  // above 12 h, below 24 h
                         s.consumers = {f};
                     });
                 }});
    t.push_back({"Consumer Near Trim", Severity::Critical, false, consumer_keys, [] {
                     return condition({10}, [](OpsSnapshot& s) {
                         auto f = consumer(s);
                         f.undelivered_age_s = 90000;  // above 24 h
                         s.consumers = {f};
                     });
                 }});
    t.push_back({"Consumer Entries Trimmed", Severity::Critical, true, {"stream", "group", "last_delivered_id", "max_deleted_id"},
                 [] {
                     AlertEngine e{AlertThresholds{}};
                     e.evaluate(snap(0s));
                     Run r;
                     auto s = snap(10s);
                     auto f = consumer(s);
                     f.trimmed_unread = true;
                     f.last_delivered_id = "1-0";
                     f.max_deleted_id = "5-0";
                     s.consumers = {f};
                     r.fire = e.evaluate(s);
                     auto s2 = snap(20s);
                     auto f2 = f;
                     f2.sampled_at = s2.now;
                     s2.consumers = {f2};  // still true: not repeated
                     r.next = e.evaluate(s2);
                     return r;
                 }});
    t.push_back({"Consumer Group Missing", Severity::Warning, false, {"stream", "group"}, [] {
                     return condition({10, 620}, [](OpsSnapshot& s) {  // 610 s: past the 10 min
                         auto f = consumer(s);
                         f.found = false;
                         s.consumers = {f};
                     });
                 }});
    return t;
}

}  // namespace

TEST_CASE("alert contract: every row of §5.3.3 has the table's title, severity, kind and details keys", "[unit][alerts]") {
    const auto table = rows();
    CHECK(table.size() == 25);  // the 25 rows of §5.3.3

    for (const auto& row : table) {
        INFO("row: " << row.title);
        std::vector<const Alert*> fired;
        const auto run = row.run();
        for (const auto& a : run.fire) {
            if (a.title == row.title) fired.push_back(&a);
        }
        REQUIRE(fired.size() == 1);  // exactly one alert for the occurrence
        const Alert& a = *fired.front();
        CHECK(a.severity == row.severity);
        CHECK_FALSE(a.resolved);

        for (const auto& key : row.spec_keys) {
            INFO("details key: " << key);
            CHECK(a.details.contains(key));
        }

        std::size_t same_title = 0, resolves = 0;
        for (const auto& n : run.next) {
            if (n.title == row.title) ++same_title;
            if (n.title == "RESOLVED: " + row.title) ++resolves;
        }
        CHECK(same_title == 0);  // nothing repeats on the next tick
        CHECK(resolves == (row.event ? 0u : 1u));  // a condition resolves once, an event never
    }
}

