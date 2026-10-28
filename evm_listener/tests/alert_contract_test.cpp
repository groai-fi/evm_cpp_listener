// The §5.3.3 alert table as a contract (plan 60, slice 5-E4c). One row per table row. Each row is driven through the
// real producers (SnapshotBuilder + AlertEngine, and the event helpers for the pushed event rows) and checked for:
//   - title and severity,
//   - kind: a condition fires once and resolves once ("RESOLVED: <title>"), an event fires once and never resolves,
//   - every `details` key the table names (with non-empty string values).
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

// A healthy leader SnapshotInputs at kT0 + at: the gates advance with `at`, so no stuck rule fires unless a row freezes them.
SnapshotInputs leader_in(std::chrono::seconds at) {
    SnapshotInputs in;
    in.now = kT0 + at;
    in.wall_ms = 1'000'000 + static_cast<std::uint64_t>(at.count()) * 1000;
    in.leader = true;
    in.chain = "base";
    in.head = 1000 + static_cast<std::uint64_t>(at.count());
    in.driver.state = evml::pipeline::PipelineState::Live;
    in.driver.g_s = 1000 + static_cast<std::uint64_t>(at.count());
    in.driver.g_f = 5000 + static_cast<std::uint64_t>(at.count());
    in.driver.c_s = *in.driver.g_s;
    in.driver.c_f = *in.driver.g_f;
    in.retention_s = 604800;
    return in;
}

evml::sink::ConsumerLag consumer(const SnapshotInputs& in) {
    evml::sink::ConsumerLag c;
    c.stream = evml::core::StreamKind::Events;
    c.group = "bot";
    c.probed_at_ms = in.wall_ms;  // a fresh probe
    c.lag = 5;
    c.last_delivered_id = "1-0";
    c.max_deleted_id = "0-0";
    return c;
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

// A condition row: `bad` is applied to SnapshotInputs at each listed time, built via SnapshotBuilder, then `good` at +10 s.
Run condition(const std::vector<int>& bad_at, const std::function<void(SnapshotInputs&)>& bad,
              const std::function<SnapshotInputs(std::chrono::seconds)>& base = leader_in) {
    AlertEngine e{AlertThresholds{}};
    SnapshotBuilder b;
    e.evaluate(b.build(base(0s)));  // baseline: trackers start from a healthy snapshot
    Run r;
    for (const int t : bad_at) {
        auto in = base(std::chrono::seconds(t));
        bad(in);
        r.fire = e.evaluate(b.build(in));
    }
    r.next = e.evaluate(b.build(leader_in(std::chrono::seconds(bad_at.back() + 10))));
    return r;
}

// A gate that stays where it was at 0 s (the "stuck" rows).
SnapshotInputs frozen(std::chrono::seconds at) {
    auto in = leader_in(0s);
    in.now = kT0 + at;
    in.wall_ms = 1'000'000 + static_cast<std::uint64_t>(at.count()) * 1000;
    return in;
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
                     return condition({10}, [](SnapshotInputs& in) {
                         in.ms_since_head = 45000;
                         in.ws_provider = "alchemy";
                     });
                 }});
    t.push_back({"Provider Failover", Severity::Warning, true, {"from", "to", "reason"},
                 [] { return pushed_event(provider_failover_event("quicknode", "infura", "failover")); }});
    t.push_back({"Provider Quota Exhausted", Severity::Warning, false,
                 {"provider", "chain", "error_code", "message", "next_probe_ts"}, [] {
                     return condition({10}, [](SnapshotInputs& in) {
                         using evml::provider::ProviderHealth;
                         using evml::provider::State;
                         in.providers = {
                             ProviderHealth{"infura", State::Up, 0, std::nullopt, "", std::nullopt},
                             ProviderHealth{"alchemy", State::Cooldown, 429, -32005, "Monthly capacity limit exceeded", 60'000ms},
                         };
                     });
                 }});
    t.push_back({"getLogs Provider Lagging", Severity::Warning, false, {"provider", "its_safe", "ws_safe"}, [] {
                     return condition({10, 140}, [](SnapshotInputs& in) {  // 130 s above 30 blocks: past the 2 min
                         in.getlogs_provider = "infura";
                         in.getlogs_safe = 100;
                         in.ws_safe = 200;
                     });
                 }});
    t.push_back({"Provider Auth Failure", Severity::Critical, false, {"provider", "http_status"}, [] {
                     return condition({10}, [](SnapshotInputs& in) {
                         using evml::provider::ProviderHealth;
                         using evml::provider::State;
                         in.providers = {
                             ProviderHealth{"alchemy", State::Up, 0, std::nullopt, "", std::nullopt},
                             ProviderHealth{"infura", State::Disabled, 401, std::nullopt, "http 401: unauthorized", 30'000ms},
                         };
                     });
                 }});
    t.push_back({"Safe Gate Stuck", Severity::Critical, false, {"G_s", "head", "seconds_since_advance"},
                 [] { return condition({650}, [](SnapshotInputs&) {}, frozen); }});
    t.push_back({"Finalized Gate Stuck", Severity::Critical, false, {"G_f", "head"},
                 [] { return condition({3700}, [](SnapshotInputs&) {}, frozen); }});
    t.push_back({"Checkpoint Lag", Severity::Critical, false, {"class", "gate", "ckpt", "lag_blocks"}, [] {
                     return condition({10, 920}, [](SnapshotInputs& in) {  // 910 s above 300 blocks: past the 15 min
                         in.driver.g_s = 2000;
                         in.driver.c_s = 1000;
                     });
                 }});
    t.push_back({"Safe Reorg", Severity::Critical, true, {"new_reorgs", "total", "fork_point"}, [] {
                     return builder_event([](SnapshotInputs&) {}, [](SnapshotInputs& in) {
                         in.driver.safe_reorgs = 1;
                         in.driver.last_fork_point = 123;
                     });
                 }});
    t.push_back({"Sink Degraded", Severity::Critical, false, {"spill_bytes", "oldest_record_age_s"}, [] {
                     return condition({10, 320}, [](SnapshotInputs& in) {
                         in.sink.spill_bytes = 100;
                         in.sink.oldest_record_age_s = 310;
                     });  // 310 s: past the 5 min
                 }});
    t.push_back({"Sink Saturated", Severity::Critical, false, {"spill_bytes", "spill_max_bytes"}, [] {
                     return condition({10}, [](SnapshotInputs& in) {
                         in.sink_saturated = true;
                         in.sink.spill_bytes = 100;
                         in.spill_max_bytes = 100;
                     });
                 }});
    t.push_back({"Redis Unreachable", Severity::Critical, false, {"since_s", "addr_family"}, [] {
                     return condition({10}, [](SnapshotInputs& in) {
                         in.redis_unreachable_s = 95;
                         in.addr_family = "ipv6";
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
                     return condition({10}, [](SnapshotInputs& in) {
                         in.driver.frozen = true;
                         in.driver.frozen_reason = "finalized regressed";
                     });
                 }});
    t.push_back({"Provider Inconsistent", Severity::Warning, true, {"provider", "new_inconsistent", "total"}, [] {
                     return builder_event([](SnapshotInputs& in) { in.provider_inconsistent_total = {{"infura", 0}}; },
                                          [](SnapshotInputs& in) { in.provider_inconsistent_total = {{"infura", 1}}; });
                 }});
    t.push_back({"Safe Sweep Unreliable", Severity::Warning, false, {"count_1h", "provider", "last_block"}, [] {
                     return condition({10}, [](SnapshotInputs& in) {  // 4 late logs against a baseline of 0
                         in.safe_late_total = 4;
                         in.safe_late_last_block = 4985;
                         in.getlogs_provider = "ankr";
                     });
                 }});
    t.push_back({"Pipeline Stalled", Severity::Critical, false, {"class", "cursor", "gate", "provider", "detail"}, [] {
                     return condition({10}, [](SnapshotInputs& in) {
                         in.driver.stalled = true;
                         in.driver.stall_detail = "no agreement after max_bad_attempts";
                         in.getlogs_provider = "infura";
                     });
                 }});
    t.push_back({"Leader Changed", Severity::Warning, true, {"old_host", "new_host"},
                 [] { return pushed_event(leader_changed_event("hostB", "hostA")); }});
    t.push_back({"Redis Misconfigured", Severity::Critical, false, {"maxmemory_policy", "appendonly", "redis_version"}, [] {
                     return condition({10}, [](SnapshotInputs& in) {
                         in.redis_misconfigured = RedisMisconfig{"allkeys-lru", "no", "8.4.7"};
                     });
                 }});
    t.push_back({"Consumer Lagging", Severity::Warning, false, consumer_keys, [] {
                     return condition({10}, [](SnapshotInputs& in) {
                         auto c = consumer(in);
                         c.undelivered_age_s = 50000;  // above 12 h, below 24 h
                         in.consumers = {c};
                     });
                 }});
    t.push_back({"Consumer Near Trim", Severity::Critical, false, consumer_keys, [] {
                     return condition({10}, [](SnapshotInputs& in) {
                         auto c = consumer(in);
                         c.undelivered_age_s = 90000;  // above 24 h
                         in.consumers = {c};
                     });
                 }});
    t.push_back({"Consumer Entries Trimmed", Severity::Critical, true, {"stream", "group", "last_delivered_id", "max_deleted_id"},
                 [] {
                     evml::sink::GroupFacts gf;
                     gf.stream_exists = true;
                     gf.group_found = true;
                     gf.last_delivered_id = "1-0";
                     gf.max_deleted_id = "5-0";
                     gf.lag = 0;
                     gf.now_ms = 1'000'000;
                     auto c = evml::sink::evaluate_consumer_lag(
                         evml::core::ConsumerGroupCfg{evml::core::StreamKind::Events, "bot"}, gf);
                     return builder_event([](SnapshotInputs&) {},
                                          [c](SnapshotInputs& in) {
                                              auto cur = c;
                                              cur.probed_at_ms = in.wall_ms;
                                              in.consumers = {cur};
                                          });
                 }});
    t.push_back({"Consumer Group Missing", Severity::Warning, false, {"stream", "group"}, [] {
                     return condition({10, 620}, [](SnapshotInputs& in) {  // 610 s: past the 10 min
                         auto c = consumer(in);
                         c.missing = true;
                         in.consumers = {c};
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
            REQUIRE(a.details.contains(key));
            if (a.details.at(key).is_string()) CHECK_FALSE(a.details.at(key).as_string().empty());
        }
        if (row.title == "Safe Sweep Unreliable") CHECK(a.details.at("last_block").as_uint64() == 4985);

        std::size_t same_title = 0, resolves = 0;
        for (const auto& n : run.next) {
            if (n.title == row.title) ++same_title;
            if (n.title == "RESOLVED: " + row.title) ++resolves;
        }
        CHECK(same_title == 0);  // nothing repeats on the next tick
        CHECK(resolves == (row.event ? 0u : 1u));  // a condition resolves once, an event never
    }
}

