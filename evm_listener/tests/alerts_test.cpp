#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <string>
#include <vector>

#include "ops/alerts.hpp"

using namespace evml::ops;
using namespace std::chrono_literals;

namespace {

const AlertClock::time_point kT0 = AlertClock::time_point{} + 1000s;

AlertThresholds thresholds() {
    AlertThresholds t;  // the plan's fixed numbers; the config-driven ones are set explicitly
    t.head_stall_ms = 30000;
    t.safe_gate_stuck_s = 600;
    t.progress_stall_ms = 60000;
    t.consumer_lag_warn_s = 43200;
    t.consumer_lag_crit_s = 86400;
    return t;
}

// A healthy leader snapshot at kT0 + `at`: gates move forward with `at` so no stuck rule fires unless a test freezes them.
OpsSnapshot snap(std::chrono::seconds at, bool leader = true) {
    OpsSnapshot s;
    s.now = kT0 + at;
    s.leader = leader;
    s.chain = "base";
    s.head = 1000 + at.count();
    s.g_s = 1000 + at.count();
    s.g_f = 5000 + at.count();
    s.c_s = *s.g_s;
    s.c_f = *s.g_f;
    return s;
}

std::vector<std::string> titles(const std::vector<Alert>& a) {
    std::vector<std::string> t;
    for (const auto& x : a) t.push_back(x.title);
    return t;
}

bool has(const std::vector<Alert>& a, const std::string& title) {
    const auto t = titles(a);
    return std::find(t.begin(), t.end(), title) != t.end();
}

const Alert& get(const std::vector<Alert>& a, const std::string& title) {
    const auto it = std::find_if(a.begin(), a.end(), [&](const Alert& x) { return x.title == title; });
    REQUIRE(it != a.end());
    return *it;
}

AlertEvent event(AlertEvent::Kind k, std::string subject = "") {
    AlertEvent e;
    e.kind = k;
    e.subject = std::move(subject);
    e.message = "something happened";
    return e;
}

}  // namespace

TEST_CASE("alerts: a healthy leader raises nothing", "[unit][alerts]") {
    AlertEngine e(thresholds());
    for (int i = 0; i < 100; ++i) CHECK(e.evaluate(snap(std::chrono::seconds(i * 10))).empty());
}

TEST_CASE("alerts: a condition fires once, re-notifies every 15 min and resolves once", "[unit][alerts]") {
    AlertEngine e(thresholds());
    auto at = [](int s) { return std::chrono::seconds(s); };
    // The safe gate is frozen at 1000 from t = 0.
    auto frozen = [&](int s) {
        auto x = snap(at(s));
        x.g_s = 1000;
        x.c_s = 1000;
        return x;
    };
    CHECK(e.evaluate(frozen(0)).empty());
    CHECK(e.evaluate(frozen(600)).empty());  // exactly the limit: not yet "more than"
    const auto fired = e.evaluate(frozen(610));
    REQUIRE(fired.size() == 1);
    CHECK(fired[0].title == "Safe Gate Stuck");
    CHECK(fired[0].severity == Severity::Critical);
    CHECK_FALSE(fired[0].resolved);
    CHECK(fired[0].details.at("G_s").as_uint64() == 1000);
    CHECK(fired[0].details.at("seconds_since_advance").to_number<int>() == 610);
    CHECK(e.evaluate(frozen(620)).empty());   // the next tick: nothing
    CHECK(e.evaluate(frozen(1500)).empty());  // 890 s after the fire
    const auto again = e.evaluate(frozen(1510));  // 900 s after the fire
    REQUIRE(again.size() == 1);
    CHECK(again[0].title == "Safe Gate Stuck");

    const auto resolved = e.evaluate(snap(at(1520)));  // the gate moved
    REQUIRE(resolved.size() == 1);
    CHECK(resolved[0].title == "RESOLVED: Safe Gate Stuck");
    CHECK(resolved[0].severity == Severity::Info);
    CHECK(resolved[0].resolved);
    CHECK(e.evaluate(snap(at(1530))).empty());
}

TEST_CASE("alerts: a standby raises only the host-local rows and drops leader state without a resolve", "[unit][alerts]") {
    AlertEngine e(thresholds());
    auto stuck = snap(0s);
    stuck.g_s = 1000;
    e.evaluate(stuck);
    auto later = snap(700s);
    later.g_s = 1000;
    later.c_s = 1000;
    REQUIRE(has(e.evaluate(later), "Safe Gate Stuck"));

    auto standby = snap(710s, false);
    standby.sink_saturated = true;                 // leader-only: ignored
    standby.redis_unreachable_s = 61;              // host-local
    standby.events = {event(AlertEvent::Kind::SafeReorg), event(AlertEvent::Kind::SinkFenced), event(AlertEvent::Kind::LeaderChanged)};
    const auto out = e.evaluate(standby);
    CHECK(titles(out) == std::vector<std::string>{"Redis Unreachable", "Sink Fenced", "Leader Changed"});  // no RESOLVED, no Safe Reorg

    // Back in the lead later: the old condition is gone and its tracker restarted, so nothing fires at once.
    auto lead = snap(720s);
    lead.g_s = 1000;
    lead.c_s = 1000;
    CHECK_FALSE(has(e.evaluate(lead), "Safe Gate Stuck"));
}

TEST_CASE("alerts: Redis Unreachable needs more than 60 s; Redis Misconfigured is host-local", "[unit][alerts]") {
    AlertEngine e(thresholds());
    auto s = snap(0s, false);
    s.redis_unreachable_s = 60;
    CHECK(e.evaluate(s).empty());
    s.redis_unreachable_s = 61;
    s.addr_family = "ipv6";
    s.redis_misconfigured = RedisMisconfig{"allkeys-lru", "no", "8.4.7"};
    s.now += 10s;
    const auto out = e.evaluate(s);
    CHECK(get(out, "Redis Unreachable").details.at("addr_family").as_string() == "ipv6");
    CHECK(get(out, "Redis Misconfigured").details.at("maxmemory_policy").as_string() == "allkeys-lru");
    s.redis_unreachable_s.reset();
    s.redis_misconfigured.reset();
    s.now += 10s;
    const auto cleared = e.evaluate(s);
    CHECK(titles(cleared) == std::vector<std::string>{"RESOLVED: Redis Misconfigured", "RESOLVED: Redis Unreachable"});
}

TEST_CASE("alerts: WebSocket Stalled follows head_stall_ms of the config", "[unit][alerts]") {
    AlertEngine e(thresholds());
    auto s = snap(0s);
    s.ms_since_head = 30000;
    s.ws_provider = "quicknode";
    CHECK(e.evaluate(s).empty());
    s.ms_since_head = 31000;
    s.now += 10s;
    s.g_s = *s.g_s + 10;  // keep the gates moving
    const auto out = e.evaluate(s);
    REQUIRE(out.size() == 1);
    CHECK(out[0].title == "WebSocket Stalled");
    CHECK(out[0].details.at("seconds_since_last_head").to_number<int>() == 31);
    CHECK(out[0].details.at("liveness_mode").as_string() == "heads");
    CHECK(out[0].details.at("provider").as_string() == "quicknode");
}

TEST_CASE("alerts: provider rows: auth failure, quota exhausted (once per incident, critical with no healthy provider)", "[unit][alerts]") {
    AlertEngine e(thresholds());
    auto s = snap(0s);
    ProviderFact a;
    a.name = "infura";
    a.auth_http_status = 401;
    ProviderFact q;
    q.name = "alchemy";
    q.quota_exhausted = true;
    q.quota_error_code = "429";
    q.quota_message = "Monthly capacity limit exceeded";
    s.providers = {a, q};
    auto out = e.evaluate(s);
    CHECK(get(out, "Provider Auth Failure").severity == Severity::Critical);
    CHECK(get(out, "Provider Auth Failure").details.at("http_status").to_number<int>() == 401);
    CHECK(get(out, "Provider Quota Exhausted").severity == Severity::Warning);
    CHECK(get(out, "Provider Quota Exhausted").details.at("chain").as_string() == "base");

    // 15 min later: the auth failure re-notifies, the quota one does not (once per incident).
    s = snap(900s);
    s.providers = {a, q};
    out = e.evaluate(s);
    CHECK(titles(out) == std::vector<std::string>{"Provider Auth Failure"});

    // A different incident with no healthy provider left is critical.
    AlertEngine e2(thresholds());
    auto s2 = snap(0s);
    s2.providers = {q};
    s2.any_healthy_provider = false;
    CHECK(get(e2.evaluate(s2), "Provider Quota Exhausted").severity == Severity::Critical);
}

TEST_CASE("alerts: getLogs Provider Lagging needs 30+ blocks for 2 min", "[unit][alerts]") {
    AlertEngine e(thresholds());
    auto lag = [&](int at, std::uint64_t behind) {
        auto s = snap(std::chrono::seconds(at));
        s.getlogs_provider = "ankr";
        s.getlogs_safe = *s.g_s - behind;
        s.ws_safe = *s.g_s;
        return s;
    };
    CHECK(e.evaluate(lag(0, 31)).empty());
    CHECK(e.evaluate(lag(60, 31)).empty());
    CHECK(e.evaluate(lag(119, 31)).empty());
    CHECK(has(e.evaluate(lag(120, 31)), "getLogs Provider Lagging"));
    CHECK(has(e.evaluate(lag(130, 10)), "RESOLVED: getLogs Provider Lagging"));
    // exactly 30 blocks is not "more than 30"
    AlertEngine e2(thresholds());
    CHECK(e2.evaluate(lag(0, 30)).empty());
    CHECK(e2.evaluate(lag(200, 30)).empty());
}

TEST_CASE("alerts: Finalized Gate Stuck after 60 min", "[unit][alerts]") {
    AlertEngine e(thresholds());
    auto s = [&](int at) {
        auto x = snap(std::chrono::seconds(at));
        x.g_f = 900;
        x.c_f = 900;
        return x;
    };
    CHECK(e.evaluate(s(0)).empty());
    CHECK(e.evaluate(s(3600)).empty());
    const auto out = e.evaluate(s(3610));
    REQUIRE(out.size() == 1);
    CHECK(out[0].title == "Finalized Gate Stuck");
    CHECK(out[0].details.at("G_f").as_uint64() == 900);
}

TEST_CASE("alerts: Checkpoint Lag is per class and needs 15 min", "[unit][alerts]") {
    AlertEngine e(thresholds());
    auto s = [&](int at, std::uint64_t safe_lag, std::uint64_t final_lag) {
        auto x = snap(std::chrono::seconds(at));
        x.c_s = *x.g_s - safe_lag;
        x.c_f = *x.g_f - final_lag;
        return x;
    };
    CHECK(e.evaluate(s(0, 301, 1999)).empty());
    CHECK(e.evaluate(s(899, 301, 1999)).empty());
    auto out = e.evaluate(s(900, 301, 1999));
    REQUIRE(out.size() == 1);
    CHECK(out[0].title == "Checkpoint Lag");
    CHECK(out[0].details.at("class").as_string() == "safe");
    CHECK(out[0].details.at("lag_blocks").as_uint64() == 301);
    out = e.evaluate(s(910, 301, 2001));  // the final class starts its own 15 min
    CHECK(out.empty());
    out = e.evaluate(s(1810, 301, 2001));
    CHECK(titles(out) == std::vector<std::string>{"Checkpoint Lag", "Checkpoint Lag"});  // final fires, safe re-notifies
}

TEST_CASE("alerts: sink rows: Degraded after 5 min of spill, Saturated at once", "[unit][alerts]") {
    AlertEngine e(thresholds());
    auto s = [&](int at, std::uint64_t spill, bool sat) {
        auto x = snap(std::chrono::seconds(at));
        x.spill_bytes = spill;
        x.spill_max_bytes = 1000;
        x.oldest_record_age_s = 5;
        x.sink_saturated = sat;
        return x;
    };
    CHECK(e.evaluate(s(0, 10, false)).empty());
    CHECK(e.evaluate(s(299, 10, false)).empty());
    CHECK(titles(e.evaluate(s(300, 10, false))) == std::vector<std::string>{"Sink Degraded"});
    const auto sat = e.evaluate(s(310, 1000, true));
    REQUIRE(sat.size() == 1);
    CHECK(sat[0].title == "Sink Saturated");
    CHECK(sat[0].details.at("spill_max_bytes").as_uint64() == 1000);
    CHECK(titles(e.evaluate(s(320, 0, false))) == std::vector<std::string>{"RESOLVED: Sink Degraded", "RESOLVED: Sink Saturated"});
}

TEST_CASE("alerts: a frozen finalized gate raises the critical gate alert", "[unit][alerts]") {
    AlertEngine e(thresholds());
    auto s = snap(0s);
    s.frozen = true;
    s.frozen_reason = "finalized hash mismatch";
    const auto out = e.evaluate(s);
    REQUIRE(out.size() == 1);
    CHECK(out[0].title == "Provider Inconsistency / Gate Regressed (finalized)");
    CHECK(out[0].severity == Severity::Critical);
}

TEST_CASE("alerts: Safe Sweep Unreliable needs more than 3 late logs inside an hour", "[unit][alerts]") {
    AlertEngine e(thresholds());
    auto s = [&](int at, std::uint64_t late) {
        auto x = snap(std::chrono::seconds(at));
        x.safe_late_total = late;
        x.safe_late_provider = "ankr";
        return x;
    };
    CHECK(e.evaluate(s(0, 0)).empty());
    CHECK(e.evaluate(s(10, 3)).empty());  // 3 is not more than 3
    const auto out = e.evaluate(s(20, 4));
    REQUIRE(out.size() == 1);
    CHECK(out[0].title == "Safe Sweep Unreliable");
    CHECK(out[0].details.at("count_1h").as_uint64() == 4);
    // an hour later nothing new has happened: the window has moved past them
    CHECK(titles(e.evaluate(s(3700, 4))) == std::vector<std::string>{"RESOLVED: Safe Sweep Unreliable"});
    // a rebuilt sweeper restarts the total at 0
    CHECK(e.evaluate(s(3710, 0)).empty());
}

TEST_CASE("alerts: Pipeline Stalled: a cursor that missed 3 progress deadlines, or a Stalled sweep", "[unit][alerts]") {
    AlertEngine e(thresholds());  // deadline 60 s x 3 = 180 s
    auto s = [&](int at) {
        auto x = snap(std::chrono::seconds(at));
        x.c_s = 900;  // the safe cursor stays behind its gate
        return x;
    };
    CHECK(e.evaluate(s(0)).empty());
    CHECK(e.evaluate(s(179)).empty());
    auto out = e.evaluate(s(180));
    REQUIRE(out.size() == 1);
    CHECK(out[0].title == "Pipeline Stalled");
    CHECK(out[0].details.at("class").as_string() == "safe");
    CHECK(out[0].details.at("cursor").as_uint64() == 900);

    auto sweep = snap(190s);
    sweep.stalled_detail = "range too large at the minimum";
    sweep.stalled_provider = "ankr";
    out = e.evaluate(sweep);
    CHECK(has(out, "Pipeline Stalled"));
    CHECK(get(out, "Pipeline Stalled").details.at("class").as_string() == "sweep");
    CHECK(has(out, "RESOLVED: Pipeline Stalled"));  // the cursor one cleared (the cursor caught up in `sweep`)
}

TEST_CASE("alerts: consumer rows: lagging, near trim, missing group, unknown snapshot, trimmed event", "[unit][alerts]") {
    AlertEngine e(thresholds());
    auto consumer = [&](AlertClock::time_point now, double undelivered, double pending) {
        ConsumerFact f;
        f.stream = "evml:s:events";
        f.group = "g1";
        f.sampled_at = now;
        f.undelivered_age_s = undelivered;
        f.pending_age_s = pending;
        f.lag = 5;
        f.retention_s = 172800;
        return f;
    };
    auto at = [&](int s, ConsumerFact f) {
        auto x = snap(std::chrono::seconds(s));
        f.sampled_at = x.now - 10s;
        x.consumers = {f};
        return x;
    };
    CHECK(e.evaluate(at(0, consumer(kT0, 100, 100))).empty());
    auto out = e.evaluate(at(10, consumer(kT0, 43201, 0)));
    REQUIRE(out.size() == 1);
    CHECK(out[0].title == "Consumer Lagging");
    CHECK(out[0].severity == Severity::Warning);
    out = e.evaluate(at(20, consumer(kT0, 0, 86401)));  // pending age counts too
    CHECK(titles(out) == std::vector<std::string>{"Consumer Near Trim", "RESOLVED: Consumer Lagging"});
    CHECK(get(out, "Consumer Near Trim").severity == Severity::Critical);

    // A probe snapshot older than 120 s is unknown: no resolve, no new alert.
    auto stale = snap(400s);
    auto f = consumer(kT0, 0, 86401);
    f.sampled_at = stale.now - 121s;
    stale.consumers = {f};
    CHECK(e.evaluate(stale).empty());

    // A missing group fires after 10 min.
    AlertEngine e2(thresholds());
    auto missing = [&](int s) {
        auto x = at(s, consumer(kT0, 0, 0));
        x.consumers[0].found = false;
        return x;
    };
    CHECK(e2.evaluate(missing(0)).empty());
    CHECK(e2.evaluate(missing(599)).empty());
    CHECK(titles(e2.evaluate(missing(600))) == std::vector<std::string>{"Consumer Group Missing"});

    // Entries trimmed: an event once, not repeated while it stays true, rate-limited per group.
    AlertEngine e3(thresholds());
    auto trimmed = [&](int s, bool t) {
        auto x = at(s, consumer(kT0, 0, 0));
        x.consumers[0].trimmed_unread = t;
        x.consumers[0].last_delivered_id = "1-1";
        x.consumers[0].max_deleted_id = "2-2";
        return x;
    };
    out = e3.evaluate(trimmed(0, true));
    REQUIRE(out.size() == 1);
    CHECK(out[0].title == "Consumer Entries Trimmed");
    CHECK(out[0].severity == Severity::Critical);
    CHECK(out[0].details.at("last_delivered_id").as_string() == "1-1");
    CHECK(e3.evaluate(trimmed(10, true)).empty());
    CHECK(e3.evaluate(trimmed(20, false)).empty());
    CHECK(e3.evaluate(trimmed(30, true)).empty());  // a new occurrence inside the 15 min limit is dropped
    CHECK(titles(e3.evaluate(trimmed(1000, false))).empty());
    CHECK(titles(e3.evaluate(trimmed(1010, true))) == std::vector<std::string>{"Consumer Entries Trimmed"});
}

TEST_CASE("alerts: events fire once per occurrence; provider and group names are rate-limited per 15 min", "[unit][alerts]") {
    AlertEngine e(thresholds());
    auto s = snap(0s);
    s.events = {event(AlertEvent::Kind::ProviderFailover)};
    auto out = e.evaluate(s);
    REQUIRE(out.size() == 1);
    CHECK(out[0].title == "Provider Failover");
    CHECK(out[0].severity == Severity::Warning);
    CHECK_FALSE(out[0].resolved);
    CHECK(e.evaluate(snap(10s)).empty());  // none on the next tick, and no resolve

    // Safe Reorg: every occurrence pages (no subject, no rate limit).
    s = snap(20s);
    s.events = {event(AlertEvent::Kind::SafeReorg), event(AlertEvent::Kind::SafeReorg)};
    out = e.evaluate(s);
    CHECK(titles(out) == std::vector<std::string>{"Safe Reorg", "Safe Reorg"});
    CHECK(out[0].severity == Severity::Critical);

    // Provider Inconsistent: one per provider per 15 min.
    s = snap(30s);
    s.events = {event(AlertEvent::Kind::ProviderInconsistent, "infura"), event(AlertEvent::Kind::ProviderInconsistent, "infura"),
                event(AlertEvent::Kind::ProviderInconsistent, "ankr")};
    out = e.evaluate(s);
    REQUIRE(out.size() == 2);
    CHECK(out[0].key == "Provider Inconsistent:infura");
    CHECK(out[1].key == "Provider Inconsistent:ankr");
    s = snap(100s);
    s.events = {event(AlertEvent::Kind::ProviderInconsistent, "infura")};
    CHECK(e.evaluate(s).empty());
    s = snap(931s);
    s.events = {event(AlertEvent::Kind::ProviderInconsistent, "infura")};
    CHECK(e.evaluate(s).size() == 1);

    // The remaining kinds and their severities.
    s = snap(940s);
    s.events = {event(AlertEvent::Kind::SinkInvariant), event(AlertEvent::Kind::WsMissedLogs), event(AlertEvent::Kind::SinkFenced),
                event(AlertEvent::Kind::LeaderChanged)};
    out = e.evaluate(s);
    REQUIRE(out.size() == 4);
    CHECK(out[0].severity == Severity::Critical);  // Sink Invariant
    CHECK(out[1].severity == Severity::Warning);   // WS Missed Logs
    CHECK(out[2].severity == Severity::Warning);   // Sink Fenced
    CHECK(out[3].severity == Severity::Warning);   // Leader Changed
}

TEST_CASE("alerts: thresholds come from the config", "[unit][alerts]") {
    evml::core::Config cfg;
    cfg.watchdog.head_stall_ms = 1234;
    cfg.watchdog.safe_gate_stuck_s = 77;
    cfg.watchdog.progress_stall_ms = 55;
    cfg.sink.consumer_lag_warn_s = 11;
    cfg.sink.consumer_lag_crit_s = 22;
    const auto t = AlertThresholds::from_config(cfg);
    CHECK(t.head_stall_ms == 1234);
    CHECK(t.safe_gate_stuck_s == 77);
    CHECK(t.progress_stall_ms == 55);
    CHECK(t.consumer_lag_warn_s == 11);
    CHECK(t.consumer_lag_crit_s == 22);
}
