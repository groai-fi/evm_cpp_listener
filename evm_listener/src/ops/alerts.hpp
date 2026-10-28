#pragma once
// Alert rule engine (plan 60 §5.3.1 / §5.3.3, slice 5-E1). Pure: no I/O, no clock, no threads. The host fills an
// OpsSnapshot every 10 s and calls AlertEngine::evaluate(); the result is the list of alerts to send now (E2 sends them).
//
// Two kinds (rev 10):
//  - Condition: a state that is true or false now. One "fire", a re-notify every 15 min while it stays true, one
//    "RESOLVED:" (severity info) when it clears. Rules with a duration ("for 15 min") are tracked inside the engine
//    from the snapshots, so the host only reports current values.
//  - Event: something that happened. One alert per occurrence, no resolve. Events that name a provider or a group are
//    rate-limited to one per 15 min per name.
// Ownership (D7): pipeline, provider, sink and consumer rows are leader-only; Redis Unreachable / Redis Misconfigured /
// Sink Fenced / Leader Changed are host-local (any role).
#include <chrono>
#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <boost/json/object.hpp>

#include "core/config.hpp"

namespace evml::ops {

using AlertClock = std::chrono::steady_clock;

enum class Severity { Critical, Warning, Info };
const char* to_string(Severity s);

struct Alert {
    Severity severity{Severity::Info};
    std::string title;    // "RESOLVED: <title>" on a resolve
    std::string message;  // one line
    boost::json::object details;
    std::string key;      // stable id of the rule instance (title + subject)
    bool resolved{false};
};

/// Thresholds: the first group comes from the config, the rest are the fixed numbers of the §5.3.3 table.
struct AlertThresholds {
    std::uint32_t head_stall_ms{30000};         // [watchdog] head_stall_ms
    std::uint32_t safe_gate_stuck_s{600};       // [watchdog] safe_gate_stuck_s
    std::uint32_t progress_stall_ms{0};         // [watchdog] progress_stall_ms: one "progress deadline"
    std::uint32_t consumer_lag_warn_s{43200};   // [sink] consumer_lag_warn_s
    std::uint32_t consumer_lag_crit_s{86400};   // [sink] consumer_lag_crit_s

    std::uint32_t finalized_gate_stuck_s{3600};
    std::uint64_t ckpt_lag_safe_blocks{300};
    std::uint64_t ckpt_lag_final_blocks{2000};
    std::uint32_t ckpt_lag_s{900};
    std::uint64_t getlogs_lag_blocks{30};
    std::uint32_t getlogs_lag_s{120};
    std::uint32_t sink_degraded_s{300};
    std::uint32_t redis_unreachable_s{60};
    std::uint64_t safe_late_max{3};             // more than this many late logs ...
    std::uint32_t safe_late_window_s{3600};     // ... within this window
    std::uint32_t consumer_group_missing_s{600};
    std::uint32_t consumer_snapshot_max_age_s{120};  // 2 x the 60 s consumer-lag probe interval; older = unknown
    std::uint32_t pipeline_stall_deadlines{3};  // missed progress deadlines before Pipeline Stalled
    std::uint32_t renotify_s{900};
    std::uint32_t event_rate_limit_s{900};

    static AlertThresholds from_config(const core::Config& cfg);
};

struct ProviderFact {
    std::string name;
    std::optional<int> auth_http_status;  // 401 / 403 seen and not yet cleared
    bool quota_exhausted{false};
    std::string quota_error_code;
    std::string quota_message;
    std::optional<std::uint64_t> next_probe_ts;
};

struct ConsumerFact {
    std::string stream;
    std::string group;
    AlertClock::time_point sampled_at{};  // when the probe ran
    bool found{true};                     // the configured group exists
    double undelivered_age_s{0};
    double pending_age_s{0};
    std::int64_t lag{-1};                 // -1 = Redis cannot compute it
    std::uint64_t retention_s{0};
    bool trimmed_unread{false};
    std::string last_delivered_id;
    std::string max_deleted_id;
};

struct RedisMisconfig {
    std::string maxmemory_policy;
    std::string appendonly;
    std::string redis_version;
};

/// Something that happened since the last snapshot (host-fed).
struct AlertEvent {
    enum class Kind { ProviderFailover, SafeReorg, SinkFenced, SinkInvariant, WsMissedLogs, ProviderInconsistent, LeaderChanged };
    Kind kind{Kind::ProviderFailover};
    std::string subject;  // provider name for ProviderInconsistent (rate-limit key); empty otherwise
    std::string message;  // one line
    boost::json::object details;
};

struct OpsSnapshot {
    AlertClock::time_point now{};
    bool leader{false};

    // Host-local facts.
    std::optional<std::uint32_t> redis_unreachable_s;  // set while Redis connect/PING keeps failing
    std::string addr_family;
    std::optional<RedisMisconfig> redis_misconfigured;

    // Leader facts. nullopt = not known yet: no alert is raised on it.
    std::string chain;                           // chain name for the Provider Quota Exhausted details
    std::optional<std::uint64_t> head, g_s, g_f;
    std::uint64_t c_s{0}, c_f{0};
    std::optional<std::uint32_t> ms_since_head;  // newHeads silence of the active WS provider
    std::string ws_provider;
    bool frozen{false};                          // finalized anomaly: release frozen
    std::string frozen_reason;
    std::string stalled_detail;                  // a sweep returned Stalled (empty = no)
    std::string stalled_provider;
    bool sink_saturated{false};
    std::uint64_t spill_bytes{0};
    std::uint64_t spill_max_bytes{0};
    std::uint64_t oldest_record_age_s{0};
    std::uint64_t safe_late_total{0};            // running total of late logs (SweepStats::late)
    std::string safe_late_provider;
    std::uint64_t safe_late_last_block{0};
    std::optional<std::uint64_t> getlogs_safe, ws_safe;
    std::string getlogs_provider;
    std::vector<ProviderFact> providers;
    bool any_healthy_provider{true};
    std::vector<ConsumerFact> consumers;

    std::vector<AlertEvent> events;
};

class AlertEngine {
public:
    explicit AlertEngine(AlertThresholds t) : t_(std::move(t)) {}

    /// @brief Alerts to send for this snapshot (fires, re-notifies, resolves, events), in that order.
    /// Example: a gate stuck for 11 min with `safe_gate_stuck_s = 600` fires once; evaluating again 10 s later returns
    /// nothing; 15 min after the fire it re-notifies; when the gate moves it returns one "RESOLVED:" alert.
    std::vector<Alert> evaluate(const OpsSnapshot& s);

private:
    struct Candidate {
        std::string key;
        Severity severity{Severity::Critical};
        std::string title;
        std::string message;
        boost::json::object details;
        bool leader_only{true};
        bool renotify{true};
    };
    struct Firing {
        Severity severity{Severity::Critical};
        std::string title;
        bool leader_only{true};
        AlertClock::time_point last_sent{};
    };

    // How long `value` has stayed the same (the clock restarts on every change; 0 on the first call).
    std::chrono::seconds unchanged_for(const std::string& name, std::uint64_t value, AlertClock::time_point now);
    // True once `cond` has been true without a break for at least `limit`.
    bool sustained(const std::string& name, bool cond, AlertClock::time_point now, std::chrono::seconds limit);
    std::vector<Candidate> candidates(const OpsSnapshot& s, std::set<std::string>& unknown_prefixes);
    void emit_event(const OpsSnapshot& s, const AlertEvent& e, std::vector<Alert>& out);
    void reset_leader_state();

    AlertThresholds t_;
    std::map<std::string, Firing> firing_;
    std::map<std::string, std::pair<std::uint64_t, AlertClock::time_point>> unchanged_;
    std::map<std::string, AlertClock::time_point> sustained_since_;
    std::map<std::string, AlertClock::time_point> event_last_;
    std::deque<std::pair<AlertClock::time_point, std::uint64_t>> late_samples_;
    std::set<std::string> trimmed_seen_;
};

}  // namespace evml::ops
