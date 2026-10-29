#include "ops/alerts.hpp"

#include <algorithm>

namespace evml::ops {

namespace json = boost::json;

namespace {

json::value jv(const std::optional<std::uint64_t>& v) { return v ? json::value(*v) : json::value(nullptr); }

std::string num(std::uint64_t v) { return std::to_string(v); }

std::string consumer_id(const ConsumerFact& f) { return f.stream + "/" + f.group; }

struct EventInfo {
    Severity severity;
    const char* title;
    bool host_local;
};

EventInfo event_info(AlertEvent::Kind k) {
    switch (k) {
        case AlertEvent::Kind::ProviderFailover: return {Severity::Warning, "Provider Failover", false};
        case AlertEvent::Kind::SafeReorg: return {Severity::Critical, "Safe Reorg", false};
        case AlertEvent::Kind::SinkFenced: return {Severity::Warning, "Sink Fenced", true};
        case AlertEvent::Kind::SinkInvariant: return {Severity::Critical, "Sink Invariant", false};
        case AlertEvent::Kind::WsMissedLogs: return {Severity::Warning, "WS Missed Logs", false};
        case AlertEvent::Kind::ProviderInconsistent: return {Severity::Warning, "Provider Inconsistent", false};
        case AlertEvent::Kind::LeaderChanged: return {Severity::Warning, "Leader Changed", true};
    }
    return {Severity::Warning, "?", false};
}

}  // namespace

const char* to_string(Severity s) {
    switch (s) {
        case Severity::Critical: return "critical";
        case Severity::Warning: return "warning";
        case Severity::Info: return "info";
    }
    return "?";
}

AlertThresholds AlertThresholds::from_config(const core::Config& cfg) {
    AlertThresholds t;
    t.head_stall_ms = cfg.watchdog.head_stall_ms;
    t.safe_gate_stuck_s = cfg.watchdog.safe_gate_stuck_s;
    t.progress_stall_ms = cfg.watchdog.progress_stall_ms;
    t.consumer_lag_warn_s = cfg.sink.consumer_lag_warn_s;
    t.consumer_lag_crit_s = cfg.sink.consumer_lag_crit_s;
    return t;
}

std::chrono::seconds AlertEngine::unchanged_for(const std::string& name, std::uint64_t value, AlertClock::time_point now) {
    auto [it, inserted] = unchanged_.try_emplace(name, value, now);
    if (!inserted && it->second.first != value) it->second = {value, now};
    return std::chrono::duration_cast<std::chrono::seconds>(now - it->second.second);
}

bool AlertEngine::sustained(const std::string& name, bool cond, AlertClock::time_point now, std::chrono::seconds limit) {
    if (!cond) {
        sustained_since_.erase(name);
        return false;
    }
    const auto it = sustained_since_.try_emplace(name, now).first;
    return now - it->second >= limit;
}

void AlertEngine::reset_leader_state() {
    for (auto it = firing_.begin(); it != firing_.end();) it = it->second.leader_only ? firing_.erase(it) : std::next(it);
    unchanged_.clear();
    sustained_since_.clear();
    suppressed_inconsistent_.clear();
    late_samples_.clear();
    trimmed_seen_.clear();
}

std::vector<AlertEngine::Candidate> AlertEngine::candidates(const OpsSnapshot& s, std::set<std::string>& unknown) {
    std::vector<Candidate> c;
    const auto add = [&c](std::string key, Severity sev, std::string title, std::string message, json::object details,
                          bool leader_only = true, bool renotify = true) {
        c.push_back(Candidate{std::move(key), sev, std::move(title), std::move(message), std::move(details), leader_only,
                              renotify});
    };
    using std::chrono::seconds;
    const auto now = s.now;

    // ---- host-local rows: any role ----
    if (s.redis_unreachable_s && *s.redis_unreachable_s > t_.redis_unreachable_s) {
        add("redis_unreachable", Severity::Critical, "Redis Unreachable",
            "Redis has been unreachable for " + num(*s.redis_unreachable_s) + " s",
            {{"since_s", *s.redis_unreachable_s}, {"addr_family", s.addr_family}}, false);
    }
    if (s.redis_misconfigured) {
        const auto& m = *s.redis_misconfigured;
        add("redis_misconfigured", Severity::Critical, "Redis Misconfigured",
            "Redis needs noeviction and persistence: maxmemory-policy=" + m.maxmemory_policy + " appendonly=" + m.appendonly,
            {{"maxmemory_policy", m.maxmemory_policy}, {"appendonly", m.appendonly}, {"redis_version", m.redis_version}}, false);
    }
    if (!s.leader) return c;

    // ---- leader-only rows (D7) ----
    const auto lag_blocks = [&]() -> json::value {
        return s.head ? json::value(*s.head > s.c_s ? *s.head - s.c_s : 0) : json::value(nullptr);
    };
    if (s.ms_since_head && *s.ms_since_head > t_.head_stall_ms) {
        add("ws_stalled", Severity::Critical, "WebSocket Stalled",
            "no newHeads from " + s.ws_provider + " for " + num(*s.ms_since_head / 1000) + " s",
            {{"seconds_since_last_head", *s.ms_since_head / 1000},
             {"liveness_mode", "heads"},
             {"head_lag_blocks", lag_blocks()},
             {"last_processed_block", s.c_s},
             {"provider", s.ws_provider}});
    }

    for (const auto& p : s.providers) {
        if (p.quota_exhausted) {
            add("quota:" + p.name, s.any_healthy_provider ? Severity::Warning : Severity::Critical,
                "Provider Quota Exhausted", p.name + " quota exhausted: " + p.quota_message,
                {{"provider", p.name},
                 {"chain", s.chain},
                 {"error_code", p.quota_error_code},
                 {"message", p.quota_message},
                 {"next_probe_ts", jv(p.next_probe_ts)}},
                true, false);  // once per incident
        }
        if (p.auth_http_status) {
            add("auth:" + p.name, Severity::Critical, "Provider Auth Failure",
                p.name + " rejected our credentials" +
                    (*p.auth_http_status > 0 ? " (HTTP " + std::to_string(*p.auth_http_status) + ")" : ""),
                {{"provider", p.name},
                 {"http_status", *p.auth_http_status > 0 ? json::value(*p.auth_http_status) : json::value(nullptr)}});
        }
    }

    const bool getlogs_lagging = s.getlogs_safe && s.ws_safe && *s.ws_safe > *s.getlogs_safe + t_.getlogs_lag_blocks;
    if (sustained("getlogs_lag", getlogs_lagging, now, seconds(t_.getlogs_lag_s))) {
        add("getlogs_lagging", Severity::Warning, "getLogs Provider Lagging",
            s.getlogs_provider + " safe is " + num(*s.ws_safe - *s.getlogs_safe) + " blocks behind the WS provider",
            {{"provider", s.getlogs_provider}, {"its_safe", *s.getlogs_safe}, {"ws_safe", *s.ws_safe}});
    }

    if (s.g_s) {
        const auto idle = unchanged_for("g_s", *s.g_s, now);
        if (idle > seconds(t_.safe_gate_stuck_s)) {
            add("safe_gate_stuck", Severity::Critical, "Safe Gate Stuck",
                "safe gate has not advanced for " + num(idle.count()) + " s",
                {{"G_s", *s.g_s}, {"head", jv(s.head)}, {"seconds_since_advance", idle.count()}});
        }
    }
    if (s.g_f) {
        const auto idle = unchanged_for("g_f", *s.g_f, now);
        if (idle > seconds(t_.finalized_gate_stuck_s)) {
            add("finalized_gate_stuck", Severity::Critical, "Finalized Gate Stuck",
                "finalized gate has not advanced for " + num(idle.count()) + " s",
                {{"G_f", *s.g_f}, {"head", jv(s.head)}, {"seconds_since_advance", idle.count()}});
        }
    }

    const auto ckpt_lag = [&](const char* cls, const std::optional<std::uint64_t>& gate, std::uint64_t ckpt, std::uint64_t limit) {
        const std::uint64_t lag = gate && *gate > ckpt ? *gate - ckpt : 0;
        if (sustained(std::string("ckpt_lag:") + cls, lag > limit, now, seconds(t_.ckpt_lag_s))) {
            add(std::string("ckpt_lag:") + cls, Severity::Critical, "Checkpoint Lag",
                std::string(cls) + " checkpoint is " + num(lag) + " blocks behind its gate",
                {{"class", cls}, {"gate", *gate}, {"ckpt", ckpt}, {"lag_blocks", lag}});
        }
    };
    ckpt_lag("safe", s.g_s, s.c_s, t_.ckpt_lag_safe_blocks);
    ckpt_lag("final", s.g_f, s.c_f, t_.ckpt_lag_final_blocks);

    // Pipeline Stalled: a cursor behind its gate that has not moved for N progress deadlines, or a sweep that
    // returned Stalled.
    const auto cursor_stall = [&](const char* cls, std::uint64_t cursor, std::uint64_t ceiling) {
        const auto idle = unchanged_for(std::string("cursor:") + cls, cursor, now);  // tracked on every snapshot
        if (t_.progress_stall_ms == 0 || ceiling <= cursor) return;
        const auto limit = seconds(static_cast<std::int64_t>(t_.pipeline_stall_deadlines) * t_.progress_stall_ms / 1000);
        if (idle < limit) return;
        add(std::string("pipeline_stalled:") + cls, Severity::Critical, "Pipeline Stalled",
            std::string(cls) + " cursor has not moved for " + num(idle.count()) + " s",
            {{"class", cls}, {"cursor", cursor}, {"gate", ceiling}, {"provider", s.getlogs_provider}, {"detail", "no progress"}});
    };
    if (s.g_s) cursor_stall("safe", s.c_s, *s.g_s);
    if (s.g_f) cursor_stall("final", s.c_f, std::min(*s.g_f, s.c_s));
    if (!s.stalled_detail.empty()) {
        add("pipeline_stalled:sweep", Severity::Critical, "Pipeline Stalled", "a sweep stalled: " + s.stalled_detail,
            {{"class", "sweep"}, {"cursor", s.c_s}, {"gate", jv(s.g_s)}, {"provider", s.stalled_provider}, {"detail", s.stalled_detail}});
    }

    if (s.frozen) {
        add("gate_regressed", Severity::Critical, "Provider Inconsistency / Gate Regressed (finalized)",
            "release frozen: " + s.frozen_reason, {{"reason", s.frozen_reason}});
    }

    if (sustained("spill", s.spill_bytes > 0, now, seconds(t_.sink_degraded_s))) {
        add("sink_degraded", Severity::Critical, "Sink Degraded",
            "spill has held " + num(s.spill_bytes) + " bytes for over " + num(t_.sink_degraded_s) + " s",
            {{"spill_bytes", s.spill_bytes}, {"oldest_record_age_s", s.oldest_record_age_s}});
    }
    if (s.sink_saturated) {
        add("sink_saturated", Severity::Critical, "Sink Saturated", "spill cap reached: the pipeline is frozen",
            {{"spill_bytes", s.spill_bytes}, {"spill_max_bytes", s.spill_max_bytes}});
    }

    // Safe Sweep Unreliable: more than `safe_late_max` late logs inside the window. The total restarts at 0 with
    // every new sweeper, so a drop clears the history.
    if (!late_samples_.empty() && s.safe_late_total < late_samples_.back().second) late_samples_.clear();
    late_samples_.emplace_back(now, s.safe_late_total);
    while (late_samples_.size() > 1 && late_samples_[1].first <= now - seconds(t_.safe_late_window_s)) late_samples_.pop_front();
    const std::uint64_t late = s.safe_late_total - late_samples_.front().second;
    if (late > t_.safe_late_max) {
        add("safe_sweep_unreliable", Severity::Warning, "Safe Sweep Unreliable",
            num(late) + " late logs in the last hour", {{"count_1h", late}, {"provider", s.safe_late_provider}, {"last_block", s.safe_late_last_block}});
    }

    for (const auto& f : s.consumers) {
        const std::string id = consumer_id(f);
        const std::string prefix = "consumer:" + id + ":";
        if (now - f.sampled_at > seconds(t_.consumer_snapshot_max_age_s)) {  // unknown: keep the state as it is
            unknown.insert(prefix);
            continue;
        }
        const bool missing = sustained("cmiss:" + id, !f.found, now, seconds(t_.consumer_group_missing_s));
        if (missing) {
            add(prefix + "missing", Severity::Warning, "Consumer Group Missing", "group " + f.group + " not found on " + f.stream,
                {{"stream", f.stream}, {"group", f.group}});
        }
        if (!f.found) continue;
        const double age = std::max(f.undelivered_age_s, f.pending_age_s);
        json::object d{{"stream", f.stream},
                       {"group", f.group},
                       {"undelivered_age_s", f.undelivered_age_s},
                       {"pending_age_s", f.pending_age_s},
                       {"lag", f.lag},
                       {"retention_s", f.retention_s}};
        if (age > t_.consumer_lag_crit_s) {
            add(prefix + "near_trim", Severity::Critical, "Consumer Near Trim",
                "group " + f.group + " is " + num(static_cast<std::uint64_t>(age)) + " s behind", std::move(d));
        } else if (age > t_.consumer_lag_warn_s) {
            add(prefix + "lagging", Severity::Warning, "Consumer Lagging",
                "group " + f.group + " is " + num(static_cast<std::uint64_t>(age)) + " s behind", std::move(d));
        }
    }
    return c;
}

void AlertEngine::emit_event(const OpsSnapshot& s, const AlertEvent& e, std::vector<Alert>& out) {
    const auto info = event_info(e.kind);
    if (!info.host_local && !s.leader) return;  // D7
    std::string key = info.title;
    json::object details = e.details;
    std::uint64_t inc_delta = 0;
    if (e.kind == AlertEvent::Kind::ProviderInconsistent) {
        if (const auto* p = details.if_contains("new_inconsistent")) {
            if (p->is_uint64()) inc_delta = p->as_uint64();
            else if (p->is_int64() && p->as_int64() > 0) inc_delta = static_cast<std::uint64_t>(p->as_int64());
        }
    }
    if (!e.subject.empty()) {
        key += ":" + e.subject;
        const auto it = event_last_.find(key);
        if (it != event_last_.end() && s.now - it->second < std::chrono::seconds(t_.event_rate_limit_s)) {
            if (e.kind == AlertEvent::Kind::ProviderInconsistent && inc_delta > 0) {
                suppressed_inconsistent_[key] += inc_delta;
            }
            return;
        }
        event_last_[key] = s.now;
        if (e.kind == AlertEvent::Kind::ProviderInconsistent) {
            if (auto sit = suppressed_inconsistent_.find(key); sit != suppressed_inconsistent_.end()) {
                if (sit->second > 0) details["new_inconsistent"] = inc_delta + sit->second;
                suppressed_inconsistent_.erase(sit);
            }
        }
    }
    out.push_back(Alert{info.severity, info.title, e.message, std::move(details), std::move(key), false});
}

std::vector<Alert> AlertEngine::evaluate(const OpsSnapshot& s) {
    if (!s.leader) reset_leader_state();
    std::set<std::string> unknown;
    const auto cands = candidates(s, unknown);

    std::vector<Alert> out;
    std::set<std::string> active;
    for (const auto& c : cands) {
        active.insert(c.key);
        const auto it = firing_.find(c.key);
        if (it == firing_.end()) {
            firing_[c.key] = Firing{c.severity, c.title, c.leader_only, s.now};
        } else if (c.renotify && s.now - it->second.last_sent >= std::chrono::seconds(t_.renotify_s)) {
            it->second.last_sent = s.now;
        } else {
            continue;
        }
        out.push_back(Alert{c.severity, c.title, c.message, c.details, c.key, false});
    }

    for (auto it = firing_.begin(); it != firing_.end();) {
        const bool is_unknown = std::any_of(unknown.begin(), unknown.end(), [&](const std::string& p) { return it->first.rfind(p, 0) == 0; });
        if (active.count(it->first) != 0 || is_unknown) {
            ++it;
            continue;
        }
        out.push_back(Alert{Severity::Info, "RESOLVED: " + it->second.title, it->second.title + " cleared", {}, it->first, true});
        it = firing_.erase(it);
    }

    // Consumer Entries Trimmed: an event on the first snapshot that shows the loss (not repeated while it stays true).
    if (s.leader) {
        for (const auto& f : s.consumers) {
            if (s.now - f.sampled_at > std::chrono::seconds(t_.consumer_snapshot_max_age_s)) continue;
            const std::string id = consumer_id(f);
            if (!f.trimmed_unread) {
                trimmed_seen_.erase(id);
            } else if (trimmed_seen_.insert(id).second) {
                const std::string& effective_max_deleted =
                    (f.max_deleted_id.empty() || f.max_deleted_id == "0-0") && !f.first_entry_id.empty()
                        ? f.first_entry_id
                        : f.max_deleted_id;
                AlertEvent ev;
                ev.message = "entries were trimmed before group " + f.group + " read them";
                ev.details = {{"stream", f.stream},
                              {"group", f.group},
                              {"last_delivered_id", f.last_delivered_id},
                              {"max_deleted_id", effective_max_deleted}};
                const std::string key = "Consumer Entries Trimmed:" + id;
                const auto it = event_last_.find(key);
                if (it != event_last_.end() && s.now - it->second < std::chrono::seconds(t_.event_rate_limit_s)) continue;
                event_last_[key] = s.now;
                out.push_back(Alert{Severity::Critical, "Consumer Entries Trimmed", ev.message, ev.details, key, false});
            }
        }
    }

    for (const auto& e : s.events) emit_event(s, e, out);
    return out;
}

}  // namespace evml::ops
