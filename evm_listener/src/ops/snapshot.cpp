#include "ops/snapshot.hpp"

#include <algorithm>
#include <utility>

namespace evml::ops {

namespace json = boost::json;

AlertEvent provider_failover_event(const std::optional<std::string>& from, const std::string& to, std::string_view reason) {
    AlertEvent e;
    e.kind = AlertEvent::Kind::ProviderFailover;
    const std::string verb = reason == "switch_back" ? "switched back" : "failed over";
    e.message = "the WS provider " + verb + " from " + from.value_or("none") + " to " + to;
    e.details = {{"from", from ? json::value(*from) : json::value(nullptr)}, {"to", to}, {"reason", std::string(reason)}};
    return e;
}

AlertEvent leader_changed_event(const std::string& old_host, const std::string& new_host) {
    AlertEvent e;
    e.kind = AlertEvent::Kind::LeaderChanged;
    e.message = "leadership moved from " + old_host + " to " + new_host;
    e.details = {{"old_host", old_host}, {"new_host", new_host}};
    return e;
}

AlertEvent sink_fenced_event(const std::string& why, std::uint64_t discarded_records) {
    AlertEvent e;
    e.kind = AlertEvent::Kind::SinkFenced;
    e.message = why;
    e.details = {{"discarded_records", discarded_records}};
    return e;
}

void SnapshotBuilder::push_event(AlertEvent e) { pending_.push_back(std::move(e)); }

OpsSnapshot SnapshotBuilder::build(const SnapshotInputs& in) {
    OpsSnapshot s;
    s.now = in.now;
    s.leader = in.leader;
    s.chain = in.chain;
    s.redis_unreachable_s = in.redis_unreachable_s;
    s.addr_family = in.addr_family;
    s.redis_misconfigured = in.redis_misconfigured;
    s.events = std::move(pending_);
    pending_.clear();

    if (!in.leader) {
        prev_.reset();  // the next term's counters start from zero
        return s;
    }

    const auto& d = in.driver;
    s.head = in.head;
    s.g_s = d.g_s;
    s.g_f = d.g_f;
    s.c_s = d.c_s;
    s.c_f = d.c_f;
    s.frozen = d.frozen;
    s.frozen_reason = d.frozen_reason;
    if (d.stalled) {
        s.stalled_detail = d.stall_detail;
        s.stalled_provider = in.getlogs_provider;
    }
    s.sink_saturated = in.sink_saturated;
    s.spill_bytes = in.sink.spill_bytes;
    s.spill_max_bytes = in.spill_max_bytes;
    s.oldest_record_age_s = in.sink.oldest_record_age_s;
    s.safe_late_total = in.safe_late_total;
    s.safe_late_last_block = in.safe_late_last_block;
    s.safe_late_provider = in.getlogs_provider;

    for (const auto& c : in.consumers) {
        if (c.probed_at_ms == 0) continue;  // never probed: nothing to judge yet
        ConsumerFact f;
        f.stream = core::to_string(c.stream);
        f.group = c.group;
        const auto age = std::chrono::milliseconds(in.wall_ms > c.probed_at_ms ? in.wall_ms - c.probed_at_ms : 0);
        f.sampled_at = in.now - age;
        f.found = !c.missing;
        f.undelivered_age_s = static_cast<double>(c.undelivered_age_s);
        f.pending_age_s = static_cast<double>(c.pending_age_s);
        f.lag = c.lag ? static_cast<std::int64_t>(*c.lag) : -1;
        f.retention_s = in.retention_s;
        f.trimmed_unread = c.trimmed_unread;
        f.last_delivered_id = c.last_delivered_id;
        f.max_deleted_id = c.max_deleted_id;
        f.first_entry_id = c.first_entry_id;
        s.consumers.push_back(std::move(f));
    }

    s.ms_since_head = in.ms_since_head;
    s.ws_provider = in.ws_provider;
    s.getlogs_provider = in.getlogs_provider;
    s.ws_safe = in.ws_safe;
    s.getlogs_safe = in.getlogs_safe;

    // Cooldown = monthly quota used up, Disabled = credentials rejected (provider_book.hpp). Other states raise no
    // condition alert; Provider Failover is an event pushed by the host.
    bool any_up = in.providers.empty();  // no provider facts given: do not claim an outage
    for (const auto& h : in.providers) {
        any_up = any_up || h.state == provider::State::Up;
        ProviderFact f;
        f.name = h.name;
        f.quota_exhausted = h.state == provider::State::Cooldown;
        if (f.quota_exhausted) {
            f.quota_message = h.fault_message;
            f.quota_error_code = h.rpc_code ? std::to_string(*h.rpc_code)
                                            : (h.http_status > 0 ? std::to_string(h.http_status) : "");
        }
        if (h.state == provider::State::Disabled) f.auth_http_status = h.http_status;  // 0 = not an HTTP answer
        if (h.next_probe_in) f.next_probe_ts = in.wall_ms / 1000 + static_cast<std::uint64_t>(h.next_probe_in->count()) / 1000;
        s.providers.push_back(std::move(f));
    }
    s.any_healthy_provider = any_up;

    const Counters cur{d.safe_reorgs, in.sink.below_finalized_total, in.ws_missed_total, in.provider_inconsistent_total};
    if (prev_) {
        if (cur.safe_reorgs > prev_->safe_reorgs) {
            AlertEvent e;
            e.kind = AlertEvent::Kind::SafeReorg;
            e.message = "a reverification found released safe-class logs that changed";
            e.details = {{"new_reorgs", cur.safe_reorgs - prev_->safe_reorgs}, {"total", cur.safe_reorgs}};
            if (d.last_fork_point) e.details["fork_point"] = *d.last_fork_point;
            s.events.push_back(std::move(e));
        }
        if (cur.below_finalized > prev_->below_finalized) {
            AlertEvent e;
            e.kind = AlertEvent::Kind::SinkInvariant;
            e.message = "the sink refused an emit at or below the finalized checkpoint";
            e.details = {{"new_refusals", cur.below_finalized - prev_->below_finalized}, {"total", cur.below_finalized}};
            s.events.push_back(std::move(e));
        }
        if (cur.ws_missed > prev_->ws_missed) {
            AlertEvent e;
            e.kind = AlertEvent::Kind::WsMissedLogs;
            e.subject = in.ws_provider.empty() ? "ws" : in.ws_provider;
            e.message = "the sweep found logs the WS stream never delivered";
            e.details = {{"new_missed", cur.ws_missed - prev_->ws_missed},
                         {"total", cur.ws_missed},
                         {"provider", in.ws_provider}};
            s.events.push_back(std::move(e));
        }
        for (const auto& [name, total] : cur.inconsistent) {
            const auto it = prev_->inconsistent.find(name);
            const std::uint64_t before = it == prev_->inconsistent.end() ? 0 : it->second;
            if (total <= before) continue;
            AlertEvent e;
            e.kind = AlertEvent::Kind::ProviderInconsistent;
            e.subject = name;
            e.message = name + " returned data that failed validation";
            e.details = {{"provider", name}, {"new_inconsistent", total - before}, {"total", total}};
            s.events.push_back(std::move(e));
        }
    }
    prev_ = cur;
    return s;
}

}  // namespace evml::ops
