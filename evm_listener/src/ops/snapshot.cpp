#include "ops/snapshot.hpp"

#include <algorithm>
#include <utility>

namespace evml::ops {

namespace json = boost::json;

void SnapshotBuilder::push_event(AlertEvent e) { pending_.push_back(std::move(e)); }

OpsSnapshot SnapshotBuilder::build(const SnapshotInputs& in) {
    OpsSnapshot s;
    s.now = in.now;
    s.leader = in.leader;
    s.chain = in.chain;
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
    if (d.stalled) s.stalled_detail = d.stall_detail;
    s.sink_saturated = in.sink_saturated;
    s.spill_bytes = in.sink.spill_bytes;
    s.spill_max_bytes = in.spill_max_bytes;
    s.safe_late_total = in.safe_late_total;

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
        s.consumers.push_back(std::move(f));
    }

    const Counters cur{d.safe_reorgs, in.sink.below_finalized_total, in.ws_missed_total};
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
            e.message = "the sweep found logs the WS stream never delivered";
            e.details = {{"new_missed", cur.ws_missed - prev_->ws_missed}, {"total", cur.ws_missed}};
            s.events.push_back(std::move(e));
        }
    }
    prev_ = cur;
    return s;
}

}  // namespace evml::ops
