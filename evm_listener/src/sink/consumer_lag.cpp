#include "sink/consumer_lag.hpp"

#include <charconv>
#include <stdexcept>

namespace evml::sink {

namespace {

std::uint64_t parse_part(const std::string& s, std::size_t from, std::size_t to, const std::string& whole) {
    std::uint64_t v = 0;
    const auto [p, ec] = std::from_chars(s.data() + from, s.data() + to, v);
    if (from == to || ec != std::errc{} || p != s.data() + to)
        throw std::runtime_error("redis: stream id is not '<ms>-<seq>': '" + whole + "'");
    return v;
}

std::uint64_t age_s(const std::string& id, std::uint64_t now_ms) {
    const StreamId sid = StreamId::parse(id);
    return sid.ms >= now_ms ? 0 : (now_ms - sid.ms) / 1000;
}

}  // namespace

StreamId StreamId::parse(const std::string& s) {
    const auto dash = s.find('-');
    if (dash == std::string::npos) throw std::runtime_error("redis: stream id is not '<ms>-<seq>': '" + s + "'");
    return StreamId{parse_part(s, 0, dash, s), parse_part(s, dash + 1, s.size(), s)};
}

/// @brief Evaluates consumer lag metrics and flags data loss if entries were trimmed before consumption.
ConsumerLag evaluate_consumer_lag(const core::ConsumerGroupCfg& cfg, const GroupFacts& f) {
    ConsumerLag out;
    out.stream = cfg.stream;
    out.group = cfg.group;
    if (!f.stream_exists || !f.group_found) {
        // Group or stream does not exist in Redis yet (consumer hasn't started or name mismatched).
        out.missing = true;
        return out;
    }
    out.lag = f.lag;
    out.last_delivered_id = f.last_delivered_id;
    out.max_deleted_id = f.max_deleted_id;
    // Calculate waiting age in seconds from entry arrival timestamp in Redis.
    if (f.first_undelivered_id) out.undelivered_age_s = age_s(*f.first_undelivered_id, f.now_ms);
    if (f.smallest_pending_id) out.pending_age_s = age_s(*f.smallest_pending_id, f.now_ms);

    const StreamId last_delivered = StreamId::parse(f.last_delivered_id);
    // Flag data loss if deleted/trimmed entry ID is higher than last delivered ID.
    if (!f.max_deleted_id.empty() && StreamId::parse(f.max_deleted_id) > last_delivered) out.trimmed_unread = true;
    // Flag data loss if pending unacked entry was purged from the stream head.
    if (f.smallest_pending_id && f.first_entry_id &&
        StreamId::parse(*f.smallest_pending_id) < StreamId::parse(*f.first_entry_id))
        out.trimmed_unread = true;
    // Redis counts every entry ever appended. Each one is either read by the group, still waiting (lag), or gone
    // (MINID trim never updates max-deleted-entry-id, so this is the rule that catches unread entries lost to retention).
    // Only provable when Redis knows both counters. A group that never delivered anything (last-delivered 0-0) has
    // read zero entries even though Redis reports entries-read as nil for it; otherwise an unknown entries-read is not flagged.
    std::optional<std::uint64_t> read = f.entries_read;
    if (!read && last_delivered == StreamId{0, 0}) read = 0;
    // Arithmetic check: if total appended exceeds read + lag, intermediate entries were evicted by retention.
    if (f.entries_added && read && f.lag && *f.entries_added > *read + *f.lag) out.trimmed_unread = true;
    return out;
}

}  // namespace evml::sink
