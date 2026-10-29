#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "core/config.hpp"

namespace evml::sink {

// Consumer-group lag probe (plan 4.10). MINID retention trims entries whether or not a consumer read them, so the
// listener watches the groups the downstream bots created.

// "<ms>-<seq>", the id of a stream entry. Redis assigns ids from its own clock, so `ms` is Redis arrival time.
/// @brief Parses and compares Redis stream entry identifiers format "<millisecond_timestamp>-<sequence>".
struct StreamId {
    std::uint64_t ms{};
    std::uint64_t seq{};

    // Throws std::runtime_error on anything that is not "<digits>-<digits>" (never defaulted).
    /// @brief Parses stream ID string; throws std::runtime_error if format is invalid.
    static StreamId parse(const std::string& s);

    friend bool operator==(const StreamId&, const StreamId&) = default;
    friend bool operator<(const StreamId& a, const StreamId& b) { return a.ms != b.ms ? a.ms < b.ms : a.seq < b.seq; }
    friend bool operator>(const StreamId& a, const StreamId& b) { return b < a; }
};

// What the probe read from Redis for one {stream, group}, before any judgement (so the judgement is a pure function).
/// @brief Raw telemetry facts collected from Redis commands (XINFO GROUPS, XINFO STREAM, XPENDING).
struct GroupFacts {
    bool stream_exists{false};  // false: no such key yet
    bool group_found{false};
    std::string last_delivered_id;                   // XINFO GROUPS last-delivered-id
    std::optional<std::uint64_t> lag;                // XINFO GROUPS lag; nil when Redis cannot compute it
    std::optional<std::uint64_t> entries_read;       // XINFO GROUPS entries-read; nil when unknown
    std::optional<std::uint64_t> entries_added;      // XINFO STREAM entries-added: every entry ever appended
    std::string max_deleted_id;                      // XINFO STREAM max-deleted-entry-id ("0-0" when nothing was deleted)
    std::optional<std::string> first_entry_id;       // XINFO STREAM first-entry (nil on an empty stream)
    std::optional<std::string> first_undelivered_id; // first entry after last-delivered-id (XRANGE (id + COUNT 1)
    std::optional<std::string> smallest_pending_id;  // XPENDING summary (nil when nothing is pending)
    std::uint64_t now_ms{};                          // Redis TIME in ms
};

/// @brief Processed lag evaluation snapshot for one stream consumer group.
/// Watch out: `trimmed_unread == true` indicates downstream data loss caused by retention trim overtaking consumers.
struct ConsumerLag {
    core::StreamKind stream{core::StreamKind::Events};
    std::string group;
    bool missing{false};                    // no such group (or no such stream yet): the consumer never started, or the name is wrong
    std::uint64_t undelivered_age_s{0};     // how long the oldest unread entry has waited; 0 if none
    std::uint64_t pending_age_s{0};         // age of the oldest delivered-but-unacked entry; 0 if none
    std::optional<std::uint64_t> lag;       // entry count, for the metric only (nil when Redis cannot compute it)
    bool trimmed_unread{false};             // entries were trimmed before this group read or acked them: data loss happened
    std::string last_delivered_id;          // XINFO GROUPS last-delivered-id
    std::string max_deleted_id;             // XINFO STREAM max-deleted-entry-id ("0-0" when nothing was deleted)
    std::string first_entry_id;             // XINFO STREAM first-entry id (empty on an empty stream)
    std::uint64_t probed_at_ms{0};          // listener wall clock when the probe ran (set by the sink): age of this snapshot
};

/// @brief Evaluates consumer group health, computing lag ages and detecting retention trims.
ConsumerLag evaluate_consumer_lag(const core::ConsumerGroupCfg& cfg, const GroupFacts& facts);

}  // namespace evml::sink
