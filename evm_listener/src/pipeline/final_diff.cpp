#include "pipeline/final_diff.hpp"

#include <algorithm>
#include <unordered_set>

#include "pipeline/event.hpp"

namespace evml::pipeline {

// Partitions safe logs into matched, late (newly appeared), and vanished (reorged out).
FinalDiff diff_released(std::uint64_t chain_id, const std::vector<ReleasedEntry>& released,
                        const std::vector<rpc::Log>& sweep_logs) {
    FinalDiff out;

    // Index all previously released event IDs from Redis ZSET
    std::unordered_set<std::string> released_ids;
    for (const auto& r : released) released_ids.insert(r.event_id);

    // Identify late logs: present in canonical sweep but never emitted during safe sweep
    std::unordered_set<std::string> sweep_ids;
    for (const auto& l : sweep_logs) {
        const auto id = make_event_id(chain_id, l.block_hash, l.log_index);
        sweep_ids.insert(id);
        if (released_ids.count(id) == 0) out.late.push_back(l); // Missed during safe sweep
    }

    // Separate matched canonical logs from vanished ghost logs
    for (const auto& r : released) {
        if (sweep_ids.count(r.event_id) != 0)
            out.matched.push_back(r.event_id); // Confirmed canonical; still present
        else
            out.vanished.push_back(r);         // Vanished in reorg; must emit revert
    }
    // Sort retractions deterministically by block number then event ID (Invariant I6)
    std::sort(out.vanished.begin(), out.vanished.end(), [](const ReleasedEntry& a, const ReleasedEntry& b) {
        return a.block_number != b.block_number ? a.block_number < b.block_number : a.event_id < b.event_id;
    });
    return out;
}

}  // namespace evml::pipeline
