#include "pipeline/final_diff.hpp"

#include <algorithm>
#include <unordered_set>

#include "pipeline/event.hpp"

namespace evml::pipeline {

FinalDiff diff_released(std::uint64_t chain_id, const std::vector<ReleasedEntry>& released,
                        const std::vector<rpc::Log>& sweep_logs) {
    FinalDiff out;

    std::unordered_set<std::string> released_ids;
    for (const auto& r : released) released_ids.insert(r.event_id);

    std::unordered_set<std::string> sweep_ids;
    for (const auto& l : sweep_logs) {
        const auto id = make_event_id(chain_id, l.block_hash, l.log_index);
        sweep_ids.insert(id);
        if (released_ids.count(id) == 0) out.late.push_back(l);
    }

    for (const auto& r : released) {
        if (sweep_ids.count(r.event_id) != 0)
            out.matched.push_back(r.event_id);
        else
            out.vanished.push_back(r);
    }
    std::sort(out.vanished.begin(), out.vanished.end(), [](const ReleasedEntry& a, const ReleasedEntry& b) {
        return a.block_number != b.block_number ? a.block_number < b.block_number : a.event_id < b.event_id;
    });
    return out;
}

}  // namespace evml::pipeline
