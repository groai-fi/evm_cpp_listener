#include "pipeline/pending_buffer.hpp"

#include <set>
#include <stdexcept>

namespace evml::pipeline {

const char* to_string(StageResult r) {
    switch (r) {
        case StageResult::Added: return "Added";
        case StageResult::Duplicate: return "Duplicate";
        case StageResult::Removed: return "Removed";
        case StageResult::RemovedUnknown: return "RemovedUnknown";
    }
    return "?";
}

PendingBuffer::PendingBuffer(std::size_t max_logs) : max_logs_(max_logs) {
    if (max_logs == 0) throw std::invalid_argument("PendingBuffer: max_logs must be >= 1");
}

// Stages a new log into the buffer or processes an EVM log removal notification.
StageResult PendingBuffer::stage(const rpc::Log& log, const std::string& source) {
    if (log.removed) {
        // Handle EVM log removal: find and erase previously staged entry
        const auto b = buckets_.find(log.block_number);
        if (b == buckets_.end()) return StageResult::RemovedUnknown;
        const auto h = b->second.find(log.block_hash);
        if (h == b->second.end()) return StageResult::RemovedUnknown;
        if (h->second.erase(log.log_index) == 0) return StageResult::RemovedUnknown;
        --total_;
        if (h->second.empty()) b->second.erase(h); // Clean up empty hash container
        if (b->second.empty()) buckets_.erase(b);   // Clean up empty block bucket
        return StageResult::Removed;
    }

    // Insert new entry into block_number -> block_hash -> log_index map
    auto& by_index = buckets_[log.block_number][log.block_hash];
    if (!by_index.emplace(log.log_index, Staged{log, source}).second) return StageResult::Duplicate;
    ++total_;
    evict_to_cap(); // Enforce soft upper bound on memory usage
    return StageResult::Added;
}

// Evicts oldest block buckets when total staged logs exceed max_logs capacity limit.
void PendingBuffer::evict_to_cap() {
    while (total_ > max_logs_ && !buckets_.empty()) {
        const auto oldest = buckets_.begin();
        for (const auto& [hash, logs] : oldest->second) {
            total_ -= logs.size();
            evicted_total_ += logs.size();
        }
        buckets_.erase(oldest); // FIFO eviction of lowest block heights
    }
}

// Reconciles getLogs sweep against staged WS logs; records missed/extra counts and purges swept range.
CrossCheck PendingBuffer::reconcile(std::uint64_t from, std::uint64_t to, const std::vector<rpc::Log>& sweep,
                                    const std::function<bool(const rpc::Log&)>& in_scope) {
    CrossCheck out;
    std::unordered_map<core::Hash32, std::set<std::uint64_t>> swept;
    // Map swept logs to check whether WS delivered every log found by eth_getLogs
    for (const auto& l : sweep) {
        if (l.block_number < from || l.block_number > to) continue;
        swept[l.block_hash].insert(l.log_index);
        const auto b = buckets_.find(l.block_number);
        bool staged = false;
        if (b != buckets_.end()) {
            const auto h = b->second.find(l.block_hash);
            staged = h != b->second.end() && h->second.count(l.log_index) != 0;
        }
        if (!staged) ++out.ws_missed; // Sweeper found a log that WebSocket never delivered
    }

    // Reconcile and prune buffer buckets within [from, to]
    for (auto b = buckets_.lower_bound(from); b != buckets_.end() && b->first <= to;) {
        for (auto h = b->second.begin(); h != b->second.end();) {
            const auto sw = swept.find(h->first);
            for (auto s = h->second.begin(); s != h->second.end();) {
                if (!in_scope(s->second.log)) {
                    ++s; // Skip contracts/topics not in current sweep scope
                    continue;
                }
                if (sw == swept.end() || sw->second.count(s->first) == 0) ++out.ws_extra;
                s = h->second.erase(s);
                --total_;
            }
            h = h->second.empty() ? b->second.erase(h) : std::next(h);
        }
        b = b->second.empty() ? buckets_.erase(b) : std::next(b);
    }
    return out;
}

// Drops all buckets at or above fork block number after an unsafe chain reorganization.
std::size_t PendingBuffer::purge_from(std::uint64_t number) {
    std::size_t dropped = 0;
    const auto first = buckets_.lower_bound(number);
    for (auto b = first; b != buckets_.end(); ++b)
        for (const auto& [hash, logs] : b->second) dropped += logs.size();
    buckets_.erase(first, buckets_.end()); // Truncate all orphaned fork blocks
    total_ -= dropped;
    return dropped;
}

}  // namespace evml::pipeline
