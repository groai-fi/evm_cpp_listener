#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "rpc/codec.hpp"

namespace evml::pipeline {

enum class StageResult {
    Added,
    Duplicate,       // same (blockHash, logIndex) already staged
    Removed,         // removed:true and the staged entry was erased
    RemovedUnknown,  // removed:true for something that was never staged (nothing to do)
};
const char* to_string(StageResult r);

struct CrossCheck {
    std::size_t ws_missed{};  // in the sweep result only: WS never delivered it (ws_missed_logs_total)
    std::size_t ws_extra{};   // staged by WS only: orphaned or not yet indexed (ws_extra_logs_total)
};

// In-memory buffer staging real-time WebSocket logs strictly for cross-checking against getLogs sweeps (plan 3.4).
// getLogs remains the single source of truth for release; staged logs are never emitted directly.
// Keyed hierarchically: block_number -> block_hash -> log_index.
// Pure and single-threaded.
class PendingBuffer {
public:
    // `max_logs` caps the number of staged logs; oldest buckets are evicted when capacity is exceeded.
    explicit PendingBuffer(std::size_t max_logs);

    // Stages incoming WS log; idempotent by (blockHash, logIndex).
    // Watch out: If log.removed is true, erases existing staged entry instead of inserting.
    StageResult stage(const rpc::Log& log, const std::string& source);

    // Compares staged logs across [from, to] against getLogs sweep result; drops reconciled entries.
    // Watch out: Logs where in_scope returns false (e.g. unpolled finality classes) are preserved.
    CrossCheck reconcile(std::uint64_t from, std::uint64_t to, const std::vector<rpc::Log>& sweep,
                         const std::function<bool(const rpc::Log&)>& in_scope);

    // Purges all staged buckets at or above `number` (e.g. on unsafe chain reorgs).
    // Watch out: Call immediately on head fork to prevent stale logs from corrupting cross-check telemetry.
    std::size_t purge_from(std::uint64_t number);

    std::size_t size() const { return total_; }
    std::size_t bucket_count() const { return buckets_.size(); }
    std::uint64_t evicted_total() const { return evicted_total_; }

private:
    struct Staged {
        rpc::Log log;
        std::string first_source;
    };
    using Bucket = std::unordered_map<core::Hash32, std::map<std::uint64_t, Staged>>;

    void evict_to_cap();

    std::size_t max_logs_;
    std::size_t total_{0};
    std::uint64_t evicted_total_{0};
    std::map<std::uint64_t, Bucket> buckets_;
};

}  // namespace evml::pipeline
