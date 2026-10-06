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

// WS staging, cross-check only (plan 3.4). getLogs stays the single source of truth for release; nothing here is
// ever emitted. Buckets are keyed by block number, then blockHash (a reorg can stage two hashes for one height),
// then logIndex. Pure and single-threaded.
class PendingBuffer {
public:
    // `max_logs` caps the number of staged logs; the oldest (lowest-numbered) buckets are evicted beyond it.
    explicit PendingBuffer(std::size_t max_logs);

    // Idempotent by (blockHash, logIndex). `removed:true` erases the staged entry instead.
    StageResult stage(const rpc::Log& log, const std::string& source);

    // Compare the staged logs of blocks [from, to] with a sweep result and drop what was compared. WS-side logs
    // for which `in_scope` is false (e.g. a class the sweep did not cover) are neither compared nor dropped.
    CrossCheck reconcile(std::uint64_t from, std::uint64_t to, const std::vector<rpc::Log>& sweep,
                         const std::function<bool(const rpc::Log&)>& in_scope);

    // Unsafe-reorg purge: drop every bucket at or above `number` (the fork point + 1). Returns logs dropped.
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
