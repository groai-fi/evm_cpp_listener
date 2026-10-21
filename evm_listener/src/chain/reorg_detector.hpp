#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "core/hex.hpp"

namespace evml::chain {

struct BlockRef {
    std::uint64_t number{};
    core::Hash32 hash;
    core::Hash32 parent;
};

enum class HeadKind {
    Seeded,     // ring was empty; this head is its first entry
    Extended,   // number == top+1 and parent == top.hash: the normal case
    Duplicate,  // already in the ring with the same hash (e.g. replay after a failover)
    NeedWalk,   // a gap, or a parent/hash mismatch: run a ForkWalk (plan 3.6)
};
const char* to_string(HeadKind k);

class ForkWalk;

struct ReorgInfo {
    std::uint64_t fork_point{};  // F: the highest block shared by the old and the new chain
    std::uint64_t old_top{};     // ring top before the change
    std::uint64_t depth{};       // old_top - F; 0 when the walk only filled a gap
};

// Ring buffer storing up to `capacity` canonical block headers (number -> {hash, parent}) (plan 3.6).
// Detects head forks, block gaps, and safe reorgs without performing blocking network I/O.
// Concurrency model: Pure state machine; single-threaded execution.
class ReorgDetector {
public:
    explicit ReorgDetector(std::size_t capacity);

    // Processes an incoming block header from the active WebSocket newHeads stream.
    // Extended/Seeded/Duplicate are applied immediately; NeedWalk leaves ring untouched until resolved.
    // Watch out: Returns NeedWalk on parent hash mismatch, block number gap, or alternative hash at known height.
    HeadKind on_head(const BlockRef& head);

    // Begins an ancestral walk to locate the common ancestor fork point for a NeedWalk head.
    ForkWalk begin_walk(const BlockRef& head) const;

    // Commits a successfully resolved ForkWalk to the ring buffer.
    // Truncates orphaned blocks above fork point F and inserts the new canonical branch.
    // Watch out: Throws std::logic_error if the ring mutated (version mismatch) while walk was in-flight.
    ReorgInfo apply(const ForkWalk& walk);

    // Evaluates whether the detected fork crossed below the last released block cursor C_s.
    // Watch out: Returns true if fork_point < C_s, requiring an immediate reverify_released() sweep.
    static bool crosses_safe(const ReorgInfo& info, std::uint64_t c_s) { return info.fork_point < c_s; }

    // Drops all ring entries; invoked upon provider failover to re-seed from new provider.
    void reset() { ring_.clear(); }

    std::optional<std::uint64_t> top() const;
    std::optional<core::Hash32> hash_at(std::uint64_t number) const;
    std::size_t size() const { return ring_.size(); }

private:
    friend class ForkWalk;
    void insert(const BlockRef& b);
    std::size_t capacity_;
    std::map<std::uint64_t, BlockRef> ring_;
    std::uint64_t version_{0};  // bumped on every ring mutation; guards apply() against stale walks
};

// Stepper over the new chain's ancestry: `need()` names the next block number the caller must fetch (by number,
// from an HTTP provider), `feed()` supplies it. Walks back until an ancestor matches the ring.
class ForkWalk {
public:
    enum class State {
        NeedBlock,    // fetch need() and feed() it
        Done,         // fork point found; pass to ReorgDetector::apply()
        BeyondRing,   // no common ancestor within the ring: depth exceeds hash_ring_size, escalate
        Inconsistent  // a fetched block did not match the hash its child committed to: provider fault or a
                      // reorg mid-walk; discard and restart with a fresh begin_walk()
    };

    State state() const { return state_; }
    // Valid only in NeedBlock.
    std::uint64_t need() const { return cursor_.number - 1; }
    // Hash the fetched block must have (valid only in NeedBlock).
    const core::Hash32& expected_hash() const { return cursor_.parent; }

    void feed(const BlockRef& fetched);

    // Valid only in Done.
    std::uint64_t fork_point() const { return fork_point_; }
    // New chain above the fork point, ascending by number (valid only in Done).
    const std::vector<BlockRef>& new_chain() const { return chain_; }

private:
    friend class ReorgDetector;
    ForkWalk(const ReorgDetector& det, const BlockRef& head);
    void resolve();  // check the cursor's parent against the ring and update state_

    const ReorgDetector* det_;
    std::uint64_t version_;
    BlockRef cursor_;                // oldest block of the new chain collected so far
    std::vector<BlockRef> chain_;    // descending while walking, reversed once Done
    State state_{State::NeedBlock};
    std::uint64_t fork_point_{};
};

}  // namespace evml::chain
