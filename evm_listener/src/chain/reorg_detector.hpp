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

// Hash ring `number -> {hash, parent}` (plan 3.6). Pure: no I/O, no clock. Single-threaded.
// The caller (async) fetches ancestors that the walk asks for, so the detector never blocks.
class ReorgDetector {
public:
    explicit ReorgDetector(std::size_t capacity);

    // Feed a head from the ACTIVE session. Extended/Seeded/Duplicate are applied immediately;
    // NeedWalk leaves the ring untouched until apply() succeeds.
    HeadKind on_head(const BlockRef& head);

    // Begin resolving a NeedWalk head. Valid for any head, but only meaningful after NeedWalk.
    ForkWalk begin_walk(const BlockRef& head) const;

    // Commit a successfully completed walk: drops ring entries above the fork point and inserts the new
    // chain. Throws std::logic_error if the walk is not Done or the ring changed since begin_walk().
    ReorgInfo apply(const ForkWalk& walk);

    // Safe-reorg trigger: the fork point is at or below the last released block C_s (plan 3.6).
    static bool crosses_safe(const ReorgInfo& info, std::uint64_t c_s) { return info.fork_point < c_s; }

    // Drop everything (after a provider failover the ring is re-seeded from the new provider).
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
