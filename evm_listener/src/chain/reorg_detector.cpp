#include "chain/reorg_detector.hpp"

#include <algorithm>
#include <stdexcept>

namespace evml::chain {

const char* to_string(HeadKind k) {
    switch (k) {
        case HeadKind::Seeded: return "Seeded";
        case HeadKind::Extended: return "Extended";
        case HeadKind::Duplicate: return "Duplicate";
        case HeadKind::NeedWalk: return "NeedWalk";
    }
    return "?";
}

ReorgDetector::ReorgDetector(std::size_t capacity) : capacity_(capacity) {
    if (capacity == 0) throw std::invalid_argument("ReorgDetector: capacity must be >= 1");
}

// Inserts a new block reference into the ring, evicting the oldest block if capacity is exceeded.
void ReorgDetector::insert(const BlockRef& b) {
    ring_[b.number] = b;
    while (ring_.size() > capacity_) ring_.erase(ring_.begin()); // Evict oldest entry past capacity
    ++version_; // Invalidate in-flight walks
}

std::optional<std::uint64_t> ReorgDetector::top() const {
    if (ring_.empty()) return std::nullopt;
    return ring_.rbegin()->first;
}

std::optional<core::Hash32> ReorgDetector::hash_at(std::uint64_t number) const {
    const auto it = ring_.find(number);
    if (it == ring_.end()) return std::nullopt;
    return it->second.hash;
}

std::optional<BlockRef> ReorgDetector::ref_at(std::uint64_t number) const {
    const auto it = ring_.find(number);
    if (it == ring_.end()) return std::nullopt;
    return it->second;
}

std::optional<std::uint64_t> ReorgDetector::timestamp_of(std::uint64_t number, const core::Hash32& hash) const {
    const auto it = ring_.find(number);
    if (it == ring_.end() || !(it->second.hash == hash) || it->second.timestamp == 0) return std::nullopt;
    return it->second.timestamp;
}

// Evaluates incoming head against ring top; categorizes into Seeded, Extended, Duplicate, or NeedWalk.
HeadKind ReorgDetector::on_head(const BlockRef& head) {
    if (ring_.empty()) {
        insert(head); // First head observation; seeds ring
        return HeadKind::Seeded;
    }
    const BlockRef& t = ring_.rbegin()->second;
    if (head.number == t.number + 1) {
        if (head.parent == t.hash) {
            insert(head); // Contiguous extension: parent hash matches current ring top
            return HeadKind::Extended;
        }
        return HeadKind::NeedWalk; // Same height + 1 but parent hash mismatched (reorg at head)
    }
    if (head.number <= t.number) {
        const auto it = ring_.find(head.number);
        if (it != ring_.end() && it->second.hash == head.hash) return HeadKind::Duplicate; // Idempotent duplicate
        return HeadKind::NeedWalk;  // different hash at a known height, or older than the ring
    }
    return HeadKind::NeedWalk;  // gap above the top: missed intervening block headers
}

// Instantiates a ForkWalk session to step backward through ancestors to find fork point F.
ForkWalk ReorgDetector::begin_walk(const BlockRef& head) const { return ForkWalk(*this, head); }

// Commits completed fork walk; drops orphaned blocks above fork point and splices in winning fork branch.
ReorgInfo ReorgDetector::apply(const ForkWalk& walk) {
    if (walk.state() != ForkWalk::State::Done) throw std::logic_error("ReorgDetector::apply: walk is not Done");
    if (walk.det_ != this || walk.version_ != version_)
        throw std::logic_error("ReorgDetector::apply: ring changed since begin_walk(); restart the walk");

    const std::uint64_t old_top = ring_.empty() ? 0 : ring_.rbegin()->first;
    const std::uint64_t f = walk.fork_point();
    ring_.erase(ring_.upper_bound(f), ring_.end()); // Discard all blocks above common ancestor fork point
    for (const auto& b : walk.new_chain()) insert(b); // Splice in newly discovered branch
    return ReorgInfo{f, old_top, old_top > f ? old_top - f : 0};
}

ForkWalk::ForkWalk(const ReorgDetector& det, const BlockRef& head)
    : det_(&det), version_(det.version_), cursor_(head), chain_{head} {
    resolve();
}

// Tests cursor's parent against ring; transitions to Done (found), NeedBlock (keep walking), or BeyondRing.
void ForkWalk::resolve() {
    const auto& ring = det_->ring_;
    if (ring.empty() || cursor_.number == 0) {
        state_ = State::BeyondRing;
        return;
    }
    const std::uint64_t p = cursor_.number - 1;
    if (p < ring.begin()->first) {
        state_ = State::BeyondRing; // Reorg depth exceeds ring capacity; manual intervention needed
        return;
    }
    const auto it = ring.find(p);  // absent when p is above the top (gap): fetch it
    // Check if parent block hash in ring matches parent hash in child block header
    if (it != ring.end() && it->second.hash == cursor_.parent) {
        fork_point_ = p; // Common ancestor located
        std::reverse(chain_.begin(), chain_.end()); // Put chain in ascending block number order
        state_ = State::Done;
        return;
    }
    state_ = State::NeedBlock; // Must fetch parent block from RPC
}

// Feeds fetched ancestor block into the walk; verifies cryptographic hash continuity.
void ForkWalk::feed(const BlockRef& fetched) {
    if (state_ != State::NeedBlock) throw std::logic_error("ForkWalk::feed: not waiting for a block");
    // Verify block number matches need() and block hash matches child's parent hash
    if (fetched.number != need() || !(fetched.hash == cursor_.parent)) {
        state_ = State::Inconsistent; // Hash mismatch: reorg occurred mid-walk or provider returned corrupt block
        return;
    }
    chain_.push_back(fetched);
    cursor_ = fetched; // Step backward to parent
    resolve();
}

}  // namespace evml::chain
