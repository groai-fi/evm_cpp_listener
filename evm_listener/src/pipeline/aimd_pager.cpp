#include "pipeline/aimd_pager.hpp"

#include <algorithm>
#include <stdexcept>

namespace evml::pipeline {

// Computes next contiguous block range strictly between cursor and gate up to size.
// Watch out: Returns std::nullopt when cursor >= gate (nothing to sweep).
std::optional<BlockRange> next_range(std::uint64_t cursor, std::uint64_t gate, std::uint32_t size) {
    if (size == 0) throw std::invalid_argument("next_range: size must be > 0");
    if (cursor >= gate) return std::nullopt; // Invariant I1: no blocks pending when cursor caught up with gate
    const std::uint64_t step = std::min<std::uint64_t>(gate - cursor, size);  // gate > cursor here: cannot overflow
    return BlockRange{cursor + 1, cursor + step}; // Contiguous half-open interval (cursor, cursor + step]
}

AimdPager::AimdPager(std::uint32_t max_range, std::uint32_t min_range, std::uint32_t grow_after)
    : max_(max_range), min_(std::min(min_range, max_range)), grow_after_(grow_after), range_(max_range) {
    if (max_range == 0) throw std::invalid_argument("AimdPager: max_range must be > 0");
    if (min_range == 0) throw std::invalid_argument("AimdPager: min_range must be > 0");
    if (grow_after == 0) throw std::invalid_argument("AimdPager: grow_after must be > 0");
}

// Registers successful query; doubles query page range after grow_after consecutive successes.
void AimdPager::on_success() {
    if (range_ >= max_) {
        streak_ = 0;
        return;
    }
    // Additive step: double page size once success threshold is reached
    if (++streak_ >= grow_after_) {
        range_ = static_cast<std::uint32_t>(std::min<std::uint64_t>(max_, static_cast<std::uint64_t>(range_) * 2));
        streak_ = 0;
    }
}

// Multiplicative decrease step: shrinks block range on provider RangeTooLarge error.
// Watch out: Returns Shrink::AtMin when range cannot shrink further; triggers address-split fallback.
AimdPager::Shrink AimdPager::on_range_too_large(std::optional<std::uint32_t> suggested) {
    streak_ = 0; // Reset success streak immediately on range limit error
    if (range_ <= min_) return Shrink::AtMin;
    // Prefer provider's suggested limit if smaller than current range, otherwise halve range
    std::uint32_t next = (suggested && *suggested < range_) ? *suggested : range_ / 2;
    range_ = std::max(next, min_); // Clamp to min_range
    return Shrink::Shrunk;
}

}  // namespace evml::pipeline
