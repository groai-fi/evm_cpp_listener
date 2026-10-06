#include "pipeline/aimd_pager.hpp"

#include <algorithm>
#include <stdexcept>

namespace evml::pipeline {

std::optional<BlockRange> next_range(std::uint64_t cursor, std::uint64_t gate, std::uint32_t size) {
    if (size == 0) throw std::invalid_argument("next_range: size must be > 0");
    if (cursor >= gate) return std::nullopt;
    const std::uint64_t step = std::min<std::uint64_t>(gate - cursor, size);  // gate > cursor here: cannot overflow
    return BlockRange{cursor + 1, cursor + step};
}

AimdPager::AimdPager(std::uint32_t max_range, std::uint32_t min_range, std::uint32_t grow_after)
    : max_(max_range), min_(std::min(min_range, max_range)), grow_after_(grow_after), range_(max_range) {
    if (max_range == 0) throw std::invalid_argument("AimdPager: max_range must be > 0");
    if (min_range == 0) throw std::invalid_argument("AimdPager: min_range must be > 0");
    if (grow_after == 0) throw std::invalid_argument("AimdPager: grow_after must be > 0");
}

void AimdPager::on_success() {
    if (range_ >= max_) {
        streak_ = 0;
        return;
    }
    if (++streak_ >= grow_after_) {
        range_ = static_cast<std::uint32_t>(std::min<std::uint64_t>(max_, static_cast<std::uint64_t>(range_) * 2));
        streak_ = 0;
    }
}

AimdPager::Shrink AimdPager::on_range_too_large(std::optional<std::uint32_t> suggested) {
    streak_ = 0;
    if (range_ <= min_) return Shrink::AtMin;
    std::uint32_t next = (suggested && *suggested < range_) ? *suggested : range_ / 2;
    range_ = std::max(next, min_);
    return Shrink::Shrunk;
}

}  // namespace evml::pipeline
