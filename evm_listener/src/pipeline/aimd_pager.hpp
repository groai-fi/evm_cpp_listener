#pragma once

#include <cstdint>
#include <optional>

namespace evml::pipeline {

// Inclusive block range.
struct BlockRange {
    std::uint64_t from{};
    std::uint64_t to{};
    friend bool operator==(const BlockRange&, const BlockRange&) = default;
};

// The next page of a sweep: (cursor, min(gate, cursor + size)]. nullopt when the cursor has reached the gate
// (nothing to sweep). Throws std::invalid_argument for size == 0 (programmer error).
std::optional<BlockRange> next_range(std::uint64_t cursor, std::uint64_t gate, std::uint32_t size);

// Additive-increase / multiplicative-decrease page size for eth_getLogs (plan 3.5), one instance per provider:
//  - starts at that provider's measured maximum;
//  - RangeTooLarge: shrink to the provider-suggested size when it is smaller than the current one, else halve
//    (never below min_range);
//  - after `grow_after` consecutive successes, double (never above the maximum).
// min_range is clamped to the maximum (QuickNode's 5-block cap < the configured minimum of 10).
class AimdPager {
public:
    // Throws std::invalid_argument for max_range == 0, min_range == 0 or grow_after == 0.
    AimdPager(std::uint32_t max_range, std::uint32_t min_range, std::uint32_t grow_after = 5);

    std::uint32_t range() const { return range_; }
    std::uint32_t max() const { return max_; }
    std::uint32_t min() const { return min_; }

    void on_success();

    enum class Shrink { Shrunk, AtMin };
    // AtMin: the request is still too large at the minimum: the caller must split by address, and stall (I5)
    // if that fails too.
    Shrink on_range_too_large(std::optional<std::uint32_t> suggested);

private:
    std::uint32_t max_, min_, grow_after_;
    std::uint32_t range_;
    std::uint32_t streak_{0};
};

}  // namespace evml::pipeline
