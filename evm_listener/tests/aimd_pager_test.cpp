#include <catch2/catch_test_macros.hpp>

#include "pipeline/aimd_pager.hpp"
#include "pipeline/recent_set.hpp"

using namespace evml::pipeline;

TEST_CASE("next_range pages after the cursor up to the gate and stops at the gate", "[unit][pager]") {
    CHECK(next_range(100, 100, 500) == std::nullopt);
    CHECK(next_range(101, 100, 500) == std::nullopt);  // cursor beyond a lower gate (after a provider failover)
    CHECK(next_range(100, 105, 500) == BlockRange{101, 105});
    CHECK(next_range(100, 1000, 500) == BlockRange{101, 600});
    CHECK(next_range(0, 3, 5) == BlockRange{1, 3});
    CHECK(next_range(UINT64_MAX - 10, UINT64_MAX, UINT32_MAX) == BlockRange{UINT64_MAX - 9, UINT64_MAX});  // no overflow
    CHECK_THROWS_AS(next_range(0, 10, 0), std::invalid_argument);
}

TEST_CASE("pager: rejects invalid construction", "[unit][pager]") {
    CHECK_THROWS_AS(AimdPager(0, 10), std::invalid_argument);
    CHECK_THROWS_AS(AimdPager(500, 0), std::invalid_argument);
    CHECK_THROWS_AS(AimdPager(500, 10, 0), std::invalid_argument);
}

TEST_CASE("pager: starts at the provider max and halves on RangeTooLarge down to min_range", "[unit][pager]") {
    AimdPager p(500, 10);
    CHECK(p.range() == 500);
    CHECK(p.on_range_too_large(std::nullopt) == AimdPager::Shrink::Shrunk);
    CHECK(p.range() == 250);
    p.on_range_too_large(std::nullopt);  // 125
    p.on_range_too_large(std::nullopt);  // 62
    p.on_range_too_large(std::nullopt);  // 31
    p.on_range_too_large(std::nullopt);  // 15
    CHECK(p.range() == 15);
    CHECK(p.on_range_too_large(std::nullopt) == AimdPager::Shrink::Shrunk);
    CHECK(p.range() == 10);  // 7 clamped to min_range
    CHECK(p.on_range_too_large(std::nullopt) == AimdPager::Shrink::AtMin);
    CHECK(p.range() == 10);
}

TEST_CASE("pager: a provider-suggested limit is used when smaller, otherwise the range halves", "[unit][pager]") {
    AimdPager p(10000, 10);
    p.on_range_too_large(500u);
    CHECK(p.range() == 500);
    p.on_range_too_large(900u);  // suggestion is not smaller than the current range: halve instead
    CHECK(p.range() == 250);
    p.on_range_too_large(3u);  // suggestion below min_range is clamped
    CHECK(p.range() == 10);
}

TEST_CASE("pager: min_range is clamped to a smaller provider max (QuickNode: 5 blocks)", "[unit][pager]") {
    AimdPager p(5, 10);
    CHECK(p.min() == 5);
    CHECK(p.range() == 5);
    CHECK(p.on_range_too_large(std::nullopt) == AimdPager::Shrink::AtMin);  // cannot shrink: split by address
}

TEST_CASE("pager: grows back by doubling after 5 consecutive successes and never above max", "[unit][pager]") {
    AimdPager p(500, 10);
    p.on_range_too_large(std::nullopt);  // 250
    p.on_range_too_large(std::nullopt);  // 125
    for (int i = 0; i < 4; ++i) p.on_success();
    CHECK(p.range() == 125);  // 4 successes: not yet
    p.on_success();
    CHECK(p.range() == 250);
    for (int i = 0; i < 5; ++i) p.on_success();
    CHECK(p.range() == 500);
    for (int i = 0; i < 50; ++i) p.on_success();
    CHECK(p.range() == 500);  // capped at the provider max
}

TEST_CASE("pager: a failure resets the success streak", "[unit][pager]") {
    AimdPager p(500, 10);
    p.on_range_too_large(std::nullopt);  // 250
    for (int i = 0; i < 4; ++i) p.on_success();
    p.on_range_too_large(std::nullopt);  // 125, streak reset
    for (int i = 0; i < 4; ++i) p.on_success();
    CHECK(p.range() == 125);  // 4 since the reset: still not grown
    p.on_success();
    CHECK(p.range() == 250);
}

TEST_CASE("pager: converges on a provider whose true limit is 37 blocks", "[unit][pager]") {
    AimdPager p(10000, 10);
    int calls = 0;
    // Simulated provider: rejects anything above 37, never suggests a limit.
    while (calls++ < 100) {
        if (p.range() > 37) {
            p.on_range_too_large(std::nullopt);
        } else {
            p.on_success();
            if (p.range() <= 37 && calls > 20) break;
        }
    }
    CHECK(p.range() <= 37);
    CHECK(p.range() >= 18);  // within a factor of two of the real limit
}

TEST_CASE("recent set: insert reports novelty and evicts the least recently used", "[unit][dedup]") {
    CHECK_THROWS_AS(RecentSet(0), std::invalid_argument);
    RecentSet s(3);
    CHECK(s.insert("a"));
    CHECK(s.insert("b"));
    CHECK(s.insert("c"));
    CHECK_FALSE(s.insert("a"));  // duplicate, refreshed: now b is the oldest
    CHECK(s.insert("d"));        // evicts b
    CHECK(s.size() == 3);
    CHECK_FALSE(s.contains("b"));
    CHECK(s.contains("a"));
    CHECK(s.contains("c"));
    CHECK(s.contains("d"));
    CHECK(s.insert("b"));  // b was evicted, so it is new again (Redis is the authority beyond this window)
}
