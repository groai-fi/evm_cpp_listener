#include <catch2/catch_test_macros.hpp>

#include "provider/token_bucket.hpp"

using evml::provider::TokenBucket;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

TEST_CASE("token bucket: the burst goes out at once, then one call per 1/rps", "[unit][token_bucket]") {
    const auto t0 = Clock::now();
    TokenBucket b(2, 4, t0);  // rps 2, burst 4
    for (int i = 0; i < 4; ++i) CHECK(b.reserve(t0) == Clock::duration::zero());
    CHECK(b.reserve(t0) == 500ms);   // 5th waits for one token
    CHECK(b.reserve(t0) == 1000ms);  // reservations queue
    CHECK(b.reserve(t0) == 1500ms);
}

TEST_CASE("token bucket: refills with time and never above burst", "[unit][token_bucket]") {
    const auto t0 = Clock::now();
    TokenBucket b(2, 4, t0);
    for (int i = 0; i < 4; ++i) b.reserve(t0);
    CHECK(b.reserve(t0 + 500ms) == Clock::duration::zero());  // 1 token refilled in 500 ms
    CHECK(b.reserve(t0 + 500ms) == 500ms);                    // empty again

    TokenBucket idle(2, 4, t0);
    const auto later = t0 + 3600s;  // an hour idle must not bank more than `burst`
    for (int i = 0; i < 4; ++i) CHECK(idle.reserve(later) == Clock::duration::zero());
    CHECK(idle.reserve(later) == 500ms);
}

TEST_CASE("token bucket: rejects an invalid shape", "[unit][token_bucket]") {
    const auto t0 = Clock::now();
    CHECK_THROWS_AS(TokenBucket(0, 4, t0), std::invalid_argument);
    CHECK_THROWS_AS(TokenBucket(5, 4, t0), std::invalid_argument);
    CHECK_NOTHROW(TokenBucket(4, 4, t0));
}
