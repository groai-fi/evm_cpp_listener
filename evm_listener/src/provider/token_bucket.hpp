#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <stdexcept>

namespace evml::provider {

/// @brief Token bucket pacing (rate limiter): refills `rps` tokens per second up to `burst`.
/// Time is passed in, so it is deterministic in tests. Not thread-safe (the pool is single-threaded).
/// Example: rps=2, burst=4 lets 4 calls go out at once, then one call every 500 ms.
class TokenBucket {
public:
    using Clock = std::chrono::steady_clock;

    /// @throws std::invalid_argument if rps == 0 or burst < rps.
    TokenBucket(std::uint32_t rps, std::uint32_t burst, Clock::time_point now)
        : rps_(rps), burst_(burst), tokens_(static_cast<double>(burst)), last_(now) {
        if (rps == 0) throw std::invalid_argument("TokenBucket: rps must be >= 1");
        if (burst < rps) throw std::invalid_argument("TokenBucket: burst must be >= rps");
    }

    /// Reserves one token and returns how long the caller must wait before using it (zero = go now).
    /// Reservations queue: with an empty bucket, three callers get waits of 1/rps, 2/rps and 3/rps.
    Clock::duration reserve(Clock::time_point now) {
        if (now > last_) {
            const double elapsed = std::chrono::duration<double>(now - last_).count();
            tokens_ = std::min(static_cast<double>(burst_), tokens_ + elapsed * rps_);
            last_ = now;
        }
        tokens_ -= 1.0;
        if (tokens_ >= 0.0) return Clock::duration::zero();
        return std::chrono::ceil<Clock::duration>(std::chrono::duration<double>(-tokens_ / rps_));
    }

private:
    std::uint32_t rps_;
    std::uint32_t burst_;
    double tokens_;
    Clock::time_point last_;
};

}  // namespace evml::provider
