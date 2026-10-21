#include "rpc/backoff.hpp"

#include <algorithm>
#include <stdexcept>

namespace evml::rpc {

/// @brief Initializes backoff parameters and seed.
/// @param seed PRNG seed for reproducible or randomized jitter.
/// @param base Initial base delay (must be > 0).
/// @param cap Maximum backoff window cap (must be >= base).
/// @param min_live Minimum active duration before attempt count resets.
/// @warning Throws std::invalid_argument if base <= 0 or cap < base.
Backoff::Backoff(std::uint64_t seed, ms base, ms cap, ms min_live)
    : base_(base), cap_(cap), min_live_(min_live), rng_(seed) {
    // Validate positive non-zero base delay
    if (base_.count() <= 0) throw std::invalid_argument("Backoff: base must be > 0");
    // Validate cap upper bound satisfies cap >= base
    if (cap_ < base_) throw std::invalid_argument("Backoff: cap must be >= base");
}

/// @brief Computes deterministic upper bound window for current attempt count: min(cap, base * 2^attempt).
/// @return Upper bound duration in milliseconds.
Backoff::ms Backoff::window() const {
    // Clamp shift exponent to 40 to prevent 64-bit signed integer overflow on large attempt counts
    const std::uint32_t shift = std::min<std::uint32_t>(attempt_, 40);
    const std::int64_t base = base_.count();
    // Guard against shift overflow: check if base exceeds cap shifted down
    const std::int64_t grown = (base > (cap_.count() >> shift)) ? cap_.count() : (base << shift);
    // Return window bounded strictly by configured cap
    return ms(std::min<std::int64_t>(grown, cap_.count()));
}

/// @brief Draws a full-jitter delay uniformly in [0, window()] and increments attempt counter.
/// @return Full-jitter sleep delay in milliseconds.
/// @note Full jitter distributes reconnection load across the interval, avoiding thundering herd.
Backoff::ms Backoff::next() {
    // Compute current backoff upper window
    const auto w = window();
    // Uniform integer distribution across [0, window] interval for full jitter
    std::uniform_int_distribution<std::int64_t> dist(0, w.count());
    // Increment consecutive attempt counter
    ++attempt_;
    // Draw pseudo-random delay
    return ms(dist(rng_));
}

/// @brief Resets attempt counter if the session stayed healthy for at least min_live.
/// @param live_for Duration the connection stayed continuously active and healthy.
/// @note Prevents rapid flap-reconnect cycles from resetting backoff escalation prematurely.
void Backoff::report_live(ms live_for) {
    // Only reset consecutive counter when connection proved sustained stability
    if (live_for >= min_live_) attempt_ = 0;
}

}  // namespace evml::rpc
