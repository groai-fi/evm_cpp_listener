#pragma once

#include <chrono>
#include <cstdint>
#include <random>

namespace evml::rpc {

// Full-jitter exponential backoff: delay = uniform(0, min(cap, base * 2^attempt)).
// The attempt counter resets only via report_live() after the session stayed Live for `min_live`,
// so a connect-then-die flap keeps escalating instead of hammering the provider.
/// @brief Full-jitter exponential backoff generator with connection stability tracking.
/// Guarantees:
/// - Anti-Flapping: Attempt counter does not reset on connect; only resets after session stays live for `min_live` (default 30s).
/// - Full Jitter: Uniform distribution in [0, min(cap, base * 2^attempt)] avoids synchronization spikes.
class Backoff {
public:
    using ms = std::chrono::milliseconds;

    // Throws std::invalid_argument for base <= 0 or cap < base.
    /// @brief Configures backoff with seed, base delay, maximum cap, and minimum stability duration.
    explicit Backoff(std::uint64_t seed, ms base = ms(250), ms cap = ms(30000), ms min_live = ms(30000));

    // Delay before the next attempt; advances the attempt counter.
    /// @brief Generates jittered delay duration and increments consecutive attempt counter.
    ms next();

    // Upper bound of the jitter window for the *next* call (for tests/metrics).
    /// @brief Computes upper bound window for current attempt count without advancing.
    ms window() const;

    // Session ended after being Live for `live_for`: resets the counter iff live_for >= min_live.
    /// @brief Resets attempt counter to 0 if connection remained live for at least `min_live`.
    void report_live(ms live_for);

    /// @brief Manually resets consecutive attempt count to 0.
    void reset() { attempt_ = 0; }
    /// @brief Current consecutive attempt count.
    std::uint32_t attempt() const { return attempt_; }

private:
    ms base_, cap_, min_live_;
    std::uint32_t attempt_{0};
    std::mt19937_64 rng_;
};

}  // namespace evml::rpc
