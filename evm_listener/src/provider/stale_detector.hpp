#pragma once

#include <chrono>
#include <cstdint>
#include <optional>

namespace evml::provider {

struct StaleThresholds {
    std::uint32_t ws_lag_blocks{5};                   // WS head behind the getLogs provider's head by MORE than this...
    std::chrono::seconds ws_lag_for{30};              // ...continuously for this long => WS provider is stale
    std::uint32_t safe_lag_blocks{30};                // getLogs provider's `safe` behind the WS provider's by MORE than this...
    std::chrono::seconds safe_lag_for{120};           // ...continuously for this long => getLogs provider is stale
    std::chrono::seconds head_stall{15};              // no newHeads at all for this long => WS session must fail
    std::uint32_t divergence_blocks{200};             // ~1 L1 epoch: heads/tags this far apart => WARN
};

// Pure stale/lag detection for the head tracker (plan 2.6). No I/O, no clock: every call takes `now`.
// "WS side" = the active WebSocket provider (newHeads stream + cross-check reads of its tags);
// "HTTP side" = the current getLogs provider (head + its own `safe` tag). Single-threaded.
class StaleDetector {
public:
    using TimePoint = std::chrono::steady_clock::time_point;

    explicit StaleDetector(TimePoint start, StaleThresholds t = {});

    void on_ws_head(std::uint64_t number, TimePoint now);
    void on_http_head(std::uint64_t number, TimePoint now);
    void on_ws_safe(std::uint64_t number, TimePoint now);
    void on_http_safe(std::uint64_t number, TimePoint now);

    // The WS / getLogs provider changed: forget everything learned about the old one (values and lag timers).
    void reset_ws(TimePoint now);
    void reset_http(TimePoint now);

    bool ws_head_stalled(TimePoint now) const;  // newHeads silence
    bool ws_stale(TimePoint now) const;         // WS head lags the getLogs provider's head
    bool getlogs_stale(TimePoint now) const;    // getLogs `safe` lags the WS provider's `safe`

    // Largest head/safe distance between the two sides when it exceeds divergence_blocks (WARN material).
    std::optional<std::uint64_t> divergence() const;

private:
    void evaluate(TimePoint now);

    StaleThresholds t_;
    std::optional<std::uint64_t> ws_head_, http_head_, ws_safe_, http_safe_;
    TimePoint ws_baseline_;  // last newHeads arrival, or the last reset/start before the first one
    std::optional<TimePoint> ws_lag_since_, safe_lag_since_;
};

}  // namespace evml::provider
