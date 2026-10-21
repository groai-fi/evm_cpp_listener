#pragma once

#include <chrono>
#include <cstdint>
#include <optional>

namespace evml::provider {

/// @brief Configurable block and time thresholds for stale node and stall detection.
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
/// @brief Pure evaluation state machine tracking head block and safe tag divergence across dual providers.
/// Guarantees:
/// - Silent Stall Catch: Flags WS stalls if no newHeads are received within `head_stall` duration (default 15s).
/// - Cross-Provider Lag: Detects if WS head lags HTTP head by > 5 blocks for > 30s, or getLogs safe tag lags by > 30 blocks for > 120s.
/// - Deterministic: Pure logic with zero I/O; accepts explicit `TimePoint now` on all methods for mockability.
class StaleDetector {
public:
    using TimePoint = std::chrono::steady_clock::time_point;

    /// @brief Initializes detector with starting timestamp and configured thresholds.
    explicit StaleDetector(TimePoint start, StaleThresholds t = {});

    /// @brief Ingests latest newHeads block number from active WebSocket provider.
    void on_ws_head(std::uint64_t number, TimePoint now);
    /// @brief Ingests latest block number from HTTP getLogs provider.
    void on_http_head(std::uint64_t number, TimePoint now);
    /// @brief Ingests safe tag block number from active WebSocket provider.
    void on_ws_safe(std::uint64_t number, TimePoint now);
    /// @brief Ingests safe tag block number from HTTP getLogs provider.
    void on_http_safe(std::uint64_t number, TimePoint now);

    // The WS / getLogs provider changed: forget everything learned about the old one (values and lag timers).
    /// @brief Resets WS head metrics and baseline timestamp when WebSocket provider fails over.
    void reset_ws(TimePoint now);
    /// @brief Resets HTTP metrics and lag timers when getLogs provider changes.
    void reset_http(TimePoint now);

    /// @brief Returns true if newHeads stream has been completely silent longer than head_stall threshold.
    bool ws_head_stalled(TimePoint now) const;  // newHeads silence
    /// @brief Returns true if WS head has continuously lagged HTTP head by > ws_lag_blocks for > ws_lag_for.
    bool ws_stale(TimePoint now) const;         // WS head lags the getLogs provider's head
    /// @brief Returns true if HTTP safe tag has continuously lagged WS safe tag by > safe_lag_blocks for > safe_lag_for.
    bool getlogs_stale(TimePoint now) const;    // getLogs `safe` lags the WS provider's `safe`

    // Largest head/safe distance between the two sides when it exceeds divergence_blocks (WARN material).
    /// @brief Returns maximum block distance between WS and HTTP if it exceeds divergence threshold (default 200 blocks).
    std::optional<std::uint64_t> divergence() const;

private:
    void evaluate(TimePoint now);

    StaleThresholds t_;
    std::optional<std::uint64_t> ws_head_, http_head_, ws_safe_, http_safe_;
    TimePoint ws_baseline_;  // last newHeads arrival, or the last reset/start before the first one
    std::optional<TimePoint> ws_lag_since_, safe_lag_since_;
};

}  // namespace evml::provider
