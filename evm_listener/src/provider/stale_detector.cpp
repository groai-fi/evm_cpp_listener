#include "provider/stale_detector.hpp"

#include <algorithm>

namespace evml::provider {

namespace {
std::uint64_t distance(std::uint64_t a, std::uint64_t b) { return a > b ? a - b : b - a; }
}  // namespace

StaleDetector::StaleDetector(TimePoint start, StaleThresholds t)
    : t_(t), session_baseline_(start), last_head_at_(start) {}

void StaleDetector::on_ws_head(std::uint64_t number, TimePoint now) {
    ws_head_ = number;
    session_baseline_ = now;
    last_head_at_ = now;
    evaluate(now);
}
void StaleDetector::on_http_head(std::uint64_t number, TimePoint now) {
    http_head_ = number;
    evaluate(now);
}
void StaleDetector::on_ws_safe(std::uint64_t number, TimePoint now) {
    ws_safe_ = number;
    evaluate(now);
}
void StaleDetector::on_http_safe(std::uint64_t number, TimePoint now) {
    http_safe_ = number;
    evaluate(now);
}

void StaleDetector::reset_ws(TimePoint now) {
    ws_head_.reset();
    ws_safe_.reset();
    session_baseline_ = now;
    ws_lag_since_.reset();
    safe_lag_since_.reset();  // the safe comparison involves the WS side too
}

void StaleDetector::reset_http(TimePoint) {
    http_head_.reset();
    http_safe_.reset();
    ws_lag_since_.reset();
    safe_lag_since_.reset();
}

void StaleDetector::evaluate(TimePoint now) {
    // Check if HTTP head block is ahead of WS head by more than ws_lag_blocks (default 5).
    const bool head_lag = ws_head_ && http_head_ && *http_head_ > *ws_head_ + t_.ws_lag_blocks;
    if (!head_lag) {
        ws_lag_since_.reset();
    } else if (!ws_lag_since_) {
        // Start continuous lag timer.
        ws_lag_since_ = now;
    }

    // Check if WS safe tag block is ahead of HTTP safe tag by more than safe_lag_blocks (default 30).
    const bool safe_lag = ws_safe_ && http_safe_ && *ws_safe_ > *http_safe_ + t_.safe_lag_blocks;
    if (!safe_lag) {
        safe_lag_since_.reset();
    } else if (!safe_lag_since_) {
        // Start continuous safe tag lag timer.
        safe_lag_since_ = now;
    }
}

std::chrono::milliseconds StaleDetector::ws_head_silence(TimePoint now) const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(now - last_head_at_);
}

bool StaleDetector::ws_head_stalled(TimePoint now) const {
    // Triggers if elapsed time since last received newHeads block (or session reset) exceeds head_stall (default 15s).
    return now - session_baseline_ >= t_.head_stall;
}

bool StaleDetector::ws_stale(TimePoint now) const {
    // Triggers if WS head has lagged HTTP head continuously for more than ws_lag_for (default 30s).
    return ws_lag_since_ && now - *ws_lag_since_ >= t_.ws_lag_for;
}

bool StaleDetector::getlogs_stale(TimePoint now) const {
    // Triggers if getLogs safe tag has lagged WS safe tag continuously for more than safe_lag_for (default 120s).
    return safe_lag_since_ && now - *safe_lag_since_ >= t_.safe_lag_for;
}

std::optional<std::uint64_t> StaleDetector::divergence() const {
    // Calculate maximum absolute block distance between WS and HTTP for both head and safe tags.
    std::uint64_t worst = 0;
    if (ws_head_ && http_head_) worst = std::max(worst, distance(*ws_head_, *http_head_));
    if (ws_safe_ && http_safe_) worst = std::max(worst, distance(*ws_safe_, *http_safe_));
    if (worst > t_.divergence_blocks) return worst;
    return std::nullopt;
}

}  // namespace evml::provider
