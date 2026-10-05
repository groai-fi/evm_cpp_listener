#include "provider/stale_detector.hpp"

#include <algorithm>

namespace evml::provider {

namespace {
std::uint64_t distance(std::uint64_t a, std::uint64_t b) { return a > b ? a - b : b - a; }
}  // namespace

StaleDetector::StaleDetector(TimePoint start, StaleThresholds t) : t_(t), ws_baseline_(start) {}

void StaleDetector::on_ws_head(std::uint64_t number, TimePoint now) {
    ws_head_ = number;
    ws_baseline_ = now;
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
    ws_baseline_ = now;
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
    const bool head_lag = ws_head_ && http_head_ && *http_head_ > *ws_head_ + t_.ws_lag_blocks;
    if (!head_lag) {
        ws_lag_since_.reset();
    } else if (!ws_lag_since_) {
        ws_lag_since_ = now;
    }

    const bool safe_lag = ws_safe_ && http_safe_ && *ws_safe_ > *http_safe_ + t_.safe_lag_blocks;
    if (!safe_lag) {
        safe_lag_since_.reset();
    } else if (!safe_lag_since_) {
        safe_lag_since_ = now;
    }
}

bool StaleDetector::ws_head_stalled(TimePoint now) const { return now - ws_baseline_ >= t_.head_stall; }

bool StaleDetector::ws_stale(TimePoint now) const { return ws_lag_since_ && now - *ws_lag_since_ >= t_.ws_lag_for; }

bool StaleDetector::getlogs_stale(TimePoint now) const {
    return safe_lag_since_ && now - *safe_lag_since_ >= t_.safe_lag_for;
}

std::optional<std::uint64_t> StaleDetector::divergence() const {
    std::uint64_t worst = 0;
    if (ws_head_ && http_head_) worst = std::max(worst, distance(*ws_head_, *http_head_));
    if (ws_safe_ && http_safe_) worst = std::max(worst, distance(*ws_safe_, *http_safe_));
    if (worst > t_.divergence_blocks) return worst;
    return std::nullopt;
}

}  // namespace evml::provider
