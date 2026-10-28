#include <catch2/catch_test_macros.hpp>

#include "provider/stale_detector.hpp"

using namespace evml::provider;
using namespace std::chrono_literals;

namespace {
const StaleDetector::TimePoint T0{};
StaleDetector::TimePoint at(std::chrono::seconds s) { return T0 + s; }
}  // namespace

TEST_CASE("stale: WS head lag must exceed 5 blocks continuously for 30 s") {
    StaleDetector d(at(0s));
    d.on_ws_head(1000, at(0s));
    d.on_http_head(1005, at(0s));  // lag == 5: not "more than 5"
    CHECK_FALSE(d.ws_stale(at(1000s)));

    d.on_http_head(1006, at(10s));  // lag 6: timer starts
    CHECK_FALSE(d.ws_stale(at(39s)));
    CHECK(d.ws_stale(at(40s)));
}

TEST_CASE("stale: catching up resets the WS lag timer") {
    StaleDetector d(at(0s));
    d.on_ws_head(1000, at(0s));
    d.on_http_head(1010, at(0s));
    CHECK_FALSE(d.ws_stale(at(29s)));

    d.on_ws_head(1008, at(29s));  // lag 2: recovered
    CHECK_FALSE(d.ws_stale(at(100s)));

    d.on_http_head(1020, at(100s));  // lags again: a fresh 30 s window
    CHECK_FALSE(d.ws_stale(at(129s)));
    CHECK(d.ws_stale(at(130s)));
}

TEST_CASE("stale: a WS head AHEAD of the getLogs head is never WS-stale") {
    StaleDetector d(at(0s));
    d.on_ws_head(2000, at(0s));
    d.on_http_head(1000, at(0s));
    CHECK_FALSE(d.ws_stale(at(1000s)));
}

TEST_CASE("stale: getLogs safe tag lag must exceed 30 blocks for 2 min") {
    StaleDetector d(at(0s));
    d.on_ws_safe(5000, at(0s));
    d.on_http_safe(4970, at(0s));  // lag == 30: fine
    CHECK_FALSE(d.getlogs_stale(at(1000s)));

    d.on_http_safe(4969, at(10s));  // lag 31
    CHECK_FALSE(d.getlogs_stale(at(129s)));
    CHECK(d.getlogs_stale(at(130s)));

    d.on_http_safe(4990, at(131s));  // caught up
    CHECK_FALSE(d.getlogs_stale(at(131s)));
}

TEST_CASE("stale: newHeads silence is detected from the start, reset and last head") {
    StaleDetector d(at(0s));
    CHECK_FALSE(d.ws_head_stalled(at(14s)));
    CHECK(d.ws_head_stalled(at(15s)));  // never saw a head since start

    d.on_ws_head(1, at(20s));
    CHECK_FALSE(d.ws_head_stalled(at(34s)));
    CHECK(d.ws_head_stalled(at(35s)));

    d.reset_ws(at(100s));  // new WS provider: its own 15 s grace starts at reset
    CHECK_FALSE(d.ws_head_stalled(at(114s)));
    CHECK(d.ws_head_stalled(at(115s)));
}

TEST_CASE("stale: resets forget the old provider's lag state") {
    StaleDetector d(at(0s));
    d.on_ws_head(1000, at(0s));
    d.on_http_head(1100, at(0s));
    d.on_ws_safe(1000, at(0s));
    d.on_http_safe(700, at(0s));  // 300 blocks apart: above the 200-block divergence threshold
    REQUIRE(d.divergence().has_value());

    d.reset_http(at(50s));  // getLogs provider replaced
    CHECK_FALSE(d.ws_stale(at(1000s)));
    CHECK_FALSE(d.getlogs_stale(at(1000s)));
    CHECK_FALSE(d.divergence().has_value());

    d.on_http_head(1100, at(60s));  // the new one lags too: timer restarts from here, not from t=0
    CHECK_FALSE(d.ws_stale(at(89s)));
    CHECK(d.ws_stale(at(90s)));

    d.reset_ws(at(100s));
    CHECK_FALSE(d.ws_stale(at(1000s)));
}

TEST_CASE("stale: divergence reports the worst head/safe distance above 200 blocks") {
    StaleDetector d(at(0s));
    d.on_ws_head(1000, at(0s));
    d.on_http_head(1200, at(0s));  // 200: not above the threshold
    CHECK_FALSE(d.divergence().has_value());

    d.on_ws_safe(5000, at(0s));
    d.on_http_safe(4700, at(0s));  // 300
    d.on_http_head(1201, at(0s));  // 201
    REQUIRE(d.divergence().has_value());
    CHECK(*d.divergence() == 300);
}

TEST_CASE("stale: custom thresholds are honoured") {
    StaleThresholds t;
    t.ws_lag_blocks = 1;
    t.ws_lag_for = 5s;
    StaleDetector d(at(0s), t);
    d.on_ws_head(10, at(0s));
    d.on_http_head(12, at(0s));
    CHECK_FALSE(d.ws_stale(at(4s)));
    CHECK(d.ws_stale(at(5s)));
}

TEST_CASE("stale: ws_head_silence is the time since the last head, or since start / reset") {
    StaleDetector d(at(0s));
    CHECK(d.ws_head_silence(at(7s)) == 7000ms);  // nothing received yet: counts from start
    d.on_ws_head(1000, at(10s));
    CHECK(d.ws_head_silence(at(12s)) == 2000ms);
    d.reset_ws(at(20s));
    CHECK(d.ws_head_silence(at(21s)) == 1000ms);
}
