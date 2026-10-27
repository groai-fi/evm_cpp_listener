#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>

#include "ops/watchdog.hpp"

using evml::ops::Watchdog;
using Catch::Matchers::ContainsSubstring;

namespace {

struct Harness {
    std::uint64_t t = 1'000;  // fake monotonic ms
    int aborts = 0;
    std::string last_message;
    Watchdog wd;

    explicit Harness(std::uint64_t stall_ms = 30'000)
        : wd(make_options(stall_ms)) {
        wd.arm();
    }

    Watchdog::Options make_options(std::uint64_t stall_ms) {
        Watchdog::Options o;
        o.stall_ms = stall_ms;
        o.now = [this] { return t; };
        o.on_abort = [this](std::string_view m) {
            ++aborts;
            last_message = std::string(m);
        };
        return o;
    }

    // Advance the fake clock in 1 s steps (the watchdog's wake period), beating the given sources each step.
    void run_seconds(int seconds, bool beat_io, bool beat_sink) {
        for (int i = 0; i < seconds; ++i) {
            t += 1'000;
            if (beat_io) wd.beat_io();
            if (beat_sink) wd.beat_sink();
            wd.check_once();
        }
    }
};

}  // namespace

TEST_CASE("watchdog: does not fire while both beats are fresh", "[watchdog]") {
    Harness h;
    h.wd.set_sink_registered(true);
    h.run_seconds(120, true, true);
    CHECK(h.aborts == 0);
}

TEST_CASE("watchdog: aborts when the io tick stalls past 30 s", "[watchdog]") {
    Harness h;
    h.run_seconds(30, false, false);
    CHECK(h.aborts == 0);  // exactly 30 s old is not yet "past 30 s"
    h.run_seconds(1, false, false);
    CHECK(h.aborts == 1);
    CHECK_THAT(h.last_message, ContainsSubstring("\"heartbeat\":\"io_tick\""));
    CHECK_THAT(h.last_message, ContainsSubstring("\"limit_ms\":\"30000\""));
}

TEST_CASE("watchdog: aborts when the sink loop stalls while io is healthy", "[watchdog]") {
    Harness h;
    h.wd.set_sink_registered(true);
    h.run_seconds(31, true, false);
    CHECK(h.aborts == 1);
    CHECK_THAT(h.last_message, ContainsSubstring("\"heartbeat\":\"sink_loop\""));
}

TEST_CASE("watchdog: ignores the sink beat while no sink is registered (standby)", "[watchdog]") {
    Harness h;
    h.run_seconds(120, true, false);
    CHECK(h.aborts == 0);
}

TEST_CASE("watchdog: registering the sink resets its baseline", "[watchdog]") {
    Harness h;
    h.run_seconds(100, true, false);
    h.wd.set_sink_registered(true);  // sink_beat_ was never written; must not look 100 s old
    h.run_seconds(1, true, false);
    CHECK(h.aborts == 0);
}

TEST_CASE("watchdog: a host pause (own wake gap > interval + tolerance) does not abort", "[watchdog]") {
    Harness h;
    h.wd.set_sink_registered(true);
    h.run_seconds(5, true, true);
    h.t += 60'000;  // docker pause / VM suspend: nothing ran for 60 s
    h.wd.check_once();
    CHECK(h.aborts == 0);
    h.run_seconds(1, true, true);
    CHECK(h.aborts == 0);
    // After the pause the clock restarts: a real stall is still caught 30 s later.
    h.run_seconds(31, false, false);
    CHECK(h.aborts == 1);
}

TEST_CASE("watchdog: fires only once", "[watchdog]") {
    Harness h;
    h.run_seconds(40, false, false);
    CHECK(h.aborts == 1);
}

TEST_CASE("watchdog: the abort line follows the core/log schema and carries the last known state", "[watchdog]") {
    Harness h;
    Watchdog::State s;
    s.role = 1;
    s.driver_state = 2;
    s.c_s = 111;
    s.c_f = 99;
    s.g_s = 222;
    s.g_f = 200;
    s.sink_mode = 3;
    h.wd.publish_state(s);
    h.run_seconds(31, false, false);
    REQUIRE(h.aborts == 1);
    const std::string& m = h.last_message;
    CHECK(m.front() == '{');
    CHECK(m.back() == '\n');
    CHECK_THAT(m, ContainsSubstring("\"ts\":\"20"));  // ISO-8601 UTC, 21st century
    CHECK_THAT(m, ContainsSubstring("Z\",\"level\":\"critical\""));
    CHECK_THAT(m, ContainsSubstring("\"message\":\"watchdog abort: heartbeat stale\""));
    CHECK_THAT(m, ContainsSubstring("\"component\":\"watchdog\""));
    CHECK_THAT(m, ContainsSubstring("\"stale_ms\":\"31000\""));
    CHECK_THAT(m, ContainsSubstring("\"role\":\"1\""));
    CHECK_THAT(m, ContainsSubstring("\"driver_state\":\"2\""));
    CHECK_THAT(m, ContainsSubstring("\"c_s\":\"111\""));
    CHECK_THAT(m, ContainsSubstring("\"c_f\":\"99\""));
    CHECK_THAT(m, ContainsSubstring("\"g_s\":\"222\""));
    CHECK_THAT(m, ContainsSubstring("\"g_f\":\"200\""));
    CHECK_THAT(m, ContainsSubstring("\"sink_mode\":\"3\""));
}

TEST_CASE("watchdog: stale_heartbeat() applies the same rule as the abort", "[watchdog]") {
    Harness h;
    CHECK(h.wd.stale_heartbeat() == nullptr);
    h.t += 30'000;
    CHECK(h.wd.stale_heartbeat() == nullptr);  // exactly 30 s: not stale
    h.t += 1;
    REQUIRE(h.wd.stale_heartbeat() != nullptr);
    CHECK(std::string(h.wd.stale_heartbeat()) == "io_tick");
    h.wd.beat_io();
    CHECK(h.wd.stale_heartbeat() == nullptr);

    h.wd.set_sink_registered(true);
    h.t += 31'000;
    h.wd.beat_io();
    REQUIRE(h.wd.stale_heartbeat() != nullptr);
    CHECK(std::string(h.wd.stale_heartbeat()) == "sink_loop");
    h.wd.set_sink_registered(false);  // standby again
    CHECK(h.wd.stale_heartbeat() == nullptr);
}

TEST_CASE("watchdog: the sink writes its beat through the watchdog-owned slot", "[watchdog]") {
    Harness h;
    h.wd.set_sink_registered(true);
    h.run_seconds(29, true, true);
    h.wd.sink_beat_slot()->store(h.t);  // what RedisSink::run() does with RedisSinkOptions::loop_beat
    h.run_seconds(25, true, false);
    CHECK(h.aborts == 0);  // 25 s since the slot write
    h.run_seconds(6, true, false);
    CHECK(h.aborts == 1);
}

TEST_CASE("watchdog: a threshold is required (no hidden default)", "[watchdog]") {
    Watchdog::Options o;  // stall_ms left at 0
    CHECK_THROWS_AS(Watchdog(o), std::invalid_argument);
    Watchdog::Options p;
    p.stall_ms = 30'000;
    p.check_interval_ms = 0;
    CHECK_THROWS_AS(Watchdog(p), std::invalid_argument);
}

TEST_CASE("watchdog: start() twice is a logic error", "[watchdog]") {
    Watchdog::Options o;
    o.stall_ms = 60'000;
    Watchdog wd(o);
    wd.start();
    CHECK_THROWS_AS(wd.start(), std::logic_error);
    wd.stop();
}

TEST_CASE("watchdog: real thread aborts a process whose io thread never reports", "[watchdog]") {
    std::atomic<bool> aborted{false};
    Watchdog::Options o;
    o.stall_ms = 150;
    o.check_interval_ms = 20;
    o.on_abort = [&](std::string_view) { aborted = true; };
    Watchdog wd(o);
    wd.start();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!aborted && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    wd.stop();
    CHECK(aborted.load());
}

TEST_CASE("watchdog: real thread stays quiet while beats keep coming", "[watchdog]") {
    std::atomic<bool> aborted{false};
    Watchdog::Options o;
    o.stall_ms = 5'000;  // generous: a loaded CI box (ASan, parallel tests) may deschedule us for a while
    o.check_interval_ms = 20;
    o.on_abort = [&](std::string_view) { aborted = true; };
    Watchdog wd(o);
    wd.start();
    for (int i = 0; i < 25; ++i) {
        wd.beat_io();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    wd.stop();
    CHECK_FALSE(aborted.load());
}
