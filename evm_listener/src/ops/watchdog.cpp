#include "ops/watchdog.hpp"

#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>

namespace evml::ops {

namespace {

std::uint64_t steady_now_ms() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

void abort_process(std::string_view message) {
    // write(2) is async-signal-safe and allocation-free; stdout is where core/log writes, so the line stays in the
    // same stream and schema. The result is irrelevant because we abort next.
    const ssize_t ignored = ::write(STDOUT_FILENO, message.data(), message.size());
    (void)ignored;
    std::abort();
}

std::uint64_t age_ms(std::uint64_t now, std::uint64_t beat, std::uint64_t baseline) {
    const std::uint64_t ref = std::max(beat, baseline);
    return now > ref ? now - ref : 0;  // beat written after `now` was sampled counts as fresh
}

// "YYYY-MM-DDTHH:MM:SS.mmmZ" from the wall clock without gmtime_r (which takes a libc lock: not safe while wedged).
// Civil-from-days algorithm (H. Hinnant), valid for the proleptic Gregorian calendar.
void format_iso_utc_ms(char (&out)[96]) {
    const auto ms_total = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
    const long long days = ms_total / 86'400'000;
    const long long ms_of_day = ms_total % 86'400'000;
    const long long z = days + 719468;
    const long long era = (z >= 0 ? z : z - 146096) / 146097;
    const long long doe = z - era * 146097;
    const long long yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const long long doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const long long mp = (5 * doy + 2) / 153;
    const int d = static_cast<int>(doy - (153 * mp + 2) / 5 + 1);
    const int m = static_cast<int>(mp < 10 ? mp + 3 : mp - 9);
    const long long y = yoe + era * 400 + (m <= 2 ? 1 : 0);
    std::snprintf(out, sizeof out, "%04lld-%02d-%02dT%02d:%02d:%02d.%03dZ", y, m, d,
                  static_cast<int>(ms_of_day / 3'600'000), static_cast<int>(ms_of_day / 60'000 % 60),
                  static_cast<int>(ms_of_day / 1000 % 60), static_cast<int>(ms_of_day % 1000));
}

constexpr unsigned long long ull(std::uint64_t v) { return static_cast<unsigned long long>(v); }

}  // namespace

Watchdog::Watchdog(Options opts) : opts_(std::move(opts)) {
    if (opts_.stall_ms == 0) throw std::invalid_argument("Watchdog: stall_ms must be > 0");
    if (opts_.check_interval_ms == 0) throw std::invalid_argument("Watchdog: check_interval_ms must be > 0");
    if (!opts_.now) opts_.now = steady_now_ms;
    if (!opts_.on_abort) opts_.on_abort = abort_process;
}

Watchdog::~Watchdog() { stop(); }

void Watchdog::arm() {
    const std::uint64_t now = opts_.now();
    io_beat_.store(now, std::memory_order_relaxed);
    last_check_ms_.store(now, std::memory_order_relaxed);
    baseline_ms_.store(now, std::memory_order_relaxed);
}

void Watchdog::start() {
    if (thread_.joinable()) throw std::logic_error("Watchdog: already started");
    arm();
    stop_.store(false, std::memory_order_relaxed);
    thread_ = std::thread([this] { run(); });
}

void Watchdog::stop() {
    stop_.store(true, std::memory_order_relaxed);
    if (thread_.joinable()) thread_.join();
}

void Watchdog::beat_io() { io_beat_.store(opts_.now(), std::memory_order_relaxed); }

void Watchdog::beat_sink() { sink_beat_.store(opts_.now(), std::memory_order_relaxed); }

void Watchdog::set_sink_registered(bool registered) {
    if (registered) sink_beat_.store(opts_.now(), std::memory_order_relaxed);
    sink_registered_.store(registered, std::memory_order_release);
}

void Watchdog::publish_state(const State& s) {
    state_[0].store(s.role, std::memory_order_relaxed);
    state_[1].store(s.driver_state, std::memory_order_relaxed);
    state_[2].store(s.c_s, std::memory_order_relaxed);
    state_[3].store(s.c_f, std::memory_order_relaxed);
    state_[4].store(s.g_s, std::memory_order_relaxed);
    state_[5].store(s.g_f, std::memory_order_relaxed);
    state_[6].store(s.sink_mode, std::memory_order_relaxed);
}

const char* Watchdog::stale_heartbeat() const {
    const std::uint64_t now = opts_.now();
    const std::uint64_t baseline = baseline_ms_.load(std::memory_order_relaxed);
    if (age_ms(now, io_beat_.load(std::memory_order_relaxed), baseline) > opts_.stall_ms) return "io_tick";
    if (sink_registered_.load(std::memory_order_acquire) &&
        age_ms(now, sink_beat_.load(std::memory_order_relaxed), baseline) > opts_.stall_ms)
        return "sink_loop";
    return nullptr;
}

void Watchdog::check_once() {
    if (fired()) return;
    const std::uint64_t now = opts_.now();
    const std::uint64_t last = last_check_ms_.exchange(now, std::memory_order_relaxed);
    const std::uint64_t gap = now > last ? now - last : 0;
    if (gap > opts_.check_interval_ms + opts_.pause_tolerance_ms) {
        // We were not scheduled for far longer than asked: docker pause / VM suspend. Beats could not have run either.
        baseline_ms_.store(now, std::memory_order_relaxed);
        return;
    }
    const std::uint64_t baseline = baseline_ms_.load(std::memory_order_relaxed);
    if (const char* which = stale_heartbeat()) {
        const std::uint64_t beat = std::string_view(which) == "io_tick" ? io_beat_.load(std::memory_order_relaxed)
                                                                         : sink_beat_.load(std::memory_order_relaxed);
        fire(which, age_ms(now, beat, baseline));
    }
}

void Watchdog::fire(const char* which, std::uint64_t age) {
    fired_.store(true, std::memory_order_release);
    char ts[96];
    format_iso_utc_ms(ts);
    char buf[1024];  // stack only: no allocation while wedged. Same keys as core/log (ts, level, message, component).
    const int n = std::snprintf(
        buf, sizeof buf,
        "{\"ts\":\"%s\",\"level\":\"critical\",\"message\":\"watchdog abort: heartbeat stale\",\"component\":\"watchdog\","
        "\"heartbeat\":\"%s\",\"stale_ms\":\"%llu\",\"limit_ms\":\"%llu\",\"role\":\"%llu\",\"driver_state\":\"%llu\","
        "\"c_s\":\"%llu\",\"c_f\":\"%llu\",\"g_s\":\"%llu\",\"g_f\":\"%llu\",\"sink_mode\":\"%llu\"}\n",
        ts, which, ull(age), ull(opts_.stall_ms), ull(state_[0].load(std::memory_order_relaxed)),
        ull(state_[1].load(std::memory_order_relaxed)), ull(state_[2].load(std::memory_order_relaxed)),
        ull(state_[3].load(std::memory_order_relaxed)), ull(state_[4].load(std::memory_order_relaxed)),
        ull(state_[5].load(std::memory_order_relaxed)), ull(state_[6].load(std::memory_order_relaxed)));
    const std::size_t len = n < 0 ? 0 : std::min(static_cast<std::size_t>(n), sizeof buf - 1);
    opts_.on_abort(std::string_view(buf, len));
}

void Watchdog::run() {
    const auto slice = std::chrono::milliseconds(std::min<std::uint64_t>(opts_.check_interval_ms, 50));
    while (!stop_.load(std::memory_order_relaxed) && !fired()) {
        std::this_thread::sleep_for(slice);
        if (opts_.now() - last_check_ms_.load(std::memory_order_relaxed) >= opts_.check_interval_ms) check_once();
    }
}

}  // namespace evml::ops
