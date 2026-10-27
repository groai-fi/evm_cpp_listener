#pragma once
// Process watchdog (plan 60 §5.1, slice 5-B1): one extra thread checks two heartbeats (io tick, sink loop). A heartbeat
// stale for more than `stall_ms` while the watchdog itself ran on time aborts the process, so the orchestrator restarts it.
// The check path takes no locks and allocates nothing, so it still works when the rest of the process is wedged.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <string_view>
#include <thread>

namespace evml::ops {

class Watchdog {
public:
    using NowMs = std::function<std::uint64_t()>;                    // monotonic milliseconds (steady_clock epoch)
    using AbortFn = std::function<void(std::string_view message)>;   // must not return in production

    /// Raw numeric codes published by the io thread each tick; printed in the abort line as "last known state".
    /// The host defines the meaning of role / driver_state / sink_mode codes (this class only stores and prints them).
    struct State {
        std::uint64_t role{};
        std::uint64_t driver_state{};
        std::uint64_t c_s{};
        std::uint64_t c_f{};
        std::uint64_t g_s{};
        std::uint64_t g_f{};
        std::uint64_t sink_mode{};
    };

    struct Options {
        std::uint64_t stall_ms = 0;                // [watchdog].process_stall_ms; required (0 throws): no hidden default
        std::uint64_t check_interval_ms = 1'000;   // watchdog wake period
        std::uint64_t pause_tolerance_ms = 5'000;  // wake overrun beyond this = host pause (D2): reset baselines, skip check
        NowMs now;                                 // default: steady_clock
        AbortFn on_abort;                          // default: write(1) the line, then std::abort() (exit 134)
    };

    /// @throws std::invalid_argument if stall_ms or check_interval_ms is 0.
    explicit Watchdog(Options opts);
    ~Watchdog();
    Watchdog(const Watchdog&) = delete;
    Watchdog& operator=(const Watchdog&) = delete;

    /// @brief Start the thread. Both beats count as fresh at this instant. @throws std::logic_error if already started.
    void start();
    /// @brief Stop and join the thread. Idempotent.
    void stop();

    /// @brief Called by the io thread on every tick.
    void beat_io();
    /// @brief Stores a sink beat from the calling thread (tests). Production: the sink writes through sink_beat_slot().
    void beat_sink();
    /// @brief Storage the sink loop stores its steady-clock ms into (RedisSinkOptions::loop_beat). Owned by the watchdog,
    /// so it outlives every per-leadership sink.
    std::atomic<std::uint64_t>* sink_beat_slot() { return &sink_beat_; }
    /// @brief Sink beat is checked only while registered (leader). Registering resets its baseline to now; unregister
    /// BEFORE destroying the sink.
    void set_sink_registered(bool registered);

    /// @brief Called by the io thread each tick; atomics only.
    void publish_state(const State& s);

    /// @brief Name of the heartbeat that is stale right now ("io_tick" / "sink_loop"), or nullptr when healthy.
    /// Same rule as the abort (stall_ms, pause baselines) and safe from any thread: backs `/healthz`.
    const char* stale_heartbeat() const;

    /// @brief Mark both beats fresh and reset baselines to now. start() calls it; tests call it before check_once().
    void arm();
    /// @brief One check at the current time. Exposed so tests can drive it with an injected clock.
    void check_once();

    /// @brief True once the abort callback has been invoked (only observable when on_abort returns, i.e. in tests).
    bool fired() const { return fired_.load(std::memory_order_acquire); }

private:
    void run();
    void fire(const char* which, std::uint64_t age_ms);

    Options opts_;
    std::atomic<std::uint64_t> io_beat_{0};
    std::atomic<std::uint64_t> sink_beat_{0};
    std::atomic<bool> sink_registered_{false};
    std::atomic<bool> stop_{false};
    std::atomic<bool> fired_{false};
    std::atomic<std::uint64_t> last_check_ms_{0};
    std::atomic<std::uint64_t> baseline_ms_{0};
    std::atomic<std::uint64_t> state_[7]{};
    std::thread thread_;
};

}  // namespace evml::ops
