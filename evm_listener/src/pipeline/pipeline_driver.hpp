#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include <boost/asio/awaitable.hpp>

#include "core/config.hpp"
#include "pipeline/log_source.hpp"
#include "pipeline/sink.hpp"
#include "pipeline/sweeper.hpp"
#include "rpc/backoff.hpp"

namespace evml::pipeline {

// Process-level pipeline state (plan 3.2). Failed is terminal: the host exits with code 2 and prints status().fatal.
// Draining is terminal for the driver: the host flushes the sink (Phase 4), closes sessions and exits 0.
enum class PipelineState { Init, Recovering, CatchingUp, Live, Draining, Failed };
const char* to_string(PipelineState s);

struct DriverOptions {
    // Every Nth working tick runs the final sweep even though the safe sweep still has work, so a long catch-up
    // cannot starve finalization. The safe sweep otherwise has priority (bulkhead, rev 5 G4).
    std::uint32_t final_every{4};
    // Live iff both sweeps are within this many blocks of their gate (plan 3.2: "<= 1 page"; Infura's page is 500).
    std::uint64_t live_lag_blocks{500};
    std::chrono::milliseconds pause_poll{200};   // wait while the sink is saturated
    std::chrono::milliseconds idle_poll{1000};   // wait when there is no work or the pipeline is frozen
    std::chrono::milliseconds backoff_base{250};
    std::chrono::milliseconds backoff_cap{30000};
    std::uint64_t seed{1};                       // jitter seed (production: random)
};

struct TickResult {
    std::chrono::milliseconds wait{0};  // how long the host should sleep before the next tick (0: more work now)
    bool done{false};                   // Draining or Failed: stop ticking
};

// What the alert engine (Phase 5) evaluates. PipelineStalled <=> missed_deadlines >= 3 || stalled.
struct DriverStatus {
    PipelineState state{PipelineState::Init};
    std::string fatal;  // Failed: why (exit 2)
    std::uint64_t c_s{0}, c_f{0};
    std::optional<std::uint64_t> g_s, g_f;
    bool frozen{false};
    std::string frozen_reason;
    std::uint32_t safe_missed_deadlines{0};   // consecutive progress deadlines missed by the safe sweep
    std::uint32_t final_missed_deadlines{0};
    bool stalled{false};                      // a sweep reported Stalled and has not progressed since
    std::string stall_detail;
    bool reverify_pending{false};
    std::uint64_t safe_reorgs{0};             // completed reverifications that found a difference (SafeReorg)
    std::uint64_t reverify_clean{0};          // completed reverifications that found no difference
    std::optional<std::uint64_t> last_fork_point;
};

// Schedules the sweeps (plan 3.2, 3.5, rev 5 G3/G4). Single-threaded: call tick() from one executor, one at a time.
// The host owns timers and I/O: it feeds gate values (from the FinalityGate), forwards reorg signals, sleeps for
// TickResult::wait and calls tick() again. Everything the driver decides is therefore deterministic and testable
// with a fake clock.
//
// One tick does one unit of work, in this priority: recover (first ticks) -> reverify (if signalled) -> safe sweep
// -> final sweep (also every `final_every`-th working tick). Retry/Stalled results back off with full jitter and are
// retried forever (I5: never skip); a cursor that does not move for `watchdog.progress_stall_ms` while its gate is
// ahead gets its provider reported bad (forcing a switch), and three such deadlines in a row make status() report
// a pipeline stall.
class PipelineDriver {
public:
    // Throws std::invalid_argument for watchdog.progress_stall_ms == 0 or final_every == 0.
    PipelineDriver(const core::Config& cfg, Sweeper& sweeper, Sink& sink, LogSource& source,
                   std::function<std::uint64_t()> now_ms, DriverOptions opt = {});

    // Updates latest gates reported by active RPC provider (nullopt until observed).
    // Watch out: g_s_ (safe gate) advances independently of g_f_ (finalized gate).
    void set_gates(std::optional<std::uint64_t> g_s, std::optional<std::uint64_t> g_f);

    // Signals a chain reorg affecting safe-tier blocks; schedules reverification on the next tick.
    // Watch out: safe-class logs emitted in (C_f, C_s] that vanished in the fork will be retracted as reverts.
    void on_safe_reorg_signal(std::string reason);

    // Freezes pipeline permanently when finalized blocks regress or mismatch hashes (FinalizedAnomaly).
    // Watch out: Permanently stops event release until operator intervention to prevent corrupted state.
    void on_gate_frozen(std::string reason);

    // Requests graceful shutdown (SIGTERM/SIGINT); finishes inflight work and exits next tick with done=true.
    // Watch out: Does not immediately kill sessions; host must flush sink before terminating.
    void request_drain() { drain_ = true; }

    // Core state-machine step: executes one unit of work (recover, reverify, safe sweep, or final sweep).
    // Returns TickResult indicating required backoff wait duration and whether pipeline is terminated.
    // Watch out: Must be invoked sequentially from a single Asio strand/executor (single-threaded driver).
    boost::asio::awaitable<TickResult> tick();

    // Returns point-in-time snapshot of pipeline health, cursors, stalls, and alert metrics.
    // Watch out: Thread-safe read only if tick() is quiescent or invoked within the same strand.
    DriverStatus status() const;

private:
    struct Progress {
        std::uint64_t last_cursor{0};
        std::optional<std::uint64_t> behind_since;
        std::uint32_t missed{0};
    };

    // Restores checkpoint state from Redis on startup; validates contract coverage against manifest.
    // Watch out: Fails immediately if checkpoint is partially populated or contract history is uncovered.
    boost::asio::awaitable<TickResult> recover();

    // Evaluates forward progress against watchdog timeout; reports bad provider if stalled behind ceiling.
    // Watch out: Multiple missed deadlines trigger pipeline stall alerts in status().
    void update_progress(Progress& p, const char* cls, std::uint64_t cursor, std::optional<std::uint64_t> ceiling,
                         std::uint64_t now);

    // Maps Sweeper StepStatus to appropriate tick backoff sleep duration and stall tracking.
    TickResult after_step(StepStatus status, const std::string& detail);

    // Recalculates CatchingUp vs Live state based on cursor distance to provider gates.
    void refresh_state();

    // Returns safe ceiling: upper bound for safe sweep (provider safe gate).
    std::optional<std::uint64_t> safe_ceiling() const { return g_s_; }

    // Returns final ceiling: clamped to min(g_f, c_s) to uphold Invariant I2 (C_f <= C_s).
    // Watch out: Finalized sweep can NEVER advance beyond the current safe cursor C_s.
    std::optional<std::uint64_t> final_ceiling() const;

    // Transitions pipeline state to Failed with a fatal diagnostic reason string.
    TickResult fail(std::string why);

    const core::Config& cfg_;
    Sweeper& sweeper_;
    Sink& sink_;
    LogSource& source_;
    std::function<std::uint64_t()> now_ms_;
    DriverOptions opt_;
    rpc::Backoff backoff_;
    std::chrono::milliseconds progress_stall_;

    PipelineState state_{PipelineState::Init};
    std::string fatal_;
    std::optional<std::uint64_t> g_s_, g_f_;
    bool drain_{false};
    bool frozen_{false};
    std::string frozen_reason_;
    bool reverify_pending_{false};
    std::string reverify_reason_;
    bool stalled_{false};
    std::string stall_detail_;
    std::uint64_t safe_reorgs_{0};
    std::uint64_t reverify_clean_{0};
    std::optional<std::uint64_t> last_fork_;
    std::uint64_t work_ticks_{0};
    Progress safe_progress_, final_progress_;
};

}  // namespace evml::pipeline
