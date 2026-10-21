#include "pipeline/pipeline_driver.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "pipeline/coverage.hpp"

namespace evml::pipeline {

namespace asio = boost::asio;
using std::chrono::milliseconds;

const char* to_string(PipelineState s) {
    switch (s) {
        case PipelineState::Init: return "init";
        case PipelineState::Recovering: return "recovering";
        case PipelineState::CatchingUp: return "catching_up";
        case PipelineState::Live: return "live";
        case PipelineState::Draining: return "draining";
        case PipelineState::Failed: return "failed";
    }
    return "unknown";
}

PipelineDriver::PipelineDriver(const core::Config& cfg, Sweeper& sweeper, Sink& sink, LogSource& source,
                               std::function<std::uint64_t()> now_ms, DriverOptions opt)
    : cfg_(cfg),
      sweeper_(sweeper),
      sink_(sink),
      source_(source),
      now_ms_(std::move(now_ms)),
      opt_(opt),
      backoff_(opt.seed, opt.backoff_base, opt.backoff_cap),
      progress_stall_(cfg.watchdog.progress_stall_ms) {
    if (cfg.watchdog.progress_stall_ms == 0) {
        throw std::invalid_argument("PipelineDriver: watchdog.progress_stall_ms must be > 0");
    }
    if (opt.final_every == 0) throw std::invalid_argument("PipelineDriver: final_every must be > 0");
}

void PipelineDriver::set_gates(std::optional<std::uint64_t> g_s, std::optional<std::uint64_t> g_f) {
    g_s_ = g_s;
    g_f_ = g_f;
}

void PipelineDriver::on_safe_reorg_signal(std::string reason) {
    reverify_pending_ = true;
    reverify_reason_ = std::move(reason);
}

void PipelineDriver::on_gate_frozen(std::string reason) {
    frozen_ = true;
    frozen_reason_ = std::move(reason);
}

// Invariant I2 (Dual Finality): clamp final ceiling to C_s so finalized blocks never outrun safe cursor.
std::optional<std::uint64_t> PipelineDriver::final_ceiling() const {
    if (!g_f_) return std::nullopt;
    return std::min(*g_f_, sweeper_.c_s()); // Clamp G_f to C_s (Invariant I2)
}

// Terminates pipeline with fatal error; requires operator manual intervention to restart.
TickResult PipelineDriver::fail(std::string why) {
    state_ = PipelineState::Failed;
    fatal_ = std::move(why);
    return TickResult{milliseconds{0}, true};
}

// Restores cursors and contract manifest from Redis sink checkpoint; verifies coverage.
// Watch out: Returns idle_poll if Redis is unreachable; never throws on temporary sink outage.
asio::awaitable<TickResult> PipelineDriver::recover() {
    state_ = PipelineState::Recovering;
    const auto loaded = co_await sink_.load_checkpoint();
    // Redis unreachable or the sink not yet readable: stay in Recovering and ask again (plan 4.3: never exit for this).
    if (!loaded) co_return TickResult{opt_.idle_poll, false};
    const Checkpoint& ck = *loaded;
    // Checkpoint must be either completely absent (first boot) or fully populated (safe, finalized, contracts)
    const int present = (ck.safe ? 1 : 0) + (ck.finalized ? 1 : 0) + (ck.contracts ? 1 : 0);
    const auto manifest = contract_manifest(cfg_);

    if (present == 0) {
        if (!g_s_) co_return TickResult{opt_.idle_poll, false};  // first run: wait for the safe gate
        const std::uint64_t c0 = initial_cursor(cfg_, *g_s_);
        // Invariant I1: set initial sweep cursors to the deployment block or safe gate
        sweeper_.set_cursors(c0, c0);
        sink_.init_checkpoint(c0, manifest);  // one atomic write: never a partial checkpoint (rev 6, E)
    } else if (present != 3) {
        // Partial checkpoint indicates split-brain or incomplete manual restore; halt immediately
        co_return fail(std::string("partial checkpoint: safe=") + (ck.safe ? "set" : "missing") +
                       " finalized=" + (ck.finalized ? "set" : "missing") +
                       " contracts=" + (ck.contracts ? "set" : "missing") +
                       "; operator action required (restore the keys or delete all of them to start fresh)");
    } else {
        std::vector<std::string> uncovered;
        try {
            // Verify all configured contracts have contiguous history in Redis up to C_s (Invariant I1)
            uncovered = uncovered_contracts(cfg_, *ck.contracts, *ck.safe);
        } catch (const std::runtime_error& e) {
            co_return fail(std::string("stored contracts manifest unreadable: ") + e.what());
        }
        if (!uncovered.empty()) {
            std::string names;
            for (const auto& n : uncovered) names += (names.empty() ? "" : ", ") + n;
            co_return fail("history not covered for contract(s): " + names +
                           "; run with --backfill-contract <name> for each");
        }
        // Invariant I2 check: finalized cursor cannot be ahead of safe cursor in persisted storage
        if (*ck.finalized > *ck.safe) {
            co_return fail("invalid checkpoint: finalized " + std::to_string(*ck.finalized) + " > safe " +
                           std::to_string(*ck.safe));
        }
        sweeper_.set_cursors(*ck.safe, *ck.finalized);
        if (*ck.contracts != manifest) sink_.set_contracts(manifest); // Sync new additions to manifest
        // Events of the previous run may have been lost in a discarded spill, including a revert that never
        // reached Redis. Re-check the released window now instead of waiting for the final sweep (rev 6, F).
        // Nothing is released above C_f when C_f == C_s, so there is nothing to verify then.
        if (*ck.safe > *ck.finalized) on_safe_reorg_signal("recovered from a checkpoint with a released window");
    }

    safe_progress_ = Progress{sweeper_.c_s(), std::nullopt, 0};
    final_progress_ = Progress{sweeper_.c_f(), std::nullopt, 0};
    state_ = PipelineState::CatchingUp; // Transition to CatchingUp once checkpoint is valid
    co_return TickResult{milliseconds{0}, false};
}

// Monitors sweep cursor advancement and fires watchdog alerts if stuck behind ceiling.
// Watch out: After progress_stall_ms without movement, reports provider to trigger failover.
void PipelineDriver::update_progress(Progress& p, const char* cls, std::uint64_t cursor,
                                     std::optional<std::uint64_t> ceiling, std::uint64_t now) {
    if (cursor != p.last_cursor) {
        p.last_cursor = cursor;
        p.behind_since.reset(); // Reset stall timer upon any forward movement
        p.missed = 0;
    }
    if (!ceiling || cursor >= *ceiling) {
        p.behind_since.reset(); // No work pending; cancel stall timer
        p.missed = 0;
        return;
    }
    if (!p.behind_since) {
        p.behind_since = now; // Arm stall detection timer
        return;
    }
    // Check if stalled for longer than progress_stall_ms watchdog deadline
    if (now - *p.behind_since >= static_cast<std::uint64_t>(progress_stall_.count())) {
        ++p.missed;
        p.behind_since = now;
        if (auto provider = source_.current_provider()) {
            // Signal provider degradation to trigger failover in provider pool
            source_.report_bad(*provider, std::string("no ") + cls + " sweep progress within the deadline");
        }
    }
}

// Maps sweep outcome to next tick timing; resets backoff on progress, increases backoff on retry.
TickResult PipelineDriver::after_step(StepStatus status, const std::string& detail) {
    switch (status) {
        case StepStatus::Advanced:
            backoff_.reset(); // Reset exponential backoff after successful forward progress
            stalled_ = false;
            stall_detail_.clear();
            return TickResult{milliseconds{0}, false}; // Immediate next tick
        case StepStatus::Retry:
            return TickResult{backoff_.next(), false}; // Exponential backoff with full jitter
        case StepStatus::Stalled:
            stalled_ = true;
            stall_detail_ = detail;
            return TickResult{backoff_.next(), false};
        case StepStatus::Paused:
            return TickResult{opt_.pause_poll, false}; // Wait fixed interval while sink is saturated
        case StepStatus::NoWork:
            return TickResult{opt_.idle_poll, false}; // Sleep idle interval waiting for new head
    }
    return TickResult{opt_.idle_poll, false};
}

// Evaluates whether pipeline has caught up to the live head within live_lag_blocks threshold.
void PipelineDriver::refresh_state() {
    if (state_ != PipelineState::CatchingUp && state_ != PipelineState::Live) return;
    const auto lag = [](std::optional<std::uint64_t> ceiling, std::uint64_t cursor) {
        return ceiling && *ceiling > cursor ? *ceiling - cursor : 0;
    };
    // Live requires both safe and final cursors to be within live_lag_blocks of their respective gates
    const bool live = g_s_ && g_f_ && lag(safe_ceiling(), sweeper_.c_s()) <= opt_.live_lag_blocks &&
                      lag(final_ceiling(), sweeper_.c_f()) <= opt_.live_lag_blocks;
    state_ = live ? PipelineState::Live : PipelineState::CatchingUp;
}

// Orchestrates a single tick of the event ingestion pipeline.
// Priority: Recovery -> Reverify Reorgs -> Safe Sweep -> Final Sweep (AIMD interleaved).
// Watch out: Returns done=true on shutdown (Draining) or fatal invariant failure (Failed).
asio::awaitable<TickResult> PipelineDriver::tick() {
    if (state_ == PipelineState::Failed) co_return TickResult{milliseconds{0}, true};
    if (drain_) {
        state_ = PipelineState::Draining; // Graceful stop requested
        co_return TickResult{milliseconds{0}, true};
    }
    if (state_ == PipelineState::Init || state_ == PipelineState::Recovering) co_return co_await recover();
    if (frozen_) co_return TickResult{opt_.idle_poll, false}; // Frozen on FinalizedAnomaly; do not advance

    const std::uint64_t now = now_ms_();
    update_progress(safe_progress_, "safe", sweeper_.c_s(), safe_ceiling(), now);
    update_progress(final_progress_, "final", sweeper_.c_f(), final_ceiling(), now);

    // Highest operational priority: re-verify safe window on reorg signal and emit reverts
    if (reverify_pending_) {
        const ReverifyResult r = co_await sweeper_.reverify_released();
        switch (r.status) {
            case StepStatus::Advanced:
                reverify_pending_ = false; // Successfully re-verified canonical fork
                if (r.reorg) {
                    ++safe_reorgs_;
                    last_fork_ = r.fork_point;
                }
                break;
            case StepStatus::NoWork: reverify_pending_ = false; break;
            default: break;  // Retry / Stalled / Paused: keep the flag
        }
        const TickResult t = after_step(r.status == StepStatus::NoWork ? StepStatus::Advanced : r.status, r.detail);
        refresh_state();
        co_return t;
    }

    const bool safe_work = g_s_ && *g_s_ > sweeper_.c_s();
    const auto fc = final_ceiling();
    const bool final_work = fc && *fc > sweeper_.c_f();

    if (!safe_work && !final_work) {
        refresh_state();
        co_return TickResult{opt_.idle_poll, false}; // Both sweeps caught up with gates
    }

    // Interleave final sweep every final_every ticks to avoid starving finalization during catch-up
    bool run_final = !safe_work;
    if (safe_work && final_work) run_final = (++work_ticks_ % opt_.final_every) == 0;

    TickResult t;
    if (run_final) {
        const StepResult r = co_await sweeper_.final_step(*g_f_);
        if (r.safe_reorg) on_safe_reorg_signal("final sweep: released safe logs vanished");
        t = after_step(r.status, r.detail);
    } else {
        const StepResult r = co_await sweeper_.safe_step(*g_s_);
        t = after_step(r.status, r.detail);
    }
    refresh_state();
    co_return t;
}

// Produces telemetry snapshot of cursors, gates, reorg count, and watchdog stalls.
DriverStatus PipelineDriver::status() const {
    DriverStatus s;
    s.state = state_;
    s.fatal = fatal_;
    s.c_s = sweeper_.c_s();
    s.c_f = sweeper_.c_f();
    s.g_s = g_s_;
    s.g_f = g_f_;
    s.frozen = frozen_;
    s.frozen_reason = frozen_reason_;
    s.safe_missed_deadlines = safe_progress_.missed;
    s.final_missed_deadlines = final_progress_.missed;
    s.stalled = stalled_;
    s.stall_detail = stall_detail_;
    s.reverify_pending = reverify_pending_;
    s.safe_reorgs = safe_reorgs_;
    s.last_fork_point = last_fork_;
    return s;
}

}  // namespace evml::pipeline
