#include "pipeline/pipeline_driver.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "pipeline/coverage.hpp"

namespace evml::pipeline {

namespace asio = boost::asio;
using std::chrono::milliseconds;

struct PipelineDriver::State {
    struct Progress {
        std::uint64_t last_cursor{0};
        std::optional<std::uint64_t> behind_since;
        std::uint32_t missed{0};
    };

    const core::Config& cfg;
    Sweeper& sweeper;
    Sink& sink;
    LogSource& source;
    std::function<std::uint64_t()> now_ms;
    DriverOptions opt;
    rpc::Backoff backoff;
    std::chrono::milliseconds progress_stall;

    PipelineState state{PipelineState::Init};
    std::string fatal;
    std::optional<std::uint64_t> g_s, g_f;
    bool drain{false};
    bool frozen{false};
    std::string frozen_reason;
    bool reverify_pending{false};
    std::string reverify_reason;
    bool stalled{false};
    std::string stall_detail;
    std::uint64_t safe_reorgs{0};
    std::uint64_t reverify_clean{0};
    std::optional<std::uint64_t> last_fork;
    std::uint64_t work_ticks{0};
    Progress safe_progress, final_progress;

    State(const core::Config& c, Sweeper& sw, Sink& snk, LogSource& src, std::function<std::uint64_t()> now,
          DriverOptions o)
        : cfg(c),
          sweeper(sw),
          sink(snk),
          source(src),
          now_ms(std::move(now)),
          opt(o),
          backoff(opt.seed, opt.backoff_base, opt.backoff_cap),
          progress_stall(c.watchdog.progress_stall_ms) {}

    [[nodiscard]] std::optional<std::uint64_t> safe_ceiling() const noexcept { return g_s; }

    // Invariant I2 (Dual Finality): clamp final ceiling to C_s so finalized blocks never outrun safe cursor.
    [[nodiscard]] std::optional<std::uint64_t> final_ceiling() const noexcept {
        if (!g_f) return std::nullopt;
        return std::min(*g_f, sweeper.c_s()); // Clamp G_f to C_s (Invariant I2)
    }

    // Terminates pipeline with fatal error; requires operator manual intervention to restart.
    [[nodiscard]] TickResult fail(std::string why) {
        state = PipelineState::Failed;
        fatal = std::move(why);
        return TickResult{milliseconds{0}, true};
    }

    void on_safe_reorg_signal(std::string reason) {
        reverify_pending = true;
        reverify_reason = std::move(reason);
    }

    // Monitors sweep cursor advancement and fires watchdog alerts if stuck behind ceiling.
    // Watch out: After progress_stall_ms without movement, reports provider to trigger failover.
    void update_progress(Progress& p, const char* cls, std::uint64_t cursor, std::optional<std::uint64_t> ceiling,
                         std::uint64_t now) {
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
        if (now - *p.behind_since >= static_cast<std::uint64_t>(progress_stall.count())) {
            ++p.missed;
            p.behind_since = now;
            if (auto provider = source.current_provider()) {
                // Signal provider degradation to trigger failover in provider pool
                source.report_bad(*provider, std::string("no ") + cls + " sweep progress within the deadline");
            }
        }
    }

    // Maps sweep outcome to next tick timing; resets backoff on progress, increases backoff on retry.
    [[nodiscard]] TickResult after_step(StepStatus status, const std::string& detail) {
        switch (status) {
            case StepStatus::Advanced:
                backoff.reset(); // Reset exponential backoff after successful forward progress
                stalled = false;
                stall_detail.clear();
                return TickResult{milliseconds{0}, false}; // Immediate next tick
            case StepStatus::Retry:
                return TickResult{backoff.next(), false}; // Exponential backoff with full jitter
            case StepStatus::Stalled:
                stalled = true;
                stall_detail = detail;
                return TickResult{backoff.next(), false};
            case StepStatus::Paused:
                return TickResult{opt.pause_poll, false}; // Wait fixed interval while sink is saturated
            case StepStatus::NoWork:
                return TickResult{opt.idle_poll, false}; // Sleep idle interval waiting for new head
        }
        return TickResult{opt.idle_poll, false};
    }

    // Evaluates whether pipeline has caught up to the live head within live_lag_blocks threshold.
    void refresh_state() noexcept {
        if (state != PipelineState::CatchingUp && state != PipelineState::Live) return;
        const auto lag = [](std::optional<std::uint64_t> ceiling, std::uint64_t cursor) {
            return ceiling && *ceiling > cursor ? *ceiling - cursor : 0;
        };
        // Live requires both safe and final cursors to be within live_lag_blocks of their respective gates
        const bool live = g_s && g_f && lag(safe_ceiling(), sweeper.c_s()) <= opt.live_lag_blocks &&
                          lag(final_ceiling(), sweeper.c_f()) <= opt.live_lag_blocks;
        state = live ? PipelineState::Live : PipelineState::CatchingUp;
    }
};

PipelineDriver::PipelineDriver(const core::Config& cfg, Sweeper& sweeper, Sink& sink, LogSource& source,
                               std::function<std::uint64_t()> now_ms, DriverOptions opt)
    : state_(std::make_shared<State>(cfg, sweeper, sink, source, std::move(now_ms), opt)) {
    if (cfg.watchdog.progress_stall_ms == 0) {
        throw std::invalid_argument("PipelineDriver: watchdog.progress_stall_ms must be > 0");
    }
    if (opt.final_every == 0) throw std::invalid_argument("PipelineDriver: final_every must be > 0");
}

void PipelineDriver::set_gates(std::optional<std::uint64_t> g_s, std::optional<std::uint64_t> g_f) noexcept {
    state_->g_s = g_s;
    state_->g_f = g_f;
}

void PipelineDriver::on_safe_reorg_signal(std::string reason) { state_->on_safe_reorg_signal(std::move(reason)); }

void PipelineDriver::on_gate_frozen(std::string reason) {
    state_->frozen = true;
    state_->frozen_reason = std::move(reason);
}

void PipelineDriver::request_drain() noexcept { state_->drain = true; }

// Restores cursors and contract manifest from Redis sink checkpoint; verifies coverage.
// Watch out: Returns idle_poll if Redis is unreachable; never throws on temporary sink outage.
asio::awaitable<TickResult> PipelineDriver::recover(std::shared_ptr<State> st) {
    st->state = PipelineState::Recovering;
    const auto loaded = co_await st->sink.load_checkpoint();
    // Redis unreachable or the sink not yet readable: stay in Recovering and ask again (plan 4.3: never exit for this).
    if (!loaded) co_return TickResult{st->opt.idle_poll, false};
    const Checkpoint& ck = *loaded;
    // Checkpoint must be either completely absent (first boot) or fully populated (safe, finalized, contracts)
    const int present = (ck.safe ? 1 : 0) + (ck.finalized ? 1 : 0) + (ck.contracts ? 1 : 0);
    const auto manifest = contract_manifest(st->cfg);

    if (present == 0) {
        if (!st->g_s) co_return TickResult{st->opt.idle_poll, false};  // first run: wait for the safe gate
        const std::uint64_t c0 = initial_cursor(st->cfg, *st->g_s);
        // Invariant I1: set initial sweep cursors to the deployment block or safe gate
        st->sweeper.set_cursors(c0, c0);
        st->sink.init_checkpoint(c0, manifest);  // one atomic write: never a partial checkpoint (rev 6, E)
    } else if (present != 3) {
        // Partial checkpoint indicates split-brain or incomplete manual restore; halt immediately
        co_return st->fail(std::string("partial checkpoint: safe=") + (ck.safe ? "set" : "missing") +
                           " finalized=" + (ck.finalized ? "set" : "missing") +
                           " contracts=" + (ck.contracts ? "set" : "missing") +
                           "; operator action required (restore the keys or delete all of them to start fresh)");
    } else {
        std::vector<std::string> uncovered;
        try {
            // Verify all configured contracts have contiguous history in Redis up to C_s (Invariant I1)
            uncovered = uncovered_contracts(st->cfg, *ck.contracts, *ck.safe);
        } catch (const std::runtime_error& e) {
            co_return st->fail(std::string("stored contracts manifest unreadable: ") + e.what());
        }
        if (!uncovered.empty()) {
            std::string names;
            for (const auto& n : uncovered) names += (names.empty() ? "" : ", ") + n;
            co_return st->fail("history not covered for contract(s): " + names +
                               "; run with --backfill-contract <name> for each");
        }
        // Invariant I2 check: finalized cursor cannot be ahead of safe cursor in persisted storage
        if (*ck.finalized > *ck.safe) {
            co_return st->fail("invalid checkpoint: finalized " + std::to_string(*ck.finalized) + " > safe " +
                               std::to_string(*ck.safe));
        }
        st->sweeper.set_cursors(*ck.safe, *ck.finalized);
        if (*ck.contracts != manifest) st->sink.set_contracts(manifest); // Sync new additions to manifest
        // Events of the previous run may have been lost in a discarded spill, including a revert that never
        // reached Redis. Re-check the released window now instead of waiting for the final sweep (rev 6, F).
        // Nothing is released above C_f when C_f == C_s, so there is nothing to verify then.
        if (*ck.safe > *ck.finalized) st->on_safe_reorg_signal("recovered from a checkpoint with a released window");
    }

    st->safe_progress = State::Progress{st->sweeper.c_s(), std::nullopt, 0};
    st->final_progress = State::Progress{st->sweeper.c_f(), std::nullopt, 0};
    st->state = PipelineState::CatchingUp; // Transition to CatchingUp once checkpoint is valid
    co_return TickResult{milliseconds{0}, false};
}

// Orchestrates a single tick of the event ingestion pipeline.
// Priority: Recovery -> Reverify Reorgs -> Safe Sweep -> Final Sweep (AIMD interleaved).
// Watch out: Returns done=true on shutdown (Draining) or fatal invariant failure (Failed).
asio::awaitable<TickResult> PipelineDriver::tick() {
    auto st = state_;
    if (st->state == PipelineState::Failed) co_return TickResult{milliseconds{0}, true};
    if (st->drain) {
        st->state = PipelineState::Draining; // Graceful stop requested
        co_return TickResult{milliseconds{0}, true};
    }
    if (st->state == PipelineState::Init || st->state == PipelineState::Recovering) co_return co_await recover(st);
    if (st->frozen) co_return TickResult{st->opt.idle_poll, false}; // Frozen on FinalizedAnomaly; do not advance

    const std::uint64_t now = st->now_ms();
    st->update_progress(st->safe_progress, "safe", st->sweeper.c_s(), st->safe_ceiling(), now);
    st->update_progress(st->final_progress, "final", st->sweeper.c_f(), st->final_ceiling(), now);

    // Highest operational priority: re-verify safe window on reorg signal and emit reverts
    if (st->reverify_pending) {
        const ReverifyResult r = co_await st->sweeper.reverify_released();
        switch (r.status) {
            case StepStatus::Advanced:
                st->reverify_pending = false; // Successfully re-verified canonical fork
                if (r.reorg) {
                    ++st->safe_reorgs;
                    st->last_fork = r.fork_point;
                } else {
                    ++st->reverify_clean;
                }
                break;
            case StepStatus::NoWork:
                st->reverify_pending = false;
                ++st->reverify_clean;
                break;
            default: break;  // Retry / Stalled / Paused: keep the flag
        }
        const TickResult t = st->after_step(r.status == StepStatus::NoWork ? StepStatus::Advanced : r.status, r.detail);
        st->refresh_state();
        co_return t;
    }

    const bool safe_work = st->g_s && *st->g_s > st->sweeper.c_s();
    const auto fc = st->final_ceiling();
    const bool final_work = fc && *fc > st->sweeper.c_f();

    if (!safe_work && !final_work) {
        st->refresh_state();
        co_return TickResult{st->opt.idle_poll, false}; // Both sweeps caught up with gates
    }

    // Interleave final sweep every final_every ticks to avoid starving finalization during catch-up
    bool run_final = !safe_work;
    if (safe_work && final_work) run_final = (++st->work_ticks % st->opt.final_every) == 0;

    TickResult t;
    if (run_final) {
        const StepResult r = co_await st->sweeper.final_step(*st->g_f);
        if (r.safe_reorg) st->on_safe_reorg_signal("final sweep: released safe logs vanished");
        t = st->after_step(r.status, r.detail);
    } else {
        const StepResult r = co_await st->sweeper.safe_step(*st->g_s);
        t = st->after_step(r.status, r.detail);
    }
    st->refresh_state();
    co_return t;
}

// Produces telemetry snapshot of cursors, gates, reorg count, and watchdog stalls.
DriverStatus PipelineDriver::status() const {
    DriverStatus s;
    s.state = state_->state;
    s.fatal = state_->fatal;
    s.c_s = state_->sweeper.c_s();
    s.c_f = state_->sweeper.c_f();
    s.g_s = state_->g_s;
    s.g_f = state_->g_f;
    s.frozen = state_->frozen;
    s.frozen_reason = state_->frozen_reason;
    s.safe_missed_deadlines = state_->safe_progress.missed;
    s.final_missed_deadlines = state_->final_progress.missed;
    s.stalled = state_->stalled;
    s.stall_detail = state_->stall_detail;
    s.reverify_pending = state_->reverify_pending;
    s.safe_reorgs = state_->safe_reorgs;
    s.reverify_clean = state_->reverify_clean;
    s.last_fork_point = state_->last_fork;
    return s;
}

}  // namespace evml::pipeline
