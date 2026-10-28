#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/asio/awaitable.hpp>

#include "core/config.hpp"
#include "pipeline/aimd_pager.hpp"
#include "pipeline/event.hpp"
#include "pipeline/last_type_map.hpp"
#include "pipeline/log_source.hpp"
#include "pipeline/sink.hpp"

namespace evml::pipeline {

enum class StepStatus {
    NoWork,   // cursor already at its ceiling
    Advanced, // one page swept, emitted and checkpointed
    Retry,    // transient: nothing emitted, cursor unchanged (a provider failure, an unreadable block)
    Stalled,  // cannot proceed without operator action (I5): CRITICAL, cursor unchanged. Also: bad responses persisted
              // through max_bad_attempts (no agreement between getLogs and the canonical-hash source)
    Paused,   // sink saturated (plan 4.6): nothing done
};
const char* to_string(StepStatus s);

struct StepResult {
    StepStatus status{StepStatus::NoWork};
    std::optional<BlockRange> range;  // the page that was swept (Advanced)
    std::string detail;               // reason for Retry/Stalled
    bool safe_reorg{false};           // final sweep found released safe-class logs that vanished: raise SafeReorg
};

// Outcome of reverify_released(). status: NoWork (nothing released above C_f), Advanced (the whole window was
// verified), Retry / Stalled (a page could not be fetched or validated; progress so far is kept) or Paused.
struct ReverifyResult {
    StepStatus status{StepStatus::NoWork};
    std::string detail;                       // reason for Retry/Stalled
    bool reorg{false};                        // released logs vanished or new ones appeared: raise SafeReorg
    std::optional<std::uint64_t> fork_point;  // reorg only: highest block both views agree on; C_s was rewound to it
};

struct SweepStats {
    std::uint64_t emitted_safe{0};     // safe sweep log emissions
    std::uint64_t emitted_final{0};    // final sweep log emissions (finalized-class + late)
    std::uint64_t deduped{0};          // skipped by the in-process layer
    std::uint64_t reverts{0};
    std::uint64_t late{0};             // safe-class logs the safe sweep missed (safe_missed_total)
    std::uint64_t matched{0};
    std::uint64_t inconsistent{0};     // discarded responses (provider_inconsistency_total)
    std::uint64_t range_shrinks{0};    // RangeTooLarge
    std::uint64_t failure_shrinks{0};  // rate limit / timeout / 5xx on a wide span (rev 5, G1)
    std::uint64_t volume_shrinks{0};   // page returned more than max_page_logs (rev 5, G6)
    std::uint64_t reverified_new{0};   // logs emitted by reverify_released that the safe sweep had not released
    std::uint64_t address_splits{0};
};

struct SweeperOptions {
    std::uint32_t min_range{10};         // [backfill] min_range; clamped per provider by AimdPager
    std::size_t recent_capacity{8192};   // layer-1 dedup window
    // A response that fails validation or the canonical-hash check is discarded, the provider is reported bad and
    // the page is retried; no agreement after this many attempts => Stalled (rev 5, G2). Must be >= 1.
    std::uint32_t max_bad_attempts{3};
    // Soft memory cap: a page with more logs than this still goes out, but the next page is smaller (rev 5, G6).
    // Phase 5 wires it to sink queue capacity / 2.
    std::size_t max_page_logs{5000};
    // Observer, default empty (no behaviour change). Called once per Advanced page, after its emissions and
    // checkpoint. `logs` are the page's in-scope logs: safe-class for Finality::Safe, finalized-class for
    // Finality::Finalized. Not called for Retry/Paused/NoWork/Stalled or by reverify_released. Must not throw.
    std::function<void(core::Finality kind, const BlockRange& range, const std::vector<rpc::Log>& logs)> on_page;
};

// The two sweeps (plan 3.5) over one LogSource and one Sink. Single-threaded: call from one executor, and do not
// run two steps concurrently (the sweeps are serialised). getLogs is the source of truth for release; a step
// either completes (emit..., checkpoint, cursor moves) or leaves the cursor untouched (I5).
// Enforces Invariant I1 (contiguous block coverage) and Invariant I2 (C_f <= C_s).
class Sweeper {
public:
    Sweeper(const core::Config& cfg, EventContext ctx, LogSource& source, Sink& sink,
            std::function<std::uint64_t()> now_ms, SweeperOptions opt = {});

    // Sets starting cursors from persistent checkpoint; inclusive upper bound of already swept blocks.
    // Watch out: Throws std::invalid_argument if C_f > C_s to strictly uphold Invariant I2.
    void set_cursors(std::uint64_t c_s, std::uint64_t c_f);
    std::uint64_t c_s() const { return c_s_; }
    std::uint64_t c_f() const { return c_f_; }

    // Safe-reorg rewind: pulls C_s back to fork_point; clamps C_f to keep C_f <= C_s (Invariant I2).
    // Watch out: Only safe cursor rewinds in memory; persisted checkpoint advances monotonically (Invariant I3).
    void rewind_safe(std::uint64_t fork_point);

    // Advances safe sweep by one AIMD page in (C_s, g_s]; emits safe-class events and adds to released ZSET.
    // Watch out: Returns StepStatus::Paused if sink is saturated or degraded.
    boost::asio::awaitable<StepResult> safe_step(std::uint64_t g_s);

    // Advances finalized sweep by one page in (C_f, min(g_f, C_s)]; diffs against released ZSET.
    // Emits finalized-class events, retracts vanished safe logs via reverts, and trims released ZSET.
    // Watch out: Requires sink.readable() to avoid diffing against incomplete released set.
    boost::asio::awaitable<StepResult> final_step(std::uint64_t g_f);

    // Immediate re-check of (C_f, C_s] on safe reorg signal; reconciles canonical fork against released set.
    // Vanished safe logs emit reverts with full original fields; new logs emit as safe-class.
    // Watch out: Idempotent across transient provider retries; preserves lowest pending fork point.
    boost::asio::awaitable<ReverifyResult> reverify_released();

    const SweepStats& stats() const { return stats_; }

private:
    struct Fetched {
        StepStatus status{StepStatus::Advanced};
        std::string detail;
        BlockRange range{};
        std::vector<rpc::Log> logs;  // sorted by (block, logIndex), validated, canonical
        std::string provider;
        std::map<std::uint64_t, BlockInfo> blocks;  // every block that has logs
    };

    boost::asio::awaitable<Fetched> fetch_page(std::uint64_t cursor, std::uint64_t ceiling, const core::LogFilter& filter,
                                               std::optional<core::Finality> only_class);
    AimdPager& pager_for(const std::string& provider);
    Event build_event(const rpc::Log& log, const Fetched& f, EventType type) const;

    const core::Config& cfg_;
    EventContext ctx_;
    LogSource& source_;
    Sink& sink_;
    std::function<std::uint64_t()> now_ms_;
    SweeperOptions opt_;
    LastTypeMap recent_;
    std::unordered_map<std::string, AimdPager> pagers_;
    std::uint64_t c_s_{0}, c_f_{0};
    std::optional<std::uint64_t> pending_fork_;  // lowest fork point found by an unfinished reverify_released
    SweepStats stats_;
};

}  // namespace evml::pipeline
