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
    Retry,    // transient: nothing emitted, cursor unchanged (a provider failure, bad response, unreadable block)
    Stalled,  // cannot proceed without operator action (I5): CRITICAL, cursor unchanged
    Paused,   // sink saturated (plan 4.6): nothing done
};
const char* to_string(StepStatus s);

struct StepResult {
    StepStatus status{StepStatus::NoWork};
    std::optional<BlockRange> range;  // the page that was swept (Advanced)
    std::string detail;               // reason for Retry/Stalled
    bool safe_reorg{false};           // final sweep found released safe-class logs that vanished: raise SafeReorg
};

struct SweepStats {
    std::uint64_t emitted_safe{0};     // safe sweep log emissions
    std::uint64_t emitted_final{0};    // final sweep log emissions (finalized-class + late)
    std::uint64_t deduped{0};          // skipped by the in-process layer
    std::uint64_t reverts{0};
    std::uint64_t late{0};             // safe-class logs the safe sweep missed (safe_missed_total)
    std::uint64_t matched{0};
    std::uint64_t inconsistent{0};     // discarded responses (provider_inconsistency_total)
    std::uint64_t range_shrinks{0};
    std::uint64_t address_splits{0};
};

struct SweeperOptions {
    std::uint32_t min_range{10};         // [backfill] min_range; clamped per provider by AimdPager
    std::size_t recent_capacity{8192};   // layer-1 dedup window
};

// The two sweeps (plan 3.5) over one LogSource and one Sink. Single-threaded: call from one executor, and do not
// run two steps concurrently (the sweeps are serialised). getLogs is the source of truth for release; a step
// either completes (emit..., checkpoint, cursor moves) or leaves the cursor untouched (I5).
class Sweeper {
public:
    Sweeper(const core::Config& cfg, EventContext ctx, LogSource& source, Sink& sink,
            std::function<std::uint64_t()> now_ms, SweeperOptions opt = {});

    // Cursor = last block already swept (inclusive). Set from the loaded checkpoint (or start_block - 1) before the
    // first step. Throws std::invalid_argument if c_f > c_s (I2).
    void set_cursors(std::uint64_t c_s, std::uint64_t c_f);
    std::uint64_t c_s() const { return c_s_; }
    std::uint64_t c_f() const { return c_f_; }

    // Safe-reorg response (plan 3.6 step 4): C_s = min(C_s, fork); C_f follows to keep C_f <= C_s.
    void rewind_safe(std::uint64_t fork_point);

    // One page of safe-class logs in (C_s, g_s]: emitted with finality=safe and tracked in `released`.
    boost::asio::awaitable<StepResult> safe_step(std::uint64_t g_s);

    // One page of ALL logs in (C_f, min(g_f, C_s)]: finalized-class emitted, safe-class diffed against `released`.
    boost::asio::awaitable<StepResult> final_step(std::uint64_t g_f);

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
    SweepStats stats_;
};

}  // namespace evml::pipeline
