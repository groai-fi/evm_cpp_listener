#include "pipeline/sweeper.hpp"

#include <algorithm>
#include <set>
#include <stdexcept>

#include "pipeline/final_diff.hpp"
#include "pipeline/log_validation.hpp"

namespace evml::pipeline {

namespace {
struct StringHash {
    using is_transparent = void;
    [[nodiscard]] std::size_t operator()(std::string_view sv) const noexcept {
        return std::hash<std::string_view>{}(sv);
    }
};

bool is_safe_class(const core::Config& cfg, const rpc::Log& log) {
    const auto* route = cfg.route(log.address, log.topics.at(0));  // validated before this is called
    return route != nullptr && route->finality == core::Finality::Safe;
}
}  // namespace

struct Sweeper::State {
    const core::Config cfg;
    EventContext ctx;
    LogSource& source;
    Sink& sink;
    std::function<std::uint64_t()> now_ms;
    SweeperOptions opt;
    LastTypeMap recent;
    std::unordered_map<std::string, AimdPager, StringHash, std::equal_to<>> pagers;
    std::uint64_t c_s{0}, c_f{0};
    std::optional<std::uint64_t> pending_fork;  // lowest fork point found by an unfinished reverify_released
    SweepStats stats;

    State(const core::Config& c, EventContext x, LogSource& src, Sink& snk, std::function<std::uint64_t()> now,
          SweeperOptions o)
        : cfg(c),
          ctx(std::move(x)),
          source(src),
          sink(snk),
          now_ms(std::move(now)),
          opt(std::move(o)),
          recent(opt.recent_capacity) {}

    AimdPager& pager_for(std::string_view provider) {
        const auto it = pagers.find(provider);
        if (it != pagers.end()) return it->second;
        for (const auto& p : cfg.getlogs_chain) {
            if (p.name == provider)
                return pagers.emplace(std::string(provider), AimdPager(p.max_getlogs_range, opt.min_range))
                    .first->second;
        }
        throw std::logic_error("Sweeper: provider '" + std::string(provider) + "' is not in the getLogs chain");
    }

    [[nodiscard]] Event build_event(const rpc::Log& log, const Fetched& f, EventType type) const {
        const auto now = now_ms();
        return make_event(log, cfg, ctx, type, f.blocks.at(log.block_number).timestamp, f.provider, now, now);
    }

    void rewind_safe(std::uint64_t fork_point) noexcept {
        c_s = std::min(c_s, fork_point);  // Rewind safe cursor to common ancestor
        c_f = std::min(c_f, c_s);         // Invariant I2: ensure C_f <= C_s
    }
};

Sweeper::Sweeper(const core::Config& cfg, EventContext ctx, LogSource& source, Sink& sink,
                 std::function<std::uint64_t()> now_ms, SweeperOptions opt)
    : state_(std::make_shared<State>(cfg, std::move(ctx), source, sink, std::move(now_ms), std::move(opt))) {
    if (state_->opt.max_bad_attempts == 0) throw std::invalid_argument("Sweeper: max_bad_attempts must be >= 1");
}

// Configures initial sweep cursors from persistent checkpoint.
// Watch out: Throws std::invalid_argument if C_f > C_s (Invariant I2: Dual Finality).
void Sweeper::set_cursors(std::uint64_t c_s, std::uint64_t c_f) {
    if (c_f > c_s) throw std::invalid_argument("Sweeper::set_cursors: C_f > C_s violates I2");
    state_->c_s = c_s;
    state_->c_f = c_f;
}

std::uint64_t Sweeper::c_s() const noexcept {
    return state_->c_s;
}
std::uint64_t Sweeper::c_f() const noexcept {
    return state_->c_f;
}
const SweepStats& Sweeper::stats() const noexcept {
    return state_->stats;
}

// Rewinds in-memory safe cursor to the fork point after a chain reorganization.
// Watch out: Pulls C_f backward only if C_s rewinds below C_f to preserve Invariant I2.
void Sweeper::rewind_safe(std::uint64_t fork_point) noexcept {
    state_->rewind_safe(fork_point);
}

// Fetches one page of logs from the current provider using AIMD dynamic page sizing.
// Reconciles block hashes with canonical block references and retries on inconsistency.
// Watch out: Returns StepStatus::Stalled if max_bad_attempts is reached without agreement.
boost::asio::awaitable<Sweeper::Fetched> Sweeper::fetch_page(std::shared_ptr<State> st, std::uint64_t cursor,
                                                             std::uint64_t ceiling, core::LogFilter filter,
                                                             std::optional<core::Finality> only_class) {
    Fetched out;

    // No contract of this class: an empty address list would mean "every address" to eth_getLogs, so do not ask.
    if (filter.addresses.empty()) {
        out.range = BlockRange{cursor + 1, ceiling};  // Advance cursor without provider query
        co_return out;
    }

    std::string last_problem;
    for (std::uint32_t attempt = 1; attempt <= st->opt.max_bad_attempts; ++attempt) {
        out = Fetched{};

        const auto first = st->source.current_provider();
        if (!first) {
            out.status = StepStatus::Retry;
            out.detail = "no usable getLogs provider";
            co_return out;
        }

        bool direct = false;  // one request for the whole filter (not split by address)
        AimdPager* pg = &st->pager_for(*first);
        for (;;) {
            // Invariant I1: calculate contiguous page range using AIMD window
            const auto r = next_range(cursor, ceiling, pg->range());
            if (!r) throw std::logic_error("Sweeper::fetch_page: called with cursor at the ceiling");

            LogResult res = co_await st->source.get_logs(*r, filter);
            if (res.ok) {
                out.range = *r;
                out.logs = std::move(res.logs);
                out.provider = res.provider;
                direct = true;
                break;
            }
            if (res.kind != rpc::FailKind::RangeTooLarge) {
                // Congestion rule (G1): a provider that rate-limits, times out or errors on a wide span may really be
                // saying "this query is too expensive" (Infura answered a 2000-block range with 429). Halve once.
                AimdPager& failed = st->pager_for(res.provider);
                const std::uint64_t span = r->to - r->from + 1;
                if ((res.kind == rpc::FailKind::Retry || res.kind == rpc::FailKind::RateLimited) &&
                    span > failed.min()) {
                    (void)failed.on_range_too_large(std::nullopt);  // Multiplicative decrease on congestion
                    ++st->stats.failure_shrinks;
                }
                out.status = StepStatus::Retry;
                out.detail = "getLogs failed on " + res.provider + ": " + res.error;
                co_return out;
            }

            ++st->stats.range_shrinks;
            pg = &st->pager_for(res.provider);
            if (pg->on_range_too_large(res.range_limit) == AimdPager::Shrink::Shrunk) continue;

            // Already at the minimum range and still too large: one request per address (plan 3.5).
            ++st->stats.address_splits;
            std::vector<rpc::Log> merged;
            std::string last_provider = res.provider;
            for (const auto& addr : filter.addresses) {
                core::LogFilter one;
                one.addresses = {addr};
                one.topic0s = filter.topic0s;
                LogResult r2 = co_await st->source.get_logs(*r, one);
                if (!r2.ok) {
                    out.status = r2.kind == rpc::FailKind::RangeTooLarge ? StepStatus::Stalled : StepStatus::Retry;
                    out.detail = std::string(out.status == StepStatus::Stalled
                                                 ? "range still too large at the minimum size for a single address on "
                                                 : "getLogs failed while splitting by address on ") +
                                 r2.provider + ": " + r2.error;
                    co_return out;
                }
                last_provider = r2.provider;
                merged.insert(merged.end(), std::make_move_iterator(r2.logs.begin()),
                              std::make_move_iterator(r2.logs.end()));
            }
            out.range = *r;
            out.logs = std::move(merged);
            out.provider = last_provider;
            break;
        }

        std::string problem;
        if (const auto violation = validate_logs(out.logs, out.range.from, out.range.to, st->cfg, only_class)) {
            problem = "invalid getLogs response from " + out.provider + ": " + *violation;
        } else {
            // Sort deterministically by block number then log index
            std::sort(out.logs.begin(), out.logs.end(), [](const rpc::Log& a, const rpc::Log& b) {
                return a.block_number != b.block_number ? a.block_number < b.block_number : a.log_index < b.log_index;
            });

            // I3: every event-block must still be canonical, and the log must belong to it.
            for (const auto& l : out.logs) {
                auto it = out.blocks.find(l.block_number);
                if (it == out.blocks.end()) {
                    const auto info = co_await st->source.block(l.block_number);
                    if (!info) {
                        out.status = StepStatus::Retry;
                        out.detail = "could not read block " + std::to_string(l.block_number);
                        co_return out;
                    }
                    it = out.blocks.emplace(l.block_number, *info).first;
                }
                // Verify log block_hash against the canonical header hash to prevent phantom events
                if (!(it->second.hash == l.block_hash)) {
                    problem = "log from " + out.provider + " carries blockHash " + l.block_hash.to_hex() +
                              " but block " + std::to_string(l.block_number) + " is " + it->second.hash.to_hex();
                    break;
                }
            }
        }

        if (problem.empty()) {
            if (direct) st->pager_for(out.provider).on_success();  // Additive increase on success streak
            if (out.logs.size() > st->opt.max_page_logs) {  // soft memory cap (G6): keep this page, shrink the next
                (void)st->pager_for(out.provider).on_range_too_large(std::nullopt);
                ++st->stats.volume_shrinks;
            }
            co_return out;
        }

        // Do not trust either side (G2): penalise the provider that answered, and ask again (the adapter steers the
        // next get_logs to another provider).
        ++st->stats.inconsistent;
        st->source.report_bad(out.provider, problem);
        last_problem = std::move(problem);
    }

    out = Fetched{};
    out.status = StepStatus::Stalled;
    out.detail = "no agreement after " + std::to_string(st->opt.max_bad_attempts) + " attempts: " + last_problem;
    co_return out;
}

// Executes a single page sweep for safe-tier logs; emits events and commits safe checkpoint.
// Watch out: Emits events with finality=Safe and registers them in Redis released ZSET.
boost::asio::awaitable<StepResult> Sweeper::safe_step(std::uint64_t g_s) {
    return safe_step_impl(state_, g_s);
}

boost::asio::awaitable<StepResult> Sweeper::safe_step_impl(std::shared_ptr<State> st, std::uint64_t g_s) {
    if (st->sink.saturated()) co_return StepResult{StepStatus::Paused, std::nullopt, "sink saturated", false};
    if (st->c_s >= g_s) co_return StepResult{};  // Cursor already at safe gate; nothing to do

    auto f = co_await fetch_page(st, st->c_s, g_s, st->cfg.filter(core::Finality::Safe), core::Finality::Safe);
    if (f.status != StepStatus::Advanced) co_return StepResult{f.status, std::nullopt, std::move(f.detail), false};

    for (const auto& log : f.logs) {
        const Event ev = st->build_event(log, f, EventType::Log);
        // Layer-1 in-memory dedup: filter duplicates before hitting Redis
        if (!st->recent.admit(ev.event_id, EventType::Log)) {
            ++st->stats.deduped;
            continue;
        }
        st->sink.emit(make_emission(ev, ev.block_number));  // Emits log and adds to released ZSET
        ++st->stats.emitted_safe;
    }
    st->sink.checkpoint(CheckpointKind::Safe, f.range.to);  // Invariant I3: advance safe checkpoint
    st->c_s = f.range.to;                                   // Invariant I1: advance in-memory safe cursor
    if (st->opt.on_page) st->opt.on_page(core::Finality::Safe, f.range, f.logs);
    co_return StepResult{StepStatus::Advanced, f.range, {}, false};
}

// Executes a single page sweep for finalized logs; diffs against released set to detect reorgs.
// Emits finalized-class logs and retracts vanished logs as reverts (Invariant I6).
// Watch out: Pauses if Redis sink is not readable to avoid diffing against incomplete released set.
boost::asio::awaitable<StepResult> Sweeper::final_step(std::uint64_t g_f) {
    return final_step_impl(state_, g_f);
}

boost::asio::awaitable<StepResult> Sweeper::final_step_impl(std::shared_ptr<State> st, std::uint64_t g_f) {
    if (st->sink.saturated()) co_return StepResult{StepStatus::Paused, std::nullopt, "sink saturated", false};
    const std::uint64_t ceiling = std::min(g_f, st->c_s);  // I2: never past what the safe sweep covered
    if (st->c_f >= ceiling) co_return StepResult{};
    // The diff needs the released set: without it the fetch below would be wasted (rev 6, 4d). Nothing is done.
    if (!st->sink.readable()) co_return StepResult{StepStatus::Paused, std::nullopt, "sink not readable", false};

    auto f = co_await fetch_page(st, st->c_f, ceiling, st->cfg.filter(), std::nullopt);
    if (f.status != StepStatus::Advanced) co_return StepResult{f.status, std::nullopt, std::move(f.detail), false};

    std::vector<rpc::Log> safe_logs;
    for (const auto& l : f.logs)
        if (is_safe_class(st->cfg, l)) safe_logs.push_back(l);

    const auto released = co_await st->sink.released_range(f.range.from, f.range.to);
    // Unavailable (sink degraded/replaying): diffing against a partial view would skip vanished logs (rev 6, A).
    // Nothing has been emitted for this page yet, so pausing leaves the cursor untouched (I5).
    if (!released) co_return StepResult{StepStatus::Paused, std::nullopt, "released set unavailable", false};
    const FinalDiff diff = diff_released(st->cfg.chain.chain_id, *released, safe_logs);

    std::set<std::string> late_ids;
    for (const auto& l : diff.late)
        late_ids.insert(make_event_id(st->cfg.chain.chain_id, l.block_hash, l.log_index));

    // Invariant I6: Retractions first, then replacement/late logs in (block, logIndex) order.
    for (const auto& v : diff.vanished) {
        if (!st->recent.admit(v.event_id, EventType::Revert)) {  // already retracted: a revert is never repeated (I6)
            ++st->stats.deduped;
            continue;
        }
        st->sink.emit(make_revert(v, st->now_ms()));  // Emits revert with original before-image fields
        ++st->stats.reverts;
    }
    for (const auto& log : f.logs) {
        Event ev = st->build_event(log, f, EventType::Log);
        const bool safe_class = is_safe_class(st->cfg, log);
        if (safe_class && late_ids.count(ev.event_id) == 0) continue;  // matched: already released, nothing to emit
        if (!st->recent.admit(ev.event_id, EventType::Log)) {
            ++st->stats.deduped;
            continue;
        }
        if (safe_class) {
            ev.finality = core::Finality::Finalized;  // it is final now; no `released` record needed
            ++st->stats.late;
            st->stats.last_late_block = ev.block_number;
        }
        st->sink.emit(make_emission(ev, std::nullopt));  // Finalized logs bypass released tracking
        ++st->stats.emitted_final;
    }
    st->stats.matched += diff.matched.size();

    st->sink.final_trim(f.range.to);                             // Trim released ZSET <= C_f to bound memory
    st->sink.checkpoint(CheckpointKind::Finalized, f.range.to);  // Advance finalized checkpoint in Redis
    st->c_f = f.range.to;                                        // Invariant I2: advance finalized cursor
    if (st->opt.on_page) {
        std::vector<rpc::Log> final_logs;
        for (const auto& l : f.logs)
            if (!is_safe_class(st->cfg, l)) final_logs.push_back(l);
        st->opt.on_page(core::Finality::Finalized, f.range, final_logs);
    }
    co_return StepResult{StepStatus::Advanced, f.range, {}, !diff.vanished.empty()};
}

boost::asio::awaitable<StepResult> Sweeper::backfill_step(std::uint64_t ceiling, core::Finality emit_finality) {
    return backfill_step_impl(state_, ceiling, emit_finality);
}

boost::asio::awaitable<StepResult> Sweeper::backfill_step_impl(std::shared_ptr<State> st, std::uint64_t ceiling,
                                                               core::Finality emit_finality) {
    if (st->sink.saturated()) co_return StepResult{StepStatus::Paused, std::nullopt, "sink saturated", false};
    if (st->c_s >= ceiling) co_return StepResult{};

    auto f = co_await fetch_page(st, st->c_s, ceiling, st->cfg.filter(emit_finality), emit_finality);
    if (f.status != StepStatus::Advanced) co_return StepResult{f.status, std::nullopt, std::move(f.detail), false};

    const bool track_released = emit_finality == core::Finality::Safe;
    for (const auto& log : f.logs) {
        Event ev = st->build_event(log, f, EventType::Log);
        ev.finality = emit_finality;
        if (!st->recent.admit(ev.event_id, EventType::Log)) {
            ++st->stats.deduped;
            continue;
        }
        st->sink.emit(make_emission(ev, track_released ? std::optional<std::uint64_t>(ev.block_number) : std::nullopt));
        if (track_released)
            ++st->stats.emitted_safe;
        else
            ++st->stats.emitted_final;
    }
    st->c_s = f.range.to;
    if (st->opt.on_page) st->opt.on_page(emit_finality, f.range, f.logs);
    co_return StepResult{StepStatus::Advanced, f.range, {}, false};
}

// Re-verifies released safe-class logs against the canonical chain across (C_f, C_s].
// Emits reverts for logs that disappeared in the reorg and emits new logs on the winning branch.
// Watch out: Pulls C_s back to the lowest fork point, ensuring no gaps in subsequent sweeps (Invariant I1).
boost::asio::awaitable<ReverifyResult> Sweeper::reverify_released() {
    return reverify_released_impl(state_);
}

boost::asio::awaitable<ReverifyResult> Sweeper::reverify_released_impl(std::shared_ptr<State> st) {
    st->source.clear_cache();
    const std::uint64_t ceiling = st->c_s;  // snapshot: the sweeps are serialised, so C_s cannot move under us
    std::uint64_t cursor = st->c_f;
    // Tracks lowest block number that deviated from canonical chain
    const auto note_fork = [&st](std::uint64_t changed_block) {
        const std::uint64_t fork = changed_block - 1;  // changed_block > C_f >= 0, so this cannot underflow
        st->pending_fork = st->pending_fork ? std::min(*st->pending_fork, fork) : fork;
    };

    if (cursor >= ceiling && !st->pending_fork) co_return ReverifyResult{};

    while (cursor < ceiling) {
        if (st->sink.saturated()) co_return ReverifyResult{StepStatus::Paused, "sink saturated", false, std::nullopt};
        if (!st->sink.readable())
            co_return ReverifyResult{StepStatus::Paused, "sink not readable", false, std::nullopt};

        auto f = co_await fetch_page(st, cursor, ceiling, st->cfg.filter(core::Finality::Safe), core::Finality::Safe);
        if (f.status != StepStatus::Advanced)
            co_return ReverifyResult{f.status, std::move(f.detail), false, std::nullopt};

        const auto released = co_await st->sink.released_range(f.range.from, f.range.to);
        if (!released) co_return ReverifyResult{StepStatus::Paused, "released set unavailable", false, std::nullopt};
        const FinalDiff diff = diff_released(st->cfg.chain.chain_id, *released, f.logs);

        std::set<std::string> new_ids;
        for (const auto& l : diff.late)
            new_ids.insert(make_event_id(st->cfg.chain.chain_id, l.block_hash, l.log_index));

        // Retractions first (Invariant I6: Revert Fidelity), then replacement logs in order.
        for (const auto& v : diff.vanished) {
            note_fork(v.block_number);
            if (!st->recent.admit(v.event_id, EventType::Revert)) {  // already retracted (I6)
                ++st->stats.deduped;
                continue;
            }
            st->sink.emit(make_revert(v, st->now_ms()));  // Retract ghost log with exact before-image data
            ++st->stats.reverts;
        }
        for (const auto& log : f.logs) {
            const Event ev = st->build_event(log, f, EventType::Log);
            if (new_ids.count(ev.event_id) == 0) continue;  // matched: still canonical
            note_fork(log.block_number);
            if (!st->recent.admit(ev.event_id, EventType::Log)) {
                ++st->stats.deduped;
                continue;
            }
            st->sink.emit(make_emission(ev, ev.block_number));  // still above C_f: tracked for the final-sweep diff
            ++st->stats.reverified_new;
        }
        st->stats.matched += diff.matched.size();
        cursor = f.range.to;  // Step page forward
    }

    ReverifyResult out;
    out.status = StepStatus::Advanced;
    if (st->pending_fork) {
        out.reorg = true;
        out.fork_point = *st->pending_fork;
        st->rewind_safe(*st->pending_fork);  // Rewind safe cursor to common ancestor of fork
        st->pending_fork.reset();
    }
    co_return out;
}

}  // namespace evml::pipeline
