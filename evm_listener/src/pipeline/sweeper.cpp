#include "pipeline/sweeper.hpp"

#include <algorithm>
#include <set>
#include <stdexcept>

#include "pipeline/final_diff.hpp"
#include "pipeline/log_validation.hpp"

namespace evml::pipeline {

namespace {
bool is_safe_class(const core::Config& cfg, const rpc::Log& log) {
    const auto* route = cfg.route(log.address, log.topics.at(0));  // validated before this is called
    return route != nullptr && route->finality == core::Finality::Safe;
}
}  // namespace

const char* to_string(StepStatus s) {
    switch (s) {
        case StepStatus::NoWork: return "NoWork";
        case StepStatus::Advanced: return "Advanced";
        case StepStatus::Retry: return "Retry";
        case StepStatus::Stalled: return "Stalled";
        case StepStatus::Paused: return "Paused";
    }
    return "?";
}

Sweeper::Sweeper(const core::Config& cfg, EventContext ctx, LogSource& source, Sink& sink,
                 std::function<std::uint64_t()> now_ms, SweeperOptions opt)
    : cfg_(cfg),
      ctx_(std::move(ctx)),
      source_(source),
      sink_(sink),
      now_ms_(std::move(now_ms)),
      opt_(opt),
      recent_(opt.recent_capacity) {
    if (opt.max_bad_attempts == 0) throw std::invalid_argument("Sweeper: max_bad_attempts must be >= 1");
}

void Sweeper::set_cursors(std::uint64_t c_s, std::uint64_t c_f) {
    if (c_f > c_s) throw std::invalid_argument("Sweeper::set_cursors: C_f > C_s violates I2");
    c_s_ = c_s;
    c_f_ = c_f;
}

void Sweeper::rewind_safe(std::uint64_t fork_point) {
    c_s_ = std::min(c_s_, fork_point);
    c_f_ = std::min(c_f_, c_s_);
}

AimdPager& Sweeper::pager_for(const std::string& provider) {
    const auto it = pagers_.find(provider);
    if (it != pagers_.end()) return it->second;
    for (const auto& p : cfg_.getlogs_chain) {
        if (p.name == provider)
            return pagers_.emplace(provider, AimdPager(p.max_getlogs_range, opt_.min_range)).first->second;
    }
    throw std::logic_error("Sweeper: provider '" + provider + "' is not in the getLogs chain");
}

Event Sweeper::build_event(const rpc::Log& log, const Fetched& f, EventType type) const {
    const auto now = now_ms_();
    return make_event(log, cfg_, ctx_, type, f.blocks.at(log.block_number).timestamp, f.provider, now, now);
}

boost::asio::awaitable<Sweeper::Fetched> Sweeper::fetch_page(std::uint64_t cursor, std::uint64_t ceiling,
                                                             const core::LogFilter& filter,
                                                             std::optional<core::Finality> only_class) {
    Fetched out;

    // No contract of this class: an empty address list would mean "every address" to eth_getLogs, so do not ask.
    if (filter.addresses.empty()) {
        out.range = BlockRange{cursor + 1, ceiling};
        co_return out;
    }

    std::string last_problem;
    for (std::uint32_t attempt = 1; attempt <= opt_.max_bad_attempts; ++attempt) {
        out = Fetched{};

        const auto first = source_.current_provider();
        if (!first) {
            out.status = StepStatus::Retry;
            out.detail = "no usable getLogs provider";
            co_return out;
        }

        bool direct = false;  // one request for the whole filter (not split by address)
        AimdPager* pg = &pager_for(*first);
        for (;;) {
            const auto r = next_range(cursor, ceiling, pg->range());
            if (!r) throw std::logic_error("Sweeper::fetch_page: called with cursor at the ceiling");

            LogResult res = co_await source_.get_logs(*r, filter);
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
                AimdPager& failed = pager_for(res.provider);
                const std::uint64_t span = r->to - r->from + 1;
                if ((res.kind == rpc::FailKind::Retry || res.kind == rpc::FailKind::RateLimited) &&
                    span > failed.min()) {
                    failed.on_range_too_large(std::nullopt);
                    ++stats_.failure_shrinks;
                }
                out.status = StepStatus::Retry;
                out.detail = "getLogs failed on " + res.provider + ": " + res.error;
                co_return out;
            }

            ++stats_.range_shrinks;
            pg = &pager_for(res.provider);
            if (pg->on_range_too_large(res.range_limit) == AimdPager::Shrink::Shrunk) continue;

            // Already at the minimum range and still too large: one request per address (plan 3.5).
            ++stats_.address_splits;
            std::vector<rpc::Log> merged;
            std::string last_provider = res.provider;
            for (const auto& addr : filter.addresses) {
                core::LogFilter one;
                one.addresses = {addr};
                one.topic0s = filter.topic0s;
                LogResult r2 = co_await source_.get_logs(*r, one);
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
        if (const auto violation = validate_logs(out.logs, out.range.from, out.range.to, cfg_, only_class)) {
            problem = "invalid getLogs response from " + out.provider + ": " + *violation;
        } else {
            std::sort(out.logs.begin(), out.logs.end(), [](const rpc::Log& a, const rpc::Log& b) {
                return a.block_number != b.block_number ? a.block_number < b.block_number : a.log_index < b.log_index;
            });

            // I3: every event-block must still be canonical, and the log must belong to it.
            for (const auto& l : out.logs) {
                auto it = out.blocks.find(l.block_number);
                if (it == out.blocks.end()) {
                    const auto info = co_await source_.block(l.block_number);
                    if (!info) {
                        out.status = StepStatus::Retry;
                        out.detail = "could not read block " + std::to_string(l.block_number);
                        co_return out;
                    }
                    it = out.blocks.emplace(l.block_number, *info).first;
                }
                if (!(it->second.hash == l.block_hash)) {
                    problem = "log from " + out.provider + " carries blockHash " + l.block_hash.to_hex() +
                              " but block " + std::to_string(l.block_number) + " is " + it->second.hash.to_hex();
                    break;
                }
            }
        }

        if (problem.empty()) {
            if (direct) pager_for(out.provider).on_success();
            if (out.logs.size() > opt_.max_page_logs) {  // soft memory cap (G6): keep this page, shrink the next
                pager_for(out.provider).on_range_too_large(std::nullopt);
                ++stats_.volume_shrinks;
            }
            co_return out;
        }

        // Do not trust either side (G2): penalise the provider that answered, and ask again (the adapter steers the
        // next get_logs to another provider).
        ++stats_.inconsistent;
        source_.report_bad(out.provider, problem);
        last_problem = std::move(problem);
    }

    out = Fetched{};
    out.status = StepStatus::Stalled;
    out.detail = "no agreement after " + std::to_string(opt_.max_bad_attempts) + " attempts: " + last_problem;
    co_return out;
}

boost::asio::awaitable<StepResult> Sweeper::safe_step(std::uint64_t g_s) {
    if (sink_.saturated()) co_return StepResult{StepStatus::Paused, std::nullopt, "sink saturated", false};
    if (c_s_ >= g_s) co_return StepResult{};

    auto f = co_await fetch_page(c_s_, g_s, cfg_.filter(core::Finality::Safe), core::Finality::Safe);
    if (f.status != StepStatus::Advanced) co_return StepResult{f.status, std::nullopt, std::move(f.detail), false};

    for (const auto& log : f.logs) {
        const Event ev = build_event(log, f, EventType::Log);
        if (!recent_.admit(ev.event_id, EventType::Log)) {
            ++stats_.deduped;
            continue;
        }
        sink_.emit(make_emission(ev, ev.block_number));
        ++stats_.emitted_safe;
    }
    sink_.checkpoint(CheckpointKind::Safe, f.range.to);
    c_s_ = f.range.to;
    co_return StepResult{StepStatus::Advanced, f.range, {}, false};
}

boost::asio::awaitable<StepResult> Sweeper::final_step(std::uint64_t g_f) {
    if (sink_.saturated()) co_return StepResult{StepStatus::Paused, std::nullopt, "sink saturated", false};
    const std::uint64_t ceiling = std::min(g_f, c_s_);  // I2: never past what the safe sweep covered
    if (c_f_ >= ceiling) co_return StepResult{};

    auto f = co_await fetch_page(c_f_, ceiling, cfg_.filter(), std::nullopt);
    if (f.status != StepStatus::Advanced) co_return StepResult{f.status, std::nullopt, std::move(f.detail), false};

    std::vector<rpc::Log> safe_logs;
    for (const auto& l : f.logs)
        if (is_safe_class(cfg_, l)) safe_logs.push_back(l);

    const auto released = co_await sink_.released_range(f.range.from, f.range.to);
    // Unavailable (sink degraded/replaying): diffing against a partial view would skip vanished logs (rev 6, A).
    // Nothing has been emitted for this page yet, so pausing leaves the cursor untouched (I5).
    if (!released) co_return StepResult{StepStatus::Paused, std::nullopt, "released set unavailable", false};
    const FinalDiff diff = diff_released(cfg_.chain.chain_id, *released, safe_logs);

    std::set<std::string> late_ids;
    for (const auto& l : diff.late) late_ids.insert(make_event_id(cfg_.chain.chain_id, l.block_hash, l.log_index));

    // Retractions first, then the replacement/late logs in (block, logIndex) order.
    for (const auto& v : diff.vanished) {
        if (!recent_.admit(v.event_id, EventType::Revert)) {  // already retracted: a revert is never repeated (I6)
            ++stats_.deduped;
            continue;
        }
        sink_.emit(make_revert(v, now_ms_()));
        ++stats_.reverts;
    }
    for (const auto& log : f.logs) {
        Event ev = build_event(log, f, EventType::Log);
        const bool safe_class = is_safe_class(cfg_, log);
        if (safe_class && late_ids.count(ev.event_id) == 0) continue;  // matched: already released, nothing to emit
        if (!recent_.admit(ev.event_id, EventType::Log)) {
            ++stats_.deduped;
            continue;
        }
        if (safe_class) {
            ev.finality = core::Finality::Finalized;  // it is final now; no `released` record needed
            ++stats_.late;
        }
        sink_.emit(make_emission(ev, std::nullopt));
        ++stats_.emitted_final;
    }
    stats_.matched += diff.matched.size();

    sink_.final_trim(f.range.to);
    sink_.checkpoint(CheckpointKind::Finalized, f.range.to);
    c_f_ = f.range.to;
    co_return StepResult{StepStatus::Advanced, f.range, {}, !diff.vanished.empty()};
}

boost::asio::awaitable<ReverifyResult> Sweeper::reverify_released() {
    const std::uint64_t ceiling = c_s_;  // snapshot: the sweeps are serialised, so C_s cannot move under us
    std::uint64_t cursor = c_f_;
    const auto note_fork = [this](std::uint64_t changed_block) {
        const std::uint64_t fork = changed_block - 1;  // changed_block > C_f >= 0, so this cannot underflow
        pending_fork_ = pending_fork_ ? std::min(*pending_fork_, fork) : fork;
    };

    if (cursor >= ceiling && !pending_fork_) co_return ReverifyResult{};

    while (cursor < ceiling) {
        if (sink_.saturated()) co_return ReverifyResult{StepStatus::Paused, "sink saturated", false, std::nullopt};

        auto f = co_await fetch_page(cursor, ceiling, cfg_.filter(core::Finality::Safe), core::Finality::Safe);
        if (f.status != StepStatus::Advanced)
            co_return ReverifyResult{f.status, std::move(f.detail), false, std::nullopt};

        const auto released = co_await sink_.released_range(f.range.from, f.range.to);
        if (!released) co_return ReverifyResult{StepStatus::Paused, "released set unavailable", false, std::nullopt};
        const FinalDiff diff = diff_released(cfg_.chain.chain_id, *released, f.logs);

        std::set<std::string> new_ids;
        for (const auto& l : diff.late) new_ids.insert(make_event_id(cfg_.chain.chain_id, l.block_hash, l.log_index));

        // Retractions first, then the replacement logs in (block, logIndex) order.
        for (const auto& v : diff.vanished) {
            note_fork(v.block_number);
            if (!recent_.admit(v.event_id, EventType::Revert)) {  // already retracted (I6)
                ++stats_.deduped;
                continue;
            }
            sink_.emit(make_revert(v, now_ms_()));
            ++stats_.reverts;
        }
        for (const auto& log : f.logs) {
            const Event ev = build_event(log, f, EventType::Log);
            if (new_ids.count(ev.event_id) == 0) continue;  // matched: still canonical
            note_fork(log.block_number);
            if (!recent_.admit(ev.event_id, EventType::Log)) {
                ++stats_.deduped;
                continue;
            }
            sink_.emit(make_emission(ev, ev.block_number));  // still above C_f: tracked for the final-sweep diff
            ++stats_.reverified_new;
        }
        stats_.matched += diff.matched.size();
        cursor = f.range.to;
    }

    ReverifyResult out;
    out.status = StepStatus::Advanced;
    if (pending_fork_) {
        out.reorg = true;
        out.fork_point = *pending_fork_;
        rewind_safe(*pending_fork_);
        pending_fork_.reset();
    }
    co_return out;
}

}  // namespace evml::pipeline
