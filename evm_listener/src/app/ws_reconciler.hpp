#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "chain/reorg_detector.hpp"
#include "core/config.hpp"
#include "pipeline/log_source.hpp"
#include "pipeline/pending_buffer.hpp"
#include "rpc/codec.hpp"

namespace evml::app {

// Per-leadership-term WebSocket vs. getLogs sweep cross-checker and live watermark state machine (plan 3.4, G3).
// Single-threaded (runs on the host io thread).
class WsReconciler {
public:
    WsReconciler(const core::Config& cfg, std::size_t pending_max_logs);

    // Stages a routed WS log into PendingBuffer; logs a warning and drops unrouted logs.
    void on_ws_log(const rpc::Log& log, const std::string& provider);

    // Cross-checks a swept page of `kind` logs against PendingBuffer across [range.from, range.to].
    // Pre-live and already-reconciled sub-ranges are evicted from PendingBuffer without counting misses.
    pipeline::CrossCheck on_sweep_page(core::Finality kind, const pipeline::BlockRange& range,
                                       const std::vector<rpc::Log>& logs);

    // Latches `live_from_block_ = c_s + 1` once the pipeline enters Live with an active WS provider.
    void on_live_tick(std::uint64_t c_s, bool has_active_ws);

    // Updates live watermark state when the active WS provider changes or drops.
    void on_ws_provider_changed(bool has_new_provider, std::optional<std::uint64_t> c_s);

    // Reseeds `live_from_block_` on the first WS head after a provider switch or disconnect.
    void on_head(std::uint64_t head_number, std::optional<std::uint64_t> live_c_s);

    // Purges orphaned staged logs and advances `live_from_block_` after a fork walk resolves.
    void on_fork_resolved(const chain::ReorgInfo& info, std::uint64_t new_head_number);

    // Purges all staged logs and advances `live_from_block_` when the head ring is reset.
    void on_ring_resync(std::uint64_t new_head_number);

    std::uint64_t evicted_total() const { return pending_.evicted_total(); }
    std::optional<std::uint64_t> live_from_block() const { return live_from_block_; }
    bool reseed_pending() const { return reseed_live_watermark_; }

private:
    const core::Config& cfg_;
    pipeline::PendingBuffer pending_;
    std::optional<std::uint64_t> live_from_block_;
    bool reseed_live_watermark_{false};
    std::uint64_t reconciled_safe_to_{0};
    std::uint64_t reconciled_final_to_{0};
};

}  // namespace evml::app

