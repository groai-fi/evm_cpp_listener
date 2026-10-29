#include "app/ws_reconciler.hpp"

#include <algorithm>

#include "core/log.hpp"

namespace evml::app {

namespace log = core::log;

WsReconciler::WsReconciler(const core::Config& cfg, std::size_t pending_max_logs)
    : cfg_(cfg), pending_(pending_max_logs) {}

void WsReconciler::on_ws_log(const rpc::Log& l, const std::string& provider) {
    if (l.topics.empty() || cfg_.route(l.address, l.topics.front()) == nullptr) {
        log::warn("host", "ws log matches no configured route, not staged",
                  {{"provider", provider}, {"block", std::to_string(l.block_number)}});
        return;
    }
    pending_.stage(l, provider);
}

pipeline::CrossCheck WsReconciler::on_sweep_page(core::Finality kind, const pipeline::BlockRange& range,
                                                 const std::vector<rpc::Log>& logs) {
    // `logs` holds one class (safe for a safe page, finalized for a final page), so only staged logs of that class
    // are in scope; the other class waits for its own sweep.
    const bool want_safe = kind == core::Finality::Safe;
    const auto in_scope = [this, want_safe](const rpc::Log& l) {
        const auto* route = cfg_.route(l.address, l.topics.at(0));  // stage() only admits routed logs
        return route != nullptr && (route->finality == core::Finality::Safe) == want_safe;
    };
    std::uint64_t& reconciled_to = want_safe ? reconciled_safe_to_ : reconciled_final_to_;
    // Historical catch-up pages below live_from_block_, or blocks already reconciled before a safe reorg rewound the
    // cursor, cannot be cross-checked against PendingBuffer again. Evict any staged entries in that range without counting misses.
    if (!live_from_block_) {
        reconciled_to = std::max(reconciled_to, range.to);
        (void)pending_.reconcile(range.from, range.to, logs, in_scope);
        return {};
    }
    const std::uint64_t check_from = std::max(*live_from_block_, reconciled_to + 1);
    reconciled_to = std::max(reconciled_to, range.to);
    if (range.to < check_from) {
        (void)pending_.reconcile(range.from, range.to, logs, in_scope);
        return {};
    }
    pipeline::BlockRange check_range = range;
    const std::vector<rpc::Log>* check_logs = &logs;
    std::vector<rpc::Log> filtered_logs;
    if (range.from < check_from) {
        (void)pending_.reconcile(range.from, check_from - 1, {}, in_scope);
        check_range.from = check_from;
        filtered_logs.reserve(logs.size());
        for (const auto& l : logs) {
            if (l.block_number >= check_from) filtered_logs.push_back(l);
        }
        check_logs = &filtered_logs;
    }
    const auto cc = pending_.reconcile(check_range.from, check_range.to, *check_logs, in_scope);
    if (cc.ws_missed != 0 || cc.ws_extra != 0) {
        log::warn("host", "ws cross-check mismatch",
                  {{"kind", want_safe ? "safe" : "finalized"},
                   {"from", std::to_string(check_range.from)},
                   {"to", std::to_string(check_range.to)},
                   {"ws_missed", std::to_string(cc.ws_missed)},
                   {"ws_extra", std::to_string(cc.ws_extra)}});
    }
    return cc;
}

void WsReconciler::on_live_tick(std::uint64_t c_s, bool has_active_ws) {
    if (!live_from_block_ && !reseed_live_watermark_ && has_active_ws) {
        live_from_block_ = c_s + 1;
    }
}

void WsReconciler::on_ws_provider_changed(bool has_new_provider, std::optional<std::uint64_t> c_s) {
    if (!has_new_provider) {
        live_from_block_.reset();
        reseed_live_watermark_ = true;
    } else if (live_from_block_) {
        if (c_s) {
            live_from_block_ = std::max(*live_from_block_, *c_s + 1);
        }
        reseed_live_watermark_ = true;
    }
}

void WsReconciler::on_head(std::uint64_t head_number, std::optional<std::uint64_t> live_c_s) {
    if (reseed_live_watermark_) {
        reseed_live_watermark_ = false;
        if (live_from_block_) {
            live_from_block_ = std::max(*live_from_block_, head_number);
        } else if (live_c_s) {
            live_from_block_ = std::max(*live_c_s + 1, head_number);
        }
    }
}

void WsReconciler::on_fork_resolved(const chain::ReorgInfo& info, std::uint64_t new_head_number) {
    // depth 0 = a gap that was only filled: nothing was replaced, but WS missed the gap blocks.
    if (info.depth > 0) {
        pending_.purge_from(info.fork_point + 1);  // orphaned branch's WS logs
        if (live_from_block_) {
            live_from_block_ = std::max(*live_from_block_, new_head_number + 1);
        }
    } else if (new_head_number > info.old_top && live_from_block_) {
        live_from_block_ = std::max(*live_from_block_, new_head_number + 1);
    }
}

void WsReconciler::on_ring_resync(std::uint64_t new_head_number) {
    pending_.purge_from(0);  // the old branch is unknown: staged logs cannot be trusted
    if (live_from_block_) {
        live_from_block_ = std::max(*live_from_block_, new_head_number + 1);
    }
}

}  // namespace evml::app

