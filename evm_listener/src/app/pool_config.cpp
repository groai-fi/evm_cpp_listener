#include "app/pool_config.hpp"

#include <algorithm>
#include <chrono>

namespace evml::app {

/// @brief Constructs runtime ProviderPool configuration from validated core::Config.
provider::PoolConfig make_pool_config(const core::Config& cfg, const std::string& user_agent) {
    using std::chrono::milliseconds;
    provider::PoolConfig p;

    // Deduplicate endpoints so a provider configured for both WS streaming and HTTP getLogs shares state.
    const auto upsert = [&p](const std::string& name) -> provider::ProviderEndpoint& {
        auto it = std::find_if(p.providers.begin(), p.providers.end(),
                               [&](const provider::ProviderEndpoint& e) { return e.name == name; });
        if (it == p.providers.end()) {
            p.providers.push_back({name, "", ""});
            return p.providers.back();
        }
        return *it;
    };
    // Populate WebSocket endpoints and record operator-defined failover order.
    for (const auto& src : cfg.ws_chain) {
        auto& e = upsert(src.name);
        e.ws_url = src.ws_url;
        if (!src.http_url.empty()) e.http_url = src.http_url;
        e.reconcile_rps = src.reconcile_rps;
        e.reconcile_burst = src.reconcile_burst;
        p.ws_order.push_back(src.name);
    }
    // Populate HTTP endpoints and record operator-defined failover order.
    for (const auto& src : cfg.getlogs_chain) {
        auto& e = upsert(src.name);
        e.http_url = src.http_url;
        e.reconcile_rps = src.reconcile_rps;
        e.reconcile_burst = src.reconcile_burst;
        p.getlogs_order.push_back(src.name);
    }

    // Assign core chain identity and transport telemetry attributes.
    p.chain_id = cfg.chain.chain_id;
    p.user_agent = user_agent;
    p.subscribe_heads = true;
    p.logs = cfg.filter();

    // Map network request and subscription timeout limits.
    p.subscribe_timeout = milliseconds(cfg.watchdog.subscribe_timeout_ms);
    p.http_timeout = milliseconds(cfg.backfill.request_timeout_ms);

    // Transfer failover threshold and cooldown policies.
    p.policy.failover_after = cfg.redundancy.failover_after;
    p.policy.prefer_primary_after = std::chrono::seconds(cfg.redundancy.prefer_primary_after_s);
    p.policy.quota_cooldown = std::chrono::seconds(cfg.redundancy.quota_cooldown_s);

    // Configure staleness detection parameters (block lag and head silence stall).
    p.stale.ws_lag_blocks = cfg.redundancy.stale_after_blocks;
    p.stale.head_stall = std::chrono::ceil<std::chrono::seconds>(milliseconds(cfg.watchdog.head_stall_ms));

    // Configure block tag polling cadences for unfinalized and finalized safety gates.
    p.safe_poll = milliseconds(cfg.chain.safe_poll_ms);
    p.finalized_poll = milliseconds(cfg.chain.finalized_poll_ms);

    // Per-provider pacing for reconcile calls (call_to).
    p.reconcile_rps = cfg.redundancy.reconcile_rps;
    p.reconcile_burst = cfg.redundancy.reconcile_burst;
    return p;
}

}  // namespace evml::app
