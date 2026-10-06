#include "app/pool_config.hpp"

#include <algorithm>
#include <chrono>

namespace evml::app {

provider::PoolConfig make_pool_config(const core::Config& cfg, const std::string& user_agent) {
    using std::chrono::milliseconds;
    provider::PoolConfig p;

    const auto upsert = [&p](const std::string& name) -> provider::ProviderEndpoint& {
        auto it = std::find_if(p.providers.begin(), p.providers.end(),
                               [&](const provider::ProviderEndpoint& e) { return e.name == name; });
        if (it == p.providers.end()) {
            p.providers.push_back({name, "", ""});
            return p.providers.back();
        }
        return *it;
    };
    for (const auto& src : cfg.ws_chain) {
        auto& e = upsert(src.name);
        e.ws_url = src.ws_url;
        if (!src.http_url.empty()) e.http_url = src.http_url;
        p.ws_order.push_back(src.name);
    }
    for (const auto& src : cfg.getlogs_chain) {
        auto& e = upsert(src.name);
        e.http_url = src.http_url;
        p.getlogs_order.push_back(src.name);
    }

    p.chain_id = cfg.chain.chain_id;
    p.user_agent = user_agent;
    p.subscribe_heads = true;
    p.logs = cfg.filter();

    p.subscribe_timeout = milliseconds(cfg.watchdog.subscribe_timeout_ms);
    p.http_timeout = milliseconds(cfg.backfill.request_timeout_ms);

    p.policy.failover_after = cfg.redundancy.failover_after;
    p.policy.prefer_primary_after = std::chrono::seconds(cfg.redundancy.prefer_primary_after_s);
    p.policy.quota_cooldown = std::chrono::seconds(cfg.redundancy.quota_cooldown_s);

    p.stale.ws_lag_blocks = cfg.redundancy.stale_after_blocks;
    p.stale.head_stall = std::chrono::ceil<std::chrono::seconds>(milliseconds(cfg.watchdog.head_stall_ms));

    p.safe_poll = milliseconds(cfg.chain.safe_poll_ms);
    p.finalized_poll = milliseconds(cfg.chain.finalized_poll_ms);
    return p;
}

}  // namespace evml::app
