#include <catch2/catch_test_macros.hpp>

#include <fstream>
#include <map>
#include <sstream>

#include <boost/asio/io_context.hpp>

#include "app/pool_config.hpp"
#include "core/error.hpp"

using namespace std::chrono_literals;

namespace {

using EnvMap = std::map<std::string, std::string>;

evml::core::Config load_base(const EnvMap& env) {
    std::ifstream f(std::string(EVML_CONFIG_DIR) + "/base.toml", std::ios::binary);
    REQUIRE(f.good());
    std::ostringstream ss;
    ss << f.rdbuf();
    return evml::core::Config::parse(ss.str(), "test.toml", [&](const std::string& k) {
        const auto it = env.find(k);
        return it == env.end() ? std::string() : it->second;
    });
}

EnvMap full_env() {
    return {
        {"BASE_WSS_URL_QUICKNODE", "wss://qn.example/SECRETQN"},
        {"BASE_RPC_URL_QUICKNODE", "https://qn.example/SECRETQN"},
        {"BASE_WSS_URL", "wss://infura.example/ws/v3/SECRETINFURA"},
        {"BASE_RPC_URL", "https://infura.example/v3/SECRETINFURA"},
        {"BASE_WSS_URL_ALCHEMY", "wss://alchemy.example/v2/SECRETALCHEMY"},
        {"BASE_RPC_URL_ALCHEMY", "https://alchemy.example/v2/SECRETALCHEMY"},
        {"BASE_RPC_URL_ANKR", "https://ankr.example/base/SECRETANKR"},
        {"N8N_ALERT_WEBHOOK_URL", "https://n8n.example/webhook/x"},
        {"N8N_ALERT_WEBHOOK_TOKEN", "SECRETTOKEN"},
        {"DEADMAN_PING_URL", "https://hc-ping.example/SECRETUUID"},
        {"EVML_HOST_ID", "a"},
    };
}

}  // namespace

TEST_CASE("make_pool_config maps base.toml onto the pool", "[unit][pool_config]") {
    const auto cfg = load_base(full_env());
    const auto p = evml::app::make_pool_config(cfg, "evm-listener/test");

    CHECK(p.ws_order == std::vector<std::string>{"quicknode", "infura", "alchemy"});
    CHECK(p.getlogs_order == std::vector<std::string>{"infura", "alchemy", "ankr", "base_public", "quicknode"});
    REQUIRE(p.providers.size() == 5);  // a provider in both chains is ONE endpoint

    const auto find = [&](const std::string& n) -> const evml::provider::ProviderEndpoint& {
        for (const auto& e : p.providers) {
            if (e.name == n) return e;
        }
        FAIL("provider not mapped: " << n);
        throw std::logic_error("unreachable");
    };
    CHECK(find("quicknode").ws_url == "wss://qn.example/SECRETQN");
    CHECK(find("quicknode").http_url == "https://qn.example/SECRETQN");
    CHECK(find("quicknode").reconcile_rps == 10u);
    CHECK(find("quicknode").reconcile_burst == 15u);
    CHECK(find("ankr").ws_url.empty());
    CHECK_FALSE(find("ankr").reconcile_rps.has_value());
    CHECK_FALSE(find("ankr").reconcile_burst.has_value());
    CHECK(find("base_public").http_url == "https://mainnet.base.org");

    CHECK(p.chain_id == 8453);
    CHECK(p.user_agent == "evm-listener/test");
    CHECK(p.subscribe_heads);
    REQUIRE(p.logs.has_value());
    CHECK(p.logs->addresses.size() == 5);
    CHECK_FALSE(p.logs->topic0s.empty());

    CHECK(p.policy.failover_after == 3);
    CHECK(p.policy.prefer_primary_after == 600s);
    CHECK(p.policy.quota_cooldown == 3600s);
    CHECK(p.stale.ws_lag_blocks == 5);
    CHECK(p.stale.head_stall == 15s);
    CHECK(p.safe_poll == 4000ms);
    CHECK(p.finalized_poll == 12000ms);
    CHECK(p.subscribe_timeout == 10000ms);
    CHECK(p.http_timeout == 15000ms);
    CHECK(p.reconcile_rps == 2);
    CHECK(p.reconcile_burst == 4);

    boost::asio::io_context ioc;
    CHECK_NOTHROW(evml::provider::ProviderPool::create(ioc.get_executor(), p, {}));  // the mapping is consistent
}

TEST_CASE("no enabled WS provider is rejected at the config layer, before the pool", "[unit][pool_config]") {
    EnvMap env = full_env();
    for (const auto* k : {"BASE_WSS_URL_QUICKNODE", "BASE_WSS_URL", "BASE_WSS_URL_ALCHEMY"}) env.erase(k);
    CHECK_THROWS_AS(load_base(env), evml::core::ConfigError);
}
