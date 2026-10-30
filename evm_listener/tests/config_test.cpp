#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <fstream>
#include <map>
#include <sstream>
#include <string>

#include "core/config.hpp"
#include "core/error.hpp"

using namespace evml::core;
using Catch::Matchers::ContainsSubstring;

#ifndef EVML_CONFIG_DIR
#error "EVML_CONFIG_DIR must point at evm_listener/config"
#endif

namespace {

using EnvMap = std::map<std::string, std::string>;

std::string base_toml() {
    std::ifstream f(std::string(EVML_CONFIG_DIR) + "/base.toml", std::ios::binary);
    REQUIRE(f.good());
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

// Replaces the first occurrence; REQUIREs it exists so a stale pattern can never make a test vacuous.
std::string mutate(std::string text, const std::string& from, const std::string& to) {
    const auto pos = text.find(from);
    REQUIRE(pos != std::string::npos);
    text.replace(pos, from.size(), to);
    return text;
}

// Distinctive fake secrets: they must never surface in notes or error messages.
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

Config parse_with(const std::string& text, const EnvMap& env) {
    return Config::parse(text, "test.toml", [&](const std::string& k) {
        const auto it = env.find(k);
        return it == env.end() ? std::string() : it->second;
    });
}

}  // namespace

TEST_CASE("base.toml parses: 5 contracts (2 safe, 3 finalized), 2 registry", "[unit][config]") {
    const Config c = parse_with(base_toml(), full_env());
    REQUIRE(c.contracts.size() == 5);
    int safe = 0, finalized = 0, registry = 0;
    for (const auto& k : c.contracts) {
        (k.finality == Finality::Safe ? safe : finalized) += 1;
        if (k.stream == StreamKind::Registry) ++registry;
    }
    CHECK(safe == 2);
    CHECK(finalized == 3);
    CHECK(registry == 2);
    CHECK(c.shard.id == "base_eurc_usdc");
    CHECK(c.shard.host_id == "a");
    CHECK(c.chain.chain_id == 8453);
    CHECK(c.routes.size() == 4 + 4 + 5 + 1 + 3);
}

TEST_CASE("base.toml: sink spill settings (process-scoped, ephemeral disk)", "[unit][config]") {
    const Config c = parse_with(base_toml(), full_env());
    CHECK(c.sink.spill_dir == "/tmp/evml-spill");
    CHECK(c.sink.spill_max_bytes == 536870912);
}

TEST_CASE("base.toml: the downstream consumer group is watched; removing the keys disables the probe", "[unit][config]") {
    const Config c = parse_with(base_toml(), full_env());
    REQUIRE(c.sink.consumer_groups.size() == 1);
    CHECK(c.sink.consumer_groups[0].stream == StreamKind::Events);
    CHECK(c.sink.consumer_groups[0].group == "strat:aero_eurcusdc:botB_v1");
    CHECK(c.sink.consumer_lag_warn_s == 43200);
    CHECK(c.sink.consumer_lag_crit_s == 86400);

    std::string bare = mutate(base_toml(), "consumer_groups     = [{ stream = \"events\", group = \"strat:aero_eurcusdc:botB_v1\" }]\n", "");
    bare = mutate(bare, "consumer_lag_warn_s = 43200   # 12 h = 25% of the 48 h retention\n", "");
    bare = mutate(bare, "consumer_lag_crit_s = 86400   # 24 h = 50%: half the safety margin is gone\n", "");
    const Config d = parse_with(bare, full_env());
    CHECK(d.sink.consumer_groups.empty());  // probe disabled, not an error
    CHECK(d.sink.consumer_lag_warn_s == 43200);
    CHECK(d.sink.consumer_lag_crit_s == 86400);
}

TEST_CASE("base.toml: start blocks and registry defaults", "[unit][config]") {
    const Config c = parse_with(base_toml(), full_env());
    CHECK(c.contracts[0].start_block == std::optional<std::uint64_t>(52060000));
    CHECK(c.contracts[1].start_block == std::optional<std::uint64_t>(52060000));
    CHECK(c.contracts[2].start_block == std::optional<std::uint64_t>(52060000));
    CHECK_FALSE(c.contracts[3].start_block.has_value());  // registry -> gate
    CHECK_FALSE(c.contracts[4].start_block.has_value());
}

TEST_CASE("provider chains follow the configured order", "[unit][config]") {
    const Config c = parse_with(base_toml(), full_env());
    REQUIRE(c.ws_chain.size() == 3);
    CHECK(c.ws_chain[0].name == "quicknode");
    CHECK(c.ws_chain[1].name == "infura");
    CHECK(c.ws_chain[2].name == "alchemy");
    REQUIRE(c.getlogs_chain.size() == 5);
    CHECK(c.getlogs_chain[0].name == "infura");
    CHECK(c.getlogs_chain[1].name == "alchemy");
    CHECK(c.getlogs_chain[2].name == "ankr");
    CHECK(c.getlogs_chain[3].name == "base_public");
    CHECK(c.getlogs_chain[3].http_url == "https://mainnet.base.org");
    CHECK(c.getlogs_chain[4].name == "quicknode");
    CHECK(c.getlogs_chain[4].max_getlogs_range == 5);
    // A provider enabled for both roles carries both URLs.
    CHECK(c.ws_chain[1].http_url == "https://infura.example/v3/SECRETINFURA");
    CHECK(c.getlogs_chain[0].ws_url == "wss://infura.example/ws/v3/SECRETINFURA");
}

TEST_CASE("providers with unset env are skipped and the rest keep their order", "[unit][config]") {
    EnvMap env = full_env();
    env.erase("BASE_WSS_URL_QUICKNODE");
    env.erase("BASE_RPC_URL_QUICKNODE");
    env.erase("BASE_RPC_URL_ALCHEMY");
    env.erase("BASE_WSS_URL_ALCHEMY");
    const Config c = parse_with(base_toml(), env);
    REQUIRE(c.ws_chain.size() == 1);
    CHECK(c.ws_chain[0].name == "infura");
    REQUIRE(c.getlogs_chain.size() == 3);
    CHECK(c.getlogs_chain[0].name == "infura");
    CHECK(c.getlogs_chain[1].name == "ankr");
    CHECK(c.getlogs_chain[2].name == "base_public");
    std::string all_notes;
    for (const auto& n : c.notes) all_notes += n + "\n";
    CHECK_THAT(all_notes, ContainsSubstring("env BASE_WSS_URL_QUICKNODE unset"));
    CHECK_THAT(all_notes, ContainsSubstring("env BASE_RPC_URL_ALCHEMY unset"));
}

TEST_CASE("only an HTTP provider set: exit-2 style error naming the unset WS env vars", "[unit][config]") {
    const EnvMap env = {{"BASE_RPC_URL_ANKR", "https://ankr.example/base/SECRETANKR"}};
    try {
        parse_with(base_toml(), env);
        FAIL("expected ConfigError");
    } catch (const ConfigError& e) {
        const std::string msg = e.what();
        CHECK_THAT(msg, ContainsSubstring("no WS provider enabled"));
        CHECK_THAT(msg, ContainsSubstring("BASE_WSS_URL_QUICKNODE"));
        CHECK_THAT(msg, ContainsSubstring("BASE_WSS_URL"));
        CHECK_THAT(msg, ContainsSubstring("BASE_WSS_URL_ALCHEMY"));
        CHECK(msg.find("SECRET") == std::string::npos);
    }
}

TEST_CASE("only WS providers set: no getLogs provider except the public one is still valid", "[unit][config]") {
    const EnvMap env = {{"BASE_WSS_URL", "wss://infura.example/ws/v3/SECRETINFURA"}};
    const Config c = parse_with(base_toml(), env);
    REQUIRE(c.getlogs_chain.size() == 1);
    CHECK(c.getlogs_chain[0].name == "base_public");
}

TEST_CASE("no getLogs provider at all fails with the env names", "[unit][config]") {
    const std::string text = mutate(base_toml(), "getlogs_order          = [\"infura\", \"alchemy\", \"ankr\", \"base_public\", \"quicknode\"]",
                                    "getlogs_order          = [\"infura\"]");
    const EnvMap env = {{"BASE_WSS_URL", "wss://infura.example/ws/v3/SECRETINFURA"}};
    CHECK_THROWS_WITH(parse_with(text, env), ContainsSubstring("no getLogs provider enabled; set at least one of: BASE_RPC_URL"));
}

TEST_CASE("route lookup and filters", "[unit][config]") {
    const Config c = parse_with(base_toml(), full_env());
    const auto pool1 = Address20::from_hex("0xc5E51044eB7318950B1aFb044FccFb25782C48c1");
    const auto swap = Hash32::from_hex("0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67");
    const auto* r = c.route(pool1, swap);
    REQUIRE(r != nullptr);
    CHECK(r->contract == "eurc_usdc_cl1_pool");
    CHECK(r->event == "Swap");
    CHECK(r->finality == Finality::Safe);
    CHECK(r->stream == StreamKind::Events);

    // Same topic0 on an untracked address, or a tracked address with another contract's topic0: dropped.
    const auto gauge = Address20::from_hex("0x1f6c9d116CE22b51b0BC666f86B038a6c19900B8");
    CHECK(c.route(gauge, swap) == nullptr);
    CHECK(c.route(Address20::from_hex("0x" + std::string(40, '1')), swap) == nullptr);

    const auto all = c.filter();
    CHECK(all.addresses.size() == 5);
    const auto safe = c.filter(Finality::Safe);
    CHECK(safe.addresses.size() == 2);
    CHECK(safe.topic0s.size() == 4);  // pool event set shared by both pools
    const auto fin = c.filter(Finality::Finalized);
    CHECK(fin.addresses.size() == 3);
    CHECK(fin.topic0s.size() == 5 + 1 + 3);

    const auto pool_created = Hash32::from_hex("0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2");
    const auto* reg = c.route(Address20::from_hex("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A"), pool_created);
    REQUIRE(reg != nullptr);
    CHECK(reg->stream == StreamKind::Registry);
}

TEST_CASE("notes carry no secrets and warn about disabled alerting", "[unit][config]") {
    EnvMap env = full_env();
    env.erase("N8N_ALERT_WEBHOOK_URL");
    env.erase("N8N_ALERT_WEBHOOK_TOKEN");
    env.erase("DEADMAN_PING_URL");
    env.erase("BASE_RPC_URL_ALCHEMY");
    const Config c = parse_with(base_toml(), env);
    std::string all_notes;
    for (const auto& n : c.notes) all_notes += n + "\n";
    CHECK_THAT(all_notes, ContainsSubstring("alerts disabled: env N8N_ALERT_WEBHOOK_URL unset"));
    CHECK_THAT(all_notes, ContainsSubstring("dead-man's switch disabled: env DEADMAN_PING_URL unset"));
    CHECK(all_notes.find("SECRET") == std::string::npos);
}

TEST_CASE("invalid configs fail fast with precise messages", "[unit][config]") {
    const std::string base = base_toml();
    const EnvMap env = full_env();

    SECTION("TOML syntax error") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "[lease]", "[lease"), env), ContainsSubstring("TOML syntax error"));
    }
    SECTION("unknown key is rejected, not ignored") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "[shard]\n", "[shard]\ntypo_key = 1\n"), env),
                          ContainsSubstring("[shard].typo_key: unknown key"));
    }
    SECTION("the retired wal_dir key is rejected, not ignored") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "spill_dir ", "wal_dir   "), env),
                          ContainsSubstring("[sink].wal_dir: unknown key"));
    }
    SECTION("missing table") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "[backfill]", "[backfill_x]"), env), ContainsSubstring("unknown key"));
    }
    SECTION("bad shard id") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "\"base_eurc_usdc\"", "\"Base-EURC\""), env),
                          ContainsSubstring("[shard].id: must be non-empty [a-z0-9_]+"));
    }
    SECTION("short address") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "0xc5E51044eB7318950B1aFb044FccFb25782C48c1", "0xc5E51044"), env),
                          ContainsSubstring("[[contracts]][0].address: expected 0x + 40 hex digits"));
    }
    SECTION("duplicate address (case-insensitive)") {
        const std::string text = mutate(base, "0xE846373C1a92B167b4E9cd5d8E4d6B1Db9E90EC7", "0xC5E51044EB7318950B1AFB044FCCFB25782C48C1");
        CHECK_THROWS_WITH(parse_with(text, env), ContainsSubstring("duplicate contract address"));
    }
    SECTION("duplicate contract name") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "\"eurc_usdc_cl50_pool\"", "\"eurc_usdc_cl1_pool\""), env),
                          ContainsSubstring("duplicate contract name"));
    }
    SECTION("non-canonical event signature (named params)") {
        const std::string text = mutate(base, "\"Deposit(address,uint256,uint128)\"", "\"Deposit(address user,uint256,uint128)\"");
        CHECK_THROWS_WITH(parse_with(text, env), ContainsSubstring("non-canonical event signature"));
    }
    SECTION("duplicate event within a contract") {
        const std::string text = mutate(base, "\"ClaimRewards(address,uint256)\"", "\"Deposit(address,uint256,uint128)\"");
        CHECK_THROWS_WITH(parse_with(text, env), ContainsSubstring("duplicate event"));
    }
    SECTION("bad stream override") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "stream  = \"registry\"", "stream  = \"bogus\""), env),
                          ContainsSubstring("must be events|registry"));
    }
    SECTION("bad finality value") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "finality    = \"safe\"", "finality    = \"soon\""), env),
                          ContainsSubstring("must be one of safe|finalized|depth"));
    }
    SECTION("finality=depth is rejected in v1") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "finality    = \"safe\"", "finality    = \"depth\""), env),
                          ContainsSubstring("finality=depth is not supported in v1"));
        CHECK_THROWS_WITH(parse_with(mutate(base, "default_finality   = \"finalized\"", "default_finality   = \"depth\""), env),
                          ContainsSubstring("finality=depth is not supported in v1"));
    }
    SECTION("redundancy.mode=hedged is rejected in v1") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "mode                   = \"failover\"", "mode                   = \"hedged\""), env),
                          ContainsSubstring("mode=hedged is not supported in v1"));
    }
    SECTION("start_block < 1 is rejected") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "start_block = 52060000", "start_block = 0"), env),
                          ContainsSubstring("start_block: must be >= 1"));
    }
    SECTION("events stream requires an explicit start_block") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "start_block = 52060000   # Bot B first deployment is block 52060327 (2026-10-02); no history needed before it\n", ""), env),
                          ContainsSubstring("start_block: required for stream=events"));
    }
    SECTION("start_block must be integer or \"gate\"") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "start_block = 52060000", "start_block = \"later\""), env),
                          ContainsSubstring("expected a non-negative integer or \"gate\""));
    }
    SECTION("ws_order referencing an undeclared provider") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "[\"quicknode\", \"infura\", \"alchemy\"]", "[\"quicknode\", \"nope\"]"), env),
                          ContainsSubstring("ws_order: references undeclared provider \"nope\""));
    }
    SECTION("http_url literal must be credential-free") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "https://mainnet.base.org", "https://mainnet.base.org/v2/KEY"), env),
                          ContainsSubstring("literal URLs must be credential-free"));
        CHECK_THROWS_WITH(parse_with(mutate(base, "https://mainnet.base.org", "https://user:pw@mainnet.base.org"), env),
                          ContainsSubstring("literal URLs must be credential-free"));
    }
    SECTION("http_env and http_url are mutually exclusive") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "http_url = \"https://mainnet.base.org\"", "http_url = \"https://mainnet.base.org\"\nhttp_env = \"X\""), env),
                          ContainsSubstring("set either http_env or http_url"));
    }
    SECTION("lease renew must be below ttl") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "renew_ms = 5000", "renew_ms = 15000"), env), ContainsSubstring("[lease].renew_ms: must be < ttl_ms"));
    }
    SECTION("consumer lag thresholds: warn < crit < retention") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "consumer_lag_warn_s = 43200", "consumer_lag_warn_s = 86400"), env),
                          ContainsSubstring("[sink].consumer_lag_warn_s: must be < consumer_lag_crit_s"));
        CHECK_THROWS_WITH(parse_with(mutate(base, "consumer_lag_crit_s = 86400", "consumer_lag_crit_s = 172800"), env),
                          ContainsSubstring("[sink].consumer_lag_crit_s: must be < retention_ms / 1000"));
    }
    SECTION("consumer group: unknown stream, empty name, duplicate, wrong shape") {
        const std::string good = "{ stream = \"events\", group = \"strat:aero_eurcusdc:botB_v1\" }";
        CHECK_THROWS_WITH(parse_with(mutate(base, good, "{ stream = \"nope\", group = \"g\" }"), env),
                          ContainsSubstring("[sink].consumer_groups[0].stream: must be events|registry"));
        CHECK_THROWS_WITH(parse_with(mutate(base, good, "{ stream = \"events\", group = \"\" }"), env),
                          ContainsSubstring("[sink].consumer_groups[0].group: must not be empty"));
        CHECK_THROWS_WITH(parse_with(mutate(base, good, good + ", " + good), env),
                          ContainsSubstring("[sink].consumer_groups[1].group: duplicate group"));
        CHECK_THROWS_WITH(parse_with(mutate(base, good, "{ stream = \"events\", group = \"g\", extra = 1 }"), env),
                          ContainsSubstring("[sink].consumer_groups[0].extra: unknown key"));
        CHECK_THROWS_WITH(parse_with(mutate(base, "[" + good + "]", "\"g\""), env),
                          ContainsSubstring("[sink].consumer_groups: expected an array"));
        // The same group name on the other stream is a different group.
        const Config c = parse_with(mutate(base, good, good + ", { stream = \"registry\", group = \"strat:aero_eurcusdc:botB_v1\" }"), env);
        CHECK(c.sink.consumer_groups.size() == 2);
    }
    SECTION("out-of-range integers are rejected") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "block_time_ms      = 2000", "block_time_ms      = 0"), env),
                          ContainsSubstring("[chain].block_time_ms: out of range"));
    }
    SECTION("wrong value type") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "chain_id           = 8453", "chain_id           = \"8453\""), env),
                          ContainsSubstring("[chain].chain_id: expected integer"));
    }
    SECTION("provider URL with wrong scheme names the env var, never the value") {
        EnvMap bad = env;
        bad["BASE_WSS_URL"] = "http://infura.example/ws/v3/SECRETINFURA";
        try {
            parse_with(base, bad);
            FAIL("expected ConfigError");
        } catch (const ConfigError& e) {
            const std::string msg = e.what();
            CHECK_THAT(msg, ContainsSubstring("env BASE_WSS_URL must be a ws:// or wss:// URL"));
            CHECK(msg.find("SECRET") == std::string::npos);
        }
    }
    SECTION("webhook URL without token") {
        EnvMap bad = env;
        bad.erase("N8N_ALERT_WEBHOOK_TOKEN");
        CHECK_THROWS_WITH(parse_with(base, bad), ContainsSubstring("N8N_ALERT_WEBHOOK_URL is set but N8N_ALERT_WEBHOOK_TOKEN is not"));
    }
    SECTION("webhook must be https") {
        EnvMap bad = env;
        bad["N8N_ALERT_WEBHOOK_URL"] = "http://n8n.example/hook";
        CHECK_THROWS_WITH(parse_with(base, bad), ContainsSubstring("must be an https:// URL"));
    }
}

TEST_CASE("host_id: explicit value beats $EVML_HOST_ID, which beats hostname", "[unit][config]") {
    EnvMap env = full_env();
    CHECK(parse_with(base_toml(), env).shard.host_id == "a");
    CHECK(parse_with(mutate(base_toml(), "host_id = \"\"", "host_id = \"explicit\""), env).shard.host_id == "explicit");
    env.erase("EVML_HOST_ID");
    CHECK_FALSE(parse_with(base_toml(), env).shard.host_id.empty());  // hostname
}

TEST_CASE("base.toml: reconcile and pending-buffer keys", "[unit][config]") {
    const Config c = parse_with(base_toml(), full_env());
    CHECK(c.chain.pending_max_logs == 200000);
    CHECK(c.redundancy.reconcile_rps == 2);
    CHECK(c.redundancy.reconcile_burst == 4);
    REQUIRE(c.ws_chain.size() >= 2);
    CHECK(c.ws_chain[0].name == "quicknode");
    CHECK(c.ws_chain[0].reconcile_rps == 10u);
    CHECK(c.ws_chain[0].reconcile_burst == 15u);
    CHECK(c.ws_chain[1].name == "infura");
    CHECK_FALSE(c.ws_chain[1].reconcile_rps.has_value());
    CHECK_FALSE(c.ws_chain[1].reconcile_burst.has_value());

    // Every key is optional: a config that predates them gets the plan defaults.
    std::string bare = mutate(base_toml(), "pending_max_logs   = 200000           # staged WS logs kept for the cross-check; oldest evicted beyond this\n", "");
    bare = mutate(bare, "reconcile_rps = 10           # Discover plan allows 15 RPS; 10 RPS * 5 blocks = 50 blocks/s catch-up\n", "");
    bare = mutate(bare, "reconcile_burst = 15\n", "");
    bare = mutate(bare, "reconcile_rps          = 2      # per-provider token bucket for reconcile calls (getLogs retries, block lookups)\n", "");
    bare = mutate(bare, "reconcile_burst        = 4      # bucket capacity; must be >= reconcile_rps\n", "");
    const Config d = parse_with(bare, full_env());
    CHECK(d.chain.pending_max_logs == 200000);
    CHECK(d.redundancy.reconcile_rps == 2);
    CHECK(d.redundancy.reconcile_burst == 4);
    CHECK_FALSE(d.ws_chain[0].reconcile_rps.has_value());
    CHECK_FALSE(d.ws_chain[0].reconcile_burst.has_value());
}

TEST_CASE("reconcile and pending-buffer keys reject out-of-range values", "[unit][config]") {
    const std::string base = base_toml();
    const EnvMap env = full_env();
    CHECK_THROWS_WITH(parse_with(mutate(base, "pending_max_logs   = 200000", "pending_max_logs   = 0"), env),
                      ContainsSubstring("[chain].pending_max_logs: out of range"));
    CHECK_THROWS_WITH(parse_with(mutate(base, "reconcile_rps          = 2 ", "reconcile_rps          = 0 "), env),
                      ContainsSubstring("[redundancy].reconcile_rps: out of range"));
    CHECK_THROWS_WITH(parse_with(mutate(base, "reconcile_burst        = 4 ", "reconcile_burst        = 0 "), env),
                      ContainsSubstring("[redundancy].reconcile_burst: out of range"));
    CHECK_THROWS_WITH(parse_with(mutate(base, "reconcile_rps = 10", "reconcile_rps = 0"), env),
                      ContainsSubstring("[[providers]][0].reconcile_rps: out of range"));
    CHECK_THROWS_WITH(parse_with(mutate(base, "reconcile_burst = 15", "reconcile_burst = 5"), env),
                      ContainsSubstring("[[providers]][0].reconcile_burst: effective burst (5) must be >= effective reconcile_rps (10)"));
    SECTION("omitted per-provider reconcile_burst falls back to [redundancy].reconcile_burst and fails fast when < reconcile_rps") {
        CHECK_THROWS_WITH(
            parse_with(mutate(base, "reconcile_burst = 15\n", ""), env),
            ContainsSubstring("[[providers]][0].reconcile_burst: effective burst (4) must be >= effective reconcile_rps (10)"));
    }
    // burst below rps: 3 < 5
    const std::string low_burst = mutate(mutate(base, "reconcile_rps          = 2 ", "reconcile_rps          = 5 "),
                                         "reconcile_burst        = 4 ", "reconcile_burst        = 3 ");
    CHECK_THROWS_WITH(parse_with(low_burst, env), ContainsSubstring("[redundancy].reconcile_burst: must be >= reconcile_rps"));
    // burst equal to rps is allowed
    const std::string equal = mutate(mutate(base, "reconcile_rps          = 2 ", "reconcile_rps          = 4 "),
                                     "reconcile_burst        = 4 ", "reconcile_burst        = 4 ");
    CHECK(parse_with(equal, env).redundancy.reconcile_burst == 4);
}

TEST_CASE("$PORT replaces the port of [ops].http_listen and keeps the host", "[unit][config]") {
    EnvMap env = full_env();
    CHECK(parse_with(base_toml(), env).ops.http_listen == "[::]:9464");  // unset: config value as written

    env["PORT"] = "8080";
    CHECK(parse_with(base_toml(), env).ops.http_listen == "[::]:8080");
    CHECK(parse_with(mutate(base_toml(), "\"[::]:9464\"", "\"0.0.0.0:9464\""), env).ops.http_listen == "0.0.0.0:8080");

    env["PORT"] = "65535";
    CHECK(parse_with(base_toml(), env).ops.http_listen == "[::]:65535");

    for (const char* bad : {"0", "65536", "abc", "80x", "-1", "123456", " 80"}) {
        env["PORT"] = bad;
        INFO("PORT=" << bad);
        CHECK_THROWS_WITH(parse_with(base_toml(), env), ContainsSubstring("env PORT must be an integer 1..65535"));
    }

    env["PORT"] = "8080";
    CHECK_THROWS_WITH(parse_with(mutate(base_toml(), "\"[::]:9464\"", "\"localhost\""), env),
                      ContainsSubstring("[ops].http_listen: must be host:port to apply $PORT"));
}

TEST_CASE("base.toml: fast_stream opt-in on pools, default false on others, and fast_retention_ms validation",
          "[unit][config][keys]") {
    const EnvMap env = full_env();
    const Config c = parse_with(base_toml(), env);

    CHECK(to_string(Finality::Latest) == std::string("latest"));
    CHECK(c.sink.fast_retention_ms == 3600000);

    REQUIRE(c.contracts.size() == 5);
    CHECK(c.contracts[0].name == "eurc_usdc_cl1_pool");
    CHECK(c.contracts[0].fast_stream == true);
    CHECK(c.contracts[1].name == "eurc_usdc_cl50_pool");
    CHECK(c.contracts[1].fast_stream == true);
    CHECK(c.contracts[2].name == "eurc_usdc_cl50_gauge");
    CHECK(c.contracts[2].fast_stream == false);
    CHECK(c.contracts[3].name == "aero_cl_factory");
    CHECK(c.contracts[3].fast_stream == false);
    CHECK(c.contracts[4].name == "aero_voter");
    CHECK(c.contracts[4].fast_stream == false);

    const auto pool1 = Address20::from_hex("0xc5E51044eB7318950B1aFb044FccFb25782C48c1");
    const auto pool50 = Address20::from_hex("0xE846373C1a92B167b4E9cd5d8E4d6B1Db9E90EC7");
    const auto swap = Hash32::from_hex("0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67");
    REQUIRE(c.route(pool1, swap) != nullptr);
    CHECK(c.route(pool1, swap)->fast_stream == true);
    REQUIRE(c.route(pool50, swap) != nullptr);
    CHECK(c.route(pool50, swap)->fast_stream == true);

    const auto factory = Address20::from_hex("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A");
    const auto pool_created = Hash32::from_hex("0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2");
    REQUIRE(c.route(factory, pool_created) != nullptr);
    CHECK(c.route(factory, pool_created)->fast_stream == false);

    // Omitting fast_stream and fast_retention_ms falls back to false and 3600000 ms.
    std::string bare = mutate(base_toml(), "fast_stream = true\n", "");
    bare = mutate(bare, "fast_stream = true\n", "");
    bare = mutate(bare, "fast_retention_ms  = 3600000     # XADD MINID ~ now-1h on evml:{shard}:fast\n", "");
    const Config d = parse_with(bare, env);
    CHECK(d.contracts[0].fast_stream == false);
    CHECK(d.contracts[1].fast_stream == false);
    CHECK(d.route(pool1, swap)->fast_stream == false);
    CHECK(d.sink.fast_retention_ms == 3600000);

    // Invalid fast_retention_ms (< 1000, 0, or negative) fails fast.
    CHECK_THROWS_WITH(
        parse_with(mutate(base_toml(), "fast_retention_ms  = 3600000", "fast_retention_ms  = 0"), env),
        ContainsSubstring("[sink].fast_retention_ms: out of range (min 1000, got 0)"));
    CHECK_THROWS_WITH(
        parse_with(mutate(base_toml(), "fast_retention_ms  = 3600000", "fast_retention_ms  = -5"), env),
        ContainsSubstring("[sink].fast_retention_ms: out of range (min 1000, got -5)"));
    CHECK_THROWS_WITH(
        parse_with(mutate(base_toml(), "fast_retention_ms  = 3600000", "fast_retention_ms  = 999"), env),
        ContainsSubstring("[sink].fast_retention_ms: out of range (min 1000, got 999)"));

    // Finality::Latest is runtime-only and rejected in TOML finality settings.
    CHECK_THROWS_WITH(
        parse_with(mutate(base_toml(), "finality    = \"safe\"", "finality    = \"latest\""), env),
        ContainsSubstring("must be one of safe|finalized|depth"));
}
