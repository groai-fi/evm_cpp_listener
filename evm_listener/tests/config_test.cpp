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
    SECTION("finality=depth needs chain depth > 0") {
        CHECK_THROWS_WITH(parse_with(mutate(base, "finality    = \"safe\"", "finality    = \"depth\""), env),
                          ContainsSubstring("requires [chain].depth > 0"));
        CHECK_THROWS_WITH(parse_with(mutate(base, "default_finality   = \"finalized\"", "default_finality   = \"depth\""), env),
                          ContainsSubstring("finality=depth requires depth > 0"));
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
