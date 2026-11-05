// Startup Redis checks (plan 4.3). The decision table is tested without I/O; scenario 8 runs against the compose Redis
// and toxiproxy (REDIS_URL / TOXIPROXY_URL from the process environment only, a missing one FAILS the test).

#include "sink/redis_startup.hpp"

#include <sw/redis++/redis++.h>

#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "sink/keys.hpp"
#include "toxiproxy.hpp"

using namespace evml::sink;
using namespace std::chrono_literals;
using Catch::Matchers::ContainsSubstring;
using evml::testing::require_env;
using evml::testing::ToxiRedis;

namespace {

std::string info(const std::string& version) {
    return "# Server\r\nredis_version:" + version + "\r\nredis_mode:standalone\r\nos:Linux\r\n";
}

StartupFacts facts(const std::string& version, std::optional<std::string> policy = "noeviction",
                   std::optional<std::string> aof = "yes") {
    return StartupFacts{info(version), std::move(policy), std::move(aof)};
}

// Restores a server-wide setting even when an assertion fails.
struct ConfigGuard {
    sw::redis::Redis& redis;
    std::string name;
    std::string saved;
    ConfigGuard(sw::redis::Redis& r, std::string n) : redis(r), name(std::move(n)) {
        const auto got = redis.command<std::vector<std::string>>("CONFIG", "GET", name);
        saved = got.at(1);
    }
    void set(const std::string& v) { redis.command<std::string>("CONFIG", "SET", name, v); }
    ~ConfigGuard() {
        try {
            redis.command<std::string>("CONFIG", "SET", name, saved);
        } catch (const std::exception& e) {
            std::cerr << "ConfigGuard: restoring " << name << " failed: " << e.what() << "\n";
        }
    }
    ConfigGuard(const ConfigGuard&) = delete;
    ConfigGuard& operator=(const ConfigGuard&) = delete;
    ConfigGuard(ConfigGuard&&) = delete;
    ConfigGuard& operator=(ConfigGuard&&) = delete;
};

}  // namespace

TEST_CASE("startup: redis_version is parsed from INFO server", "[unit][sink]") {
    auto v = parse_redis_version(info("8.4.7"));
    CHECK(v.major == 8);
    CHECK(v.minor == 4);
    CHECK(v.patch == 7);
    v = parse_redis_version(info("7.0"));  // a missing patch is 0
    CHECK(v.major == 7);
    CHECK(v.patch == 0);
    v = parse_redis_version(info("7.2.4-rc1"));
    CHECK(v.patch == 4);
    CHECK_THROWS_AS(parse_redis_version("# Server\r\nos:Linux\r\n"), StartupFatal);
    CHECK_THROWS_AS(parse_redis_version(info("seven")), StartupFatal);
    CHECK_THROWS_AS(parse_redis_version(info("7")), StartupFatal);
}

TEST_CASE("startup: redis below 7.0 is fatal, 7.0 and 8.4 are accepted", "[unit][sink]") {
    CHECK_THROWS_WITH(evaluate_startup(facts("6.2.14"), true), ContainsSubstring("6.2.14 < 7.0"));
    CHECK_NOTHROW(evaluate_startup(facts("7.0.0"), true));
    CHECK_NOTHROW(evaluate_startup(facts("8.4.7"), true));
}

TEST_CASE("startup: maxmemory-policy must be noeviction when required", "[unit][sink]") {
    CHECK_THROWS_WITH(evaluate_startup(facts("8.4.7", "allkeys-lru"), true), ContainsSubstring("not 'noeviction'"));
    const auto lax = evaluate_startup(facts("8.4.7", "allkeys-lru"), false);  // not required: a warning, not an exit
    REQUIRE(lax.warnings.size() == 1);
    CHECK_THAT(lax.warnings[0], ContainsSubstring("allkeys-lru"));
    CHECK(evaluate_startup(facts("8.4.7"), true).warnings.empty());
}

TEST_CASE("startup: CONFIG refused is a warning, never an exit", "[unit][sink]") {
    const auto r = evaluate_startup(facts("8.4.7", std::nullopt, std::nullopt), true);
    CHECK(r.config_denied);
    CHECK(r.warnings.size() == 2);  // policy and persistence both unverifiable
    CHECK_FALSE(r.misconfigured);   // could not check is not the same as violated
    CHECK(r.maxmemory_policy.empty());
    CHECK(r.appendonly.empty());
}

TEST_CASE("startup: appendonly=no warns (a restart would lose the data)", "[unit][sink]") {
    const auto r = evaluate_startup(facts("8.4.7", "noeviction", "no"), true);
    REQUIRE(r.warnings.size() == 1);
    CHECK_THAT(r.warnings[0], ContainsSubstring("appendonly"));
    CHECK_FALSE(r.config_denied);
    CHECK(r.misconfigured);
    CHECK(r.maxmemory_policy == "noeviction");
    CHECK(r.appendonly == "no");
}

TEST_CASE("startup: a non-noeviction policy that is not required is flagged misconfigured; a clean Redis is not",
          "[unit][sink]") {
    const auto lax = evaluate_startup(facts("8.4.7", "allkeys-lru"), false);
    CHECK(lax.misconfigured);
    CHECK(lax.maxmemory_policy == "allkeys-lru");
    CHECK_FALSE(evaluate_startup(facts("8.4.7"), true).misconfigured);
}

TEST_CASE("startup: a reachable, correctly configured Redis passes with no warnings", "[integration][sink]") {
    // The compose Redis (redis:8.4): noeviction (the OOM test relies on it) and a version >= 7.
    const std::string url = require_env("REDIS_URL");
    sw::redis::Redis admin(url);
    StartupOptions opts;
    opts.shard = "startup_ok_" + std::to_string(std::random_device{}());
    struct KeyGuard {
        sw::redis::Redis& r;
        std::string k;
        KeyGuard(sw::redis::Redis& r_, std::string k_) : r(r_), k(std::move(k_)) {}
        ~KeyGuard() {
            try {
                r.del(k);
            } catch (...) {
            }
        }
        KeyGuard(const KeyGuard&) = delete;
        KeyGuard& operator=(const KeyGuard&) = delete;
        KeyGuard(KeyGuard&&) = delete;
        KeyGuard& operator=(KeyGuard&&) = delete;
    } guard{admin, Keys(opts.shard).tenant()};

    const auto r = await_redis_ready(url, opts);
    REQUIRE(r.has_value());
    CHECK(r->version.major >= 7);
    CHECK_FALSE(r->config_denied);
}

TEST_CASE("startup: maxmemory-policy allkeys-lru is fatal when noeviction is required (scenario 8)",
          "[integration][sink][redis_serial]") {
    const std::string url = require_env("REDIS_URL");
    sw::redis::Redis admin(url);
    ConfigGuard guard(admin, "maxmemory-policy");
    guard.set("allkeys-lru");

    const std::string shard = "startup_lru_" + std::to_string(std::random_device{}());
    struct KeyGuard {
        sw::redis::Redis& r;
        std::string k;
        KeyGuard(sw::redis::Redis& r_, std::string k_) : r(r_), k(std::move(k_)) {}
        ~KeyGuard() {
            try {
                r.del(k);
            } catch (...) {
            }
        }
        KeyGuard(const KeyGuard&) = delete;
        KeyGuard& operator=(const KeyGuard&) = delete;
        KeyGuard(KeyGuard&&) = delete;
        KeyGuard& operator=(KeyGuard&&) = delete;
    } key_guard{admin, Keys(shard).tenant()};

    StartupOptions strict;
    strict.shard = shard;
    strict.require_noeviction = true;
    CHECK_THROWS_WITH(await_redis_ready(url, strict), ContainsSubstring("not 'noeviction'"));

    StartupOptions lax;
    lax.shard = shard;
    lax.require_noeviction = false;
    const auto r = await_redis_ready(url, lax);
    REQUIRE(r.has_value());
    CHECK(r->warnings.size() == 1);
}

TEST_CASE("startup: a rejected AUTH is fatal, not retried (scenario 8)", "[integration][sink]") {
    // The compose Redis has no password, so any AUTH is refused ("AUTH called without any password configured").
    std::string url = require_env("REDIS_URL");
    const auto at = url.find("://");
    REQUIRE(at != std::string::npos);
    url.insert(at + 3, ":notthepassword@");
    CHECK_THROWS_WITH(await_redis_ready(url, StartupOptions{}), ContainsSubstring("AUTH rejected"));
}

TEST_CASE("startup: an unreachable Redis is waited for, never exited on (scenario 8)",
          "[integration][sink][redis_startup]") {
    const std::string url = require_env("REDIS_URL");
    sw::redis::Redis admin(url);
    ToxiRedis px;
    px.disable();

    StartupOptions o;
    o.shard = "startup_wait_" + std::to_string(std::random_device{}());
    struct KeyGuard {
        sw::redis::Redis& r;
        std::string k;
        KeyGuard(sw::redis::Redis& r_, std::string k_) : r(r_), k(std::move(k_)) {}
        ~KeyGuard() {
            try {
                r.del(k);
            } catch (...) {
            }
        }
        KeyGuard(const KeyGuard&) = delete;
        KeyGuard& operator=(const KeyGuard&) = delete;
        KeyGuard(KeyGuard&&) = delete;
        KeyGuard& operator=(KeyGuard&&) = delete;
    } key_guard{admin, Keys(o.shard).tenant()};

    o.probe_min = 10ms;
    o.probe_max = 50ms;
    o.io_timeout = 500ms;
    int waits = 0;
    std::string family;
    o.on_waiting = [&](std::string_view error, std::string_view fam, std::chrono::milliseconds waited) {
        REQUIRE_FALSE(error.empty());
        REQUIRE(waited >= 0ms);
        family = std::string(fam);
        if (++waits == 3) px.enable();  // Redis comes back after three failed attempts
    };
    const auto r = await_redis_ready(px.url(), o);
    REQUIRE(r.has_value());  // no exception, no exit: the process just waited in Init
    CHECK(waits >= 3);
    CHECK((family == "ipv4" || family == "ipv6"));  // the log names what the hostname resolved to
}

TEST_CASE("startup: waiting ends when stop is requested", "[integration][sink][redis_startup]") {
    ToxiRedis px;
    px.disable();
    StartupOptions o;
    o.probe_min = 5ms;
    o.probe_max = 20ms;
    o.io_timeout = 500ms;
    int waits = 0;
    o.on_waiting = [&](std::string_view, std::string_view, std::chrono::milliseconds) { ++waits; };
    o.stop = [&] { return waits >= 2; };
    CHECK_FALSE(await_redis_ready(px.url(), o).has_value());
}

TEST_CASE("startup: pure dedicated-Redis tenancy helpers and evaluate_tenancy decision table", "[unit][sink]") {
    CHECK(format_tenant_marker("local") == "evm-listener|local");
    CHECK(format_tenant_marker("staging") == "evm-listener|staging");
    CHECK(format_tenant_marker("production") == "evm-listener|production");

    CHECK(sanitize_client_name("evml:host a:startup\n\t") == "evml:host_a:startup__");

    CHECK(resolve_deploy_env("local", "redis://prod.railway.internal:6379") == "local");
    CHECK(resolve_deploy_env("staging", "redis://prod.railway.internal:6379") == "staging");
    CHECK(resolve_deploy_env("production", "redis://prod.railway.internal:6379") == "production");
    CHECK_THROWS_AS(resolve_deploy_env("dev", "redis://redis:6379"), StartupFatal);
    CHECK(resolve_deploy_env("", "redis://redis:6379/0") == "local");
    CHECK(resolve_deploy_env("", "redis://localhost:6379") == "local");
    CHECK(resolve_deploy_env("", "redis://127.0.0.1:6379") == "local");
    CHECK(resolve_deploy_env("", "redis://[::1]:6379") == "local");
    CHECK(resolve_deploy_env("", "redis://toxiproxy:16379") == "local");
    CHECK_THROWS_WITH(resolve_deploy_env("", "redis://prod.railway.internal:6379"),
                      ContainsSubstring("EVML_DEPLOY_ENV must be set"));

    CHECK(parse_redis_db("redis://redis:6379") == 0);
    CHECK(parse_redis_db("redis://redis:6379/") == 0);
    CHECK(parse_redis_db("redis://redis:6379/0") == 0);
    CHECK(parse_redis_db("redis://redis:6379/15?timeout=1s") == 15);
    CHECK_THROWS_AS(parse_redis_db("redis://redis:6379/-1"), StartupFatal);
    CHECK_THROWS_AS(parse_redis_db("redis://redis:6379/notanumber"), StartupFatal);
    CHECK_THROWS_AS(parse_redis_db("http://redis:6379/0"), StartupFatal);

    CHECK(is_allowed_test_redis_host("redis://redis:6379/0"));
    CHECK(is_allowed_test_redis_host("redis://localhost:6379"));
    CHECK(is_allowed_test_redis_host("redis://127.0.0.1:6379"));
    CHECK(is_allowed_test_redis_host("redis://[::1]:6379"));
    CHECK(is_allowed_test_redis_host("redis://toxiproxy:16379"));
    CHECK_FALSE(is_allowed_test_redis_host("redis://prod.railway.internal:6379"));
    CHECK(is_allowed_test_redis_host("redis://prod.railway.internal:6379", "1"));

    TenancyFacts clean;
    clean.keyspace_db_keys = {{0, 5}};
    clean.tenant_marker = "evm-listener|local";
    CHECK(evaluate_tenancy(clean, "local", 0).warnings.empty());

    // 1. Foreign key prefixes -> StartupFatal with tenancy_violation = true
    {
        TenancyFacts f = clean;
        f.foreign_key_prefixes = {"mds", "bull"};
        try {
            (void)evaluate_tenancy(f, "local", 0);
            FAIL("Expected StartupFatal on foreign_key_prefixes");
        } catch (const StartupFatal& e) {
            CHECK(e.tenancy_violation);
            CHECK(e.foreign_prefixes == "mds,bull");
        }
    }

    // 2. Non-zero keys in another DB -> StartupFatal with tenancy_violation = true
    {
        TenancyFacts f = clean;
        f.keyspace_db_keys = {{0, 5}, {1, 2}};
        try {
            (void)evaluate_tenancy(f, "local", 0);
            FAIL("Expected StartupFatal on foreign DB");
        } catch (const StartupFatal& e) {
            CHECK(e.tenancy_violation);
            CHECK(e.db == "db1");
        }
    }

    // 3. Missing or mismatched tenant marker -> StartupFatal with tenancy_violation = true
    {
        TenancyFacts f = clean;
        f.tenant_marker = std::nullopt;
        try {
            (void)evaluate_tenancy(f, "local", 0);
            FAIL("Expected StartupFatal on missing tenant_marker");
        } catch (const StartupFatal& e) {
            CHECK(e.tenancy_violation);
            CHECK(e.marker.empty());
        }

        f.tenant_marker = "evm-listener|production";
        try {
            (void)evaluate_tenancy(f, "staging", 0);
            FAIL("Expected StartupFatal on mismatched tenant_marker");
        } catch (const StartupFatal& e) {
            CHECK(e.tenancy_violation);
            CHECK(e.marker == "evm-listener|production");
        }
    }

    // Warnings: foreign client names, CLIENT LIST denied, SCAN budget exhausted
    {
        TenancyFacts f = clean;
        f.foreign_client_names = {"mds:host:data"};
        f.client_list_denied = true;
        f.scan_budget_exhausted = true;
        const auto rep = evaluate_tenancy(f, "local", 0);
        CHECK(rep.warnings.size() == 3);
    }
}

TEST_CASE("startup: live Redis tenancy checks reject foreign keys, foreign DBs, and mismatched tenant markers",
          "[integration][sink][redis_serial]") {
    const std::string url = require_env("REDIS_URL");
    sw::redis::Redis admin(url);
    const std::string shard = "startup_tenancy_" + std::to_string(std::random_device{}());
    const Keys keys(shard);

    struct CleanupGuard {
        sw::redis::Redis& r;
        std::string tenant_key;
        CleanupGuard(sw::redis::Redis& r_, std::string tk) : r(r_), tenant_key(std::move(tk)) {}
        ~CleanupGuard() {
            try {
                r.del("mds:test");
                r.del(tenant_key);
            } catch (...) {
            }
        }
        CleanupGuard(const CleanupGuard&) = delete;
        CleanupGuard& operator=(const CleanupGuard&) = delete;
        CleanupGuard(CleanupGuard&&) = delete;
        CleanupGuard& operator=(CleanupGuard&&) = delete;
    } cleanup{admin, keys.tenant()};

    StartupOptions opts;
    opts.shard = shard;
    opts.deploy_env = "local";
    opts.host_id = "test-host";

    // 1. Foreign key (mds:test) causes await_redis_ready to throw StartupFatal and does NOT write evml:{shard}:tenant
    admin.del(keys.tenant());
    admin.set("mds:test", "1");
    try {
        (void)await_redis_ready(url, opts);
        FAIL("Expected StartupFatal when foreign key mds:test exists");
    } catch (const StartupFatal& e) {
        CHECK(e.tenancy_violation);
        CHECK(e.foreign_prefixes == "mds");
    }
    admin.del("mds:test");
    CHECK(admin.exists(keys.tenant()) == 0);

    // 2. Non-zero key in DB 1 causes await_redis_ready to throw StartupFatal
    {
        std::string db1_url = url;
        const auto scheme_end = db1_url.find("://");
        const auto slash = db1_url.find('/', scheme_end == std::string::npos ? 0 : scheme_end + 3);
        if (slash != std::string::npos) db1_url.resize(slash);
        db1_url += "/1";
        sw::redis::Redis admin_db1(db1_url);
        struct Db1Guard {
            sw::redis::Redis& r;
            explicit Db1Guard(sw::redis::Redis& r_) : r(r_) {}
            ~Db1Guard() {
                try {
                    r.flushdb();
                } catch (...) {
                }
            }
            Db1Guard(const Db1Guard&) = delete;
            Db1Guard& operator=(const Db1Guard&) = delete;
            Db1Guard(Db1Guard&&) = delete;
            Db1Guard& operator=(Db1Guard&&) = delete;
        } db1_guard{admin_db1};

        admin_db1.set("evml:db1_probe", "1");
        try {
            (void)await_redis_ready(url, opts);
            FAIL("Expected StartupFatal when DB 1 has keys > 0");
        } catch (const StartupFatal& e) {
            CHECK(e.tenancy_violation);
            CHECK(e.db == "db1");
        }
    }
    CHECK(admin.exists(keys.tenant()) == 0);

    // 3. Mismatched tenant marker (evm-listener|production when deploy_env == "staging") throws StartupFatal
    admin.set(keys.tenant(), "evm-listener|production");
    opts.deploy_env = "staging";
    try {
        (void)await_redis_ready(url, opts);
        FAIL("Expected StartupFatal on mismatched tenant marker");
    } catch (const StartupFatal& e) {
        CHECK(e.tenancy_violation);
        CHECK(e.marker == "evm-listener|production");
    }
    admin.del(keys.tenant());

    // 4. Matching tenant marker (evm-listener|local) succeeds and writes evml:{shard}:tenant
    opts.deploy_env = "local";
    const auto rep = await_redis_ready(url, opts);
    REQUIRE(rep.has_value());
    CHECK(admin.get(keys.tenant()) == std::optional<std::string>("evm-listener|local"));
}
