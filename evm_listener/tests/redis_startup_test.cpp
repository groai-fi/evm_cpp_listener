// Startup Redis checks (plan 4.3). The decision table is tested without I/O; scenario 8 runs against the compose Redis
// and toxiproxy (REDIS_URL / TOXIPROXY_URL from the process environment only, a missing one FAILS the test).

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

#include <sw/redis++/redis++.h>

#include "sink/redis_startup.hpp"
#include "toxiproxy.hpp"

using namespace evml::sink;
using namespace std::chrono_literals;
using Catch::Matchers::ContainsSubstring;
using evml::testing::ToxiRedis;
using evml::testing::require_env;

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
}

TEST_CASE("startup: appendonly=no warns (a restart would lose the data)", "[unit][sink]") {
    const auto r = evaluate_startup(facts("8.4.7", "noeviction", "no"), true);
    REQUIRE(r.warnings.size() == 1);
    CHECK_THAT(r.warnings[0], ContainsSubstring("appendonly"));
    CHECK_FALSE(r.config_denied);
}

TEST_CASE("startup: a reachable, correctly configured Redis passes with no warnings", "[integration][sink]") {
    // The compose Redis (redis:8.4): noeviction (the OOM test relies on it) and a version >= 7.
    const auto r = await_redis_ready(require_env("REDIS_URL"), StartupOptions{});
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

    StartupOptions strict;
    strict.require_noeviction = true;
    CHECK_THROWS_WITH(await_redis_ready(url, strict), ContainsSubstring("not 'noeviction'"));

    StartupOptions lax;
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

TEST_CASE("startup: an unreachable Redis is waited for, never exited on (scenario 8)", "[integration][sink]") {
    ToxiRedis px;
    px.disable();

    StartupOptions o;
    o.probe_min = 10ms;
    o.probe_max = 50ms;
    o.io_timeout = 500ms;
    int waits = 0;
    std::string family;
    o.on_waiting = [&](const std::string& error, const std::string& fam, std::chrono::milliseconds waited) {
        REQUIRE_FALSE(error.empty());
        REQUIRE(waited >= 0ms);
        family = fam;
        if (++waits == 3) px.enable();  // Redis comes back after three failed attempts
    };
    const auto r = await_redis_ready(px.url(), o);
    REQUIRE(r.has_value());  // no exception, no exit: the process just waited in Init
    CHECK(waits >= 3);
    CHECK((family == "ipv4" || family == "ipv6"));  // the log names what the hostname resolved to
}

TEST_CASE("startup: waiting ends when stop is requested", "[integration][sink]") {
    ToxiRedis px;
    px.disable();
    StartupOptions o;
    o.probe_min = 5ms;
    o.probe_max = 20ms;
    o.io_timeout = 500ms;
    int waits = 0;
    o.on_waiting = [&](const std::string&, const std::string&, std::chrono::milliseconds) { ++waits; };
    o.stop = [&] { return waits >= 2; };
    CHECK_FALSE(await_redis_ready(px.url(), o).has_value());
}
