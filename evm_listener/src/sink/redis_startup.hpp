#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace sw::redis {
class Redis;
}  // namespace sw::redis

namespace evml::sink {

// A startup condition that a restart cannot fix (plan 4.3): the process exits with code 2 instead of looping under the
// restart policy. Unreachable Redis is NOT one of them: that waits.
/// @brief Fatal exception thrown when Redis configuration violates essential system requirements (version < 7.0, eviction enabled, or tenancy violated).
struct StartupFatal : std::runtime_error {
    using std::runtime_error::runtime_error;
    StartupFatal(const std::string& msg, std::string foreign_prefixes, std::string db, std::string marker)
        : std::runtime_error(msg),
          tenancy_violation(true),
          foreign_prefixes(std::move(foreign_prefixes)),
          db(std::move(db)),
          marker(std::move(marker)) {}

    bool tenancy_violation{false};
    std::string foreign_prefixes;
    std::string db;
    std::string marker;
};

/// @brief Facts gathered during dedicated-Redis tenancy checks (INFO keyspace, bounded SCAN, tenant marker, CLIENT LIST).
struct TenancyFacts {
    std::map<int, std::uint64_t> keyspace_db_keys;
    std::vector<std::string> foreign_key_prefixes;  // up to 5 prefixes before first ':', never values
    bool scan_budget_exhausted{false};
    std::optional<std::string> tenant_marker;
    std::vector<std::string> foreign_client_names;
    bool client_list_denied{false};
};

/// @brief Report returned by dedicated-Redis tenancy evaluation.
struct TenancyReport {
    std::vector<std::string> warnings;
};

/// @brief Formats the expected value of `evml:{<shard>}:tenant` (`"evm-listener|<deploy_env>"`).
std::string format_tenant_marker(std::string_view deploy_env);

/// @brief Replaces whitespace and control characters outside `[0x21, 0x7e]` with `'_'` for `CLIENT SETNAME`.
std::string sanitize_client_name(std::string_view raw);

/// @brief Validates `EVML_DEPLOY_ENV` (`local|staging|production`) or defaults to `local` for local dev/test Redis hosts.
std::string resolve_deploy_env(std::string_view raw_deploy_env, std::string_view redis_url);

/// @brief Parses `/<db>` from `redis_url` path (defaults to 0; throws StartupFatal on malformed URL or invalid DB).
int parse_redis_db(std::string_view redis_url);

/// @brief Returns true if `allow_destructive_env == "1"` or `redis_url` targets a local dev/test Redis host.
bool is_allowed_test_redis_host(std::string_view redis_url, std::string_view allow_destructive_env = "");

/// @brief Evaluates dedicated-Redis tenancy invariants without performing I/O.
TenancyReport evaluate_tenancy(const TenancyFacts& facts, std::string_view expected_env, int expected_db);

/// @brief Gathers runtime tenancy facts (`INFO keyspace`, bounded `SCAN`, `CLIENT LIST`) from a live Redis connection.
TenancyFacts gather_runtime_tenancy_facts(sw::redis::Redis& redis);

/// @brief Structured semver version of the target Redis server.
struct RedisVersion {
    unsigned major{};
    unsigned minor{};
    unsigned patch{};
};

// "redis_version:8.4.7" out of an `INFO server` reply. Throws StartupFatal when the field is missing or malformed.
/// @brief Extracts and parses semver from `INFO server` text output.
RedisVersion parse_redis_version(std::string_view info_server);

// What the server told us. An absent optional means the command was refused (CONFIG is often disabled on managed Redis).
/// @brief Facts gathered during startup probes (INFO server, maxmemory-policy, appendonly).
struct StartupFacts {
    std::string info_server;
    std::optional<std::string> maxmemory_policy;
    std::optional<std::string> appendonly;
};

/// @brief Pre-flight validation report containing detected version and configuration warnings.
struct StartupReport {
    RedisVersion version;
    std::vector<std::string> warnings;  // each is logged at WARN; Phase 5 turns them into RedisMisconfigured alerts
    bool config_denied{false};          // CONFIG GET refused: policy and persistence could not be verified
    std::string maxmemory_policy;       // as reported by the server; empty = CONFIG GET refused
    std::string appendonly;             // as reported by the server; empty = CONFIG GET refused
    // A verified violation: a policy other than noeviction (only reachable with require_noeviction off) or
    // appendonly "no". A refused CONFIG GET is not one (config_denied says it could not be checked).
    bool misconfigured{false};
    std::string address_family;  // "ipv6", "ipv4", "other" or "unresolved" (set by await_redis_ready)
    int expected_db{0};
    TenancyReport tenancy;
};

// The decision table of plan 4.3, with no I/O. Throws StartupFatal for: redis_version < 7.0 (the Lua scripts use the
// `#!lua` shebang, so Redis refuses them whole when over maxmemory), and a maxmemory-policy other than noeviction when
// `require_noeviction` is set (any eviction could silently delete our streams or checkpoint).
/// @brief Evaluates startup facts against engine invariants.
/// @throws StartupFatal If Redis version < 7.0 or maxmemory-policy != noeviction.
StartupReport evaluate_startup(const StartupFacts& facts, bool require_noeviction);

/// @brief Options for Redis pre-flight connection readiness loop.
struct StartupOptions {
    bool require_noeviction{true};
    std::string shard{"base_eurc_usdc"};
    std::string deploy_env{"local"};
    std::string host_id{"startup"};
    std::chrono::milliseconds probe_min{250};  // full-jitter backoff in [0, min(probe_max, probe_min * 2^n)]
    std::chrono::milliseconds probe_max{30000};
    std::chrono::milliseconds io_timeout{2000};
    // Called after every failed attempt while Redis is unreachable: the host logs it, serves /readyz 503 and raises
    // RedisUnreachable once `waited` passes 60 s. `address_family` is "ipv6", "ipv4" or "unresolved".
    std::function<void(const std::string& error, const std::string& address_family, std::chrono::milliseconds waited)> on_waiting;
    std::function<bool()> stop;  // true = give up waiting (shutdown requested)
    std::function<void(std::chrono::milliseconds)> sleep;  // empty = std::this_thread::sleep_for; tests inject
};

// Connects, then runs the §4.3 checks. Waits (does not exit) while Redis is unreachable. Returns nullopt only when
// `stop` fired. Throws StartupFatal for AUTH rejected, redis < 7.0, or noeviction required and not set.
/// @brief Connects to Redis URL and blocks until server is ready and validated.
std::optional<StartupReport> await_redis_ready(const std::string& redis_url, const StartupOptions& opts);

}  // namespace evml::sink
