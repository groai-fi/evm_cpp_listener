#pragma once

#include <chrono>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace evml::sink {

// A startup condition that a restart cannot fix (plan 4.3): the process exits with code 2 instead of looping under the
// restart policy. Unreachable Redis is NOT one of them: that waits.
/// @brief Fatal exception thrown when Redis configuration violates essential system requirements (version < 7.0 or eviction enabled).
struct StartupFatal : std::runtime_error {
    using std::runtime_error::runtime_error;
};

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
