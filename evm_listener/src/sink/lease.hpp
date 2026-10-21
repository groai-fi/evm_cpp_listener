#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include "sink/keys.hpp"
#include "sink/redis_conn.hpp"

namespace evml::sink {

// Lease acquisition and release (plan 4.8). Renewal lives in RedisSink, which owns the lease connection while it runs.
/// @brief Attempts single lease acquisition.
/// @return The newly assigned fencing token (e.g., "1|host|nonce"), or nullopt if another host holds leadership.
/// @throws ConnError On transport failure.
/// @throws std::runtime_error On Lua script syntax or execution failure.
std::optional<std::string> try_acquire_lease(RedisConn& conn, const Keys& keys, const std::string& host_id,
                                             const std::string& nonce, std::uint32_t ttl_ms);

// Gives the lease up (graceful drain). True = released, false = it was not ours any more (NOOP). Throws like above.
/// @brief Voluntarily surrenders the leader lease on graceful shutdown.
/// @return True if successfully released; false if token had already expired or was superseded.
bool release_lease(RedisConn& conn, const Keys& keys, const std::string& token);

/// @brief Options for standby leader election loop with backoff and jitter.
struct AcquireOptions {
    std::string host_id;
    std::uint32_t ttl_ms{};
    std::chrono::milliseconds poll{1000};      // how often a standby asks again while another host leads
    std::chrono::milliseconds probe_min{250};  // backoff while Redis is unreachable: full jitter in [0, min(max, min * 2^n)]
    std::chrono::milliseconds probe_max{30000};
    std::function<bool()> stop;                           // true = give up (shutdown)
    std::function<void(std::chrono::milliseconds)> sleep;  // empty = std::this_thread::sleep_for; tests inject
    std::function<std::string()> nonce;                   // empty = random 128-bit hex
};

// The standby loop: asks until it gets a token. Returns nullopt only when `stop` fired. The standby never writes.
/// @brief Blocks until the leader lease is acquired or stop condition is triggered.
/// Watch out: A host in standby never starts processing or writing to Redis until this returns a valid token.
std::optional<std::string> acquire_lease_blocking(RedisConn& conn, const Keys& keys, const AcquireOptions& opts);

// `[lease] enabled=false` (local only; plan 4.8): there is no acquire and no renew, and the returned token "0" skips the
// fence in every script. Logs the WARN the plan requires: nothing stops two hosts in this mode from both writing.
/// @brief Returns special token "0" (kNoFence) when lease election is explicitly disabled.
/// Watch out: Bypasses leader fencing; running multiple instances in this mode risks split-brain data corruption.
std::string lease_disabled_token();

}  // namespace evml::sink
