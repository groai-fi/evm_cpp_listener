#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

#include "sink/keys.hpp"
#include "sink/redis_conn.hpp"

namespace evml::sink {

// Lease acquisition and release (plan 4.8). Renewal lives in RedisSink, which owns the lease connection while it runs.
/// @brief Atomic lease grant returned by `lease.lua` (`acquire` mode), including the previous epoch's host if changed.
struct LeaseGrant {
    std::string token;
    std::optional<std::uint64_t> epoch_n;
    std::optional<std::string> old_host;  // populated only when a previous epoch existed and belonged to another host
};

using LeaseOutcome = std::optional<LeaseGrant>;
using LeaseAcquireError = ConnError;

/// @brief Attempts single lease acquisition without throwing on operational/protocol errors.
[[nodiscard]] core::Result<LeaseOutcome, LeaseAcquireError> try_acquire_lease(RedisConn& conn, const Keys& keys,
                                                                              std::string_view host_id,
                                                                              std::string_view nonce,
                                                                              std::uint32_t ttl_ms) noexcept;

/// @brief Attempts single lease acquisition.
/// @return The newly assigned LeaseGrant (token, epoch_n, old_host), or nullopt if another host holds leadership.
/// @throws ConnError On transport or protocol failure.
[[nodiscard]] std::optional<LeaseGrant> acquire_lease(RedisConn& conn, const Keys& keys, std::string_view host_id,
                                                      std::string_view nonce, std::uint32_t ttl_ms);

// Gives the lease up (graceful drain). True = released, false = it was not ours any more (NOOP). Throws like above.
/// @brief Voluntarily surrenders the leader lease on graceful shutdown without throwing on ConnError.
[[nodiscard]] core::Result<bool, ConnError> try_release_lease(RedisConn& conn, const Keys& keys,
                                                              std::string_view token) noexcept;
/// @brief Voluntarily surrenders the leader lease on graceful shutdown.
/// @return True if successfully released; false if token had already expired or was superseded.
[[nodiscard]] bool release_lease(RedisConn& conn, const Keys& keys, std::string_view token);

/// @brief The `n` of a lease token "n|host|nonce" (the epoch counter). nullopt for a token that does not start with
/// digits and '|', e.g. the lease-disabled token "0".
[[nodiscard]] std::optional<std::uint64_t> lease_token_epoch(std::string_view token) noexcept;
/// @brief The `host` of a lease token "n|host|nonce". nullopt when the token has no two '|' separators.
[[nodiscard]] std::optional<std::string> lease_token_host(std::string_view token);

/// @brief Options for standby leader election loop with backoff and jitter.
struct AcquireOptions {
    std::string host_id;
    std::uint32_t ttl_ms{};
    std::chrono::milliseconds poll{1000};  // how often a standby asks again while another host leads
    std::chrono::milliseconds probe_min{
        250};  // backoff while Redis is unreachable: full jitter in [0, min(max, min * 2^n)]
    std::chrono::milliseconds probe_max{30000};
    std::function<bool()> stop;                            // true = give up (shutdown)
    std::function<void(std::chrono::milliseconds)> sleep;  // empty = std::this_thread::sleep_for; tests inject
    std::function<std::string()> nonce;                    // empty = random 128-bit hex
    // Optional reachability callbacks while in standby: `on_waiting(error, waited)` fires on each ConnError with
    // elapsed wait since the first consecutive failure; `on_reachable()` fires when Redis responds (lease acquired or
    // held).
    std::function<void(std::string_view error, std::chrono::milliseconds waited)> on_waiting;
    std::function<void()> on_reachable;
};

// The standby loop: asks until it gets a token. Returns nullopt only when `stop` fired. The standby never writes.
/// @brief Blocks until the leader lease is acquired or stop condition is triggered.
/// Watch out: A host in standby never starts processing or writing to Redis until this returns a valid grant.
[[nodiscard]] std::optional<LeaseGrant> acquire_lease_blocking(RedisConn& conn, const Keys& keys,
                                                               const AcquireOptions& opts);

// `[lease] enabled=false` (local only; plan 4.8): there is no acquire and no renew, and the returned token "0" skips
// the fence in every script. Logs the WARN the plan requires: nothing stops two hosts in this mode from both writing.
/// @brief Returns special token "0" (kNoFence) when lease election is explicitly disabled.
/// Watch out: Bypasses leader fencing; running multiple instances in this mode risks split-brain data corruption.
[[nodiscard]] std::string lease_disabled_token();

}  // namespace evml::sink
