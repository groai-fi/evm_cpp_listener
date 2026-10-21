#include "sink/lease.hpp"

#include <algorithm>
#include <cstdio>
#include <random>
#include <stdexcept>
#include <thread>

#include "core/log.hpp"

namespace evml::sink {

namespace {

// Runs one lease script call; a flushed script cache (NOSCRIPT) reloads and retries once. Returns the reply.
std::optional<std::string> run_lease_call(RedisConn& conn, const ScriptCall& call, const char* what) {
    for (int attempt = 0; attempt < 2; ++attempt) {
        const std::vector<CallOutcome> out = conn.pipeline({call});
        if (out.size() != 1)
            throw std::logic_error(std::string("RedisConn::pipeline returned the wrong number of outcomes for lease ") + what);
        if (const auto* f = std::get_if<ScriptFail>(&out.front())) {
            if (f->kind == FailKind::NoScript && attempt == 0) {
                conn.load_scripts();
                continue;
            }
            if (f->kind == FailKind::Fatal) throw std::runtime_error(std::string("lease ") + what + " failed: " + f->message);
            throw ConnError(std::string("lease ") + what + ": " + f->message);
        }
        return std::get<ScriptOk>(out.front()).value;
    }
    throw ConnError(std::string("lease ") + what + ": scripts still missing after a reload");
}

std::string random_nonce() {
    std::random_device rd;
    char buf[33];
    std::snprintf(buf, sizeof(buf), "%08x%08x%08x%08x", rd(), rd(), rd(), rd());
    return buf;
}

}  // namespace

/// @brief Attempts single non-blocking lease acquisition via lease.lua.
std::optional<std::string> try_acquire_lease(RedisConn& conn, const Keys& keys, const std::string& host_id,
                                             const std::string& nonce, std::uint32_t ttl_ms) {
    return run_lease_call(conn, lease_acquire_call(keys, host_id, nonce, ttl_ms), "acquire");
}

/// @brief Explicitly surrenders leader lease token to allow standby to take over immediately.
bool release_lease(RedisConn& conn, const Keys& keys, const std::string& token) {
    const auto reply = run_lease_call(conn, lease_release_call(keys, token), "release");
    if (reply == "OK") return true;
    if (reply == "NOOP") return false;
    throw std::runtime_error("lease release: unexpected reply '" + reply.value_or("<nil>") + "'");
}

/// @brief Loops in standby state until leadership lease is granted or shutdown is signaled.
std::optional<std::string> acquire_lease_blocking(RedisConn& conn, const Keys& keys, const AcquireOptions& opts) {
    if (opts.host_id.empty()) throw std::invalid_argument("AcquireOptions: host_id must not be empty");
    if (opts.ttl_ms == 0) throw std::invalid_argument("AcquireOptions: ttl_ms must be > 0");

    const auto sleep = opts.sleep ? opts.sleep : [](std::chrono::milliseconds d) { std::this_thread::sleep_for(d); };
    std::mt19937_64 rng{std::random_device{}()};
    std::uint32_t failures = 0;
    bool announced_standby = false;

    while (!(opts.stop && opts.stop())) {
        try {
            // Generate random 128-bit hex nonce for lease fencing token generation.
            const std::string nonce = opts.nonce ? opts.nonce() : random_nonce();
            if (auto token = try_acquire_lease(conn, keys, opts.host_id, nonce, opts.ttl_ms)) {
                // Successfully acquired lease; host transitions from standby to active leader.
                core::log::info("lease", "lease acquired", {{"host", opts.host_id}});
                return token;
            }
            failures = 0;
            if (!announced_standby) {
                // Another active leader currently holds lease; enter polling standby loop.
                core::log::info("lease", "another host holds the lease: standby", {{"host", opts.host_id}});
                announced_standby = true;
            }
            sleep(opts.poll);
        } catch (const ConnError& e) {
            core::log::warn("lease", "lease acquire failed, redis unreachable", {{"error", e.what()}});
            // Exponential backoff with random jitter on Redis network disconnects.
            const auto ceiling = std::min<std::chrono::milliseconds::rep>(
                opts.probe_max.count(), opts.probe_min.count() << std::min<std::uint32_t>(failures++, 20));
            sleep(std::chrono::milliseconds(ceiling > 0 ? static_cast<std::chrono::milliseconds::rep>(rng() % (ceiling + 1)) : 0));
        }
    }
    return std::nullopt;
}

std::string lease_disabled_token() {
    core::log::warn("lease", "lease disabled: no acquire, no renew, writes are not fenced. NEVER run two hosts in this mode");
    return kNoFence;
}

}  // namespace evml::sink
