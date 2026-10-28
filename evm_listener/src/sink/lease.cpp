#include "sink/lease.hpp"

#include <algorithm>
#include <cstdio>
#include <random>
#include <stdexcept>
#include <thread>

#include "core/log.hpp"

namespace evml::sink {

std::optional<std::uint64_t> lease_token_epoch(const std::string& token) {
    const auto bar = token.find('|');
    if (bar == std::string::npos || bar == 0) return std::nullopt;
    std::uint64_t n = 0;
    for (std::size_t i = 0; i < bar; ++i) {
        if (token[i] < '0' || token[i] > '9') return std::nullopt;
        if (n > (UINT64_MAX - 9) / 10) return std::nullopt;  // would overflow
        n = n * 10 + static_cast<std::uint64_t>(token[i] - '0');
    }
    return n;
}

std::optional<std::string> lease_token_host(const std::string& token) {
    if (!lease_token_epoch(token)) return std::nullopt;
    const auto first = token.find('|');
    const auto last = token.rfind('|');
    if (last == first) return std::nullopt;  // "n|host" has no nonce
    return token.substr(first + 1, last - first - 1);  // a host id may itself contain '|': only the outer bars split
}

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
std::optional<LeaseGrant> try_acquire_lease(RedisConn& conn, const Keys& keys, const std::string& host_id,
                                            const std::string& nonce, std::uint32_t ttl_ms) {
    const auto reply = run_lease_call(conn, lease_acquire_call(keys, host_id, nonce, ttl_ms), "acquire");
    if (!reply) return std::nullopt;
    const auto nl = reply->find('\n');
    if (nl == std::string::npos) throw std::runtime_error("lease acquire: unexpected reply missing newline");
    LeaseGrant g;
    g.token = reply->substr(0, nl);
    g.epoch_n = lease_token_epoch(g.token);
    if (!g.epoch_n) throw std::runtime_error("lease acquire: script returned a malformed token");
    const std::string prev = reply->substr(nl + 1);
    if (!prev.empty()) {
        if (const auto prev_host = lease_token_host(prev)) {
            if (*prev_host != host_id) g.old_host = *prev_host;
        } else {
            core::log::warn("lease", "epoch key is not a lease token: Leader Changed is skipped", {});
        }
    }
    return g;
}

/// @brief Explicitly surrenders leader lease token to allow standby to take over immediately.
bool release_lease(RedisConn& conn, const Keys& keys, const std::string& token) {
    const auto reply = run_lease_call(conn, lease_release_call(keys, token), "release");
    if (reply == "OK") return true;
    if (reply == "NOOP") return false;
    throw std::runtime_error("lease release: unexpected reply '" + reply.value_or("<nil>") + "'");
}

/// @brief Loops in standby state until leadership lease is granted or shutdown is signaled.
std::optional<LeaseGrant> acquire_lease_blocking(RedisConn& conn, const Keys& keys, const AcquireOptions& opts) {
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
            if (auto grant = try_acquire_lease(conn, keys, opts.host_id, nonce, opts.ttl_ms)) {
                // Successfully acquired lease; host transitions from standby to active leader.
                core::log::info("lease", "lease acquired", {{"host", opts.host_id}});
                return grant;
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
