#include "sink/lease.hpp"

#include <algorithm>
#include <cstdio>
#include <random>
#include <stdexcept>
#include <thread>

#include "core/log.hpp"

namespace evml::sink {

std::optional<std::uint64_t> lease_token_epoch(std::string_view token) noexcept {
    const auto bar = token.find('|');
    if (bar == std::string_view::npos || bar == 0) return std::nullopt;
    std::uint64_t n = 0;
    for (std::size_t i = 0; i < bar; ++i) {
        if (token[i] < '0' || token[i] > '9') return std::nullopt;
        if (n > (UINT64_MAX - 9) / 10) return std::nullopt;  // would overflow
        n = n * 10 + static_cast<std::uint64_t>(token[i] - '0');
    }
    return n;
}

std::optional<std::string> lease_token_host(std::string_view token) {
    if (!lease_token_epoch(token)) return std::nullopt;
    const auto first = token.find('|');
    const auto last = token.rfind('|');
    if (last == first) return std::nullopt;  // "n|host" has no nonce
    return std::string(token.substr(first + 1, last - first - 1));  // a host id may itself contain '|': only the outer bars split
}

namespace {

// Runs one lease script call; a flushed script cache (NOSCRIPT) reloads and retries once. Returns the reply.
core::Result<std::optional<std::string>, ConnError> run_lease_call(RedisConn& conn, const ScriptCall& call,
                                                                   const char* what) {
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto out_res = conn.try_pipeline({call});
        if (!out_res) return out_res.error();
        const std::vector<CallOutcome>& out = *out_res;
        if (out.size() != 1)
            return ConnError(std::string("RedisConn::pipeline returned the wrong number of outcomes for lease ") + what);
        if (const auto* f = std::get_if<ScriptFail>(&out.front())) {
            if (f->kind == FailKind::NoScript && attempt == 0) {
                if (auto lr = conn.try_load_scripts(); !lr) return lr.error();
                continue;
            }
            return ConnError(std::string("lease ") + what + ": " + f->message);
        }
        return std::get<ScriptOk>(out.front()).value;
    }
    return ConnError(std::string("lease ") + what + ": scripts still missing after a reload");
}

std::string random_nonce() {
    std::random_device rd;
    char buf[33];
    std::snprintf(buf, sizeof(buf), "%08x%08x%08x%08x", rd(), rd(), rd(), rd());
    return buf;
}

}  // namespace

core::Result<std::optional<LeaseGrant>, ConnError> try_acquire_lease_result(RedisConn& conn, const Keys& keys,
                                                                            std::string_view host_id,
                                                                            std::string_view nonce,
                                                                            std::uint32_t ttl_ms) {
    const auto reply_res = run_lease_call(conn, lease_acquire_call(keys, host_id, nonce, ttl_ms), "acquire");
    if (!reply_res) return reply_res.error();
    const auto& reply = *reply_res;
    if (!reply) return std::optional<LeaseGrant>{};
    const auto nl = reply->find('\n');
    if (nl == std::string::npos) return ConnError("lease acquire: unexpected reply missing newline");
    LeaseGrant g;
    g.token = reply->substr(0, nl);
    g.epoch_n = lease_token_epoch(g.token);
    if (!g.epoch_n) return ConnError("lease acquire: script returned a malformed token");
    const std::string prev = reply->substr(nl + 1);
    if (!prev.empty()) {
        if (const auto prev_host = lease_token_host(prev)) {
            if (*prev_host != host_id) g.old_host = *prev_host;
        } else {
            core::log::warn("lease", "epoch key is not a lease token: Leader Changed is skipped", {});
        }
    }
    return std::optional<LeaseGrant>{std::move(g)};
}

/// @brief Attempts single non-blocking lease acquisition via lease.lua.
std::optional<LeaseGrant> try_acquire_lease(RedisConn& conn, const Keys& keys, std::string_view host_id,
                                            std::string_view nonce, std::uint32_t ttl_ms) {
    auto res = try_acquire_lease_result(conn, keys, host_id, nonce, ttl_ms);
    if (!res) throw res.error();
    return std::move(*res);
}

/// @brief Explicitly surrenders leader lease token without throwing on ConnError.
core::Result<bool, ConnError> try_release_lease(RedisConn& conn, const Keys& keys, std::string_view token) {
    const auto reply_res = run_lease_call(conn, lease_release_call(keys, token), "release");
    if (!reply_res) return reply_res.error();
    const auto& reply = *reply_res;
    if (reply == "OK") return true;
    if (reply == "NOOP") return false;
    return ConnError("lease release: unexpected reply '" + reply.value_or("<nil>") + "'");
}

/// @brief Explicitly surrenders leader lease token to allow standby to take over immediately.
bool release_lease(RedisConn& conn, const Keys& keys, std::string_view token) {
    auto res = try_release_lease(conn, keys, token);
    if (!res) throw res.error();
    return *res;
}

/// @brief Loops in standby state until leadership lease is granted or shutdown is signaled.
std::optional<LeaseGrant> acquire_lease_blocking(RedisConn& conn, const Keys& keys, const AcquireOptions& opts) {
    if (opts.host_id.empty()) throw std::invalid_argument("AcquireOptions: host_id must not be empty");
    if (opts.ttl_ms == 0) throw std::invalid_argument("AcquireOptions: ttl_ms must be > 0");

    const auto sleep = opts.sleep ? opts.sleep : [](std::chrono::milliseconds d) { std::this_thread::sleep_for(d); };
    std::mt19937_64 rng{std::random_device{}()};
    std::uint32_t failures = 0;
    bool announced_standby = false;
    std::optional<std::chrono::steady_clock::time_point> wait_since;

    while (!(opts.stop && opts.stop())) {
        // Generate random 128-bit hex nonce for lease fencing token generation.
        const std::string nonce = opts.nonce ? opts.nonce() : random_nonce();
        auto grant_res = try_acquire_lease_result(conn, keys, opts.host_id, nonce, opts.ttl_ms);
        if (!grant_res) {
            const auto& e = grant_res.error();
            const auto now = std::chrono::steady_clock::now();
            if (!wait_since) wait_since = now;
            if (opts.on_waiting)
                opts.on_waiting(e.what(), std::chrono::duration_cast<std::chrono::milliseconds>(now - *wait_since));
            core::log::warn("lease", "lease acquire failed, redis unreachable", {{"error", e.what()}});
            // Exponential backoff with random jitter on Redis network disconnects.
            const auto ceiling = std::min<std::chrono::milliseconds::rep>(
                opts.probe_max.count(), opts.probe_min.count() << std::min<std::uint32_t>(failures++, 20));
            sleep(std::chrono::milliseconds(
                ceiling > 0 ? static_cast<std::chrono::milliseconds::rep>(rng() % static_cast<std::uint64_t>(ceiling + 1)) : 0));
            continue;
        }
        if (*grant_res) {
            if (opts.on_reachable) opts.on_reachable();
            // Successfully acquired lease; host transitions from standby to active leader.
            core::log::info("lease", "lease acquired", {{"host", opts.host_id}});
            return std::move(**grant_res);
        }
        failures = 0;
        wait_since.reset();
        if (opts.on_reachable) opts.on_reachable();
        if (!announced_standby) {
            // Another active leader currently holds lease; enter polling standby loop.
            core::log::info("lease", "another host holds the lease: standby", {{"host", opts.host_id}});
            announced_standby = true;
        }
        sleep(opts.poll);
    }
    return std::nullopt;
}

std::string lease_disabled_token() {
    core::log::warn("lease", "lease disabled: no acquire, no renew, writes are not fenced. NEVER run two hosts in this mode");
    return kNoFence;
}

}  // namespace evml::sink
