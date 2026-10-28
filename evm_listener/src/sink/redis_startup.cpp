#include "sink/redis_startup.hpp"

#include <netdb.h>
#include <sys/socket.h>

#include <algorithm>
#include <charconv>
#include <random>
#include <thread>

#include <sw/redis++/redis++.h>

namespace evml::sink {

namespace {

bool starts_with(const std::string& s, const char* prefix) { return s.rfind(prefix, 0) == 0; }

// "ipv6" / "ipv4" / "unresolved": the first address getaddrinfo returns, which is the one a plain connect tries first
// (Railway's private network `*.railway.internal` is AAAA only).
std::string address_family(const std::string& host, int port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0 || !res) return "unresolved";
    const std::string family = res->ai_family == AF_INET6 ? "ipv6" : res->ai_family == AF_INET ? "ipv4" : "other";
    freeaddrinfo(res);
    return family;
}

bool is_auth_error(const std::string& msg) {
    return starts_with(msg, "WRONGPASS") || starts_with(msg, "NOAUTH") || starts_with(msg, "ERR AUTH") ||
           starts_with(msg, "ERR Client sent AUTH") || starts_with(msg, "ERR invalid password");
}

bool is_transient_reply(const std::string& msg) {
    return starts_with(msg, "LOADING") || starts_with(msg, "BUSY") || starts_with(msg, "MASTERDOWN") || starts_with(msg, "TRYAGAIN");
}

// CONFIG GET <name> -> the value, or nullopt when the server refuses CONFIG (managed Redis often does).
std::optional<std::string> config_get(sw::redis::Redis& redis, const std::string& name) {
    try {
        const auto reply = redis.command<std::vector<std::string>>("CONFIG", "GET", name);
        if (reply.size() != 2 || reply[0] != name)
            throw StartupFatal("CONFIG GET " + name + " returned an unexpected reply of " + std::to_string(reply.size()) + " elements");
        return reply[1];
    } catch (const sw::redis::ReplyError& e) {
        if (is_transient_reply(e.what())) throw;  // not a denial: the wait loop retries
        return std::nullopt;
    }
}

StartupFacts gather(sw::redis::Redis& redis) {
    StartupFacts f;
    if (redis.ping() != "PONG") throw StartupFatal("PING did not answer PONG");
    f.info_server = redis.info("server");
    f.maxmemory_policy = config_get(redis, "maxmemory-policy");
    f.appendonly = config_get(redis, "appendonly");
    return f;
}

}  // namespace

/// @brief Parses semver string from Redis INFO server output.
RedisVersion parse_redis_version(std::string_view info) {
    constexpr std::string_view key = "redis_version:";
    const auto pos = info.find(key);
    if (pos == std::string_view::npos) throw StartupFatal("INFO server has no redis_version field");
    auto rest = info.substr(pos + key.size());
    rest = rest.substr(0, rest.find_first_of("\r\n"));

    RedisVersion v;
    unsigned* parts[3] = {&v.major, &v.minor, &v.patch};
    const char* p = rest.data();
    const char* end = rest.data() + rest.size();
    for (int i = 0; i < 3; ++i) {
        const auto [next, ec] = std::from_chars(p, end, *parts[i]);
        if (ec != std::errc{}) throw StartupFatal("cannot parse redis_version '" + std::string(rest) + "'");
        p = next;
        if (i < 2) {
            if (p == end || *p != '.') {
                if (i == 1 && p == end) break;  // "7.0": a missing patch is read as 0
                throw StartupFatal("cannot parse redis_version '" + std::string(rest) + "'");
            }
            ++p;
        }
    }
    return v;
}

/// @brief Validates version >= 7.0 (for #!lua shebang) and noeviction policy.
StartupReport evaluate_startup(const StartupFacts& facts, bool require_noeviction) {
    StartupReport r;
    r.version = parse_redis_version(facts.info_server);
    const std::string ver = std::to_string(r.version.major) + "." + std::to_string(r.version.minor) + "." + std::to_string(r.version.patch);
    // Lua scripts use the `#!lua` shebang without allow-oom, which requires Redis >= 7.0 to reject on OOM.
    if (r.version.major < 7)
        throw StartupFatal("redis_version " + ver + " < 7.0: the embedded Lua scripts need the #!lua shebang (Redis >= 7.0)");

    r.maxmemory_policy = facts.maxmemory_policy.value_or("");
    r.appendonly = facts.appendonly.value_or("");
    if (!facts.maxmemory_policy) {
        r.config_denied = true;
        r.warnings.push_back("CONFIG GET maxmemory-policy refused: cannot verify noeviction");
    } else if (*facts.maxmemory_policy != "noeviction") {
        // Eviction enabled: Redis may silently drop stream entries or checkpoint records under memory pressure.
        const std::string what = "maxmemory-policy is '" + *facts.maxmemory_policy + "', not 'noeviction': Redis may evict our streams or checkpoint";
        if (require_noeviction) throw StartupFatal(what);
        r.warnings.push_back(what);
        r.misconfigured = true;
    }

    if (!facts.appendonly) {
        r.config_denied = true;
        r.warnings.push_back("CONFIG GET appendonly refused: cannot verify persistence");
    } else if (*facts.appendonly == "no") {
        r.misconfigured = true;
        r.warnings.push_back("appendonly is 'no': a Redis restart loses the streams and the checkpoint (a re-backfill follows)");
    }
    return r;
}

/// @brief Connects to Redis and loops until server is ready and validated.
std::optional<StartupReport> await_redis_ready(const std::string& redis_url, const StartupOptions& opts) {
    sw::redis::ConnectionOptions copts;
    try {
        copts = sw::redis::Uri(redis_url).connection_options();
    } catch (const sw::redis::Error& e) {
        throw StartupFatal(std::string("invalid Redis URL: ") + e.what());  // the URL itself is never echoed (credentials)
    }
    copts.connect_timeout = opts.io_timeout;
    copts.socket_timeout = opts.io_timeout;
    sw::redis::ConnectionPoolOptions pool;
    pool.size = 1;

    const auto sleep = opts.sleep ? opts.sleep : [](std::chrono::milliseconds d) { std::this_thread::sleep_for(d); };
    std::mt19937_64 rng{std::random_device{}()};
    const auto t0 = std::chrono::steady_clock::now();

    for (std::uint32_t attempt = 0;; ++attempt) {
        if (opts.stop && opts.stop()) return std::nullopt;

        std::string error;
        try {
            sw::redis::Redis redis(copts, pool);
            // Verify server response and evaluate version/config constraints.
            return evaluate_startup(gather(redis), opts.require_noeviction);
        } catch (const StartupFatal&) {
            throw;
        } catch (const sw::redis::ReplyError& e) {
            // Authentication errors are non-recoverable: fail fast with StartupFatal.
            if (is_auth_error(e.what())) throw StartupFatal(std::string("AUTH rejected: ") + e.what());
            if (!is_transient_reply(e.what())) throw StartupFatal(std::string("unexpected Redis error at startup: ") + e.what());
            error = e.what();
        } catch (const sw::redis::Error& e) {  // IoError, TimeoutError, ClosedError, OomError, ProtoError: Redis is not ready
            error = e.what();
        }

        if (opts.on_waiting) {
            // Diagnostic callback reporting network address family (IPv4 vs IPv6) and elapsed wait time.
            opts.on_waiting(error, address_family(copts.host, copts.port),
                            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0));
        }
        // Exponential backoff with random full jitter while awaiting Redis startup.
        const auto ceiling = std::min<std::chrono::milliseconds::rep>(
            opts.probe_max.count(), opts.probe_min.count() << std::min<std::uint32_t>(attempt, 20));
        sleep(std::chrono::milliseconds(ceiling > 0 ? static_cast<std::chrono::milliseconds::rep>(rng() % (ceiling + 1)) : 0));
    }
}

}  // namespace evml::sink
