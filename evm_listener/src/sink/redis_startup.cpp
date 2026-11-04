#include "sink/redis_startup.hpp"

#include <netdb.h>
#include <sys/socket.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <iterator>
#include <random>
#include <set>
#include <thread>
#include <utility>

#include <sw/redis++/redis++.h>

#include "sink/keys.hpp"

namespace evml::sink {

namespace {

bool starts_with(std::string_view s, std::string_view prefix) {
    return s.size() >= prefix.size() && s.substr(0, prefix.size()) == prefix;
}

std::string key_prefix_of(std::string_view key) {
    const auto colon = key.find(':');
    const std::string_view prefix = (colon == std::string_view::npos) ? key : key.substr(0, colon);
    return prefix.empty() ? std::string("<empty>") : std::string(prefix);
}

std::string join_csv(const std::vector<std::string>& items) {
    std::string out;
    for (std::size_t i = 0; i < items.size(); ++i) {
        if (i > 0) out += ",";
        out += items[i];
    }
    return out;
}

bool is_valid_deploy_env(std::string_view env) {
    return env == "local" || env == "staging" || env == "production";
}

struct ParsedRedisUrl {
    std::string scheme;
    std::string host;
    std::string_view path;
};

std::optional<ParsedRedisUrl> parse_redis_url_parts(std::string_view redis_url) {
    const auto scheme_end = redis_url.find("://");
    if (scheme_end == std::string_view::npos || scheme_end == 0) return std::nullopt;

    std::string scheme;
    scheme.reserve(scheme_end);
    for (char c : redis_url.substr(0, scheme_end)) {
        scheme.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }
    if (scheme != "redis" && scheme != "rediss") return std::nullopt;

    const std::size_t auth_start = scheme_end + 3;
    const auto suffix_start = redis_url.find_first_of("/?#", auth_start);
    const std::string_view authority = (suffix_start == std::string_view::npos)
                                           ? redis_url.substr(auth_start)
                                           : redis_url.substr(auth_start, suffix_start - auth_start);
    if (authority.empty()) return std::nullopt;

    const auto at_pos = authority.rfind('@');
    const std::string_view hostport = (at_pos == std::string_view::npos) ? authority : authority.substr(at_pos + 1);
    if (hostport.empty()) return std::nullopt;

    std::string_view raw_host;
    if (hostport.front() == '[') {
        const auto close = hostport.find(']');
        if (close == std::string_view::npos || close == 1) return std::nullopt;
        raw_host = hostport.substr(1, close - 1);
    } else if (hostport == "::1" || starts_with(hostport, "::1:")) {
        raw_host = "::1";
    } else {
        const auto colon = hostport.find(':');
        raw_host = (colon == std::string_view::npos) ? hostport : hostport.substr(0, colon);
    }
    if (raw_host.empty()) return std::nullopt;

    std::string host;
    host.reserve(raw_host.size());
    for (char c : raw_host) {
        const auto uc = static_cast<unsigned char>(c);
        if (uc <= ' ' || uc == 0x7f) return std::nullopt;
        host.push_back(static_cast<char>(std::tolower(uc)));
    }

    std::string_view path;
    if (suffix_start != std::string_view::npos && redis_url[suffix_start] == '/') {
        const auto q_or_f = redis_url.find_first_of("?#", suffix_start + 1);
        path = (q_or_f == std::string_view::npos)
                   ? redis_url.substr(suffix_start + 1)
                   : redis_url.substr(suffix_start + 1, q_or_f - (suffix_start + 1));
    }

    return ParsedRedisUrl{std::move(scheme), std::move(host), path};
}

bool is_local_redis_host(std::string_view host) {
    return host == "redis" || host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "toxiproxy";
}

std::map<int, std::uint64_t> parse_info_keyspace(std::string_view info_keyspace) {
    std::map<int, std::uint64_t> out;
    std::size_t pos = 0;
    while (pos < info_keyspace.size()) {
        const auto nl = info_keyspace.find('\n', pos);
        std::string_view line =
            nl == std::string_view::npos ? info_keyspace.substr(pos) : info_keyspace.substr(pos, nl - pos);
        pos = nl == std::string_view::npos ? info_keyspace.size() : nl + 1;
        if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
        if (line.empty() || line.front() == '#' || !starts_with(line, "db")) continue;

        const auto colon = line.find(':');
        if (colon == std::string_view::npos || colon <= 2) {
            throw StartupFatal("INFO keyspace has malformed db line '" + std::string(line) + "'");
        }
        int db_num = -1;
        const auto db_sv = line.substr(2, colon - 2);
        const auto [db_end, db_ec] = std::from_chars(db_sv.data(), db_sv.data() + db_sv.size(), db_num);
        if (db_ec != std::errc{} || db_end != db_sv.data() + db_sv.size() || db_num < 0) {
            throw StartupFatal("INFO keyspace has invalid db index in '" + std::string(line) + "'");
        }

        std::string_view rest = line.substr(colon + 1);
        std::optional<std::uint64_t> keys_val;
        std::size_t fpos = 0;
        while (fpos < rest.size()) {
            const auto comma = rest.find(',', fpos);
            const std::string_view field =
                comma == std::string_view::npos ? rest.substr(fpos) : rest.substr(fpos, comma - fpos);
            fpos = comma == std::string_view::npos ? rest.size() : comma + 1;
            if (starts_with(field, "keys=")) {
                const std::string_view val_sv = field.substr(5);
                std::uint64_t k = 0;
                const auto [k_end, k_ec] = std::from_chars(val_sv.data(), val_sv.data() + val_sv.size(), k);
                if (k_ec != std::errc{} || k_end != val_sv.data() + val_sv.size()) {
                    throw StartupFatal("INFO keyspace has invalid keys count in '" + std::string(line) + "'");
                }
                keys_val = k;
                break;
            }
        }
        if (!keys_val) {
            throw StartupFatal("INFO keyspace line has no keys= field: '" + std::string(line) + "'");
        }
        out[db_num] = *keys_val;
    }
    return out;
}

std::vector<std::string> parse_foreign_client_names(std::string_view client_list) {
    std::vector<std::string> names;
    std::size_t pos = 0;
    while (pos < client_list.size()) {
        const auto nl = client_list.find('\n', pos);
        std::string_view line =
            nl == std::string_view::npos ? client_list.substr(pos) : client_list.substr(pos, nl - pos);
        pos = nl == std::string_view::npos ? client_list.size() : nl + 1;
        if (!line.empty() && line.back() == '\r') line.remove_suffix(1);

        std::size_t tpos = 0;
        while (tpos < line.size()) {
            while (tpos < line.size() && line[tpos] == ' ') ++tpos;
            if (tpos >= line.size()) break;
            const auto space = line.find(' ', tpos);
            const std::string_view token =
                space == std::string_view::npos ? line.substr(tpos) : line.substr(tpos, space - tpos);
            tpos = space == std::string_view::npos ? line.size() : space + 1;
            if (starts_with(token, "name=")) {
                const std::string_view name = token.substr(5);
                if (!name.empty() && !starts_with(name, "evml:")) {
                    const std::string name_str(name);
                    if (std::find(names.begin(), names.end(), name_str) == names.end()) {
                        names.push_back(name_str);
                    }
                }
                break;
            }
        }
    }
    return names;
}

bool has_foreign_db_keys(const std::map<int, std::uint64_t>& keyspace_db_keys, int expected_db) {
    for (const auto& [db_num, keys_count] : keyspace_db_keys) {
        if (db_num != expected_db && keys_count > 0) {
            return true;
        }
    }
    return false;
}

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

std::string format_tenant_marker(std::string_view deploy_env) {
    return "evm-listener|" + std::string(deploy_env);
}

std::string sanitize_client_name(std::string_view raw) {
    std::string out(raw);
    for (char& c : out) {
        const auto uc = static_cast<unsigned char>(c);
        if (uc < 0x21 || uc > 0x7e) c = '_';
    }
    return out;
}

std::string resolve_deploy_env(std::string_view raw_deploy_env, std::string_view redis_url) {
    if (raw_deploy_env.empty()) {
        if (is_allowed_test_redis_host(redis_url)) {
            return "local";
        }
        throw StartupFatal(
            "EVML_DEPLOY_ENV must be set (local|staging|production) when REDIS_URL is not the local dev default");
    }
    if (!is_valid_deploy_env(raw_deploy_env)) {
        throw StartupFatal("invalid EVML_DEPLOY_ENV '" + std::string(raw_deploy_env) +
                           "': must be local, staging, or production");
    }
    return std::string(raw_deploy_env);
}

int parse_redis_db(std::string_view redis_url) {
    const auto parsed = parse_redis_url_parts(redis_url);
    if (!parsed) {
        throw StartupFatal("invalid Redis URL");
    }
    if (parsed->path.empty()) {
        return 0;
    }
    int db = -1;
    const auto [end, ec] = std::from_chars(parsed->path.data(), parsed->path.data() + parsed->path.size(), db);
    if (ec != std::errc{} || end != parsed->path.data() + parsed->path.size() || db < 0) {
        throw StartupFatal("invalid Redis database index in URL: " + std::string(parsed->path));
    }
    return db;
}

bool is_allowed_test_redis_host(std::string_view redis_url, std::string_view allow_destructive_env) {
    if (allow_destructive_env == "1") {
        return true;
    }
    const auto parsed = parse_redis_url_parts(redis_url);
    if (!parsed) {
        return false;
    }
    return is_local_redis_host(parsed->host);
}

TenancyFacts gather_runtime_tenancy_facts(sw::redis::Redis& redis) {
    TenancyFacts f;
    f.keyspace_db_keys = parse_info_keyspace(redis.info("keyspace"));

    const auto scan_t0 = std::chrono::steady_clock::now();
    sw::redis::Cursor cursor = 0;
    std::set<std::string> unique_prefixes;
    for (int iter = 0; iter < 10; ++iter) {
        std::vector<std::string> batch;
        cursor = redis.scan(cursor, 1000, std::back_inserter(batch));
        for (const auto& k : batch) {
            if (!starts_with(k, "evml:")) {
                if (unique_prefixes.size() < 5) {
                    unique_prefixes.insert(key_prefix_of(k));
                }
            }
        }
        if (cursor == 0) {
            break;
        }
        const auto elapsed = std::chrono::steady_clock::now() - scan_t0;
        if (iter + 1 >= 10 || elapsed >= std::chrono::milliseconds(200) || unique_prefixes.size() >= 5) {
            f.scan_budget_exhausted = true;
            break;
        }
    }
    f.foreign_key_prefixes.assign(unique_prefixes.begin(), unique_prefixes.end());

    try {
        const auto client_list = redis.command<std::string>("CLIENT", "LIST");
        f.foreign_client_names = parse_foreign_client_names(client_list);
    } catch (const sw::redis::ReplyError& e) {
        if (is_transient_reply(e.what())) throw;
        f.client_list_denied = true;
    }
    return f;
}

TenancyReport evaluate_tenancy(const TenancyFacts& facts, std::string_view expected_env, int expected_db) {
    if (!is_valid_deploy_env(expected_env)) {
        throw StartupFatal("invalid expected_env '" + std::string(expected_env) +
                           "': must be local, staging, or production");
    }
    if (expected_db < 0) {
        throw StartupFatal("invalid expected_db: " + std::to_string(expected_db));
    }

    if (!facts.foreign_key_prefixes.empty()) {
        std::vector<std::string> unique_prefixes;
        for (const auto& raw : facts.foreign_key_prefixes) {
            const std::string p = key_prefix_of(raw);
            if (unique_prefixes.size() < 5 &&
                std::find(unique_prefixes.begin(), unique_prefixes.end(), p) == unique_prefixes.end()) {
                unique_prefixes.push_back(p);
            }
        }
        const std::string prefixes = join_csv(unique_prefixes);
        throw StartupFatal("REDIS_TENANCY_VIOLATION: foreign keys found in Redis (prefixes: " + prefixes +
                               "): evm_listener requires a dedicated Redis instance",
                           prefixes, "", "");
    }

    std::vector<std::string> foreign_dbs;
    for (const auto& [db_num, keys_count] : facts.keyspace_db_keys) {
        if (db_num != expected_db && keys_count > 0) {
            foreign_dbs.push_back("db" + std::to_string(db_num));
        }
    }
    if (!foreign_dbs.empty()) {
        const std::string db_str = join_csv(foreign_dbs);
        throw StartupFatal("REDIS_TENANCY_VIOLATION: non-target Redis database has keys > 0 (" + db_str +
                               ", expected db" + std::to_string(expected_db) + "): sharing via SELECT is forbidden",
                           "", db_str, "");
    }

    const std::string expected_marker = format_tenant_marker(expected_env);
    if (!facts.tenant_marker.has_value() || *facts.tenant_marker != expected_marker) {
        const std::string actual = facts.tenant_marker.value_or("");
        throw StartupFatal("REDIS_TENANCY_VIOLATION: tenant marker mismatch (stored '" + actual + "', expected '" +
                               expected_marker + "')",
                           "", "", actual);
    }

    TenancyReport r;
    if (facts.client_list_denied) {
        r.warnings.push_back("CLIENT LIST denied: cannot verify connected client names");
    }
    if (!facts.foreign_client_names.empty()) {
        r.warnings.push_back("foreign named clients connected to Redis: " + join_csv(facts.foreign_client_names));
    }
    if (facts.scan_budget_exhausted) {
        r.warnings.push_back("SCAN budget exhausted before full pass (partial foreign-key coverage)");
    }
    return r;
}

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
    const int expected_db = parse_redis_db(redis_url);
    if (!is_valid_deploy_env(opts.deploy_env)) {
        throw StartupFatal("invalid EVML_DEPLOY_ENV '" + opts.deploy_env + "': must be local, staging, or production");
    }
    sw::redis::ConnectionOptions copts;
    try {
        copts = sw::redis::Uri(redis_url).connection_options();
    } catch (const sw::redis::Error& e) {
        throw StartupFatal(std::string("invalid Redis URL: ") + e.what());  // the URL itself is never echoed (credentials)
    }
    copts.connect_timeout = opts.io_timeout;
    copts.socket_timeout = opts.io_timeout;
    copts.name =
        sanitize_client_name("evml:" + (opts.host_id.empty() ? std::string("startup") : opts.host_id) + ":startup");
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
            auto rep = evaluate_startup(gather(redis), opts.require_noeviction);
            rep.expected_db = expected_db;
            TenancyFacts tf = gather_runtime_tenancy_facts(redis);
            const std::string tenant_key = Keys(opts.shard).tenant();
            if (tf.foreign_key_prefixes.empty() && !has_foreign_db_keys(tf.keyspace_db_keys, expected_db)) {
                redis.command<sw::redis::OptionalString>("SET", tenant_key, format_tenant_marker(opts.deploy_env), "NX");
            }
            if (const auto stored = redis.get(tenant_key)) {
                tf.tenant_marker = *stored;
            }
            rep.tenancy = evaluate_tenancy(tf, opts.deploy_env, expected_db);
            rep.warnings.insert(rep.warnings.end(), rep.tenancy.warnings.begin(), rep.tenancy.warnings.end());
            rep.address_family = address_family(copts.host, copts.port);
            return rep;
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
