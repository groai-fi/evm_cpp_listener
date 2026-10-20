#include "core/config.hpp"

#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <limits>
#include <set>
#include <sstream>

#include <toml++/toml.hpp>

#include "core/env.hpp"
#include "core/error.hpp"
#include "core/keccak.hpp"

namespace evml::core {

const char* to_string(Finality f) {
    switch (f) {
        case Finality::Safe: return "safe";
        case Finality::Finalized: return "finalized";
        case Finality::Depth: return "depth";
    }
    return "?";
}

const char* to_string(StreamKind s) { return s == StreamKind::Events ? "events" : "registry"; }

namespace {

// Typed, strict accessor over one TOML table. Rejects unknown keys (a typo must not silently become a default).
class Section {
public:
    Section(const toml::table& t, const std::string& origin, std::string path,
            std::initializer_list<std::string_view> allowed)
        : t_(t), origin_(origin), path_(std::move(path)) {
        for (auto&& [k, v] : t_) {
            const std::string_view key = k.str();
            if (std::find(allowed.begin(), allowed.end(), key) == allowed.end()) {
                fail(std::string(key), "unknown key");
            }
        }
    }

    [[noreturn]] void fail(const std::string& key, const std::string& what) const {
        throw ConfigError(origin_ + ": " + path_ + (key.empty() ? "" : "." + key) + ": " + what);
    }

    bool has(std::string_view k) const { return t_.contains(k); }
    const toml::node* node(std::string_view k) const { return t_.get(k); }

    template <class T>
    T req(std::string_view k, const char* type_name) const {
        const toml::node* n = t_.get(k);
        if (!n) fail(std::string(k), "missing required key");
        auto v = n->value<T>();
        if (!v) fail(std::string(k), std::string("expected ") + type_name);
        return *v;
    }
    template <class T>
    T opt(std::string_view k, T def, const char* type_name) const {
        return has(k) ? req<T>(k, type_name) : def;
    }

    std::string str(std::string_view k) const { return req<std::string>(k, "string"); }
    std::string opt_str(std::string_view k, std::string def) const { return opt<std::string>(k, std::move(def), "string"); }
    bool boolean(std::string_view k) const { return req<bool>(k, "boolean"); }

    template <class U>
    U uint(std::string_view k, std::uint64_t min = 0) const {
        return check_uint<U>(k, req<std::int64_t>(k, "integer"), min);
    }
    template <class U>
    U opt_uint(std::string_view k, U def, std::uint64_t min = 0) const {
        return has(k) ? uint<U>(k, min) : def;
    }

    std::vector<std::string> strings(std::string_view k) const {
        const toml::node* n = t_.get(k);
        if (!n) fail(std::string(k), "missing required key");
        const toml::array* a = n->as_array();
        if (!a) fail(std::string(k), "expected array of strings");
        std::vector<std::string> out;
        for (const auto& e : *a) {
            auto s = e.value<std::string>();
            if (!s) fail(std::string(k), "expected array of strings");
            out.push_back(*s);
        }
        return out;
    }

private:
    template <class U>
    U check_uint(std::string_view k, std::int64_t v, std::uint64_t min) const {
        if (v < 0 || static_cast<std::uint64_t>(v) < min || static_cast<std::uint64_t>(v) > std::numeric_limits<U>::max()) {
            fail(std::string(k), "out of range (min " + std::to_string(min) + ", got " + std::to_string(v) + ")");
        }
        return static_cast<U>(v);
    }

    const toml::table& t_;
    const std::string& origin_;
    std::string path_;
};

const toml::table& table_of(const toml::table& root, const std::string& origin, std::string_view key) {
    const toml::node* n = root.get(key);
    if (!n) throw ConfigError(origin + ": missing table [" + std::string(key) + "]");
    const toml::table* t = n->as_table();
    if (!t) throw ConfigError(origin + ": [" + std::string(key) + "] must be a table");
    return *t;
}

std::vector<const toml::table*> array_tables_of(const toml::table& root, const std::string& origin,
                                                 std::string_view key) {
    const toml::node* n = root.get(key);
    if (!n) throw ConfigError(origin + ": missing array of tables [[" + std::string(key) + "]]");
    const toml::array* a = n->as_array();
    if (!a) throw ConfigError(origin + ": [[" + std::string(key) + "]] must be an array of tables");
    std::vector<const toml::table*> out;
    for (const auto& e : *a) {
        const toml::table* t = e.as_table();
        if (!t) throw ConfigError(origin + ": [[" + std::string(key) + "]] entries must be tables");
        out.push_back(t);
    }
    return out;
}

Finality parse_finality(const Section& s, std::string_view key, const std::string& value) {
    if (value == "safe") return Finality::Safe;
    if (value == "finalized") return Finality::Finalized;
    if (value == "depth") return Finality::Depth;
    s.fail(std::string(key), "must be one of safe|finalized|depth, got \"" + value + "\"");
}

bool starts_with(const std::string& s, const char* prefix) { return s.rfind(prefix, 0) == 0; }

struct Declared {
    std::string name;
    std::string ws_env;
    std::string http_env;
    std::string http_literal;
    std::uint32_t max_getlogs_range{};
};

// Public, credential-free literal endpoints only. Anything keyed must go through *_env.
void check_credential_free(const Section& s, const std::string& url) {
    const auto scheme_end = url.find("://");
    const auto path_start = scheme_end == std::string::npos ? std::string::npos : url.find_first_of("/?#", scheme_end + 3);
    const bool bad = scheme_end == std::string::npos || url.find('@') != std::string::npos ||
                     url.find('?') != std::string::npos ||
                     (path_start != std::string::npos && url.substr(path_start) != "/");
    if (bad) {
        s.fail("http_url", "literal URLs must be credential-free (scheme://host only); use http_env for keyed endpoints");
    }
}

std::string event_name(const std::string& sig) { return sig.substr(0, sig.find('(')); }

}  // namespace

const EventRoute* Config::route(const Address20& address, const Hash32& topic0) const {
    const auto it = routes.find(RouteKey{address, topic0});
    return it == routes.end() ? nullptr : &it->second;
}

LogFilter Config::filter() const {
    LogFilter f;
    std::set<std::string> seen;
    for (const auto& c : contracts) {
        f.addresses.push_back(c.address);
        for (const auto& t : c.topics) {
            if (seen.insert(t.to_hex()).second) f.topic0s.push_back(t);
        }
    }
    return f;
}

LogFilter Config::filter(Finality only) const {
    LogFilter f;
    std::set<std::string> seen;
    for (const auto& c : contracts) {
        if (c.finality != only) continue;
        f.addresses.push_back(c.address);
        for (const auto& t : c.topics) {
            if (seen.insert(t.to_hex()).second) f.topic0s.push_back(t);
        }
    }
    return f;
}

Config Config::parse(std::string_view toml_text, const std::string& origin) {
    return parse(toml_text, origin, [](const std::string& k) { return env::get(k); });
}

Config Config::load_file(const std::filesystem::path& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) throw ConfigError("cannot read config file: " + path.string());
    std::ostringstream ss;
    ss << f.rdbuf();
    return parse(ss.str(), path.string());
}

Config Config::parse(std::string_view toml_text, const std::string& origin, const EnvLookup& env_lookup) {
    toml::table root;
    try {
        root = toml::parse(toml_text, origin);
    } catch (const toml::parse_error& e) {
        throw ConfigError(origin + ": TOML syntax error: " + std::string(e.description()) + " (line " +
                          std::to_string(e.source().begin.line) + ", col " + std::to_string(e.source().begin.column) + ")");
    }
    const Section top(root, origin, "<root>",
                      {"shard", "chain", "providers", "redundancy", "lease", "contracts", "backfill", "watchdog", "sink", "ops"});
    (void)top;

    Config cfg;

    // [shard]
    {
        const Section s(table_of(root, origin, "shard"), origin, "[shard]", {"id", "host_id"});
        cfg.shard.id = s.str("id");
        const auto& id = cfg.shard.id;
        if (id.empty() || !std::all_of(id.begin(), id.end(), [](unsigned char c) {
                return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
            })) {
            s.fail("id", "must be non-empty [a-z0-9_]+ (used in the Redis hash tag)");
        }
        cfg.shard.host_id = s.opt_str("host_id", "");
        if (cfg.shard.host_id.empty()) cfg.shard.host_id = env_lookup("EVML_HOST_ID");
        if (cfg.shard.host_id.empty()) {
            char buf[256] = {};
            if (gethostname(buf, sizeof(buf) - 1) != 0 || buf[0] == '\0') {
                s.fail("host_id", "empty and neither $EVML_HOST_ID nor the hostname is available");
            }
            cfg.shard.host_id = buf;
        }
    }

    // [chain]
    {
        const Section s(table_of(root, origin, "chain"), origin, "[chain]",
                        {"name", "chain_id", "block_time_ms", "default_finality", "depth", "hash_ring_size", "start_policy",
                         "safe_poll_ms", "finalized_poll_ms"});
        auto& c = cfg.chain;
        c.name = s.str("name");
        c.chain_id = s.uint<std::uint64_t>("chain_id", 1);
        c.block_time_ms = s.uint<std::uint32_t>("block_time_ms", 1);
        c.default_finality = parse_finality(s, "default_finality", s.str("default_finality"));
        c.depth = s.opt_uint<std::uint32_t>("depth", 0);
        if (c.default_finality == Finality::Depth && c.depth == 0) s.fail("depth", "finality=depth requires depth > 0");
        c.hash_ring_size = s.uint<std::uint32_t>("hash_ring_size", 1);
        const std::string policy = s.str("start_policy");
        if (policy == "contract_start") c.start_policy = StartPolicy::ContractStart;
        else if (policy == "gate") c.start_policy = StartPolicy::Gate;
        else s.fail("start_policy", "must be contract_start|gate, got \"" + policy + "\"");
        c.safe_poll_ms = s.uint<std::uint32_t>("safe_poll_ms", 1);
        c.finalized_poll_ms = s.uint<std::uint32_t>("finalized_poll_ms", 1);
    }

    // [[providers]] -> declared
    std::vector<Declared> declared;
    {
        const auto tables = array_tables_of(root, origin, "providers");
        std::set<std::string> names;
        for (std::size_t i = 0; i < tables.size(); ++i) {
            const Section s(*tables[i], origin, "[[providers]][" + std::to_string(i) + "]",
                            {"name", "ws_env", "http_env", "http_url", "max_getlogs_range"});
            Declared d;
            d.name = s.str("name");
            if (!names.insert(d.name).second) s.fail("name", "duplicate provider \"" + d.name + "\"");
            d.ws_env = s.opt_str("ws_env", "");
            d.http_env = s.opt_str("http_env", "");
            d.http_literal = s.opt_str("http_url", "");
            if (!d.http_env.empty() && !d.http_literal.empty()) s.fail("http_url", "set either http_env or http_url, not both");
            if (!d.http_literal.empty()) check_credential_free(s, d.http_literal);
            if (d.ws_env.empty() && d.http_env.empty() && d.http_literal.empty()) {
                s.fail("name", "provider \"" + d.name + "\" has no ws_env/http_env/http_url");
            }
            d.max_getlogs_range = s.uint<std::uint32_t>("max_getlogs_range", 1);
            declared.push_back(std::move(d));
        }
    }

    // [redundancy] + provider resolution
    {
        const Section s(table_of(root, origin, "redundancy"), origin, "[redundancy]",
                        {"mode", "ws_order", "getlogs_order", "failover_after", "stale_after_blocks", "prefer_primary_after_s",
                         "quota_cooldown_s"});
        auto& r = cfg.redundancy;
        r.mode = s.str("mode");
        if (r.mode != "failover" && r.mode != "hedged") s.fail("mode", "must be failover|hedged, got \"" + r.mode + "\"");
        r.ws_order = s.strings("ws_order");
        r.getlogs_order = s.strings("getlogs_order");
        r.failover_after = s.uint<std::uint32_t>("failover_after", 1);
        r.stale_after_blocks = s.uint<std::uint32_t>("stale_after_blocks", 1);
        r.prefer_primary_after_s = s.uint<std::uint32_t>("prefer_primary_after_s", 0);
        r.quota_cooldown_s = s.uint<std::uint32_t>("quota_cooldown_s", 1);

        const auto find = [&](const std::string& name, const char* list) -> const Declared& {
            const auto it = std::find_if(declared.begin(), declared.end(), [&](const Declared& d) { return d.name == name; });
            if (it == declared.end()) s.fail(list, "references undeclared provider \"" + name + "\"");
            return *it;
        };
        const auto check_scheme = [&](const std::string& env_name, const std::string& url, bool ws) {
            const bool ok = ws ? (starts_with(url, "wss://") || starts_with(url, "ws://"))
                               : (starts_with(url, "https://") || starts_with(url, "http://"));
            if (!ok) throw ConfigError(origin + ": env " + env_name + " must be a " + (ws ? "ws:// or wss://" : "http:// or https://") + " URL");
        };

        for (const auto& name : r.ws_order) {
            const Declared& d = find(name, "ws_order");
            if (d.ws_env.empty()) {
                cfg.notes.push_back("INFO provider \"" + name + "\" skipped for WS: no ws_env declared");
                continue;
            }
            const std::string url = env_lookup(d.ws_env);
            if (url.empty()) {
                cfg.notes.push_back("INFO provider \"" + name + "\" skipped for WS: env " + d.ws_env + " unset");
                continue;
            }
            check_scheme(d.ws_env, url, true);
            Provider p;
            p.name = name;
            p.ws_url = url;
            p.max_getlogs_range = d.max_getlogs_range;
            cfg.ws_chain.push_back(std::move(p));
        }
        for (const auto& name : r.getlogs_order) {
            const Declared& d = find(name, "getlogs_order");
            std::string url = d.http_literal;
            if (url.empty() && !d.http_env.empty()) {
                url = env_lookup(d.http_env);
                if (url.empty()) {
                    cfg.notes.push_back("INFO provider \"" + name + "\" skipped for getLogs: env " + d.http_env + " unset");
                    continue;
                }
                check_scheme(d.http_env, url, false);
            }
            if (url.empty()) {
                cfg.notes.push_back("INFO provider \"" + name + "\" skipped for getLogs: no http endpoint declared");
                continue;
            }
            Provider p;
            p.name = name;
            p.http_url = url;
            p.max_getlogs_range = d.max_getlogs_range;
            cfg.getlogs_chain.push_back(std::move(p));
        }

        // A provider enabled for both roles is one entity: merge the URLs so callers see both.
        for (auto& w : cfg.ws_chain) {
            const auto it = std::find_if(cfg.getlogs_chain.begin(), cfg.getlogs_chain.end(), [&](const Provider& g) { return g.name == w.name; });
            if (it != cfg.getlogs_chain.end()) w.http_url = it->http_url;
        }
        for (auto& g : cfg.getlogs_chain) {
            const auto it = std::find_if(cfg.ws_chain.begin(), cfg.ws_chain.end(), [&](const Provider& w) { return w.name == g.name; });
            if (it != cfg.ws_chain.end()) g.ws_url = it->ws_url;
        }

        const auto unset_names = [&](const std::vector<std::string>& order, bool ws) {
            std::string out;
            for (const auto& name : order) {
                const Declared& d = find(name, ws ? "ws_order" : "getlogs_order");
                const std::string& var = ws ? d.ws_env : d.http_env;
                if (var.empty()) continue;
                out += (out.empty() ? "" : ", ") + var;
            }
            return out;
        };
        if (cfg.ws_chain.empty()) {
            throw ConfigError(origin + ": no WS provider enabled; set at least one of: " + unset_names(r.ws_order, true));
        }
        if (cfg.getlogs_chain.empty()) {
            throw ConfigError(origin + ": no getLogs provider enabled; set at least one of: " + unset_names(r.getlogs_order, false));
        }
    }

    // [lease]
    {
        const Section s(table_of(root, origin, "lease"), origin, "[lease]", {"enabled", "ttl_ms", "renew_ms"});
        cfg.lease.enabled = s.boolean("enabled");
        cfg.lease.ttl_ms = s.uint<std::uint32_t>("ttl_ms", 1);
        cfg.lease.renew_ms = s.uint<std::uint32_t>("renew_ms", 1);
        if (cfg.lease.renew_ms >= cfg.lease.ttl_ms) s.fail("renew_ms", "must be < ttl_ms");
    }

    // [[contracts]]
    {
        const auto tables = array_tables_of(root, origin, "contracts");
        if (tables.empty()) throw ConfigError(origin + ": at least one [[contracts]] entry is required");
        std::set<std::string> names;
        std::set<std::string> addrs;
        for (std::size_t i = 0; i < tables.size(); ++i) {
            const Section s(*tables[i], origin, "[[contracts]][" + std::to_string(i) + "]",
                            {"name", "address", "start_block", "finality", "stream", "events"});
            ContractCfg c;
            c.name = s.str("name");
            if (!names.insert(c.name).second) s.fail("name", "duplicate contract name \"" + c.name + "\"");
            try {
                c.address = Address20::from_hex(s.str("address"));
            } catch (const DecodeError& e) {
                s.fail("address", e.what());
            }
            if (!addrs.insert(c.address.to_hex()).second) s.fail("address", "duplicate contract address " + c.address.to_hex());

            const std::string stream = s.opt_str("stream", "events");
            if (stream == "events") c.stream = StreamKind::Events;
            else if (stream == "registry") c.stream = StreamKind::Registry;
            else s.fail("stream", "must be events|registry, got \"" + stream + "\"");

            c.finality = s.has("finality") ? parse_finality(s, "finality", s.str("finality")) : cfg.chain.default_finality;
            if (c.finality == Finality::Depth && cfg.chain.depth == 0) s.fail("finality", "finality=depth requires [chain].depth > 0");

            if (const toml::node* n = s.node("start_block")) {
                if (auto v = n->value<std::int64_t>()) {
                    if (*v < 0) s.fail("start_block", "must be >= 0");
                    c.start_block = static_cast<std::uint64_t>(*v);
                } else if (auto g = n->value<std::string>(); g && *g == "gate") {
                    c.start_block = std::nullopt;
                } else {
                    s.fail("start_block", "expected a non-negative integer or \"gate\"");
                }
            } else if (c.stream == StreamKind::Events) {
                s.fail("start_block", "required for stream=events (integer deploy block, or \"gate\")");
            }

            c.events = s.strings("events");
            if (c.events.empty()) s.fail("events", "must list at least one event signature");
            std::set<std::string> seen_topics;
            for (const auto& sig : c.events) {
                try {
                    c.topics.push_back(topic0(sig));
                } catch (const ConfigError& e) {
                    s.fail("events", e.what());
                }
                if (!seen_topics.insert(c.topics.back().to_hex()).second) s.fail("events", "duplicate event \"" + sig + "\"");
            }
            for (std::size_t e = 0; e < c.events.size(); ++e) {
                cfg.routes.emplace(RouteKey{c.address, c.topics[e]},
                                   EventRoute{c.name, event_name(c.events[e]), c.stream, c.finality});
            }
            cfg.contracts.push_back(std::move(c));
        }
    }

    // [backfill]
    {
        const Section s(table_of(root, origin, "backfill"), origin, "[backfill]", {"min_range", "request_timeout_ms"});
        cfg.backfill.min_range = s.uint<std::uint32_t>("min_range", 1);
        cfg.backfill.request_timeout_ms = s.uint<std::uint32_t>("request_timeout_ms", 1);
    }

    // [watchdog]
    {
        const Section s(table_of(root, origin, "watchdog"), origin, "[watchdog]",
                        {"head_stall_ms", "subscribe_timeout_ms", "progress_stall_ms", "safe_gate_stuck_s", "gate_lag_alert_s",
                         "process_stall_ms"});
        auto& w = cfg.watchdog;
        w.head_stall_ms = s.uint<std::uint32_t>("head_stall_ms", 1);
        w.subscribe_timeout_ms = s.uint<std::uint32_t>("subscribe_timeout_ms", 1);
        w.progress_stall_ms = s.uint<std::uint32_t>("progress_stall_ms", 1);
        w.safe_gate_stuck_s = s.uint<std::uint32_t>("safe_gate_stuck_s", 1);
        w.gate_lag_alert_s = s.uint<std::uint32_t>("gate_lag_alert_s", 1);
        w.process_stall_ms = s.uint<std::uint32_t>("process_stall_ms", 1);
    }

    // [sink]
    {
        const Section s(table_of(root, origin, "sink"), origin, "[sink]",
                        {"redis_url_env", "retention_ms", "dedup_ttl_s", "queue_capacity", "spill_dir", "spill_max_bytes",
                         "require_noeviction", "consumer_groups", "consumer_lag_warn_s", "consumer_lag_crit_s"});
        auto& k = cfg.sink;
        k.redis_url_env = s.str("redis_url_env");
        k.retention_ms = s.uint<std::uint64_t>("retention_ms", 1);
        k.dedup_ttl_s = s.uint<std::uint32_t>("dedup_ttl_s", 1);
        k.queue_capacity = s.uint<std::uint32_t>("queue_capacity", 1);
        k.spill_dir = s.str("spill_dir");
        k.spill_max_bytes = s.uint<std::uint64_t>("spill_max_bytes", 1);
        k.require_noeviction = s.boolean("require_noeviction");

        // Consumer-group lag probe (plan 4.10): 0 < warn < crit < retention, so an alert fires while data is still safe.
        k.consumer_lag_warn_s = s.opt_uint<std::uint32_t>("consumer_lag_warn_s", k.consumer_lag_warn_s, 1);
        k.consumer_lag_crit_s = s.opt_uint<std::uint32_t>("consumer_lag_crit_s", k.consumer_lag_crit_s, 1);
        if (k.consumer_lag_warn_s >= k.consumer_lag_crit_s) s.fail("consumer_lag_warn_s", "must be < consumer_lag_crit_s");
        if (k.consumer_lag_crit_s >= k.retention_ms / 1000)
            s.fail("consumer_lag_crit_s", "must be < retention_ms / 1000: an alert after the data was trimmed is too late");

        if (const toml::node* n = s.node("consumer_groups")) {
            const toml::array* a = n->as_array();
            if (!a) s.fail("consumer_groups", "expected an array of { stream, group } tables");
            std::set<std::pair<int, std::string>> seen;
            for (std::size_t i = 0; i < a->size(); ++i) {
                const toml::table* t = (*a)[i].as_table();
                if (!t) s.fail("consumer_groups", "entries must be { stream, group } tables");
                const Section g(*t, origin, "[sink].consumer_groups[" + std::to_string(i) + "]", {"stream", "group"});
                ConsumerGroupCfg c;
                const std::string stream = g.str("stream");
                if (stream == "events") c.stream = StreamKind::Events;
                else if (stream == "registry") c.stream = StreamKind::Registry;
                else g.fail("stream", "must be events|registry, got \"" + stream + "\"");
                c.group = g.str("group");
                if (c.group.empty()) g.fail("group", "must not be empty");
                if (!seen.emplace(static_cast<int>(c.stream), c.group).second)
                    g.fail("group", "duplicate group \"" + c.group + "\" on stream " + stream);
                k.consumer_groups.push_back(std::move(c));
            }
        }
    }

    // [ops]
    {
        const Section s(table_of(root, origin, "ops"), origin, "[ops]",
                        {"http_listen", "heartbeat_s", "alert_webhook_env", "alert_webhook_token_env", "alert_service",
                         "deadman_ping_env", "deadman_ping_s", "http_user_agent"});
        auto& o = cfg.ops;
        o.http_listen = s.str("http_listen");
        o.heartbeat_s = s.uint<std::uint32_t>("heartbeat_s", 1);
        o.alert_webhook_env = s.str("alert_webhook_env");
        o.alert_webhook_token_env = s.str("alert_webhook_token_env");
        o.alert_service = s.str("alert_service");
        o.deadman_ping_env = s.str("deadman_ping_env");
        o.deadman_ping_s = s.uint<std::uint32_t>("deadman_ping_s", 1);
        o.http_user_agent = s.str("http_user_agent");

        // Never POST unauthenticated, never POST a token over plaintext.
        const std::string hook = env_lookup(o.alert_webhook_env);
        if (!hook.empty()) {
            if (env_lookup(o.alert_webhook_token_env).empty()) {
                throw ConfigError(origin + ": " + o.alert_webhook_env + " is set but " + o.alert_webhook_token_env +
                                  " is not (refusing unauthenticated alert POSTs)");
            }
            if (!starts_with(hook, "https://")) throw ConfigError(origin + ": env " + o.alert_webhook_env + " must be an https:// URL");
        } else {
            cfg.notes.push_back("WARN alerts disabled: env " + o.alert_webhook_env + " unset");
        }
        if (env_lookup(o.deadman_ping_env).empty()) {
            cfg.notes.push_back("WARN dead-man's switch disabled: env " + o.deadman_ping_env + " unset");
        }
    }

    return cfg;
}

}  // namespace evml::core
