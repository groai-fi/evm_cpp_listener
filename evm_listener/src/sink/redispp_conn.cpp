#include "sink/redispp_conn.hpp"

#include <charconv>
#include <cmath>
#include <iterator>
#include <stdexcept>
#include <unordered_map>

namespace evml::sink {

namespace {

sw::redis::Redis make_redis(std::string_view url, std::chrono::milliseconds io_timeout, std::string_view client_name) {
    sw::redis::ConnectionOptions opts = sw::redis::Uri(std::string(url)).connection_options();
    opts.connect_timeout = io_timeout;
    opts.socket_timeout = io_timeout;
    if (!client_name.empty()) {
        opts.name = sanitize_client_name(client_name);
    }
    sw::redis::ConnectionPoolOptions pool;
    pool.size = 1;
    return sw::redis::Redis(opts, pool);
}

bool starts_with(std::string_view s, std::string_view prefix) { return s.rfind(prefix, 0) == 0; }

// An error reply that means "the server cannot serve this right now", as opposed to "your command is wrong".
bool is_transient_reply(std::string_view msg) { return starts_with(msg, "LOADING") || starts_with(msg, "BUSY"); }

// Runs `f`, turning every transport-level failure into Result<T, ConnError>. Fatal error replies pass through unchanged.
template <class F>
auto try_guarded(F&& f) -> core::Result<decltype(f()), ConnError> {
    try {
        if constexpr (std::is_void_v<decltype(f())>) {
            f();
            return {};
        } else {
            return f();
        }
    } catch (const sw::redis::OomError& e) {
        return ConnError(std::string("redis OOM: ") + e.what());
    } catch (const sw::redis::ReplyError& e) {
        if (is_transient_reply(e.what())) return ConnError(std::string("redis busy: ") + e.what());
        throw;
    } catch (const sw::redis::IoError& e) {  // includes TimeoutError
        return ConnError(std::string("redis io: ") + e.what());
    } catch (const sw::redis::ClosedError& e) {
        return ConnError(std::string("redis closed: ") + e.what());
    }
}

std::uint64_t parse_u64(std::string_view s, std::string_view what) {
    std::uint64_t v = 0;
    const auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc{} || p != s.data() + s.size())
        throw std::runtime_error("redis: " + std::string(what) + " is not an unsigned integer: '" + std::string(s) + "'");
    return v;
}

// ZRANGEBYSCORE ... WITHSCORES returns scores as text; ours are block numbers, so integral.
std::uint64_t parse_score(std::string_view s, std::string_view member) {
    double d = 0;
    const auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), d);
    if (ec != std::errc{} || p != s.data() + s.size() || d < 0 || d != std::floor(d))
        throw std::runtime_error("redis: released score of '" + std::string(member) + "' is not a block number: '" +
                                 std::string(s) + "'");
    return static_cast<std::uint64_t>(d);
}

CallOutcome fail_outcome(std::string_view msg) {
    if (starts_with(msg, "NOSCRIPT")) return ScriptFail{FailKind::NoScript, std::string(msg)};
    if (is_transient_reply(msg)) return ScriptFail{FailKind::Transient, std::string(msg)};
    return ScriptFail{FailKind::Fatal, std::string(msg)};
}

// ---- raw reply walking (XINFO / XPENDING / TIME answer arrays that mix integers, strings and nils) ----

std::optional<std::string> reply_str(const redisReply& r) {
    switch (r.type) {
        case REDIS_REPLY_STRING:
        case REDIS_REPLY_STATUS: return std::string(r.str, r.len);
        case REDIS_REPLY_INTEGER: return std::to_string(r.integer);
        case REDIS_REPLY_NIL: return std::nullopt;
        default: throw std::runtime_error("redis: expected a scalar reply, got reply type " + std::to_string(r.type));
    }
}

void require_array(const redisReply& r, const char* what) {
    if (r.type != REDIS_REPLY_ARRAY)
        throw std::runtime_error(std::string("redis: ") + what + " did not answer an array (reply type " +
                                 std::to_string(r.type) + ")");
}

// The value after `key` in a flat [k1, v1, k2, v2, ...] array; nullptr when the key is absent.
const redisReply* flat_get(const redisReply& arr, std::string_view key, const char* what) {
    require_array(arr, what);
    if (arr.elements % 2 != 0) throw std::runtime_error(std::string("redis: ") + what + " has an odd element count");
    for (std::size_t i = 0; i < arr.elements; i += 2)
        if (reply_str(*arr.element[i]) == key) return arr.element[i + 1];
    return nullptr;
}

std::optional<std::string> flat_str(const redisReply& arr, std::string_view key, const char* what) {
    const redisReply* v = flat_get(arr, key, what);
    if (!v) throw std::runtime_error(std::string("redis: ") + what + " has no field '" + std::string(key) + "'");
    return reply_str(*v);
}

}  // namespace

RedisppConn::RedisppConn(std::string_view url, const Keys& keys, std::chrono::milliseconds io_timeout,
                         std::string_view client_name)
    : redis_(make_redis(url, io_timeout, client_name)), keys_(keys) {}

core::Result<TenancyFacts, ConnError> RedisppConn::try_read_tenancy_facts() {
    return try_guarded([&] { return gather_runtime_tenancy_facts(redis_); });
}

TenancyFacts RedisppConn::read_tenancy_facts() {
    auto r = try_read_tenancy_facts();
    if (!r) throw r.error();
    return std::move(*r);
}

core::Result<void, ConnError> RedisppConn::try_load_scripts() {
    auto r = try_guarded([&] {
        for (std::size_t i = 0; i < kScriptCount; ++i)
            shas_[i] = redis_.script_load(script_source(static_cast<Script>(i)));
    });
    if (!r) return r.error();
    loaded_ = true;
    return {};
}

void RedisppConn::load_scripts() {
    if (auto r = try_load_scripts(); !r) throw r.error();
}

core::Result<void, ConnError> RedisppConn::try_set_with_ttl(std::string_view key, std::string_view value,
                                                            std::uint32_t ttl_s) {
    return try_guarded([&] {
        redis_.set(sw::redis::StringView(key.data(), key.size()), sw::redis::StringView(value.data(), value.size()),
                   std::chrono::seconds(ttl_s));
    });
}

void RedisppConn::set_with_ttl(std::string_view key, std::string_view value, std::uint32_t ttl_s) {
    if (auto r = try_set_with_ttl(key, value, ttl_s); !r) throw r.error();
}

core::Result<std::vector<CallOutcome>, ConnError> RedisppConn::try_pipeline(const std::vector<ScriptCall>& calls) {
    if (!loaded_) {
        if (auto lr = try_load_scripts(); !lr) return lr.error();
    }
    return try_guarded([&] {
        // Build and dispatch non-transactional Redis pipeline containing all script invocations.
        auto pipe = redis_.pipeline(false);
        for (const auto& c : calls)
            pipe.evalsha(shas_[static_cast<std::size_t>(c.script)], c.keys.begin(), c.keys.end(), c.args.begin(),
                         c.args.end());
        auto replies = pipe.exec();

        std::vector<CallOutcome> out;
        out.reserve(calls.size());
        for (std::size_t i = 0; i < calls.size(); ++i) {
            try {
                out.push_back(ScriptOk{replies.get<sw::redis::OptionalString>(i)});
            } catch (const sw::redis::OomError& e) {
                // Server rejected script due to maxmemory before execution; classified as transient.
                out.push_back(ScriptFail{FailKind::Transient, e.what()});  // refused before it ran: nothing was written
            } catch (const sw::redis::ReplyError& e) {
                out.push_back(fail_outcome(e.what()));
            }
        }
        return out;
    });
}

std::vector<CallOutcome> RedisppConn::pipeline(const std::vector<ScriptCall>& calls) {
    auto r = try_pipeline(calls);
    if (!r) throw r.error();
    return std::move(*r);
}

core::Result<pipeline::Checkpoint, ConnError> RedisppConn::try_read_checkpoint() {
    std::unordered_map<std::string, std::string> h;
    if (auto r = try_guarded([&] {
            // Fetch all fields of the checkpoint hash (safe, finalized, contracts).
            redis_.hgetall(keys_.ckpt(), std::inserter(h, h.end()));
        });
        !r) {
        return r.error();
    }

    pipeline::Checkpoint ck;
    for (const auto& [field, value] : h) {
        if (field == "safe") ck.safe = parse_u64(value, "ckpt.safe");
        else if (field == "finalized") ck.finalized = parse_u64(value, "ckpt.finalized");
        else if (field == "contracts") ck.contracts = unpack_manifest(value);
        else throw std::runtime_error("redis: unknown field '" + field + "' in " + keys_.ckpt());
    }
    return ck;
}

pipeline::Checkpoint RedisppConn::read_checkpoint() {
    auto r = try_read_checkpoint();
    if (!r) throw r.error();
    return std::move(*r);
}

core::Result<std::vector<pipeline::ReleasedEntry>, ConnError> RedisppConn::try_read_released(std::uint64_t from,
                                                                                             std::uint64_t to) {
    // Flat [member, score, member, score, ...], ordered by score then member.
    // Query sorted set for released log entries within requested block range [from, to].
    const auto flat_res = try_guarded([&] {
        return redis_.command<std::vector<std::string>>("ZRANGEBYSCORE", keys_.released(), std::to_string(from),
                                                        std::to_string(to), "WITHSCORES");
    });
    if (!flat_res) return flat_res.error();
    const auto& flat = *flat_res;
    if (flat.size() % 2 != 0) throw std::runtime_error("redis: ZRANGEBYSCORE WITHSCORES returned an odd element count");

    std::vector<pipeline::ReleasedEntry> out;
    out.reserve(flat.size() / 2);
    constexpr std::size_t kChunk = 1000;
    // Chunked HMGET prevents overwhelming Redis memory when fetching many before-images.
    for (std::size_t first = 0; first < flat.size(); first += 2 * kChunk) {
        const std::size_t last = std::min(flat.size(), first + 2 * kChunk);
        std::vector<std::string> ids;
        for (std::size_t i = first; i < last; i += 2) ids.push_back(flat[i]);

        std::vector<sw::redis::OptionalString> images;
        if (auto r = try_guarded([&] {
                redis_.hmget(keys_.released_data(), ids.begin(), ids.end(), std::back_inserter(images));
            });
            !r) {
            return r.error();
        }
        if (images.size() != ids.size()) throw std::runtime_error("redis: HMGET returned the wrong number of values");

        for (std::size_t k = 0; k < ids.size(); ++k) {
            // Missing before-image violates revert contract and indicates corrupt database state.
            if (!images[k])
                throw std::runtime_error("redis: released member '" + ids[k] + "' has no before image in " +
                                         keys_.released_data());
            const auto image = unpack_released(*images[k]);
            out.push_back(pipeline::ReleasedEntry{ids[k], parse_score(flat[first + 2 * k + 1], ids[k]), image.stream,
                                                  image.fields});
        }
    }
    return out;
}

std::vector<pipeline::ReleasedEntry> RedisppConn::read_released(std::uint64_t from, std::uint64_t to) {
    auto r = try_read_released(from, to);
    if (!r) throw r.error();
    return std::move(*r);
}

core::Result<GroupFacts, ConnError> RedisppConn::try_read_group_facts(std::string_view stream_key,
                                                                      std::string_view group) {
    const std::string sk(stream_key);
    const std::string grp(group);
    const auto run = [this](std::vector<std::string> argv) -> core::Result<sw::redis::ReplyUPtr, ConnError> {
        return try_guarded([&] { return redis_.command(argv.begin(), argv.end()); });
    };
    GroupFacts f;

    sw::redis::ReplyUPtr groups;
    try {
        auto groups_res = run({"XINFO", "GROUPS", sk});
        if (!groups_res) return groups_res.error();
        groups = std::move(*groups_res);
    } catch (const sw::redis::ReplyError& e) {
        if (std::string(e.what()).find("no such key") != std::string::npos) return f;  // the stream does not exist yet
        throw;
    }
    f.stream_exists = true;
    require_array(*groups, "XINFO GROUPS");
    const redisReply* mine = nullptr;
    for (std::size_t i = 0; i < groups->elements && !mine; ++i)
        if (flat_str(*groups->element[i], "name", "XINFO GROUPS entry") == grp) mine = groups->element[i];
    if (!mine) return f;
    f.group_found = true;
    f.last_delivered_id = flat_str(*mine, "last-delivered-id", "XINFO GROUPS entry").value();
    if (const auto lag = flat_str(*mine, "lag", "XINFO GROUPS entry")) f.lag = parse_u64(*lag, "group lag");
    if (const auto read = flat_str(*mine, "entries-read", "XINFO GROUPS entry"))
        f.entries_read = parse_u64(*read, "group entries-read");

    const auto info_res = run({"XINFO", "STREAM", sk});
    if (!info_res) return info_res.error();
    const auto& info = *info_res;
    f.max_deleted_id = flat_str(*info, "max-deleted-entry-id", "XINFO STREAM").value_or("");
    if (const auto added = flat_str(*info, "entries-added", "XINFO STREAM"))
        f.entries_added = parse_u64(*added, "stream entries-added");
    if (const redisReply* first = flat_get(*info, "first-entry", "XINFO STREAM"); first && first->type != REDIS_REPLY_NIL) {
        require_array(*first, "XINFO STREAM first-entry");
        if (first->elements < 1) throw std::runtime_error("redis: XINFO STREAM first-entry is empty");
        f.first_entry_id = reply_str(*first->element[0]);
    }

    const auto next_res = run({"XRANGE", sk, "(" + f.last_delivered_id, "+", "COUNT", "1"});
    if (!next_res) return next_res.error();
    const auto& next = *next_res;
    require_array(*next, "XRANGE");
    if (next->elements > 0) {
        require_array(*next->element[0], "XRANGE entry");
        if (next->element[0]->elements < 1) throw std::runtime_error("redis: XRANGE entry has no id");
        f.first_undelivered_id = reply_str(*next->element[0]->element[0]);
    }

    // Summary form: [count, smallest id | nil, largest id | nil, consumers | nil]
    const auto pending_res = run({"XPENDING", sk, grp});
    if (!pending_res) return pending_res.error();
    const auto& pending = *pending_res;
    require_array(*pending, "XPENDING");
    if (pending->elements < 2) throw std::runtime_error("redis: XPENDING summary is too short");
    f.smallest_pending_id = reply_str(*pending->element[1]);

    const auto time_res = run({"TIME"});
    if (!time_res) return time_res.error();
    const auto& time = *time_res;
    require_array(*time, "TIME");
    if (time->elements != 2) throw std::runtime_error("redis: TIME did not answer [seconds, microseconds]");
    f.now_ms = parse_u64(reply_str(*time->element[0]).value(), "TIME seconds") * 1000 +
               parse_u64(reply_str(*time->element[1]).value(), "TIME microseconds") / 1000;
    return f;
}

GroupFacts RedisppConn::read_group_facts(std::string_view stream_key, std::string_view group) {
    auto r = try_read_group_facts(stream_key, group);
    if (!r) throw r.error();
    return std::move(*r);
}

}  // namespace evml::sink
