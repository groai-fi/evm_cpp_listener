#include "sink/redispp_conn.hpp"

#include <charconv>
#include <cmath>
#include <iterator>
#include <stdexcept>
#include <unordered_map>

namespace evml::sink {

namespace {

sw::redis::Redis make_redis(const std::string& url, std::chrono::milliseconds io_timeout) {
    sw::redis::ConnectionOptions opts = sw::redis::Uri(url).connection_options();
    opts.connect_timeout = io_timeout;
    opts.socket_timeout = io_timeout;
    sw::redis::ConnectionPoolOptions pool;
    pool.size = 1;
    return sw::redis::Redis(opts, pool);
}

bool starts_with(const std::string& s, const char* prefix) { return s.rfind(prefix, 0) == 0; }

// An error reply that means "the server cannot serve this right now", as opposed to "your command is wrong".
bool is_transient_reply(const std::string& msg) { return starts_with(msg, "LOADING") || starts_with(msg, "BUSY"); }

// Runs `f`, turning every transport-level failure into ConnError. Fatal error replies pass through unchanged.
template <class F>
auto guarded(F&& f) -> decltype(f()) {
    try {
        return f();
    } catch (const sw::redis::OomError& e) {
        throw ConnError(std::string("redis OOM: ") + e.what());
    } catch (const sw::redis::ReplyError& e) {
        if (is_transient_reply(e.what())) throw ConnError(std::string("redis busy: ") + e.what());
        throw;
    } catch (const sw::redis::IoError& e) {  // includes TimeoutError
        throw ConnError(std::string("redis io: ") + e.what());
    } catch (const sw::redis::ClosedError& e) {
        throw ConnError(std::string("redis closed: ") + e.what());
    }
}

std::uint64_t parse_u64(const std::string& s, const std::string& what) {
    std::uint64_t v = 0;
    const auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc{} || p != s.data() + s.size())
        throw std::runtime_error("redis: " + what + " is not an unsigned integer: '" + s + "'");
    return v;
}

// ZRANGEBYSCORE ... WITHSCORES returns scores as text; ours are block numbers, so integral.
std::uint64_t parse_score(const std::string& s, const std::string& member) {
    double d = 0;
    const auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), d);
    if (ec != std::errc{} || p != s.data() + s.size() || d < 0 || d != std::floor(d))
        throw std::runtime_error("redis: released score of '" + member + "' is not a block number: '" + s + "'");
    return static_cast<std::uint64_t>(d);
}

CallOutcome fail_outcome(const std::string& msg) {
    if (starts_with(msg, "NOSCRIPT")) return ScriptFail{FailKind::NoScript, msg};
    if (is_transient_reply(msg)) return ScriptFail{FailKind::Transient, msg};
    return ScriptFail{FailKind::Fatal, msg};
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
const redisReply* flat_get(const redisReply& arr, const std::string& key, const char* what) {
    require_array(arr, what);
    if (arr.elements % 2 != 0) throw std::runtime_error(std::string("redis: ") + what + " has an odd element count");
    for (std::size_t i = 0; i < arr.elements; i += 2)
        if (reply_str(*arr.element[i]) == key) return arr.element[i + 1];
    return nullptr;
}

std::optional<std::string> flat_str(const redisReply& arr, const std::string& key, const char* what) {
    const redisReply* v = flat_get(arr, key, what);
    if (!v) throw std::runtime_error(std::string("redis: ") + what + " has no field '" + key + "'");
    return reply_str(*v);
}

}  // namespace

RedisppConn::RedisppConn(const std::string& url, const Keys& keys, std::chrono::milliseconds io_timeout)
    : redis_(make_redis(url, io_timeout)), keys_(keys) {}

void RedisppConn::load_scripts() {
    guarded([&] {
        for (std::size_t i = 0; i < kScriptCount; ++i)
            shas_[i] = redis_.script_load(script_source(static_cast<Script>(i)));
        return 0;
    });
    loaded_ = true;
}

void RedisppConn::set_with_ttl(const std::string& key, const std::string& value, std::uint32_t ttl_s) {
    guarded([&] {
        redis_.set(key, value, std::chrono::seconds(ttl_s));
        return 0;
    });
}

std::vector<CallOutcome> RedisppConn::pipeline(const std::vector<ScriptCall>& calls) {
    if (!loaded_) load_scripts();
    return guarded([&] {
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
                out.push_back(ScriptFail{FailKind::Transient, e.what()});  // refused before it ran: nothing was written
            } catch (const sw::redis::ReplyError& e) {
                out.push_back(fail_outcome(e.what()));
            }
        }
        return out;
    });
}

pipeline::Checkpoint RedisppConn::read_checkpoint() {
    std::unordered_map<std::string, std::string> h;
    guarded([&] {
        redis_.hgetall(keys_.ckpt(), std::inserter(h, h.end()));
        return 0;
    });

    pipeline::Checkpoint ck;
    for (const auto& [field, value] : h) {
        if (field == "safe") ck.safe = parse_u64(value, "ckpt.safe");
        else if (field == "finalized") ck.finalized = parse_u64(value, "ckpt.finalized");
        else if (field == "contracts") ck.contracts = unpack_manifest(value);
        else throw std::runtime_error("redis: unknown field '" + field + "' in " + keys_.ckpt());
    }
    return ck;
}

std::vector<pipeline::ReleasedEntry> RedisppConn::read_released(std::uint64_t from, std::uint64_t to) {
    // Flat [member, score, member, score, ...], ordered by score then member.
    const auto flat = guarded([&] {
        return redis_.command<std::vector<std::string>>("ZRANGEBYSCORE", keys_.released(), std::to_string(from),
                                                        std::to_string(to), "WITHSCORES");
    });
    if (flat.size() % 2 != 0) throw std::runtime_error("redis: ZRANGEBYSCORE WITHSCORES returned an odd element count");

    std::vector<pipeline::ReleasedEntry> out;
    out.reserve(flat.size() / 2);
    constexpr std::size_t kChunk = 1000;
    for (std::size_t first = 0; first < flat.size(); first += 2 * kChunk) {
        const std::size_t last = std::min(flat.size(), first + 2 * kChunk);
        std::vector<std::string> ids;
        for (std::size_t i = first; i < last; i += 2) ids.push_back(flat[i]);

        std::vector<sw::redis::OptionalString> images;
        guarded([&] {
            redis_.hmget(keys_.released_data(), ids.begin(), ids.end(), std::back_inserter(images));
            return 0;
        });
        if (images.size() != ids.size()) throw std::runtime_error("redis: HMGET returned the wrong number of values");

        for (std::size_t k = 0; k < ids.size(); ++k) {
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

GroupFacts RedisppConn::read_group_facts(const std::string& stream_key, const std::string& group) {
    const auto run = [this](std::vector<std::string> argv) -> sw::redis::ReplyUPtr {
        return guarded([&] { return redis_.command(argv.begin(), argv.end()); });
    };
    GroupFacts f;

    sw::redis::ReplyUPtr groups;
    try {
        groups = run({"XINFO", "GROUPS", stream_key});
    } catch (const sw::redis::ReplyError& e) {
        if (std::string(e.what()).find("no such key") != std::string::npos) return f;  // the stream does not exist yet
        throw;
    }
    f.stream_exists = true;
    require_array(*groups, "XINFO GROUPS");
    const redisReply* mine = nullptr;
    for (std::size_t i = 0; i < groups->elements && !mine; ++i)
        if (flat_str(*groups->element[i], "name", "XINFO GROUPS entry") == group) mine = groups->element[i];
    if (!mine) return f;
    f.group_found = true;
    f.last_delivered_id = flat_str(*mine, "last-delivered-id", "XINFO GROUPS entry").value();
    if (const auto lag = flat_str(*mine, "lag", "XINFO GROUPS entry")) f.lag = parse_u64(*lag, "group lag");
    if (const auto read = flat_str(*mine, "entries-read", "XINFO GROUPS entry"))
        f.entries_read = parse_u64(*read, "group entries-read");

    const auto info = run({"XINFO", "STREAM", stream_key});
    f.max_deleted_id = flat_str(*info, "max-deleted-entry-id", "XINFO STREAM").value_or("");
    if (const auto added = flat_str(*info, "entries-added", "XINFO STREAM"))
        f.entries_added = parse_u64(*added, "stream entries-added");
    if (const redisReply* first = flat_get(*info, "first-entry", "XINFO STREAM"); first && first->type != REDIS_REPLY_NIL) {
        require_array(*first, "XINFO STREAM first-entry");
        if (first->elements < 1) throw std::runtime_error("redis: XINFO STREAM first-entry is empty");
        f.first_entry_id = reply_str(*first->element[0]);
    }

    const auto next = run({"XRANGE", stream_key, "(" + f.last_delivered_id, "+", "COUNT", "1"});
    require_array(*next, "XRANGE");
    if (next->elements > 0) {
        require_array(*next->element[0], "XRANGE entry");
        if (next->element[0]->elements < 1) throw std::runtime_error("redis: XRANGE entry has no id");
        f.first_undelivered_id = reply_str(*next->element[0]->element[0]);
    }

    // Summary form: [count, smallest id | nil, largest id | nil, consumers | nil]
    const auto pending = run({"XPENDING", stream_key, group});
    require_array(*pending, "XPENDING");
    if (pending->elements < 2) throw std::runtime_error("redis: XPENDING summary is too short");
    f.smallest_pending_id = reply_str(*pending->element[1]);

    const auto time = run({"TIME"});
    require_array(*time, "TIME");
    if (time->elements != 2) throw std::runtime_error("redis: TIME did not answer [seconds, microseconds]");
    f.now_ms = parse_u64(reply_str(*time->element[0]).value(), "TIME seconds") * 1000 +
               parse_u64(reply_str(*time->element[1]).value(), "TIME microseconds") / 1000;
    return f;
}

}  // namespace evml::sink
