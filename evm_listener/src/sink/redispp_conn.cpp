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

}  // namespace evml::sink
