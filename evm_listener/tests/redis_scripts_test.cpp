// Integration tests of the Lua scripts against a real Redis (plan "Tests" 1, 5, 7, 10, 11, 14, 15, 17).
//
// REDIS_URL comes from the process environment only (the dev container sets it), never from a .env file: these tests
// create keys, and the OOM test changes `maxmemory`, so they must not be pointed at a real deployment by accident.
// A missing or unreachable Redis FAILS the tests; it never skips them.
//
// Every test works in its own random shard id, so the keys of concurrent tests never overlap. Tests tagged
// [redis_serial] change server-wide state (maxmemory, SCRIPT FLUSH) and are run alone by ctest (RUN_SERIAL).

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <sw/redis++/redis++.h>

#include "pipeline/sink.hpp"
#include "sink/lua_scripts.hpp"
#include "sink/script_runner.hpp"

using namespace evml::sink;
using evml::pipeline::CheckpointKind;
using evml::pipeline::Emission;
using evml::pipeline::EventType;
using evml::pipeline::Field;
using Catch::Matchers::ContainsSubstring;

namespace {

std::string redis_url() {
    const char* v = std::getenv("REDIS_URL");
    if (!v || !*v) FAIL("REDIS_URL is not set: the Redis integration tests must run in the dev container (never skipped)");
    return v;
}

std::string random_shard() {
    std::random_device rd;
    return "t_" + std::to_string(rd()) + std::to_string(rd());
}

struct Rig {
    std::string url;
    sw::redis::Redis redis;
    Keys keys;
    ScriptRunner runner;

    Rig() : url(redis_url()), redis(url), keys(random_shard()), runner(redis) {}
    ~Rig() {
        try {
            std::vector<std::string> mine;
            redis.keys(keys.pattern(), std::back_inserter(mine));
            if (!mine.empty()) redis.del(mine.begin(), mine.end());
        } catch (const std::exception& e) {
            std::cerr << "Rig cleanup of shard keys failed: " << e.what() << "\n";
        }
    }

    // Raw reads for assertions.
    long long xlen(const std::string& key) { return redis.command<long long>("XLEN", key); }
    long long zcard(const std::string& key) { return redis.command<long long>("ZCARD", key); }
    long long hlen(const std::string& key) { return redis.command<long long>("HLEN", key); }
    long long exists(const std::string& key) { return redis.command<long long>("EXISTS", key); }
    sw::redis::OptionalString hget(const std::string& key, const std::string& f) {
        return redis.command<sw::redis::OptionalString>("HGET", key, f);
    }
    std::vector<std::string> zrange_all(const std::string& key) {
        return redis.command<std::vector<std::string>>("ZRANGE", key, "0", "-1");
    }

    std::string run(const ScriptCall& c) {
        const auto r = runner.run(c);
        REQUIRE(r.has_value());
        return *r;
    }
};

Emission mk(const std::string& id, EventType type, std::optional<std::uint64_t> released = std::nullopt,
            std::uint64_t block = 105) {
    Emission e;
    e.stream = evml::core::StreamKind::Events;
    e.type = type;
    e.event_id = id;
    e.dedup_key = "dedup:" + id;
    e.fields = {{"v", "1"}, {"type", evml::pipeline::to_string(type)}, {"event_id", id}, {"block_number", std::to_string(block)}};
    e.released_block = released;
    return e;
}

EmitParams params(const std::string& token = kNoFence, std::uint64_t minid_ms = 0) {
    return EmitParams{604800, minid_ms, token};
}

// Restores the server's maxmemory when it goes out of scope, even if a CHECK fails or throws.
struct MaxMemoryGuard {
    sw::redis::Redis& redis;
    std::string old_value;
    explicit MaxMemoryGuard(sw::redis::Redis& r) : redis(r) {
        const auto cfg = redis.command<std::unordered_map<std::string, std::string>>("CONFIG", "GET", "maxmemory");
        const auto it = cfg.find("maxmemory");
        REQUIRE(it != cfg.end());
        old_value = it->second;
    }
    ~MaxMemoryGuard() {
        try {
            redis.command<void>("CONFIG", "SET", "maxmemory", old_value);
        } catch (const std::exception& e) {
            std::cerr << "FAILED TO RESTORE maxmemory=" << old_value << ": " << e.what() << "\n";
        }
    }
};

}  // namespace

// 1. Dedup state machine (N1 / I6).
TEST_CASE("redis scripts: the dedup state machine", "[integration][redis]") {
    Rig r;
    const auto stream = r.keys.events();

    SECTION("the same log twice is one entry") {
        CHECK(r.run(emit_call(r.keys, mk("a", EventType::Log), params())) != "DUP");
        CHECK(r.run(emit_call(r.keys, mk("a", EventType::Log), params())) == "DUP");
        CHECK(r.xlen(stream) == 1);
    }
    SECTION("log, revert, log, revert is four entries") {
        for (const auto type : {EventType::Log, EventType::Revert, EventType::Log, EventType::Revert})
            CHECK(r.run(emit_call(r.keys, mk("a", type), params())) != "DUP");
        CHECK(r.xlen(stream) == 4);
    }
    SECTION("log, revert, revert is two entries") {
        CHECK(r.run(emit_call(r.keys, mk("a", EventType::Log), params())) != "DUP");
        CHECK(r.run(emit_call(r.keys, mk("a", EventType::Revert), params())) != "DUP");
        CHECK(r.run(emit_call(r.keys, mk("a", EventType::Revert), params())) == "DUP");
        CHECK(r.xlen(stream) == 2);
    }
    SECTION("a revert with no prior log writes nothing") {
        CHECK(r.run(emit_call(r.keys, mk("a", EventType::Revert), params())) == "DUP");
        CHECK(r.xlen(stream) == 0);
        CHECK(r.exists(r.keys.dedup("dedup:a")) == 0);
    }
    SECTION("a revert removes the id from released and released_data; a log after it tracks it again") {
        CHECK(r.run(emit_call(r.keys, mk("a", EventType::Log, 105), params())) != "DUP");
        CHECK(r.zcard(r.keys.released()) == 1);
        CHECK(r.hlen(r.keys.released_data()) == 1);

        CHECK(r.run(emit_call(r.keys, mk("a", EventType::Revert), params())) != "DUP");
        CHECK(r.zcard(r.keys.released()) == 0);
        CHECK(r.hlen(r.keys.released_data()) == 0);

        CHECK(r.run(emit_call(r.keys, mk("a", EventType::Log, 105), params())) != "DUP");
        CHECK(r.zcard(r.keys.released()) == 1);
        CHECK(r.hlen(r.keys.released_data()) == 1);
    }
    SECTION("the registry stream is separate") {
        Emission e = mk("reg", EventType::Log);
        e.stream = evml::core::StreamKind::Registry;
        CHECK(r.run(emit_call(r.keys, e, params())) != "DUP");
        CHECK(r.xlen(r.keys.registry()) == 1);
        CHECK(r.xlen(r.keys.events()) == 0);
    }
}

// 5. Checkpoint fields never move backwards; finalized > safe is an error, never written.
TEST_CASE("redis scripts: checkpoint advance is set-if-greater and keeps finalized <= safe", "[integration][redis]") {
    Rig r;
    CHECK(r.run(ckpt_init_call(r.keys, kNoFence, 100, {})) == "OK");

    CHECK(r.run(ckpt_advance_call(r.keys, kNoFence, CheckpointKind::Safe, 110)) == "OK");
    CHECK(r.run(ckpt_advance_call(r.keys, kNoFence, CheckpointKind::Safe, 105)) == "NOOP");
    CHECK(r.run(ckpt_advance_call(r.keys, kNoFence, CheckpointKind::Safe, 110)) == "NOOP");
    CHECK(*r.hget(r.keys.ckpt(), "safe") == "110");

    CHECK(r.run(ckpt_advance_call(r.keys, kNoFence, CheckpointKind::Finalized, 108)) == "OK");
    CHECK(r.run(ckpt_advance_call(r.keys, kNoFence, CheckpointKind::Finalized, 101)) == "NOOP");
    CHECK(*r.hget(r.keys.ckpt(), "finalized") == "108");

    CHECK_THROWS_WITH(r.runner.run(ckpt_advance_call(r.keys, kNoFence, CheckpointKind::Finalized, 111)),
                      ContainsSubstring("finalized > safe"));
    CHECK(*r.hget(r.keys.ckpt(), "finalized") == "108");  // the violating value was never written
    CHECK(*r.hget(r.keys.ckpt(), "safe") == "110");
}

TEST_CASE("redis scripts: advancing finalized with no safe cursor is an error", "[integration][redis]") {
    Rig r;
    CHECK_THROWS_WITH(r.runner.run(ckpt_advance_call(r.keys, kNoFence, CheckpointKind::Finalized, 5)),
                      ContainsSubstring("finalized > safe"));
    CHECK(r.exists(r.keys.ckpt()) == 0);
}

// 11. ckpt.contracts round-trip; init when ckpt exists.
TEST_CASE("redis scripts: contracts manifest round-trips and init never overwrites", "[integration][redis]") {
    Rig r;
    const std::vector<std::string> manifest = {"0xaaa:52060000:safe", "0xbbb:52060000:finalized"};
    CHECK(r.run(ckpt_init_call(r.keys, kNoFence, 52060000, manifest)) == "OK");
    CHECK(*r.hget(r.keys.ckpt(), "safe") == "52060000");
    CHECK(*r.hget(r.keys.ckpt(), "finalized") == "52060000");
    CHECK(unpack_manifest(*r.hget(r.keys.ckpt(), "contracts")) == manifest);

    const std::vector<std::string> changed = {"0xaaa:52060000:safe"};
    CHECK(r.run(ckpt_init_call(r.keys, kNoFence, 1, changed)) == "EXISTS");
    CHECK(*r.hget(r.keys.ckpt(), "safe") == "52060000");
    CHECK(unpack_manifest(*r.hget(r.keys.ckpt(), "contracts")) == manifest);

    CHECK(r.run(ckpt_contracts_call(r.keys, kNoFence, changed)) == "OK");
    CHECK(unpack_manifest(*r.hget(r.keys.ckpt(), "contracts")) == changed);
}

// 10. emit with a released score, duplicate, final_trim, released_range after a reconnect.
TEST_CASE("redis scripts: tracked emits, final_trim and released_range after a reconnect", "[integration][redis]") {
    Rig r;
    const Emission e100 = mk("e100", EventType::Log, 100, 100);
    const Emission e105 = mk("e105", EventType::Log, 105, 105);
    const Emission e110 = mk("e110", EventType::Log, 110, 110);
    for (const auto* e : {&e100, &e105, &e110}) CHECK(r.run(emit_call(r.keys, *e, params())) != "DUP");

    CHECK(r.xlen(r.keys.events()) == 3);
    CHECK(r.zcard(r.keys.released()) == 3);
    CHECK(r.hlen(r.keys.released_data()) == 3);
    CHECK(*r.hget(r.keys.released_data(), "e105") == pack_released(e105.stream, e105.fields));

    CHECK(r.run(emit_call(r.keys, e105, params())) == "DUP");  // nothing changes
    CHECK(r.xlen(r.keys.events()) == 3);
    CHECK(r.zcard(r.keys.released()) == 3);
    CHECK(r.hlen(r.keys.released_data()) == 3);

    CHECK(r.run(final_trim_call(r.keys, kNoFence, 105)) == "2");  // <= 105: e100 and e105
    CHECK(r.zrange_all(r.keys.released()) == std::vector<std::string>{"e110"});
    CHECK(r.hlen(r.keys.released_data()) == 1);
    CHECK(r.hget(r.keys.released_data(), "e100") == std::nullopt);
    CHECK(r.xlen(r.keys.events()) == 3);  // the stream is not touched by a trim
    CHECK(r.run(final_trim_call(r.keys, kNoFence, 105)) == "0");

    // released_range from a brand new connection: the full before image survives.
    sw::redis::Redis fresh(r.url);
    const auto ids = fresh.command<std::vector<std::string>>("ZRANGEBYSCORE", r.keys.released(), "100", "200");
    REQUIRE(ids == std::vector<std::string>{"e110"});
    const auto data = fresh.command<sw::redis::OptionalString>("HGET", r.keys.released_data(), ids[0]);
    REQUIRE(data.has_value());
    const auto image = unpack_released(*data);
    CHECK(image.fields == e110.fields);
    CHECK(image.stream == e110.stream);
}

// 17. BELOW_FINALIZED.
TEST_CASE("redis scripts: a tracked emit at or below ckpt.finalized is rejected and writes nothing", "[integration][redis]") {
    Rig r;
    CHECK(r.run(ckpt_init_call(r.keys, kNoFence, 100, {})) == "OK");

    CHECK(r.run(emit_call(r.keys, mk("low", EventType::Log, 100, 100), params())) == "BELOW_FINALIZED");
    CHECK(r.xlen(r.keys.events()) == 0);
    CHECK(r.zcard(r.keys.released()) == 0);
    CHECK(r.hlen(r.keys.released_data()) == 0);
    CHECK(r.exists(r.keys.dedup("dedup:low")) == 0);

    CHECK(r.run(emit_call(r.keys, mk("ok", EventType::Log, 101, 101), params())) != "BELOW_FINALIZED");
    CHECK(r.xlen(r.keys.events()) == 1);

    // Untracked (finalized-class) emits carry no score and are never subject to the check.
    CHECK(r.run(emit_call(r.keys, mk("gauge", EventType::Log), params())) != "BELOW_FINALIZED");
    CHECK(r.xlen(r.keys.events()) == 2);
}

// 15. Fencing and the lease scripts.
TEST_CASE("redis scripts: a stale token is fenced on every write script", "[integration][redis]") {
    Rig r;
    const auto reply = r.runner.run(lease_acquire_call(r.keys, "hostA", "n1", 15000));
    REQUIRE(reply.has_value());
    CHECK(*reply == "1|hostA|n1\n");
    const std::string token = "1|hostA|n1";
    CHECK_FALSE(r.runner.run(lease_acquire_call(r.keys, "hostB", "n2", 15000)).has_value());  // a leader exists

    const std::string stale = "9|x|y";
    CHECK(r.run(emit_call(r.keys, mk("a", EventType::Log, 105), params(stale))) == "FENCED");
    CHECK(r.xlen(r.keys.events()) == 0);
    CHECK(r.exists(r.keys.dedup("dedup:a")) == 0);
    CHECK(r.zcard(r.keys.released()) == 0);

    CHECK(r.run(ckpt_init_call(r.keys, stale, 100, {})) == "FENCED");
    CHECK(r.exists(r.keys.ckpt()) == 0);
    CHECK(r.run(final_trim_call(r.keys, stale, 100)) == "FENCED");

    CHECK(r.run(emit_call(r.keys, mk("a", EventType::Log, 105), params(token))) != "FENCED");
    CHECK(r.xlen(r.keys.events()) == 1);
    CHECK(r.run(ckpt_init_call(r.keys, token, 100, {})) == "OK");
    CHECK(r.run(final_trim_call(r.keys, token, 100)) == "0");
}

TEST_CASE("redis scripts: lease renew resumes after a blip and is fenced after a takeover", "[integration][redis]") {
    Rig r;
    const auto a_reply = r.runner.run(lease_acquire_call(r.keys, "hostA", "n1", 15000));
    REQUIRE(a_reply.has_value());
    CHECK(*a_reply == "1|hostA|n1\n");
    const std::string a = "1|hostA|n1";
    CHECK(r.run(lease_renew_call(r.keys, a, 15000)) == "OK");

    // The lease expires and nobody else acquired: the epoch still names A, so A resumes.
    r.redis.command<long long>("DEL", r.keys.leader());
    CHECK(r.run(lease_renew_call(r.keys, a, 15000)) == "RESUMED");
    CHECK(r.redis.command<sw::redis::OptionalString>("GET", r.keys.leader()) == a);

    // The lease expires and B acquires in between: A is fenced for good.
    r.redis.command<long long>("DEL", r.keys.leader());
    const auto b_reply = r.runner.run(lease_acquire_call(r.keys, "hostB", "n2", 15000));
    REQUIRE(b_reply.has_value());
    CHECK(*b_reply == "2|hostB|n2\n1|hostA|n1");  // the epoch number grows and the previous token is returned atomically
    const std::string b = "2|hostB|n2";
    CHECK(r.run(lease_renew_call(r.keys, a, 15000)) == "FENCED");
    CHECK(r.run(emit_call(r.keys, mk("a", EventType::Log), params(a))) == "FENCED");
    CHECK(r.run(emit_call(r.keys, mk("a", EventType::Log), params(b))) != "FENCED");

    // Release by the holder only.
    CHECK(r.run(lease_release_call(r.keys, a)) == "NOOP");
    CHECK(r.run(lease_release_call(r.keys, b)) == "OK");
    CHECK(r.run(lease_release_call(r.keys, b)) == "NOOP");
    CHECK(r.run(lease_renew_call(r.keys, a, 15000)) == "FENCED");
}

// 7. Retention: MINID ~ trims whole stream nodes older than the cutoff.
TEST_CASE("redis scripts: entries older than minid are trimmed, the newest never", "[integration][redis]") {
    Rig r;
    constexpr int kTotal = 300;
    for (int i = 0; i < kTotal - 1; ++i) {
        Emission e = mk("e" + std::to_string(i), EventType::Log);
        e.fields.emplace_back("seq", std::to_string(i));
        CHECK(r.run(emit_call(r.keys, e, params())) != "DUP");
    }
    CHECK(r.xlen(r.keys.events()) == kTotal - 1);  // minid 0 trims nothing

    std::this_thread::sleep_for(std::chrono::milliseconds(20));  // the cutoff must be later than every entry above
    const auto now_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    Emission last = mk("last", EventType::Log);
    last.fields.emplace_back("seq", std::to_string(kTotal - 1));
    CHECK(r.run(emit_call(r.keys, last, params(kNoFence, now_ms))) != "DUP");

    const auto len = r.xlen(r.keys.events());
    CHECK(len < kTotal);  // approximate trimming drops whole nodes, never entries >= minid
    CHECK(len >= 1);

    using Attrs = std::vector<std::pair<std::string, std::string>>;
    using Item = std::pair<std::string, sw::redis::Optional<Attrs>>;
    std::vector<Item> newest;
    r.redis.xrevrange(r.keys.events(), "+", "-", 1, std::back_inserter(newest));
    REQUIRE(newest.size() == 1);
    REQUIRE(newest[0].second.has_value());
    bool found = false;
    for (const auto& [k, v] : *newest[0].second) found = found || (k == "event_id" && v == "last");
    CHECK(found);

    std::vector<Item> oldest;
    r.redis.xrange(r.keys.events(), "-", "+", 1, std::back_inserter(oldest));
    REQUIRE(oldest.size() == 1);
    std::string first_seq;
    for (const auto& [k, v] : *oldest[0].second)
        if (k == "seq") first_seq = v;
    REQUIRE_FALSE(first_seq.empty());
    CHECK(std::stoi(first_seq) >= 1);  // at least the first node is gone
}

// 14. The shebang claim: over maxmemory the script is refused BEFORE it runs, so nothing is half written.
TEST_CASE("redis scripts: over maxmemory emit is refused whole and the retry lands once",
          "[integration][redis][redis_serial]") {
    Rig r;
    const Emission e = mk("oom", EventType::Log, 105);
    {
        MaxMemoryGuard guard(r.redis);
        r.redis.command<void>("CONFIG", "SET", "maxmemory", "1");

        CHECK_THROWS_WITH(r.runner.run(emit_call(r.keys, e, params())), ContainsSubstring("OOM"));
        // Reads are still served; every write key is untouched.
        CHECK(r.exists(r.keys.dedup("dedup:oom")) == 0);
        CHECK(r.xlen(r.keys.events()) == 0);
        CHECK(r.zcard(r.keys.released()) == 0);
        CHECK(r.hlen(r.keys.released_data()) == 0);
    }  // maxmemory restored

    CHECK(r.run(emit_call(r.keys, e, params())) != "DUP");
    CHECK(r.xlen(r.keys.events()) == 1);
    CHECK(r.run(emit_call(r.keys, e, params())) == "DUP");
    CHECK(r.xlen(r.keys.events()) == 1);
}

// Loader: NOSCRIPT after a SCRIPT FLUSH (or a Redis restart) reloads and retries once.
TEST_CASE("redis scripts: the runner reloads the scripts after a SCRIPT FLUSH", "[integration][redis][redis_serial]") {
    Rig r;
    CHECK(r.run(emit_call(r.keys, mk("a", EventType::Log), params())) != "DUP");

    r.redis.script_flush();
    CHECK_FALSE(r.redis.script_exists(r.runner.sha(Script::Emit)));

    CHECK(r.run(emit_call(r.keys, mk("b", EventType::Log), params())) != "DUP");  // reloaded transparently
    CHECK(r.redis.script_exists(r.runner.sha(Script::Emit)));
    CHECK(r.xlen(r.keys.events()) == 2);
}

TEST_CASE("redis scripts: field and manifest codecs reject malformed input", "[unit][redis]") {
    CHECK(unpack_fields(pack_fields({{"a", "1"}, {"b", ""}})) == std::vector<Field>{{"a", "1"}, {"b", ""}});
    CHECK(unpack_fields("[]").empty());
    CHECK_THROWS_WITH(unpack_fields("{"), ContainsSubstring("invalid JSON"));
    CHECK_THROWS_WITH(unpack_fields("{}"), ContainsSubstring("not a JSON array"));
    CHECK_THROWS_WITH(unpack_fields("[[\"a\"]]"), ContainsSubstring("[name, value] string pair"));
    CHECK_THROWS_WITH(unpack_fields("[[\"a\",1]]"), ContainsSubstring("[name, value] string pair"));

    // released_data keeps the stream: it is not derivable from the fields, and a revert must go to the right one.
    for (const auto stream : {evml::core::StreamKind::Events, evml::core::StreamKind::Registry}) {
        const auto image = unpack_released(pack_released(stream, {{"a", "1"}, {"b", ""}}));
        CHECK(image.stream == stream);
        CHECK(image.fields == std::vector<Field>{{"a", "1"}, {"b", ""}});
    }
    CHECK_THROWS_WITH(unpack_released("[]"), ContainsSubstring("not a JSON object"));
    CHECK_THROWS_WITH(unpack_released("{"), ContainsSubstring("invalid JSON"));
    CHECK_THROWS_WITH(unpack_released(R"({"fields":[]})"), ContainsSubstring("needs a string `stream`"));
    CHECK_THROWS_WITH(unpack_released(R"({"stream":"events"})"), ContainsSubstring("needs a string `stream`"));
    CHECK_THROWS_WITH(unpack_released(R"({"stream":"nope","fields":[]})"), ContainsSubstring("unknown stream 'nope'"));
    CHECK_THROWS_WITH(unpack_released(R"({"stream":"events","fields":[["a"]]})"), ContainsSubstring("[name, value] string pair"));
    CHECK(unpack_manifest(pack_manifest({"x", "y"})) == std::vector<std::string>{"x", "y"});
    CHECK_THROWS_WITH(unpack_manifest("[1]"), ContainsSubstring("not a string"));
    CHECK_THROWS_WITH(unpack_manifest("nope"), ContainsSubstring("invalid JSON"));
}

TEST_CASE("redis scripts: call builders reject malformed emissions", "[unit][redis]") {
    const Keys keys("t_x");
    Emission no_fields = mk("a", EventType::Log);
    no_fields.fields.clear();
    CHECK_THROWS_WITH(emit_call(keys, no_fields, params()), ContainsSubstring("has no fields"));

    Emission bad_dedup = mk("a", EventType::Log);
    bad_dedup.dedup_key = "a";
    CHECK_THROWS_WITH(emit_call(keys, bad_dedup, params()), ContainsSubstring("malformed dedup key"));

    CHECK_THROWS_WITH(Keys("a{b}"), ContainsSubstring("no braces"));
    CHECK_THROWS_WITH(Keys(""), ContainsSubstring("non-empty"));
}

TEST_CASE("redis scripts: every embedded script starts with the #!lua shebang", "[unit][redis]") {
    for (std::size_t i = 0; i < kScriptCount; ++i) {
        const std::string src = script_source(static_cast<Script>(i));
        INFO(script_name(static_cast<Script>(i)));
        CHECK(src.rfind("#!lua\n", 0) == 0);
    }
}
