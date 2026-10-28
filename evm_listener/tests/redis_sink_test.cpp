// Unit tests of RedisSink over a fake RedisConn (plan 4 "Tests: Unit"). The fake models the Redis side with the same
// semantics as the Lua scripts (dedup state machine, fencing, set-if-greater checkpoints, released before images) and
// injects faults at chosen positions of a pipeline, so the P1 / A / P3 guarantees are proven without a Redis.
//
// Most tests do not start the sink thread: they drive pump() directly with an injected clock, which makes every
// interleaving deterministic. A few start the thread to exercise the real producer/consumer hand-off.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <chrono>
#include <deque>
#include <filesystem>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include "core/log.hpp"
#include "sink/lease.hpp"
#include "sink/redis_sink.hpp"

using namespace evml::sink;
using evml::core::StreamKind;
using evml::pipeline::Checkpoint;
using evml::pipeline::CheckpointKind;
using evml::pipeline::Emission;
using evml::pipeline::EventType;
using evml::pipeline::Field;
using evml::pipeline::ReleasedEntry;
using Catch::Matchers::ContainsSubstring;
using std::chrono::milliseconds;
namespace net = boost::asio;

namespace {

// ---------------------------------------------------------------------------------------------------------------
// The Redis side
// ---------------------------------------------------------------------------------------------------------------

struct FakeServer {
    Keys keys;
    std::optional<std::string> leader;  // reset() = the lease key expired
    std::string epoch;
    bool scripts_valid{true};           // false = SCRIPT FLUSH happened: every script call answers NOSCRIPT
    std::map<StreamKind, std::vector<std::vector<Field>>> streams;
    std::map<std::string, std::string> dedup;                                // dedup key -> last type
    std::map<std::string, std::pair<std::uint64_t, std::string>> released;  // member -> (score, before image)
    std::optional<std::uint64_t> safe, finalized;
    std::optional<std::string> contracts;
    std::vector<std::string> applied;  // every applied write, in order: "emit:<id>:<type>", "ckpt:safe:5", "trim:5", ...
    std::uint64_t next_id{0};
    std::map<std::string, std::pair<std::string, std::uint32_t>> kv;  // SET key value EX ttl: key -> (value, ttl_s)
    std::uint64_t kv_writes{0};
    std::map<std::string, GroupFacts> group_facts;  // stream key + "|" + group -> what the probe finds; absent = no stream
    std::uint64_t group_reads{0};
    std::vector<std::string> emit_minids;  // ARGV[2] (minid_ms) of every emit that passed the fence

    explicit FakeServer(Keys k) : keys(std::move(k)) {}

    bool fenced(const std::string& token) const { return token != "0" && (!leader || *leader != token); }

    // emit.lua / ckpt.lua / final_trim.lua / lease.lua (plan 4.2), same replies, same order of effects.
    CallOutcome apply(const ScriptCall& c) {
        if (!scripts_valid) return ScriptFail{FailKind::NoScript, "NOSCRIPT No matching script"};
        const auto& a = c.args;
        const auto ok = [](std::string v) { return CallOutcome{ScriptOk{std::move(v)}}; };

        switch (c.script) {
            case Script::Emit: {
                if (fenced(a[6])) return ok("FENCED");
                emit_minids.push_back(a[1]);
                const auto it = dedup.find(c.keys[0]);
                const std::optional<std::string> last = it == dedup.end() ? std::nullopt : std::optional(it->second);
                if (last && *last == a[2]) return ok("DUP");
                if (a[2] == "revert" && !last) return ok("DUP");
                if (!a[3].empty() && finalized && std::stoull(a[3]) <= *finalized) return ok("BELOW_FINALIZED");
                std::vector<Field> fields;
                for (std::size_t i = 7; i + 1 < a.size(); i += 2) fields.emplace_back(a[i], a[i + 1]);
                const StreamKind stream = c.keys[1] == keys.events() ? StreamKind::Events : StreamKind::Registry;
                streams[stream].push_back(std::move(fields));
                if (!a[3].empty()) released[a[4]] = {std::stoull(a[3]), a[5]};
                if (a[2] == "revert") released.erase(a[4]);
                dedup[c.keys[0]] = a[2];
                applied.push_back("emit:" + a[4] + ":" + a[2]);
                return ok(std::to_string(++next_id) + "-0");
            }
            case Script::Ckpt: {
                if (fenced(a[0])) return ok("FENCED");
                if (a[1] == "advance") {
                    const std::uint64_t v = std::stoull(a[3]);
                    auto& slot = a[2] == "safe" ? safe : finalized;
                    if (slot && v <= *slot) return ok("NOOP");
                    const auto s = a[2] == "safe" ? std::optional(v) : safe;
                    const auto f = a[2] == "safe" ? finalized : std::optional(v);
                    if (f && (!s || *f > *s))
                        return ScriptFail{FailKind::Fatal, "ERR ckpt advance: finalized > safe (I2)"};
                    slot = v;
                    applied.push_back("ckpt:" + a[2] + ":" + a[3]);
                    return ok("OK");
                }
                if (a[1] == "init") {
                    if (safe || finalized || contracts) return ok("EXISTS");
                    safe = finalized = std::stoull(a[2]);
                    contracts = a[3];
                    applied.push_back("ckpt:init:" + a[2]);
                    return ok("OK");
                }
                contracts = a[2];
                applied.push_back("ckpt:contracts");
                return ok("OK");
            }
            case Script::FinalTrim: {
                if (fenced(a[0])) return ok("FENCED");
                const std::uint64_t g_f = std::stoull(a[1]);
                std::size_t n = 0;
                for (auto it = released.begin(); it != released.end();) {
                    if (it->second.first <= g_f) {
                        it = released.erase(it);
                        ++n;
                    } else {
                        ++it;
                    }
                }
                applied.push_back("trim:" + a[1]);
                return ok(std::to_string(n));
            }
            case Script::Lease: {
                if (a[0] == "acquire") {
                    if (leader) return CallOutcome{ScriptOk{std::nullopt}};
                    const auto bar = epoch.find('|');
                    const std::uint64_t n = 1 + (epoch.empty() ? 0 : std::stoull(epoch.substr(0, bar)));
                    const std::string token = std::to_string(n) + "|" + a[1] + "|" + a[2];
                    epoch = token;
                    leader = token;
                    return ok(token);
                }
                if (a[0] == "renew") {
                    if (leader && *leader == a[1]) return ok("OK");
                    if (!leader && epoch == a[1]) {
                        leader = a[1];
                        return ok("RESUMED");
                    }
                    return ok("FENCED");
                }
                if (leader && *leader == a[1]) {
                    leader.reset();
                    return ok("OK");
                }
                return ok("NOOP");
            }
        }
        throw std::logic_error("FakeServer: unknown script");
    }

    std::size_t count_entries(StreamKind s, const std::string& id) const {
        const auto it = streams.find(s);
        if (it == streams.end()) return 0;
        return static_cast<std::size_t>(std::count_if(it->second.begin(), it->second.end(), [&](const auto& fields) {
            return std::any_of(fields.begin(), fields.end(),
                               [&](const Field& f) { return f.first == "event_id" && f.second == id; });
        }));
    }
    std::size_t total_entries() const {
        std::size_t n = 0;
        for (const auto& [s, v] : streams) n += v.size();
        return n;
    }
    // Types of the entries written for `id`, in stream order.
    std::vector<std::string> types_of(StreamKind s, const std::string& id) const {
        std::vector<std::string> out;
        const auto it = streams.find(s);
        if (it == streams.end()) return out;
        for (const auto& fields : it->second) {
            bool mine = false;
            std::string type;
            for (const auto& [k, v] : fields) {
                if (k == "event_id" && v == id) mine = true;
                if (k == "type") type = v;
            }
            if (mine) out.push_back(type);
        }
        return out;
    }
};

struct Fault {
    enum class Kind { None, ThrowBefore, ThrowAfter, FailAt } kind{Kind::None};
    std::size_t at{0};                 // position in the pipeline
    FailKind fail{FailKind::Transient};
    bool rest_also{false};             // FailAt: every later call fails too (what a flushed script cache does)
};

// One connection to the FakeServer. `faults` holds one entry per pipeline call (consumed in order).
class FakeConn : public RedisConn {
public:
    explicit FakeConn(FakeServer& s) : server(s) {}

    FakeServer& server;
    bool down{false};
    bool load_fixes_cache{true};
    bool aux_throws{false};  // set_with_ttl / read_group_facts throw a non-ConnError (e.g. an unreadable reply)
    std::deque<Fault> faults;
    std::vector<std::vector<Script>> pipelines;  // script kinds of every pipeline sent, recorded even when it throws
    int loads{0};

    std::vector<CallOutcome> pipeline(const std::vector<ScriptCall>& calls) override {
        std::vector<Script> kinds;
        for (const auto& c : calls) kinds.push_back(c.script);
        pipelines.push_back(std::move(kinds));

        if (down) throw ConnError("fake: connection down");
        Fault f;
        if (!faults.empty()) {
            f = faults.front();
            faults.pop_front();
        }
        if (f.kind == Fault::Kind::ThrowBefore) throw ConnError("fake: dropped before the send");

        std::vector<CallOutcome> out;
        for (std::size_t i = 0; i < calls.size(); ++i) {
            if (f.kind == Fault::Kind::ThrowAfter && i == f.at) throw ConnError("fake: dropped mid-pipeline");
            if (f.kind == Fault::Kind::FailAt && (i == f.at || (f.rest_also && i > f.at))) {
                out.push_back(ScriptFail{f.fail, "fake failure"});  // not applied; later calls may still be (non-prefix)
                continue;
            }
            out.push_back(server.apply(calls[i]));
        }
        if (f.kind == Fault::Kind::ThrowAfter && f.at >= calls.size()) throw ConnError("fake: reply lost after all landed");
        return out;
    }

    void load_scripts() override {
        if (down) throw ConnError("fake: connection down");
        ++loads;
        if (load_fixes_cache) server.scripts_valid = true;
    }

    void set_with_ttl(const std::string& key, const std::string& value, std::uint32_t ttl_s) override {
        if (down) throw ConnError("fake: connection down");
        if (aux_throws) throw std::runtime_error("fake: NOPERM this user has no permissions");
        server.kv[key] = {value, ttl_s};
        ++server.kv_writes;
    }

    GroupFacts read_group_facts(const std::string& stream_key, const std::string& group) override {
        if (down) throw ConnError("fake: connection down");
        if (aux_throws) throw std::runtime_error("fake: XINFO answered a reply we cannot read");
        ++server.group_reads;
        const auto it = server.group_facts.find(stream_key + "|" + group);
        return it == server.group_facts.end() ? GroupFacts{} : it->second;
    }

    Checkpoint read_checkpoint() override {
        if (down) throw ConnError("fake: connection down");
        Checkpoint ck;
        ck.safe = server.safe;
        ck.finalized = server.finalized;
        if (server.contracts) ck.contracts = unpack_manifest(*server.contracts);
        return ck;
    }

    std::vector<ReleasedEntry> read_released(std::uint64_t from, std::uint64_t to) override {
        if (down) throw ConnError("fake: connection down");
        std::vector<std::pair<std::pair<std::uint64_t, std::string>, std::string>> hits;
        for (const auto& [id, v] : server.released)
            if (v.first >= from && v.first <= to) hits.push_back({{v.first, id}, v.second});
        std::sort(hits.begin(), hits.end());
        std::vector<ReleasedEntry> out;
        for (const auto& [key, image_json] : hits) {
            const auto image = unpack_released(image_json);
            out.push_back(ReleasedEntry{key.second, key.first, image.stream, image.fields});
        }
        return out;
    }
};

// ---------------------------------------------------------------------------------------------------------------
// The rig
// ---------------------------------------------------------------------------------------------------------------

struct RigCfg {
    std::uint32_t queue_capacity{100000};
    std::uint64_t spill_max_bytes{1ull << 30};
    std::size_t max_batch{256};
    bool lease{true};
    std::uint32_t heartbeat_ms{0};
    std::vector<evml::core::ConsumerGroupCfg> groups{};
    std::uint64_t wall_ms{1700000000000};
    std::atomic<std::uint64_t>* loop_beat{nullptr};
};

struct Rig {
    using Clock = std::chrono::steady_clock;

    std::filesystem::path dir;
    Keys keys{"shard"};
    FakeServer server{keys};
    FakeConn* data{nullptr};   // owned by the sink
    FakeConn* lease{nullptr};  // owned by the sink
    Clock::time_point t0 = Clock::now();
    milliseconds elapsed{0};
    std::string token;
    std::unique_ptr<RedisSink> sink;

    explicit Rig(const RigCfg& cfg = {}) {
        evml::core::log::init("info");  // the sink logs through core::log, which throws until init() was called
        std::random_device rd;
        dir = std::filesystem::temp_directory_path() / ("evml-sink-test-" + std::to_string(rd()) + std::to_string(rd()));

        if (cfg.lease) {
            const auto got = server.apply(lease_acquire_call(keys, "hostA", "n1", 15000));
            token = *std::get<ScriptOk>(got).value;
        } else {
            token = kNoFence;
        }

        RedisSinkOptions o;
        o.token = token;
        o.host_id = "hostA";
        o.heartbeat_ms = cfg.heartbeat_ms;
        o.consumer_groups = cfg.groups;
        o.retention_ms = 48ull * 3600 * 1000;
        o.dedup_ttl_s = 604800;
        o.queue_capacity = cfg.queue_capacity;
        o.spill_dir = dir;
        o.spill_max_bytes = cfg.spill_max_bytes;
        o.spill_segment_bytes = 64 * 1024;
        o.lease_ttl_ms = 15000;
        o.renew_ms = 5000;
        o.max_batch = cfg.max_batch;
        o.probe_min = milliseconds(0);  // probe on every pump: tests drive time explicitly where it matters
        o.probe_max = milliseconds(0);
        o.steady_now = [this] { return t0 + elapsed; };
        o.wall_ms = [wall = cfg.wall_ms] { return wall; };
        o.loop_beat = cfg.loop_beat;

        auto d = std::make_unique<FakeConn>(server);
        auto l = std::make_unique<FakeConn>(server);
        data = d.get();
        lease = l.get();
        sink = std::make_unique<RedisSink>(std::move(o), keys, std::move(d), std::move(l));
    }
    ~Rig() {
        sink.reset();
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    void advance(milliseconds d) { elapsed += d; }

    // Pumps until nothing is pending or the sink is terminal, advancing the clock one second whenever it idles (so
    // reconnect probes and lease renewals come due). `after_pump` runs after every pump: invariants are checked there.
    template <class F>
    bool settle(F after_pump, int max_iter = 2000) {
        for (int i = 0; i < max_iter; ++i) {
            const bool worked = sink->pump();
            after_pump();
            const auto s = sink->status();
            if (s.pending_writes == 0 || s.mode == SinkMode::Fenced || s.mode == SinkMode::Failed) return true;
            if (!worked) advance(std::chrono::seconds(1));
        }
        return false;
    }
    bool settle(int max_iter = 2000) {
        return settle([] {}, max_iter);
    }
};

Emission mk(const std::string& id, EventType type = EventType::Log, std::optional<std::uint64_t> released = std::nullopt,
            std::uint64_t block = 105, StreamKind stream = StreamKind::Events) {
    Emission e;
    e.stream = stream;
    e.type = type;
    e.event_id = id;
    e.dedup_key = "dedup:" + id;
    e.fields = {{"v", "1"}, {"type", evml::pipeline::to_string(type)}, {"event_id", id}, {"block_number", std::to_string(block)}};
    e.released_block = released;
    return e;
}

std::string id_of(int i) { return "e" + std::to_string(i); }

// Runs `aw` on a private io_context while pumping the sink from this thread (no sink thread): deterministic.
template <class T>
std::optional<T> await_with_pump(RedisSink& sink, net::awaitable<T> aw, int max_iter = 1000) {
    net::io_context io;
    std::optional<T> out;
    bool done = false;
    net::co_spawn(
        io,
        [&]() -> net::awaitable<void> {
            out = co_await std::move(aw);
            done = true;
        },
        net::detached);
    for (int i = 0; i < max_iter && !done; ++i) {
        io.poll();
        sink.pump();
    }
    io.poll();
    REQUIRE(done);
    return out;
}

bool wait_for(const std::function<bool()>& pred, milliseconds timeout = milliseconds(5000)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(milliseconds(1));
    }
    return true;
}

// P1: a checkpoint applied on the server implies every event it covers was applied before it.
void check_ckpt_not_ahead(const FakeServer& s, int events, const std::string& ckpt_entry) {
    const auto at = std::find(s.applied.begin(), s.applied.end(), ckpt_entry);
    if (at == s.applied.end()) return;
    for (int i = 0; i < events; ++i) {
        const auto e = std::find(s.applied.begin(), s.applied.end(), "emit:" + id_of(i) + ":log");
        INFO("checkpoint " << ckpt_entry << " was applied before " << id_of(i));
        REQUIRE((e != s.applied.end() && e < at));
    }
}

}  // namespace

// ---------------------------------------------------------------------------------------------------------------
// Batching and barriers
// ---------------------------------------------------------------------------------------------------------------

TEST_CASE("redis sink: consecutive emits share a pipeline; every other write is a barrier sent alone", "[unit][sink]") {
    Rig r;
    for (int i = 0; i < 3; ++i) r.sink->emit(mk(id_of(i)));
    r.sink->checkpoint(CheckpointKind::Safe, 10);
    r.sink->emit(mk(id_of(3)));
    r.sink->final_trim(5);
    REQUIRE(r.settle());

    using S = Script;
    const std::vector<std::vector<S>> want{{S::Emit, S::Emit, S::Emit}, {S::Ckpt}, {S::Emit}, {S::FinalTrim}};
    CHECK(r.data->pipelines == want);
    CHECK(r.server.applied == std::vector<std::string>{"emit:e0:log", "emit:e1:log", "emit:e2:log", "ckpt:safe:10",
                                                       "emit:e3:log", "trim:5"});
    CHECK(r.sink->mode() == SinkMode::Connected);
    CHECK(r.sink->status().pending_writes == 0);
}

TEST_CASE("redis sink: a batch never exceeds max_batch emits", "[unit][sink]") {
    RigCfg cfg;
    cfg.max_batch = 4;
    Rig r(cfg);
    for (int i = 0; i < 10; ++i) r.sink->emit(mk(id_of(i)));
    REQUIRE(r.settle());
    REQUIRE(r.data->pipelines.size() == 3);
    CHECK(r.data->pipelines[0].size() == 4);
    CHECK(r.data->pipelines[1].size() == 4);
    CHECK(r.data->pipelines[2].size() == 2);
}

TEST_CASE("redis sink: duplicates are counted and are not errors", "[unit][sink]") {
    Rig r;
    r.sink->emit(mk("a"));
    r.sink->emit(mk("a"));
    REQUIRE(r.settle());
    CHECK(r.server.count_entries(StreamKind::Events, "a") == 1);
    CHECK(r.sink->status().dup_total == 1);
    CHECK(r.sink->mode() == SinkMode::Connected);
}

TEST_CASE("redis sink: BELOW_FINALIZED drops the emit, is counted, and does not fail the sink", "[unit][sink]") {
    Rig r;
    r.server.safe = 200;
    r.server.finalized = 100;
    r.sink->emit(mk("low", EventType::Log, 100, 100));
    r.sink->emit(mk("ok", EventType::Log, 150, 150));
    REQUIRE(r.settle());
    CHECK(r.sink->status().below_finalized_total == 1);
    CHECK(r.server.count_entries(StreamKind::Events, "low") == 0);
    CHECK(r.server.count_entries(StreamKind::Events, "ok") == 1);
    CHECK(r.sink->mode() == SinkMode::Connected);
}

// ---------------------------------------------------------------------------------------------------------------
// P1: a checkpoint is never ahead of a missing event
// ---------------------------------------------------------------------------------------------------------------

namespace {

constexpr int kEvents = 5;

void run_p1_scenario(const std::string& label, std::deque<Fault> data_faults, std::size_t expect_dups_at_least = 0) {
    INFO("fault scenario: " << label);
    Rig r;
    r.data->faults = std::move(data_faults);
    for (int i = 0; i < kEvents; ++i) r.sink->emit(mk(id_of(i)));
    r.sink->checkpoint(CheckpointKind::Safe, 99);

    REQUIRE(r.settle([&] { check_ckpt_not_ahead(r.server, kEvents, "ckpt:safe:99"); }));

    // Every event landed exactly once (a re-sent one collapses to DUP), and the checkpoint follows them.
    for (int i = 0; i < kEvents; ++i) CHECK(r.server.count_entries(StreamKind::Events, id_of(i)) == 1);
    CHECK(r.server.safe == 99);
    CHECK(r.sink->mode() == SinkMode::Connected);
    CHECK(r.sink->status().pending_writes == 0);
    CHECK(r.sink->status().spill_records == 0);
    CHECK(r.sink->status().dup_total >= expect_dups_at_least);
}

}  // namespace

TEST_CASE("redis sink P1: an error or timeout at every position of the emit batch re-sends the suffix in order",
          "[unit][sink]") {
    run_p1_scenario("connection dropped before the send", {{Fault::Kind::ThrowBefore}});
    for (std::size_t k = 0; k <= kEvents; ++k)  // k = how many landed before the reply was lost
        run_p1_scenario("reply lost after " + std::to_string(k) + " landed", {{Fault::Kind::ThrowAfter, k}}, k);
    for (std::size_t i = 0; i < kEvents; ++i)  // OOM-style refusal of one call; the calls after it still ran
        run_p1_scenario("transient refusal at " + std::to_string(i), {{Fault::Kind::FailAt, i, FailKind::Transient, false}});
    for (std::size_t i = 0; i < kEvents; ++i)  // flushed script cache: this call and every later one answer NOSCRIPT
        run_p1_scenario("NOSCRIPT from " + std::to_string(i), {{Fault::Kind::FailAt, i, FailKind::NoScript, true}});
}

TEST_CASE("redis sink P1: faults on the checkpoint barrier itself", "[unit][sink]") {
    // The first pipeline (the emits) is healthy; the second (the checkpoint) fails.
    run_p1_scenario("checkpoint dropped before the send", {{Fault::Kind::None}, {Fault::Kind::ThrowBefore}});
    run_p1_scenario("checkpoint reply lost", {{Fault::Kind::None}, {Fault::Kind::ThrowAfter, 1}});
    run_p1_scenario("checkpoint refused", {{Fault::Kind::None}, {Fault::Kind::FailAt, 0, FailKind::Transient, false}});
    run_p1_scenario("checkpoint NOSCRIPT", {{Fault::Kind::None}, {Fault::Kind::FailAt, 0, FailKind::NoScript, true}});
}

TEST_CASE("redis sink P1: a flushed script cache is reloaded and the batch continues", "[unit][sink]") {
    Rig r;
    r.server.scripts_valid = false;  // SCRIPT FLUSH between batches
    for (int i = 0; i < kEvents; ++i) r.sink->emit(mk(id_of(i)));
    r.sink->checkpoint(CheckpointKind::Safe, 99);
    REQUIRE(r.settle([&] { check_ckpt_not_ahead(r.server, kEvents, "ckpt:safe:99"); }));
    CHECK(r.data->loads == 1);
    CHECK(r.server.safe == 99);
    CHECK(r.sink->mode() == SinkMode::Connected);
}

TEST_CASE("redis sink P1: scripts that keep disappearing degrade the sink instead of looping", "[unit][sink]") {
    Rig r;
    r.server.scripts_valid = false;
    r.data->load_fixes_cache = false;  // every reload "succeeds" but the cache stays empty
    r.sink->emit(mk("a"));
    for (int i = 0; i < 6 && r.sink->mode() == SinkMode::Connected; ++i) r.sink->pump();
    CHECK(r.sink->mode() == SinkMode::Degraded);
    CHECK(r.server.total_entries() == 0);

    r.data->load_fixes_cache = true;  // Redis is healthy again
    REQUIRE(r.settle());
    CHECK(r.server.count_entries(StreamKind::Events, "a") == 1);
}

TEST_CASE("redis sink I6: a re-sent log/revert sequence still alternates and ends on its last type", "[unit][sink]") {
    Rig r;
    r.data->faults = {{Fault::Kind::ThrowAfter, 2}};  // the first two landed, the reply was lost
    for (const auto type : {EventType::Log, EventType::Revert, EventType::Log, EventType::Revert}) r.sink->emit(mk("a", type));
    REQUIRE(r.settle());

    // At worst the pair is repeated (consumers are idempotent on event_id), never two equal types in a row.
    const auto types = r.server.types_of(StreamKind::Events, "a");
    REQUIRE(types.size() >= 4);
    CHECK(types.front() == "log");
    CHECK(types.back() == "revert");
    for (std::size_t i = 1; i < types.size(); ++i) CHECK(types[i] != types[i - 1]);
}

// ---------------------------------------------------------------------------------------------------------------
// Outage: spill, replay, order, saturation
// ---------------------------------------------------------------------------------------------------------------

TEST_CASE("redis sink: during an outage items spill in order and replay in order after the reconnect", "[unit][sink]") {
    Rig r;
    r.data->down = true;
    for (int i = 0; i < 3; ++i) r.sink->emit(mk(id_of(i)));
    r.sink->checkpoint(CheckpointKind::Safe, 50);
    r.sink->pump();
    CHECK(r.sink->mode() == SinkMode::Degraded);
    CHECK_FALSE(r.sink->readable());
    CHECK(r.server.applied.empty());

    // New items during the outage queue behind the older ones, never around them.
    for (int i = 3; i < 6; ++i) r.sink->emit(mk(id_of(i)));
    r.sink->checkpoint(CheckpointKind::Safe, 60);
    r.sink->pump();
    CHECK(r.sink->status().spill_records >= 4);
    CHECK(r.server.applied.empty());

    r.data->down = false;
    REQUIRE(r.settle());
    CHECK(r.server.applied == std::vector<std::string>{"emit:e0:log", "emit:e1:log", "emit:e2:log", "ckpt:safe:50",
                                                       "emit:e3:log", "emit:e4:log", "emit:e5:log", "ckpt:safe:60"});
    CHECK(r.sink->mode() == SinkMode::Connected);
    CHECK(r.sink->status().spill_records == 0);
    CHECK(r.sink->status().reconnects_total == 1);
}

TEST_CASE("redis sink: items arriving while replaying go behind the spill", "[unit][sink]") {
    RigCfg cfg;
    cfg.max_batch = 2;
    Rig r(cfg);
    r.data->down = true;
    for (int i = 0; i < 6; ++i) r.sink->emit(mk(id_of(i)));
    r.sink->pump();
    REQUIRE(r.sink->mode() == SinkMode::Degraded);

    r.data->down = false;
    r.sink->pump();  // reconnect probe: renew + reload
    REQUIRE(r.sink->mode() == SinkMode::Replaying);
    r.sink->emit(mk("late"));  // arrives while replaying
    REQUIRE(r.settle());

    const auto& a = r.server.applied;
    REQUIRE(a.size() == 7);
    CHECK(a.back() == "emit:late:log");
    for (int i = 0; i < 6; ++i) CHECK(a[static_cast<std::size_t>(i)] == "emit:" + id_of(i) + ":log");
}

TEST_CASE("redis sink: every kind of item survives the spill round trip", "[unit][sink]") {
    Rig r;
    r.data->down = true;
    r.sink->init_checkpoint(100, {"m1", "m2"});
    r.sink->set_contracts({"c1"});
    r.sink->emit(mk("tracked", EventType::Log, 105, 105, StreamKind::Registry));
    r.sink->emit(mk("plain"));
    r.sink->checkpoint(CheckpointKind::Safe, 110);
    r.sink->final_trim(105);
    r.sink->checkpoint(CheckpointKind::Finalized, 105);
    r.sink->emit(mk("tracked", EventType::Revert, std::nullopt, 105, StreamKind::Registry));
    r.sink->pump();  // the first barrier fails and degrades the sink
    r.sink->pump();  // the next pump moves the rest of the queue to the spill
    REQUIRE(r.sink->mode() == SinkMode::Degraded);
    // The first item sits in the current batch, the rest in the spill.
    REQUIRE(r.sink->status().spill_records >= 5);

    r.data->down = false;
    REQUIRE(r.settle());
    CHECK(r.server.applied == std::vector<std::string>{"ckpt:init:100", "ckpt:contracts", "emit:tracked:log", "emit:plain:log",
                                                       "ckpt:safe:110", "trim:105", "ckpt:finalized:105",
                                                       "emit:tracked:revert"});
    CHECK(r.server.contracts == pack_manifest({"c1"}));
    CHECK(r.server.count_entries(StreamKind::Registry, "tracked") == 2);  // log + revert, in the registry stream
    CHECK(r.server.count_entries(StreamKind::Events, "plain") == 1);
    CHECK(r.server.released.empty());
}

TEST_CASE("redis sink: the spill cap saturates the sink, and draining clears it; items are never refused", "[unit][sink]") {
    RigCfg cfg;
    cfg.spill_max_bytes = 4000;
    Rig r(cfg);
    r.data->down = true;
    r.sink->emit(mk("first"));
    r.sink->pump();
    REQUIRE(r.sink->mode() == SinkMode::Degraded);
    CHECK_FALSE(r.sink->saturated());

    int n = 0;
    while (!r.sink->saturated() && n < 1000) {
        r.sink->emit(mk(id_of(++n)));
        r.sink->pump();
    }
    REQUIRE(r.sink->saturated());
    CHECK(r.sink->status().spill_bytes >= 4000);
    r.sink->emit(mk("over"));  // the pipeline may still hand over what it was mid-way through: accepted, not dropped
    r.sink->pump();

    r.data->down = false;
    REQUIRE(r.settle());
    CHECK_FALSE(r.sink->saturated());
    CHECK(r.server.count_entries(StreamKind::Events, "over") == 1);
    CHECK(r.server.total_entries() == static_cast<std::size_t>(n) + 2);  // first + n + over
}

TEST_CASE("redis sink: the queue soft cap saturates until the sink thread drains it", "[unit][sink]") {
    RigCfg cfg;
    cfg.queue_capacity = 3;
    Rig r(cfg);
    r.sink->emit(mk("a"));
    r.sink->emit(mk("b"));
    CHECK_FALSE(r.sink->saturated());
    r.sink->emit(mk("c"));
    CHECK(r.sink->saturated());
    r.sink->emit(mk("d"));  // still accepted
    REQUIRE(r.settle());
    CHECK_FALSE(r.sink->saturated());
    CHECK(r.server.total_entries() == 4);
}

// ---------------------------------------------------------------------------------------------------------------
// Reads (rev 6, A)
// ---------------------------------------------------------------------------------------------------------------

TEST_CASE("redis sink A: a read sees every write enqueued before it", "[unit][sink]") {
    Rig r;
    r.server.safe = 0;
    r.server.finalized = 0;
    r.sink->emit(mk("a", EventType::Log, 100, 100));
    r.sink->emit(mk("b", EventType::Log, 110, 110, StreamKind::Registry));
    r.sink->checkpoint(CheckpointKind::Safe, 120);

    const auto got = await_with_pump(*r.sink, r.sink->released_range(0, 1000));
    REQUIRE(got.has_value());
    REQUIRE(got->has_value());
    const auto& entries = **got;
    REQUIRE(entries.size() == 2);
    CHECK(entries[0].event_id == "a");
    CHECK(entries[0].block_number == 100);
    CHECK(entries[0].stream == StreamKind::Events);
    CHECK(entries[1].event_id == "b");
    CHECK(entries[1].stream == StreamKind::Registry);  // the stream survives the before image
    CHECK(entries[1].fields == mk("b", EventType::Log, 110, 110).fields);
    CHECK(r.server.safe == 120);  // the checkpoint enqueued before the read was applied before it, too
}

TEST_CASE("redis sink A: load_checkpoint returns the stored checkpoint, or an empty one on a fresh store", "[unit][sink]") {
    Rig r;
    const auto fresh = await_with_pump(*r.sink, r.sink->load_checkpoint());
    REQUIRE(fresh->has_value());
    CHECK_FALSE((*fresh)->safe.has_value());
    CHECK_FALSE((*fresh)->finalized.has_value());
    CHECK_FALSE((*fresh)->contracts.has_value());

    r.sink->init_checkpoint(100, {"x", "y"});
    r.sink->checkpoint(CheckpointKind::Safe, 130);
    const auto ck = await_with_pump(*r.sink, r.sink->load_checkpoint());
    REQUIRE(ck->has_value());
    CHECK((*ck)->safe == 130);
    CHECK((*ck)->finalized == 100);
    CHECK((*ck)->contracts == std::vector<std::string>{"x", "y"});
}

TEST_CASE("redis sink A: reads resolve to nullopt while the sink is degraded or replaying", "[unit][sink]") {
    Rig r;
    r.data->down = true;
    r.sink->emit(mk("a"));
    r.sink->pump();
    REQUIRE(r.sink->mode() == SinkMode::Degraded);

    const auto released = await_with_pump(*r.sink, r.sink->released_range(0, 10));
    REQUIRE(released.has_value());
    CHECK_FALSE(released->has_value());
    const auto ck = await_with_pump(*r.sink, r.sink->load_checkpoint());
    CHECK_FALSE(ck->has_value());
}

TEST_CASE("redis sink A: a read already queued when Redis dies resolves to nullopt, never to a partial answer", "[unit][sink]") {
    net::io_context io;  // declared before the rig: the sink's stop() abandons reads on this context's channel service
    Rig r;
    r.sink->emit(mk("a", EventType::Log, 100, 100));

    std::optional<std::optional<std::vector<ReleasedEntry>>> result;
    net::co_spawn(
        io,
        [&]() -> net::awaitable<void> {
            result = co_await r.sink->released_range(0, 1000);
        },
        net::detached);
    io.poll();  // the coroutine queued the read behind the emit and is waiting

    r.data->down = true;  // Redis dies before the sink thread gets to the batch
    for (int i = 0; i < 10 && !result; ++i) {
        r.sink->pump();
        io.poll();
    }
    REQUIRE(result.has_value());
    CHECK_FALSE(result->has_value());
    CHECK(r.sink->mode() == SinkMode::Degraded);
}

TEST_CASE("redis sink A: a read that fails on the wire degrades the sink and resolves to nullopt", "[unit][sink]") {
    Rig r;
    FakeConn* data = r.data;
    // Make only the read fail: the connection answers pipelines but not reads.
    struct ReadFails : FakeConn {
        using FakeConn::FakeConn;
        Checkpoint read_checkpoint() override { throw ConnError("fake: read timed out"); }
    };
    auto d = std::make_unique<ReadFails>(r.server);
    auto l = std::make_unique<FakeConn>(r.server);
    (void)data;
    RedisSinkOptions o;
    o.token = r.token;
    o.retention_ms = 1000;
    o.dedup_ttl_s = 10;
    o.queue_capacity = 10;
    o.spill_dir = r.dir / "second";
    o.spill_max_bytes = 1 << 20;
    o.lease_ttl_ms = 15000;
    o.renew_ms = 5000;
    o.probe_min = o.probe_max = std::chrono::hours(1);  // stay Degraded: only reads fail here, a probe would reconnect
    o.steady_now = [&r] { return r.t0 + r.elapsed; };
    RedisSink sink(std::move(o), r.keys, std::move(d), std::move(l));

    const auto ck = await_with_pump(sink, sink.load_checkpoint());
    REQUIRE(ck.has_value());
    CHECK_FALSE(ck->has_value());
    CHECK(sink.mode() == SinkMode::Degraded);
}

// ---------------------------------------------------------------------------------------------------------------
// Fencing (rev 6, P3)
// ---------------------------------------------------------------------------------------------------------------

TEST_CASE("redis sink P3: a write fenced by an expired lease renews, resumes and continues", "[unit][sink]") {
    Rig r;
    r.server.leader.reset();  // the lease key expired, but nobody took over: epoch still names us
    for (int i = 0; i < 3; ++i) r.sink->emit(mk(id_of(i)));
    r.sink->checkpoint(CheckpointKind::Safe, 9);
    REQUIRE(r.settle());

    CHECK(r.server.leader == r.token);  // renew answered RESUMED and restored the key
    CHECK(r.server.safe == 9);
    for (int i = 0; i < 3; ++i) CHECK(r.server.count_entries(StreamKind::Events, id_of(i)) == 1);
    CHECK(r.sink->mode() == SinkMode::Connected);
    CHECK(r.sink->status().discarded_total == 0);
}

TEST_CASE("redis sink P3: a write fenced by another term discards the queue and spill and goes Fenced", "[unit][sink]") {
    Rig r;
    r.server.leader = "2|hostB|n2";  // another host acquired the lease
    r.server.epoch = "2|hostB|n2";
    for (int i = 0; i < 4; ++i) r.sink->emit(mk(id_of(i)));
    r.sink->checkpoint(CheckpointKind::Safe, 9);
    r.sink->pump();

    CHECK(r.sink->mode() == SinkMode::Fenced);
    CHECK(r.sink->saturated());
    CHECK_FALSE(r.sink->readable());
    CHECK(r.server.applied.empty());  // nothing from the old term reached Redis
    CHECK(r.sink->status().discarded_total == 5);
    CHECK(r.sink->status().pending_writes == 0);

    // Whatever the pipeline still hands over is counted, never silently lost and never written.
    r.sink->emit(mk("late"));
    CHECK(r.sink->status().discarded_total == 6);
    r.sink->pump();
    CHECK(r.server.applied.empty());
}

TEST_CASE("redis sink P3: an old-term spill is never replayed after the lease was taken over during an outage", "[unit][sink]") {
    Rig r;
    r.data->down = true;
    for (int i = 0; i < 6; ++i) r.sink->emit(mk(id_of(i)));
    r.sink->pump();
    REQUIRE(r.sink->mode() == SinkMode::Degraded);

    r.server.leader = "2|hostB|n2";  // the lease expired during the outage and hostB took it
    r.server.epoch = "2|hostB|n2";
    r.data->down = false;
    REQUIRE(r.settle());  // the first op after the reconnect is a renew: it answers FENCED

    CHECK(r.sink->mode() == SinkMode::Fenced);
    CHECK(r.sink->status().discarded_total == 6);
    CHECK(r.sink->status().spill_records == 0);
    CHECK(r.server.applied.empty());
}

TEST_CASE("redis sink P3: an outage the lease survives replays the spill (RESUMED)", "[unit][sink]") {
    Rig r;
    r.data->down = true;
    for (int i = 0; i < 3; ++i) r.sink->emit(mk(id_of(i)));
    r.sink->pump();
    REQUIRE(r.sink->mode() == SinkMode::Degraded);

    r.server.leader.reset();  // the key expired, nobody took it
    r.data->down = false;
    REQUIRE(r.settle());
    CHECK(r.sink->mode() == SinkMode::Connected);
    CHECK(r.server.leader == r.token);
    for (int i = 0; i < 3; ++i) CHECK(r.server.count_entries(StreamKind::Events, id_of(i)) == 1);
}

TEST_CASE("redis sink P3: the lease is renewed on schedule on its own connection, and a takeover is noticed without writes",
          "[unit][sink]") {
    Rig r;
    r.sink->pump();
    CHECK(r.lease->pipelines.empty());  // not due yet

    r.advance(milliseconds(5000));
    r.sink->pump();
    REQUIRE(r.lease->pipelines.size() == 1);
    CHECK(r.lease->pipelines[0] == std::vector<Script>{Script::Lease});
    CHECK(r.data->pipelines.empty());  // renewals never use the data connection

    r.advance(milliseconds(4999));
    r.sink->pump();
    CHECK(r.lease->pipelines.size() == 1);  // still not due

    r.server.leader = "2|hostB|n2";
    r.server.epoch = "2|hostB|n2";
    r.advance(milliseconds(1));
    r.sink->pump();
    CHECK(r.sink->mode() == SinkMode::Fenced);
}

TEST_CASE("redis sink P3: a failed renewal is counted, not fatal, and retried on schedule", "[unit][sink]") {
    Rig r;
    r.lease->down = true;
    r.advance(milliseconds(5000));
    r.sink->pump();
    CHECK(r.sink->status().renew_failures_total == 1);
    CHECK(r.sink->mode() == SinkMode::Connected);

    r.lease->down = false;
    r.advance(milliseconds(5000));
    r.sink->pump();
    CHECK(r.sink->status().renew_failures_total == 1);
    CHECK(r.sink->mode() == SinkMode::Connected);
}

TEST_CASE("redis sink Q1: with the lease disabled the token is 0, nothing is renewed, and a spill always replays", "[unit][sink]") {
    RigCfg cfg;
    cfg.lease = false;
    Rig r(cfg);
    CHECK(r.token == kNoFence);
    r.data->down = true;
    r.sink->emit(mk("a"));
    r.sink->pump();
    REQUIRE(r.sink->mode() == SinkMode::Degraded);

    r.data->down = false;
    r.advance(std::chrono::minutes(1));
    REQUIRE(r.settle());
    CHECK(r.server.count_entries(StreamKind::Events, "a") == 1);
    CHECK(r.lease->pipelines.empty());  // no renewals at all
    CHECK(r.sink->mode() == SinkMode::Connected);
}

// ---------------------------------------------------------------------------------------------------------------
// Fail fast
// ---------------------------------------------------------------------------------------------------------------

TEST_CASE("redis sink: a script error reply is fatal, loud, and never retried", "[unit][sink]") {
    Rig r;
    r.sink->checkpoint(CheckpointKind::Finalized, 50);  // finalized with no safe cursor: ckpt.lua refuses (I2)
    r.sink->emit(mk("after"));
    r.sink->pump();

    CHECK(r.sink->mode() == SinkMode::Failed);
    CHECK_THAT(r.sink->fatal_message(), ContainsSubstring("finalized > safe"));
    CHECK_THAT(r.sink->fatal_message(), ContainsSubstring("ckpt finalized 50"));
    CHECK(r.sink->saturated());
    CHECK_FALSE(r.sink->readable());
    CHECK(r.server.total_entries() == 0);  // the emit behind the failed barrier was not sent either

    const auto sent = r.data->pipelines.size();
    r.sink->pump();
    r.sink->emit(mk("later"));
    r.sink->pump();
    CHECK(r.data->pipelines.size() == sent);  // not retried
    CHECK(r.sink->status().discarded_total >= 1);
}

TEST_CASE("redis sink: a pipeline that answers the wrong number of outcomes is a contract violation", "[unit][sink]") {
    struct ShortConn : FakeConn {
        using FakeConn::FakeConn;
        std::vector<CallOutcome> pipeline(const std::vector<ScriptCall>&) override { return {}; }
    };
    Rig r;
    RedisSinkOptions o;
    o.token = kNoFence;
    o.retention_ms = 1000;
    o.dedup_ttl_s = 10;
    o.queue_capacity = 10;
    o.spill_dir = r.dir / "short";
    o.spill_max_bytes = 1 << 20;
    RedisSink sink(std::move(o), r.keys, std::make_unique<ShortConn>(r.server), std::make_unique<FakeConn>(r.server));
    sink.emit(mk("a"));
    sink.pump();
    CHECK(sink.mode() == SinkMode::Failed);
    CHECK_THAT(sink.fatal_message(), ContainsSubstring("0 outcomes for 1 calls"));
}

// ---------------------------------------------------------------------------------------------------------------
// Threading, options
// ---------------------------------------------------------------------------------------------------------------

TEST_CASE("redis sink: the sink thread drains what producers enqueue and serves reads", "[unit][sink]") {
    Rig r;
    r.server.safe = 0;
    r.server.finalized = 0;
    r.sink->start();
    for (int i = 0; i < 1000; ++i) r.sink->emit(mk(id_of(i), EventType::Log, 100 + static_cast<std::uint64_t>(i), 100 + static_cast<std::uint64_t>(i)));
    r.sink->checkpoint(CheckpointKind::Safe, 5000);
    REQUIRE(r.sink->wait_idle(milliseconds(5000)));
    CHECK(r.server.total_entries() == 1000);
    CHECK(r.server.safe == 5000);

    net::io_context io;
    auto fut = net::co_spawn(io, r.sink->released_range(100, 109), net::use_future);
    io.run();
    const auto got = fut.get();
    REQUIRE(got.has_value());
    CHECK(got->size() == 10);

    CHECK(wait_for([&] { return r.sink->loop_heartbeat_ms() > 0; }));
    r.sink->stop();
}

TEST_CASE("redis sink: the sink loop stores its beat into the watchdog-owned slot", "[unit][sink]") {
    std::atomic<std::uint64_t> slot{0};
    RigCfg cfg;
    cfg.loop_beat = &slot;
    Rig r(cfg);
    CHECK(slot.load() == 0);
    r.sink->start();
    CHECK(wait_for([&] { return slot.load() > 0; }));
    CHECK(slot.load() == r.sink->loop_heartbeat_ms());  // same value as the sink's own counter at that iteration
    r.sink->stop();
}

TEST_CASE("redis sink: stop() resolves a queued read to nullopt instead of leaving it hanging", "[unit][sink]") {
    Rig r;
    net::io_context io;
    std::optional<std::optional<Checkpoint>> result;
    net::co_spawn(
        io,
        [&]() -> net::awaitable<void> {
            result = co_await r.sink->load_checkpoint();
        },
        net::detached);
    io.poll();  // queued, never pumped
    CHECK_FALSE(result.has_value());
    r.sink->stop();
    io.poll();
    REQUIRE(result.has_value());
    CHECK_FALSE(result->has_value());
    CHECK_FALSE(r.sink->readable());
}

TEST_CASE("redis sink: wait_idle times out while work is pending and returns once it is acked", "[unit][sink]") {
    Rig r;
    r.sink->emit(mk("a"));
    CHECK_FALSE(r.sink->wait_idle(milliseconds(5)));
    REQUIRE(r.settle());
    CHECK(r.sink->wait_idle(milliseconds(5)));
}

TEST_CASE("redis sink: options are validated", "[unit][sink]") {
    const auto make = [](auto&& mutate) {
        Rig r;
        RedisSinkOptions o;
        o.token = r.token;
        o.retention_ms = 1000;
        o.dedup_ttl_s = 10;
        o.queue_capacity = 10;
        o.spill_dir = r.dir / "opts";
        o.spill_max_bytes = 1 << 20;
        o.lease_ttl_ms = 15000;
        o.renew_ms = 5000;
        mutate(o);
        return RedisSink(std::move(o), r.keys, std::make_unique<FakeConn>(r.server), std::make_unique<FakeConn>(r.server));
    };
    CHECK_NOTHROW(make([](RedisSinkOptions&) {}));
    CHECK_THROWS_WITH(make([](RedisSinkOptions& o) { o.queue_capacity = 0; }), ContainsSubstring("queue_capacity"));
    CHECK_THROWS_WITH(make([](RedisSinkOptions& o) { o.max_batch = 0; }), ContainsSubstring("max_batch"));
    CHECK_THROWS_WITH(make([](RedisSinkOptions& o) { o.retention_ms = 0; }), ContainsSubstring("retention_ms"));
    CHECK_THROWS_WITH(make([](RedisSinkOptions& o) { o.dedup_ttl_s = 0; }), ContainsSubstring("dedup_ttl_s"));
    CHECK_THROWS_WITH(make([](RedisSinkOptions& o) { o.token.clear(); }), ContainsSubstring("token"));
    CHECK_THROWS_WITH(make([](RedisSinkOptions& o) { o.renew_ms = o.lease_ttl_ms; }), ContainsSubstring("renew_ms"));
    CHECK_THROWS_WITH(make([](RedisSinkOptions& o) {
                          o.host_id = "hostA";
                          o.heartbeat_ms = 10000;
                          o.heartbeat_ttl_s = 0;  // redis++ would set no expiry at all
                      }),
                      ContainsSubstring("heartbeat_ttl_s"));
    CHECK_THROWS_WITH(make([](RedisSinkOptions& o) {
                          o.host_id = "hostA";
                          o.heartbeat_ms = 10000;
                          o.heartbeat_ttl_s = 10;  // equal to the interval: the key would flap
                      }),
                      ContainsSubstring("heartbeat_ttl_s"));
    CHECK_NOTHROW(make([](RedisSinkOptions& o) {
        o.host_id = "hostA";
        o.heartbeat_ms = 10000;
        o.heartbeat_ttl_s = 30;
    }));
}

TEST_CASE("redis sink: from_config copies [sink] and [lease] and refuses a token that contradicts [lease] enabled", "[unit][sink]") {
    evml::core::SinkCfg sink;
    sink.retention_ms = 172800000;
    sink.dedup_ttl_s = 604800;
    sink.queue_capacity = 100000;
    sink.spill_dir = "/tmp/evml-spill";
    sink.spill_max_bytes = 536870912;
    evml::core::LeaseCfg lease;
    lease.enabled = true;
    lease.ttl_ms = 15000;
    lease.renew_ms = 5000;

    const auto o = RedisSinkOptions::from_config(sink, lease, "1|a|n");
    CHECK(o.token == "1|a|n");
    CHECK(o.retention_ms == 172800000);
    CHECK(o.dedup_ttl_s == 604800);
    CHECK(o.queue_capacity == 100000);
    CHECK(o.spill_dir == "/tmp/evml-spill");
    CHECK(o.spill_max_bytes == 536870912);
    CHECK(o.lease_ttl_ms == 15000);
    CHECK(o.renew_ms == 5000);

    CHECK_THROWS_WITH(RedisSinkOptions::from_config(sink, lease, kNoFence), ContainsSubstring("enabled but the token disables fencing"));
    lease.enabled = false;
    CHECK_THROWS_WITH(RedisSinkOptions::from_config(sink, lease, "1|a|n"), ContainsSubstring("disabled but a lease token"));
    CHECK(RedisSinkOptions::from_config(sink, lease, kNoFence).token == kNoFence);
}

// ---------------------------------------------------------------------------------------------------------------
// Heartbeat (plan 4.7), drain and lease lifecycle (plan 4.8)
// ---------------------------------------------------------------------------------------------------------------

TEST_CASE("redis sink heartbeat: written on the lease connection at the interval, not before it is due", "[unit][sink]") {
    Rig r(RigCfg{.heartbeat_ms = 1000});
    r.sink->set_heartbeat("{\"state\":\"ok\"}");

    r.sink->pump();
    REQUIRE(r.server.kv_writes == 1);
    const auto it = r.server.kv.find(r.keys.hb("hostA"));
    REQUIRE(it != r.server.kv.end());
    CHECK(it->second.first == "{\"state\":\"ok\"}");
    CHECK(it->second.second == 30);

    r.advance(milliseconds(999));
    r.sink->pump();
    CHECK(r.server.kv_writes == 1);  // not due yet
    r.advance(milliseconds(1));
    r.sink->pump();
    CHECK(r.server.kv_writes == 2);

    r.sink->set_heartbeat("{\"state\":\"newer\"}");
    r.advance(milliseconds(1000));
    r.sink->pump();
    CHECK(r.server.kv[r.keys.hb("hostA")].first == "{\"state\":\"newer\"}");
}

TEST_CASE("redis sink heartbeat: nothing is written without a payload, with interval 0, or while Degraded", "[unit][sink]") {
    SECTION("no payload yet") {
        Rig r(RigCfg{.heartbeat_ms = 1000});
        r.sink->pump();
        CHECK(r.server.kv_writes == 0);
    }
    SECTION("interval 0 disables it") {
        Rig r;
        r.sink->set_heartbeat("x");
        r.advance(milliseconds(10000));
        r.sink->pump();
        CHECK(r.server.kv_writes == 0);
    }
    SECTION("Degraded") {
        Rig r(RigCfg{.heartbeat_ms = 1000});
        r.sink->emit(mk(id_of(0)));
        r.data->down = true;
        r.advance(milliseconds(1));
        r.sink->pump();  // the first failure degrades (no payload yet: the due heartbeat writes nothing)
        REQUIRE(r.sink->mode() == SinkMode::Degraded);
        r.sink->set_heartbeat("x");
        r.advance(milliseconds(1000));
        r.sink->pump();
        CHECK(r.server.kv_writes == 0);
    }
}

TEST_CASE("redis sink heartbeat: a failed write is counted, not fatal", "[unit][sink]") {
    Rig r(RigCfg{.heartbeat_ms = 1000});
    r.sink->set_heartbeat("x");
    r.lease->down = true;
    r.sink->pump();
    CHECK(r.sink->status().heartbeat_failures_total == 1);
    CHECK(r.sink->mode() == SinkMode::Connected);

    r.lease->down = false;
    r.advance(milliseconds(1000));
    r.sink->pump();
    CHECK(r.server.kv_writes == 1);
    CHECK(r.sink->status().heartbeat_failures_total == 1);
}

TEST_CASE("redis sink heartbeat: an error that is not a connection error is counted, logged and never fails the sink", "[unit][sink]") {
    Rig r(RigCfg{.heartbeat_ms = 1000});
    r.sink->set_heartbeat("x");
    r.lease->aux_throws = true;
    r.sink->pump();
    CHECK(r.sink->status().heartbeat_failures_total == 1);
    CHECK(r.sink->mode() == SinkMode::Connected);

    r.sink->emit(mk(id_of(0)));  // the writer is unaffected
    REQUIRE(r.settle());
    CHECK(r.server.count_entries(StreamKind::Events, id_of(0)) == 1);
}

TEST_CASE("redis sink emit: MINID is the listener wall clock minus retention, and never below 0", "[unit][sink]") {
    SECTION("normal") {
        Rig r;  // wall 1700000000000, retention 48 h = 172800000 ms
        r.sink->emit(mk(id_of(0)));
        REQUIRE(r.settle());
        REQUIRE(r.server.emit_minids.size() == 1);
        CHECK(r.server.emit_minids[0] == "1699827200000");
    }
    SECTION("clock younger than the retention window clamps to 0") {
        Rig r(RigCfg{.wall_ms = 1000});
        r.sink->emit(mk(id_of(0)));
        REQUIRE(r.settle());
        REQUIRE(r.server.emit_minids.size() == 1);
        CHECK(r.server.emit_minids[0] == "0");
    }
}

TEST_CASE("redis sink drain: flushed writes then the lease is released for the standby", "[unit][sink]") {
    Rig r;
    for (int i = 0; i < 3; ++i) r.sink->emit(mk(id_of(i)));
    REQUIRE(r.settle());
    REQUIRE(r.server.leader == r.token);

    const auto d = r.sink->drain(milliseconds(0));
    CHECK(d.flushed);
    CHECK(d.unflushed == 0);
    CHECK(d.lease_released);
    CHECK_FALSE(r.server.leader.has_value());
}

TEST_CASE("redis sink drain: with Redis down it reports what was left; the lease is still released", "[unit][sink]") {
    Rig r;
    r.sink->emit(mk(id_of(0)));
    r.sink->emit(mk(id_of(1)));
    r.data->down = true;
    r.sink->pump();
    REQUIRE(r.sink->mode() == SinkMode::Degraded);

    const auto d = r.sink->drain(milliseconds(0));
    CHECK_FALSE(d.flushed);
    CHECK(d.unflushed == 2);
    CHECK(d.lease_released);  // the lease connection is separate and still works
}

TEST_CASE("redis sink drain: a lease that was lost is not released", "[unit][sink]") {
    Rig r;
    r.server.leader = "2|hostB|n2";
    r.server.epoch = "2|hostB|n2";
    r.sink->emit(mk(id_of(0)));
    r.sink->pump();
    REQUIRE(r.sink->mode() == SinkMode::Fenced);

    const auto d = r.sink->drain(milliseconds(0));
    CHECK_FALSE(d.lease_released);
    CHECK(r.server.leader == "2|hostB|n2");  // hostB keeps its lease
}

TEST_CASE("redis sink drain: with the lease disabled there is nothing to release", "[unit][sink]") {
    Rig r(RigCfg{.lease = false});
    r.sink->emit(mk(id_of(0)));
    REQUIRE(r.settle());
    const auto d = r.sink->drain(milliseconds(0));
    CHECK(d.flushed);
    CHECK_FALSE(d.lease_released);
}

TEST_CASE("redis sink options: a heartbeat needs a host id", "[unit][sink]") {
    RedisSinkOptions o;
    o.token = kNoFence;
    o.retention_ms = 1000;
    o.dedup_ttl_s = 10;
    o.queue_capacity = 10;
    o.heartbeat_ms = 1000;
    Keys keys{"shard"};
    FakeServer server{keys};
    CHECK_THROWS_WITH(RedisSink(o, keys, std::make_unique<FakeConn>(server), std::make_unique<FakeConn>(server)),
                      ContainsSubstring("host_id"));
}

TEST_CASE("lease: try_acquire answers a token, or nullopt while another host leads", "[unit][sink]") {
    evml::core::log::init("info");
    Keys keys{"shard"};
    FakeServer server{keys};
    FakeConn conn{server};

    CHECK(try_acquire_lease(conn, keys, "hostA", "n1", 15000) == "1|hostA|n1");
    CHECK_FALSE(try_acquire_lease(conn, keys, "hostB", "n2", 15000).has_value());
    server.leader.reset();  // hostA's key expired
    CHECK(try_acquire_lease(conn, keys, "hostB", "n2", 15000) == "2|hostB|n2");
}

TEST_CASE("lease: a flushed script cache is reloaded once; a down connection throws", "[unit][sink]") {
    evml::core::log::init("info");
    Keys keys{"shard"};
    FakeServer server{keys};
    FakeConn conn{server};

    server.scripts_valid = false;
    CHECK(try_acquire_lease(conn, keys, "hostA", "n1", 15000) == "1|hostA|n1");
    CHECK(conn.loads == 1);

    conn.down = true;
    CHECK_THROWS_AS(try_acquire_lease(conn, keys, "hostB", "n2", 15000), ConnError);
    CHECK_THROWS_AS(release_lease(conn, keys, "1|hostA|n1"), ConnError);
}

TEST_CASE("lease: release answers true once, then false (it is no longer ours)", "[unit][sink]") {
    evml::core::log::init("info");
    Keys keys{"shard"};
    FakeServer server{keys};
    FakeConn conn{server};
    const auto token = try_acquire_lease(conn, keys, "hostA", "n1", 15000);
    REQUIRE(token.has_value());

    CHECK(release_lease(conn, keys, *token));
    CHECK_FALSE(server.leader.has_value());
    CHECK_FALSE(release_lease(conn, keys, *token));
}

TEST_CASE("lease: the standby asks again until the leader lets go, and never writes", "[unit][sink]") {
    evml::core::log::init("info");
    Keys keys{"shard"};
    FakeServer server{keys};
    FakeConn conn{server};
    server.leader = "1|hostA|n1";
    server.epoch = "1|hostA|n1";

    int sleeps = 0;
    AcquireOptions o;
    o.host_id = "hostB";
    o.ttl_ms = 15000;
    o.poll = milliseconds(1000);
    o.nonce = [] { return std::string("n2"); };
    o.sleep = [&](milliseconds d) {
        CHECK(d == milliseconds(1000));
        if (++sleeps == 3) server.leader.reset();
    };

    CHECK(acquire_lease_blocking(conn, keys, o) == "2|hostB|n2");
    CHECK(sleeps == 3);
}

TEST_CASE("lease: the standby gives up (nullopt) only when stop fires; an unreachable Redis is retried", "[unit][sink]") {
    evml::core::log::init("info");
    Keys keys{"shard"};
    FakeServer server{keys};
    FakeConn conn{server};
    server.leader = "1|hostA|n1";
    server.epoch = "1|hostA|n1";

    int sleeps = 0;
    AcquireOptions o;
    o.host_id = "hostB";
    o.ttl_ms = 15000;
    o.probe_min = milliseconds(0);
    o.probe_max = milliseconds(0);
    o.sleep = [&](milliseconds) {
        if (++sleeps == 2) conn.down = true;
    };
    o.stop = [&] { return sleeps >= 4; };

    CHECK_FALSE(acquire_lease_blocking(conn, keys, o).has_value());
    CHECK(sleeps == 4);

    AcquireOptions bad;
    bad.host_id = "";
    bad.ttl_ms = 1;
    CHECK_THROWS_AS(acquire_lease_blocking(conn, keys, bad), std::invalid_argument);
}

TEST_CASE("lease: disabled mode hands out the no-fence token", "[unit][sink]") {
    evml::core::log::init("info");
    CHECK(lease_disabled_token() == kNoFence);
}

// ---------------------------------------------------------------------------------------------------------------
// Consumer-group lag probe (plan 4.10)
// ---------------------------------------------------------------------------------------------------------------

namespace {
const evml::core::ConsumerGroupCfg kBot{StreamKind::Events, "bot"};

GroupFacts lag_facts(const char* first_undelivered, std::uint64_t now_ms) {
    GroupFacts f;
    f.stream_exists = true;
    f.group_found = true;
    f.last_delivered_id = "1700000000000-0";
    f.lag = 3;
    f.max_deleted_id = "0-0";
    f.first_undelivered_id = first_undelivered;
    f.now_ms = now_ms;
    return f;
}
}  // namespace

TEST_CASE("redis sink lag probe: the leader probes on the lease connection every interval", "[unit][sink]") {
    Rig r(RigCfg{.groups = {kBot}});
    r.server.group_facts[r.keys.events() + "|bot"] = lag_facts("1700000000200-0", 1700000100000);

    CHECK(r.sink->consumer_lag().empty());  // nothing before the first probe
    r.sink->pump();
    REQUIRE(r.server.group_reads == 1);
    const auto snap = r.sink->consumer_lag();
    REQUIRE(snap.size() == 1);
    CHECK(snap[0].group == "bot");
    CHECK(snap[0].undelivered_age_s == 99);
    CHECK(snap[0].lag == 3);

    r.advance(milliseconds(59999));
    r.sink->pump();
    CHECK(r.server.group_reads == 1);  // not due yet
    r.advance(milliseconds(1));
    r.sink->pump();
    CHECK(r.server.group_reads == 2);
}

TEST_CASE("redis sink lag probe: an unknown group is reported as missing, not an error", "[unit][sink]") {
    Rig r(RigCfg{.groups = {kBot}});
    r.sink->pump();
    const auto snap = r.sink->consumer_lag();
    REQUIRE(snap.size() == 1);
    CHECK(snap[0].missing);
    CHECK(r.sink->mode() == SinkMode::Connected);
}

TEST_CASE("redis sink lag probe: disabled with no groups, without a lease, and while Degraded", "[unit][sink]") {
    SECTION("no groups") {
        Rig r;
        r.sink->pump();
        CHECK(r.server.group_reads == 0);
        CHECK(r.sink->consumer_lag().empty());
    }
    SECTION("lease disabled") {
        Rig r(RigCfg{.lease = false, .groups = {kBot}});
        r.sink->pump();
        CHECK(r.server.group_reads == 0);
    }
    SECTION("Degraded") {
        Rig r(RigCfg{.groups = {kBot}});
        r.sink->pump();  // first probe while Connected
        REQUIRE(r.server.group_reads == 1);
        r.sink->emit(mk(id_of(0)));
        r.data->down = true;
        r.sink->pump();
        REQUIRE(r.sink->mode() == SinkMode::Degraded);
        r.advance(milliseconds(60000));
        r.sink->pump();
        CHECK(r.server.group_reads == 1);
    }
}

TEST_CASE("redis sink lag probe: a failed probe is counted and the last snapshot stays", "[unit][sink]") {
    Rig r(RigCfg{.groups = {kBot}});
    r.server.group_facts[r.keys.events() + "|bot"] = lag_facts("1700000000200-0", 1700000100000);
    r.sink->pump();
    REQUIRE(r.sink->consumer_lag().size() == 1);
    CHECK(r.sink->consumer_lag()[0].probed_at_ms == 1700000000000);  // the listener wall clock of the probe

    r.lease->down = true;
    r.advance(milliseconds(60000));
    r.sink->pump();
    CHECK(r.sink->status().consumer_lag_failures_total == 1);
    CHECK(r.sink->mode() == SinkMode::Connected);
    const auto snap = r.sink->consumer_lag();
    REQUIRE(snap.size() == 1);
    CHECK(snap[0].undelivered_age_s == 99);
    CHECK(snap[0].probed_at_ms == 1700000000000);  // unchanged: the host can see how old the snapshot is
}

TEST_CASE("redis sink lag probe: an error that is not a connection error is counted and never fails the sink", "[unit][sink]") {
    Rig r(RigCfg{.groups = {kBot}});
    r.lease->aux_throws = true;
    r.sink->pump();
    CHECK(r.sink->status().consumer_lag_failures_total == 1);
    CHECK(r.sink->mode() == SinkMode::Connected);
    CHECK(r.sink->consumer_lag().empty());

    r.sink->emit(mk(id_of(0)));  // the writer is unaffected
    REQUIRE(r.settle());
    CHECK(r.server.count_entries(StreamKind::Events, id_of(0)) == 1);
}


TEST_CASE("redis sink options: consumer groups need a positive probe interval; from_config copies them", "[unit][sink]") {
    Keys keys{"shard"};
    FakeServer server{keys};
    RedisSinkOptions o;
    o.token = kNoFence;
    o.retention_ms = 1000;
    o.dedup_ttl_s = 10;
    o.queue_capacity = 10;
    o.consumer_groups = {kBot};
    o.consumer_lag_interval_ms = 0;
    CHECK_THROWS_WITH(RedisSink(o, keys, std::make_unique<FakeConn>(server), std::make_unique<FakeConn>(server)),
                      ContainsSubstring("consumer_lag_interval_ms"));

    evml::core::SinkCfg sink;
    sink.consumer_groups = {kBot};
    evml::core::LeaseCfg lease;
    lease.enabled = false;
    const auto copied = RedisSinkOptions::from_config(sink, lease, kNoFence);
    REQUIRE(copied.consumer_groups.size() == 1);
    CHECK(copied.consumer_groups[0].group == "bot");
}

TEST_CASE("redis sink: oldest_record_age_s is the age of the oldest unacked write and falls to 0 when all are acked", "[unit][sink]") {
    Rig r;
    CHECK(r.sink->status().oldest_record_age_s == 0);  // nothing pending

    r.data->down = true;
    r.sink->emit(mk(id_of(0)));  // stamped at t0
    r.advance(std::chrono::seconds(30));
    r.sink->emit(mk(id_of(1)));  // stamped at t0 + 30 s
    r.advance(std::chrono::seconds(30));
    CHECK(r.sink->status().oldest_record_age_s == 60);  // still queued: the oldest is e0

    r.sink->pump();  // Redis is down: the held batch / queue go to the spill
    REQUIRE(r.sink->mode() == SinkMode::Degraded);
    r.advance(std::chrono::seconds(10));
    CHECK(r.sink->status().oldest_record_age_s == 70);  // spilled or held, the stamp of e0 is kept
    CHECK(r.sink->status().pending_writes == 2);

    r.data->down = false;
    REQUIRE(r.settle());
    CHECK(r.sink->status().pending_writes == 0);
    CHECK(r.sink->status().oldest_record_age_s == 0);
}

TEST_CASE("redis sink: a partly acked batch moves oldest_record_age_s to the first item still pending", "[unit][sink]") {
    RigCfg cfg;
    cfg.max_batch = 1;  // one emit per batch: each ack removes exactly one stamp
    Rig r(cfg);
    r.sink->emit(mk(id_of(0)));
    r.advance(std::chrono::seconds(20));
    r.sink->emit(mk(id_of(1)));
    r.advance(std::chrono::seconds(20));
    CHECK(r.sink->status().oldest_record_age_s == 40);

    r.sink->pump();  // sends and acks e0 only
    REQUIRE(r.sink->status().pending_writes == 1);
    CHECK(r.sink->status().oldest_record_age_s == 20);  // e1 was enqueued 20 s ago
}

TEST_CASE("redis sink: Fenced discards every stamp, and writes dropped afterwards leave none behind", "[unit][sink]") {
    Rig r;
    r.server.leader = "2|hostB|n2";  // another host acquired the lease
    r.server.epoch = "2|hostB|n2";
    for (int i = 0; i < 4; ++i) r.sink->emit(mk(id_of(i)));
    r.advance(std::chrono::seconds(15));
    CHECK(r.sink->status().oldest_record_age_s == 15);
    r.sink->pump();
    REQUIRE(r.sink->mode() == SinkMode::Fenced);
    CHECK(r.sink->status().pending_writes == 0);
    CHECK(r.sink->status().oldest_record_age_s == 0);

    r.sink->emit(mk("late"));  // refused in a terminal mode: never stamped
    CHECK(r.sink->status().oldest_record_age_s == 0);
}

TEST_CASE("redis sink: a Failed sink keeps the stamp of the older held write when the newer queued one is discarded", "[unit][sink]") {
    Rig r;
    r.sink->checkpoint(CheckpointKind::Finalized, 50);  // stamped at t0; finalized with no safe cursor: ckpt.lua refuses (I2)
    r.advance(std::chrono::seconds(20));
    r.sink->emit(mk("after"));                          // stamped at t0 + 20 s, stays queued behind the barrier
    r.advance(std::chrono::seconds(10));
    CHECK(r.sink->status().oldest_record_age_s == 30);

    r.sink->pump();  // the barrier is held, sent and refused: the sink fails; the held write stays pending
    REQUIRE(r.sink->mode() == SinkMode::Failed);
    CHECK(r.sink->status().pending_writes == 2);
    CHECK(r.sink->status().oldest_record_age_s == 30);

    r.advance(std::chrono::seconds(5));
    r.sink->pump();  // terminal: the queued write is discarded, the held one is not
    CHECK(r.sink->status().discarded_total == 1);
    CHECK(r.sink->status().pending_writes == 1);
    CHECK(r.sink->status().oldest_record_age_s == 35);  // still the held write's age, not the discarded one's (15 s)
}

TEST_CASE("redis sink: Fenced with pending writes in the held batch, spill and queue leaves no stamp behind", "[unit][sink]") {
    RigCfg cfg;
    cfg.max_batch = 1;
    Rig r(cfg);
    r.data->down = true;
    r.sink->emit(mk(id_of(0)));
    r.sink->emit(mk(id_of(1)));
    r.sink->pump();  // Redis is down: everything goes to the spill (and one item is held)
    REQUIRE(r.sink->mode() == SinkMode::Degraded);
    r.advance(std::chrono::seconds(12));
    r.sink->emit(mk(id_of(2)));  // still in the queue
    CHECK(r.sink->status().oldest_record_age_s == 12);
    CHECK(r.sink->status().pending_writes == 3);

    r.data->down = false;
    r.server.leader = "2|hostB|n2";  // another host took the lease during the outage
    r.server.epoch = "2|hostB|n2";
    REQUIRE(r.settle());
    REQUIRE(r.sink->mode() == SinkMode::Fenced);
    CHECK(r.sink->status().pending_writes == 0);
    CHECK(r.sink->status().oldest_record_age_s == 0);
}
