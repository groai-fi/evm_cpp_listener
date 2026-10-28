// Integration tests of RedisSink + RedisppConn against the compose Redis through toxiproxy (plan 4, slice 4d:
// scenarios 2, 4, 12, 13, 16). The sink runs its own thread, exactly as in production.
//
// REDIS_URL and TOXIPROXY_URL come from the process environment only (see toxiproxy.hpp). Missing or unreachable
// services FAIL the tests; they never skip them. Every test uses its own random shard id and its own proxy, so tests
// do not see each other's keys; scenario 12 flushes the server-wide script cache and is therefore [redis_serial].

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <exception>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <sw/redis++/redis++.h>

#include "core/log.hpp"
#include "pipeline/coverage.hpp"
#include "pipeline/pipeline_driver.hpp"
#include "pipeline/sweeper.hpp"
#include "sink/lease.hpp"
#include "sink/redis_sink.hpp"
#include "sink/redispp_conn.hpp"
#include "sink/script_runner.hpp"
#include "sweep_fakes.hpp"
#include "toxiproxy.hpp"

using namespace evml::sink;
using namespace std::chrono_literals;
using evml::core::StreamKind;
using evml::pipeline::CheckpointKind;
using evml::pipeline::Emission;
using evml::pipeline::EventType;
using evml::pipeline::ReleasedEntry;
using evml::testing::ToxiRedis;
using evml::testing::require_env;

namespace net = boost::asio;

namespace {

std::string random_shard() {
    std::random_device rd;
    return "t_" + std::to_string(rd()) + std::to_string(rd());
}

std::string id_of(int i) { return "e" + std::to_string(i); }

Emission mk(const std::string& id, std::uint64_t block, StreamKind stream = StreamKind::Events,
            std::optional<std::uint64_t> released = std::nullopt) {
    Emission e;
    e.stream = stream;
    e.type = EventType::Log;
    e.event_id = id;
    e.dedup_key = "dedup:" + id;
    e.fields = {{"v", "1"}, {"type", "log"}, {"event_id", id}, {"block_number", std::to_string(block)}};
    e.released_block = released;
    return e;
}

bool wait_for(const std::function<bool()>& pred, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(5ms);
    }
    return true;
}

// Runs a sink read on a private io_context. The sink thread completes the channel from its own thread.
std::optional<std::vector<ReleasedEntry>> await_released(RedisSink& sink, std::uint64_t from, std::uint64_t to) {
    net::io_context io;
    auto fut = net::co_spawn(io, sink.released_range(from, to), net::use_future);
    io.run();
    return fut.get();
}

// The sink under test, wired to the compose Redis through a toxiproxy proxy; `admin` talks to Redis directly.
struct Live {
    std::string direct_url = require_env("REDIS_URL");
    sw::redis::Redis admin{direct_url};
    Keys keys{random_shard()};
    ToxiRedis px;
    std::filesystem::path dir;
    std::uint32_t lease_ttl_ms;
    std::string token;
    std::unique_ptr<RedisSink> sink;

    explicit Live(std::uint32_t lease_ttl = 15000) : lease_ttl_ms(lease_ttl) {
#if defined(SIGPIPE)
        std::signal(SIGPIPE, SIG_IGN);
#endif
        evml::core::log::init("info");
        std::random_device rd;
        dir = std::filesystem::temp_directory_path() / ("evml-sink-it-" + std::to_string(rd()) + std::to_string(rd()));
        ScriptRunner runner(admin);
        const auto got = runner.run(lease_acquire_call(keys, "hostA", "n1", lease_ttl_ms));
        REQUIRE(got.has_value());
        token = *got;
    }

    void start(const std::function<void(RedisSinkOptions&)>& tweak = {}) {
        RedisSinkOptions o;
        o.token = token;
        o.retention_ms = 48ull * 3600 * 1000;
        o.dedup_ttl_s = 604800;
        o.queue_capacity = 1000000;
        o.spill_dir = dir;
        o.spill_max_bytes = 1ull << 30;
        o.lease_ttl_ms = lease_ttl_ms;
        o.renew_ms = lease_ttl_ms / 3;
        o.probe_min = 50ms;
        o.probe_max = 1000ms;
        if (tweak) tweak(o);
        sink = std::make_unique<RedisSink>(std::move(o), keys, std::make_unique<RedisppConn>(px.url(), keys),
                                           std::make_unique<RedisppConn>(px.url(), keys));
        sink->start();
    }

    ~Live() {
        sink.reset();  // stops the sink thread before its keys are deleted
        try {
            std::vector<std::string> mine;
            admin.keys(keys.pattern(), std::back_inserter(mine));
            if (!mine.empty()) admin.del(mine.begin(), mine.end());
        } catch (const std::exception& e) {
            std::cerr << "Live cleanup of shard keys failed: " << e.what() << "\n";
        }
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    // The `event_id` of every entry of a stream, in stream order.
    std::vector<std::string> event_ids(StreamKind stream) {
        using Attrs = std::unordered_map<std::string, std::string>;
        using Item = std::pair<std::string, sw::redis::Optional<Attrs>>;
        std::vector<Item> items;
        admin.xrange(keys.stream(stream), "-", "+", std::back_inserter(items));
        std::vector<std::string> ids;
        ids.reserve(items.size());
        for (const auto& [sid, attrs] : items) {
            REQUIRE(attrs.has_value());
            ids.push_back(attrs->at("event_id"));
        }
        return ids;
    }

    sw::redis::OptionalString ckpt_field(const std::string& f) { return admin.hget(keys.ckpt(), f); }
    long long xlen(StreamKind stream) { return admin.command<long long>("XLEN", keys.stream(stream)); }

    void expect_exactly(int n) {
        const auto ids = event_ids(StreamKind::Events);
        REQUIRE(ids.size() == static_cast<std::size_t>(n));  // no loss, no duplicates
        for (int k = 0; k < n; ++k) REQUIRE(ids[static_cast<std::size_t>(k)] == id_of(k));  // in order
    }
};

// A background thread whose exception is kept and rethrown on join (never swallowed).
class Worker {
public:
    explicit Worker(std::function<void()> fn)
        : t_([this, fn = std::move(fn)] {
              try {
                  fn();
              } catch (...) {
                  err_ = std::current_exception();
              }
          }) {}
    void join() {
        if (t_.joinable()) t_.join();
        if (err_) std::rethrow_exception(err_);
    }
    ~Worker() {
        if (t_.joinable()) t_.join();
    }

private:
    std::exception_ptr err_;
    std::thread t_;
};

}  // namespace

// Scenario 2: Redis unreachable for 60 s while 10k events arrive: all of them land, in order, exactly once.
TEST_CASE("redis sink integration 2: a 60 s outage during 10k events loses nothing, keeps order, duplicates nothing",
          "[integration][sink]") {
    Live L;
    L.start([](RedisSinkOptions& o) { o.renew_ms = 5000; });  // the lease (15 s) expires mid-outage: RESUMED on return

    constexpr int kTotal = 10000;
    int next = 0;
    const auto burst = [&](int n) {
        for (int k = 0; k < n && next < kTotal; ++k, ++next) L.sink->emit(mk(id_of(next), 1000 + next));
    };

    burst(3000);
    L.px.disable();
    const auto t0 = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - t0 < 60s) {
        burst(25);
        std::this_thread::sleep_for(300ms);
    }
    REQUIRE(next < kTotal);  // events were still arriving until the very end of the outage
    L.px.enable();
    burst(kTotal - next);

    REQUIRE(L.sink->wait_idle(120s));
    INFO("fatal: " << L.sink->fatal_message());
    REQUIRE(L.sink->mode() != SinkMode::Failed);
    REQUIRE(L.sink->mode() != SinkMode::Fenced);
    CHECK(L.sink->status().discarded_total == 0);
    L.expect_exactly(kTotal);
}

// Scenario 4: the spill cap saturates the sink; the checkpoint in Redis stays frozen; nothing is lost after recovery.
TEST_CASE("redis sink integration 4: spill cap saturates, the checkpoint stays frozen, no loss after recovery",
          "[integration][sink]") {
    Live L;
    L.start([](RedisSinkOptions& o) { o.spill_max_bytes = 64 * 1024; });

    L.sink->init_checkpoint(999, {"a:1:x"});
    for (int i = 0; i < 10; ++i) L.sink->emit(mk(id_of(i), 1000 + i));
    L.sink->checkpoint(CheckpointKind::Safe, 1009);
    REQUIRE(L.sink->wait_idle(30s));
    REQUIRE(L.ckpt_field("safe") == std::optional<std::string>("1009"));

    L.px.disable();
    int next = 10;
    const auto t0 = std::chrono::steady_clock::now();
    while (!L.sink->saturated()) {
        REQUIRE(std::chrono::steady_clock::now() - t0 < 60s);
        L.sink->emit(mk(id_of(next), 1000 + next));
        ++next;
        std::this_thread::sleep_for(200us);
    }
    for (int k = 0; k < 20; ++k, ++next) L.sink->emit(mk(id_of(next), 1000 + next));  // mid-flight hand-over: accepted
    std::this_thread::sleep_for(300ms);
    CHECK(L.sink->saturated());
    CHECK(L.ckpt_field("safe") == std::optional<std::string>("1009"));  // C is frozen while saturated

    L.px.enable();
    L.sink->checkpoint(CheckpointKind::Safe, 1000 + next - 1);  // the pipeline resumes once the sink drains
    REQUIRE(L.sink->wait_idle(60s));
    INFO("fatal: " << L.sink->fatal_message());
    REQUIRE(L.sink->mode() == SinkMode::Connected);
    CHECK_FALSE(L.sink->saturated());
    CHECK(L.sink->status().discarded_total == 0);
    L.expect_exactly(next);
    CHECK(L.ckpt_field("safe") == std::optional<std::string>(std::to_string(1000 + next - 1)));
}

// Scenario 12 (P1): SCRIPT FLUSH between batches and reset_peer mid-batch must never let the checkpoint get ahead of a
// missing event.
TEST_CASE("redis sink integration 12: script flush and reset_peer never put the checkpoint ahead of an event",
          "[integration][sink][redis_serial]") {
    Live L;
    L.start([](RedisSinkOptions& o) {
        o.renew_ms = 100;  // the lease connection also meets the flushed script cache
        o.probe_min = 5ms;
        o.probe_max = 100ms;
    });
    L.sink->init_checkpoint(999, {"a:1:x"});
    REQUIRE(L.sink->wait_idle(30s));

    constexpr int kTotal = 3000;
    std::atomic<bool> producing{true};
    std::atomic<bool> monitoring{true};
    std::atomic<long> violations{0};
    std::atomic<long> samples{0};

    // Invariant: whatever `safe` Redis shows, the events up to it are already in the stream. `safe` is read first, the
    // stream length after it, so a late read can only make the check weaker, never produce a false alarm.
    Worker monitor([&] {
        sw::redis::Redis mon(L.direct_url);
        while (monitoring) {
            const auto safe = mon.hget(L.keys.ckpt(), "safe");
            if (safe) {
                const long long need = std::stoll(*safe) - 999;  // events 0 .. safe-1000
                const long long have = mon.command<long long>("XLEN", L.keys.events());
                if (have < need) ++violations;
                ++samples;
            }
        }
    });

    Worker chaos([&] {
        sw::redis::Redis c(L.direct_url);
        std::mt19937 rng(12345);
        while (producing) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5 + rng() % 25));
            c.command<std::string>("SCRIPT", "FLUSH");
            if (rng() % 3 == 0) {
                L.px.add_toxic("rp", "reset_peer", boost::json::object{{"timeout", 0}});
                std::this_thread::sleep_for(std::chrono::milliseconds(2 + rng() % 15));
                L.px.remove_toxic("rp");
            }
        }
    });

    for (int k = 0; k < kTotal; ++k) {
        L.sink->emit(mk(id_of(k), 1000 + k));
        if ((k + 1) % 10 == 0) L.sink->checkpoint(CheckpointKind::Safe, 1000 + k);
        if (k % 20 == 19) std::this_thread::sleep_for(1ms);
    }
    producing = false;
    chaos.join();

    REQUIRE(L.sink->wait_idle(120s));
    monitoring = false;
    monitor.join();

    INFO("fatal: " << L.sink->fatal_message());
    REQUIRE(L.sink->mode() != SinkMode::Failed);
    CHECK(L.sink->status().reconnects_total > 0);  // the faults really happened
    CHECK(samples.load() > 0);
    CHECK(violations.load() == 0);
    L.expect_exactly(kTotal);
    CHECK(L.ckpt_field("safe") == std::optional<std::string>(std::to_string(1000 + kTotal - 1)));
}

// Scenario 13 (A): a read sees every write queued before it; with the link down it answers nullopt.
TEST_CASE("redis sink integration 13: released_range sees queued emits, and is unavailable while the link is down",
          "[integration][sink]") {
    Live L;
    L.start();

    std::vector<Emission> sent;
    for (int k = 0; k < 5; ++k) {
        sent.push_back(mk(id_of(k), 100 + k, k == 4 ? StreamKind::Registry : StreamKind::Events, 100 + k));
        L.sink->emit(sent.back());
    }
    const auto got = await_released(*L.sink, 0, 1000);  // no wait_idle first: the read is queued behind the emits
    REQUIRE(got.has_value());
    REQUIRE(got->size() == sent.size());
    for (std::size_t k = 0; k < sent.size(); ++k) {
        CHECK((*got)[k].event_id == sent[k].event_id);
        CHECK((*got)[k].block_number == 100 + k);
        CHECK((*got)[k].stream == sent[k].stream);
        CHECK((*got)[k].fields == sent[k].fields);
    }

    L.px.disable();
    L.sink->emit(mk(id_of(5), 105));
    REQUIRE(wait_for([&] { return L.sink->mode() != SinkMode::Connected; }, 30s));
    CHECK_FALSE(L.sink->readable());
    CHECK_FALSE(await_released(*L.sink, 0, 1000).has_value());  // unavailable, never a partial answer

    L.px.enable();
    REQUIRE(L.sink->wait_idle(60s));
    CHECK(L.sink->mode() == SinkMode::Connected);
    CHECK(L.xlen(StreamKind::Events) == 5);  // e0..e3 and e5 (e4 is in the registry stream)
}

// Scenario 16 (P4): replaying 50k spilled items takes longer than the lease TTL; renewals on their own connection keep
// the lease alive the whole time.
TEST_CASE("redis sink integration 16: replaying 50k spilled items never lets the lease expire", "[integration][sink]") {
    constexpr std::uint32_t kTtlMs = 3000;
    Live L(kTtlMs);
    L.start([](RedisSinkOptions& o) { o.renew_ms = 500; });

    L.px.disable();
    constexpr int kTotal = 50000;
    for (int k = 0; k < kTotal; ++k) L.sink->emit(mk(id_of(k), 1000 + k));
    REQUIRE(wait_for([&] { return L.sink->status().queue_depth == 0; }, 60s));
    CHECK(L.sink->status().spill_records > 40000);

    L.px.add_toxic("slow", "latency", boost::json::object{{"latency", 40}});  // ~200 batches x 40 ms: well past the TTL
    L.px.enable();
    REQUIRE(wait_for([&] { return L.sink->mode() == SinkMode::Replaying; }, 60s));

    const auto t0 = std::chrono::steady_clock::now();
    long checks = 0;
    while (L.sink->status().pending_writes != 0) {
        const auto leader = L.admin.get(L.keys.leader());
        INFO("sink mode " << to_string(L.sink->mode()) << ", replay running for "
                          << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count()
                          << " ms");
        REQUIRE(leader.has_value());  // the lease key never disappeared
        REQUIRE(*leader == L.token);
        ++checks;
        REQUIRE(std::chrono::steady_clock::now() - t0 < 180s);
        std::this_thread::sleep_for(20ms);
    }
    const auto replay_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    INFO("replay took " << replay_ms << " ms, " << checks << " lease checks");
    REQUIRE(replay_ms > kTtlMs);  // otherwise the scenario proves nothing

    REQUIRE(L.sink->mode() != SinkMode::Failed);
    CHECK(L.sink->status().discarded_total == 0);
    L.expect_exactly(kTotal);
}

// ---------------------------------------------------------------------------------------------------------------
// Slice 4e: consumer-group lag probe (19), lease-disabled mode (18), standby takeover and fencing (9)
// ---------------------------------------------------------------------------------------------------------------

namespace {

sw::redis::ReplyUPtr redis_cmd(sw::redis::Redis& r, std::vector<std::string> argv) {
    return r.command(argv.begin(), argv.end());  // the iterator overload: no ambiguity with a trailing string literal
}

std::vector<std::string> entry_ids(Live& L, StreamKind stream) {
    using Attrs = std::unordered_map<std::string, std::string>;
    using Item = std::pair<std::string, sw::redis::Optional<Attrs>>;
    std::vector<Item> items;
    L.admin.xrange(L.keys.stream(stream), "-", "+", std::back_inserter(items));
    std::vector<std::string> ids;
    for (const auto& item : items) ids.push_back(item.first);
    return ids;
}

std::uint64_t system_now_ms() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

}  // namespace

// Scenario 19: the probe reads real XINFO GROUPS / XINFO STREAM / XRANGE / XPENDING / TIME replies and judges them.
TEST_CASE("redis sink integration 19: consumer-group lag probe on real Redis replies", "[integration][sink]") {
    Live L;
    const std::string stream = L.keys.events();
    const evml::core::ConsumerGroupCfg cfg{StreamKind::Events, "bot"};
    RedisppConn probe(L.direct_url, L.keys);

    // No stream key yet: reported as missing, not an error.
    CHECK(evaluate_consumer_lag(cfg, probe.read_group_facts(stream, "bot")).missing);

    L.start();
    for (int i = 0; i < 5; ++i) L.sink->emit(mk(id_of(i), 1000 + i));
    REQUIRE(L.sink->wait_idle(30s));
    const auto ids = entry_ids(L, StreamKind::Events);
    REQUIRE(ids.size() == 5);

    {  // the stream exists, the group does not
        const auto f = probe.read_group_facts(stream, "bot");
        CHECK(f.stream_exists);
        CHECK_FALSE(f.group_found);
        CHECK(evaluate_consumer_lag(cfg, f).missing);
    }

    redis_cmd(L.admin, {"XGROUP", "CREATE", stream, "bot", "0"});
    redis_cmd(L.admin, {"XGROUP", "CREATE", stream, "bot2", "0"});  // never reads: nothing pending, nothing delivered
    {  // a new group at 0: nothing delivered yet
        const auto f = probe.read_group_facts(stream, "bot");
        REQUIRE(f.group_found);
        CHECK(f.last_delivered_id == "0-0");
        CHECK(f.lag == 5);
        CHECK(f.first_undelivered_id == ids[0]);
        CHECK(f.first_entry_id == ids[0]);
        CHECK_FALSE(f.smallest_pending_id.has_value());
        CHECK(f.now_ms >= StreamId::parse(ids[4]).ms);
        CHECK_FALSE(evaluate_consumer_lag(cfg, f).trimmed_unread);
    }

    // Deliver three, ack the first: ids[1] and ids[2] stay pending, ids[3] is the oldest unread.
    redis_cmd(L.admin, {"XREADGROUP", "GROUP", "bot", "c1", "COUNT", "3", "STREAMS", stream, ">"});
    redis_cmd(L.admin, {"XACK", stream, "bot", ids[0]});
    {
        const auto f = probe.read_group_facts(stream, "bot");
        CHECK(f.last_delivered_id == ids[2]);
        CHECK(f.lag == 2);
        CHECK(f.first_undelivered_id == ids[3]);
        CHECK(f.smallest_pending_id == ids[1]);
        CHECK_FALSE(evaluate_consumer_lag(cfg, f).trimmed_unread);
    }

    // Ages come from the entry ids and Redis TIME.
    std::this_thread::sleep_for(2100ms);
    {
        const auto r = evaluate_consumer_lag(cfg, probe.read_group_facts(stream, "bot"));
        CHECK(r.undelivered_age_s >= 2);
        CHECK(r.undelivered_age_s < 60);
        CHECK(r.pending_age_s >= 2);
        CHECK(r.pending_age_s < 60);
        CHECK(r.lag == 2);
    }

    // Retention trims past the group: ids[1..3] (pending and unread) are gone before the bot acked or read them.
    redis_cmd(L.admin, {"XTRIM", stream, "MINID", ids[4]});
    {
        const auto f = probe.read_group_facts(stream, "bot");
        CHECK(f.first_entry_id == ids[4]);
        INFO("max-deleted-entry-id after XTRIM MINID: '" << f.max_deleted_id << "', smallest pending: "
                                                         << f.smallest_pending_id.value_or("<nil>"));
        CHECK(evaluate_consumer_lag(cfg, f).trimmed_unread);
    }
    {  // A group with nothing pending lost unread entries: MINID trim leaves max-deleted-entry-id at 0-0, the counters show it.
        const auto f = probe.read_group_facts(stream, "bot2");
        CHECK_FALSE(f.smallest_pending_id.has_value());
        CHECK(f.max_deleted_id == "0-0");
        CHECK_FALSE(f.entries_read.has_value());  // Redis keeps entries-read nil for a group created at 0 that never read
        CHECK(f.entries_added == 5);
        CHECK(f.lag == 1);  // only ids[4] is left to read
        CHECK(evaluate_consumer_lag(evml::core::ConsumerGroupCfg{StreamKind::Events, "bot2"}, f).trimmed_unread);
    }

    // The same through the sink thread: one known group (trimmed), one unknown group (missing).
    L.sink.reset();
    L.start([&](RedisSinkOptions& o) {
        o.consumer_groups = {cfg, evml::core::ConsumerGroupCfg{StreamKind::Events, "ghost"}};
        o.consumer_lag_interval_ms = 200;
    });
    REQUIRE(wait_for([&] { return L.sink->consumer_lag().size() == 2; }, 10s));
    const auto snap = L.sink->consumer_lag();
    CHECK(snap[0].group == "bot");
    CHECK(snap[0].trimmed_unread);
    CHECK_FALSE(snap[0].missing);
    CHECK(snap[1].group == "ghost");
    CHECK(snap[1].missing);
    CHECK(L.sink->mode() == SinkMode::Connected);
    CHECK(L.sink->status().consumer_lag_failures_total == 0);

    // The probe is read-only: it never touched the stream or the group.
    CHECK(L.xlen(StreamKind::Events) == 1);
}

// Scenario 18: lease disabled (token "0"). An outage replays exactly; no leader key is ever created; a restart on the
// same spill directory wipes the spill and the re-sweep lands the missing events through dedup.
TEST_CASE("redis sink integration 18: lease disabled, outage and restart", "[integration][sink]") {
    Live L;
    L.admin.del(L.keys.leader());  // Live acquired a lease for the other tests; this one runs without any
    L.admin.del(L.keys.epoch());
    L.token = kNoFence;
    L.start();

    for (int i = 0; i < 50; ++i) L.sink->emit(mk(id_of(i), 1000 + i));
    REQUIRE(L.sink->wait_idle(30s));

    L.px.disable();
    for (int i = 50; i < 100; ++i) L.sink->emit(mk(id_of(i), 1000 + i));
    REQUIRE(wait_for([&] { return L.sink->mode() == SinkMode::Degraded; }, 30s));
    L.px.enable();
    REQUIRE(L.sink->wait_idle(60s));
    INFO("fatal: " << L.sink->fatal_message());
    REQUIRE(wait_for([&] { return L.sink->mode() == SinkMode::Connected; }, 5s));
    L.expect_exactly(100);
    CHECK(L.sink->status().renew_failures_total == 0);
    CHECK(L.sink->status().discarded_total == 0);

    // Crash with a non-empty spill: the spill dies with the process.
    L.px.disable();
    for (int i = 100; i < 120; ++i) L.sink->emit(mk(id_of(i), 1000 + i));
    REQUIRE(wait_for([&] { return L.sink->mode() == SinkMode::Degraded; }, 30s));
    L.sink.reset();
    L.px.enable();

    L.start();  // same spill_dir
    CHECK(L.sink->status().spill_records == 0);
    for (int i = 90; i < 120; ++i) L.sink->emit(mk(id_of(i), 1000 + i));  // the re-sweep re-emits an overlap
    REQUIRE(L.sink->wait_idle(60s));
    L.expect_exactly(120);  // 90..99 were already there: dedup; 100..119 land now, in order
    CHECK(L.sink->status().dup_total >= 10);
    CHECK(L.sink->status().renew_failures_total == 0);

    CHECK(L.admin.exists(L.keys.leader()) == 0);  // no leader key was ever created
    CHECK(L.admin.exists(L.keys.epoch()) == 0);
}

// Scenario 9: two standbys race; exactly one leads. When the leader stops renewing, the other takes over within
// ttl + poll, with a higher term, and the old token is fenced out of the write scripts.
TEST_CASE("redis sink integration 9: standby takeover within ttl + poll, old token fenced", "[integration][sink]") {
    const std::string url = require_env("REDIS_URL");
    sw::redis::Redis admin{url};
    const Keys keys{random_shard()};
    struct Cleanup {
        sw::redis::Redis& admin;
        const Keys& keys;
        ~Cleanup() {
            try {
                std::vector<std::string> mine;
                admin.keys(keys.pattern(), std::back_inserter(mine));
                if (!mine.empty()) admin.del(mine.begin(), mine.end());
            } catch (const std::exception& e) {
                std::cerr << "scenario 9 cleanup failed: " << e.what() << "\n";
            }
        }
    } cleanup{admin, keys};
    evml::core::log::init("info");

    constexpr std::uint32_t kTtlMs = 2000;
    constexpr auto kPoll = 200ms;
    std::atomic<bool> stop{false};
    struct Result {
        std::atomic<bool> done{false};
        std::string token;
    };
    Result ra, rb;
    const auto standby = [&](const std::string& host, Result& out) {
        RedisppConn conn(url, keys);
        AcquireOptions o;
        o.host_id = host;
        o.ttl_ms = kTtlMs;
        o.poll = kPoll;
        o.stop = [&] { return stop.load(); };
        if (auto token = acquire_lease_blocking(conn, keys, o)) {
            out.token = *token;
            out.done.store(true);
        }
    };
    Worker wa([&] { standby("hostA", ra); });
    Worker wb([&] { standby("hostB", rb); });
    struct StopGuard {  // destroyed before the workers: a failing REQUIRE must not leave a standby polling forever
        std::atomic<bool>& stop;
        ~StopGuard() { stop.store(true); }
    } guard{stop};

    REQUIRE(wait_for([&] { return ra.done.load() || rb.done.load(); }, 5s));
    const auto acquired_at = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(600ms);  // three polls of the loser: it must still be waiting
    REQUIRE(ra.done.load() != rb.done.load());  // exactly one leader
    Result& winner = ra.done.load() ? ra : rb;
    Result& loser = ra.done.load() ? rb : ra;
    CHECK(admin.get(keys.leader()) == std::optional<std::string>(winner.token));
    CHECK(winner.token.rfind("1|", 0) == 0);

    // The leader never renews (no sink behind it): the key expires and the standby takes over.
    REQUIRE(wait_for([&] { return loser.done.load(); }, 10s));
    const auto takeover_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - acquired_at).count();
    INFO("takeover took " << takeover_ms << " ms after the first acquire");
    CHECK(takeover_ms < kTtlMs + 200 + 1500);  // ttl + poll + scheduling slack
    CHECK(loser.token.rfind("2|", 0) == 0);    // a higher term
    CHECK(admin.get(keys.leader()) == std::optional<std::string>(loser.token));

    ScriptRunner runner(admin);
    const auto emit_with = [&](const std::string& token) {
        return runner.run(emit_call(keys, mk("late", 1000), EmitParams{604800, system_now_ms() - 48ull * 3600 * 1000, token}));
    };
    CHECK(emit_with(winner.token) == std::optional<std::string>("FENCED"));  // the old term can no longer write
    CHECK(emit_with(loser.token) != std::optional<std::string>("FENCED"));

    wa.join();
    wb.join();
}

// ---------------------------------------------------------------------------------------------------------------
// Slice 4e: crash while Degraded (3) and FLUSHALL runbook (6), with the real driver + sweeper on the real sink
// ---------------------------------------------------------------------------------------------------------------

namespace {

using evml::pipeline::PipelineDriver;
using evml::pipeline::PipelineState;
using evml::pipeline::Sweeper;
using EventIds = std::map<StreamKind, std::vector<std::string>>;

// A canonical chain with ten logs of the first configured contract in blocks 101..110, behind a checkpoint at 100.
struct Chain {
    evml::core::Config cfg = evml::test::load_base_config();
    evml::test::FakeSource src;

    Chain() {
        for (std::uint64_t b = 101; b <= 110; ++b) {
            const auto h = static_cast<std::uint8_t>(b - 100);  // one hash per block: (blockHash, logIndex) must be unique
            src.logs.push_back(evml::test::mklog(cfg, 0, b, h, 0));
            src.blocks[b] = evml::test::binfo(h);
        }
    }
    std::vector<std::string> manifest() const { return evml::pipeline::contract_manifest(cfg); }
};

// One "process": a sweeper and a driver on a sink.
struct Process {
    std::unique_ptr<Sweeper> sw;
    std::unique_ptr<PipelineDriver> drv;

    Process(Chain& ch, evml::pipeline::Sink& sink) {
        const auto now = [] { return std::uint64_t{0}; };
        sw = std::make_unique<Sweeper>(ch.cfg, evml::pipeline::EventContext{"base_eurc_usdc", "test-host", "test"}, ch.src, sink,
                                       now);
        drv = std::make_unique<PipelineDriver>(ch.cfg, *sw, sink, ch.src, now);
        drv->set_gates(110, 108);
    }
    evml::pipeline::TickResult tick() { return evml::test::run(drv->tick()); }
    void drive_to_live() {
        for (int i = 0; i < 500; ++i) {
            const auto t = tick();
            REQUIRE_FALSE(t.done);
            if (sw->c_s() >= 110 && sw->c_f() >= 108 && drv->status().state == PipelineState::Live && t.wait.count() > 0) return;
            if (t.wait.count() > 0) std::this_thread::sleep_for(std::min<std::chrono::milliseconds>(t.wait, 50ms));
        }
        const auto st = drv->status();
        FAIL("the pipeline never reached Live: state=" << evml::pipeline::to_string(st.state) << " c_s=" << sw->c_s() << " c_f="
                                                      << sw->c_f() << " stalled=" << st.stalled << " (" << st.stall_detail
                                                      << ") fatal='" << st.fatal << "' frozen=" << st.frozen);
    }
};

// What an in-memory sink receives for the same chain: the canonical set, per stream (sorted).
EventIds canonical_ids(Chain& ch) {
    evml::test::MemorySink mem;
    mem.stored.safe = 100;
    mem.stored.finalized = 100;
    mem.stored.contracts = ch.manifest();
    Process p(ch, mem);
    p.drive_to_live();
    EventIds ids;
    for (const auto& e : mem.emitted) {
        REQUIRE(e.type == EventType::Log);
        ids[e.stream].push_back(e.event_id);
    }
    for (auto& [kind, v] : ids) std::sort(v.begin(), v.end());
    REQUIRE_FALSE(ids.empty());
    return ids;
}

void expect_streams_equal(Live& L, const EventIds& expected) {
    for (const StreamKind kind : {StreamKind::Events, StreamKind::Registry}) {
        auto got = L.event_ids(kind);
        std::sort(got.begin(), got.end());
        const auto it = expected.find(kind);
        const std::vector<std::string> want = it == expected.end() ? std::vector<std::string>{} : it->second;
        INFO("stream " << L.keys.stream(kind));
        REQUIRE(got == want);  // exactly the canonical set: no loss, no duplicates
    }
}

}  // namespace

// Scenario 3: the process dies while the sink is Degraded. The spill dies with it; the restart re-sweeps from the Redis
// checkpoint (which never got ahead of the lost events) and the stream ends up with exactly the canonical set.
TEST_CASE("redis sink integration 3: crash while Degraded, restart re-sweeps to the exact canonical set", "[integration][sink]") {
    Chain ch;
    const EventIds expected = canonical_ids(ch);

    Live L;
    L.start();
    L.sink->init_checkpoint(100, ch.manifest());
    REQUIRE(L.sink->wait_idle(30s));
    REQUIRE(L.ckpt_field("safe") == std::optional<std::string>("100"));

    auto first = std::make_unique<Process>(ch, *L.sink);
    {
        const auto recover = first->tick();  // recovery reads the checkpoint through the sink
        REQUIRE(recover.wait.count() == 0);
        REQUIRE(first->drv->status().state == PipelineState::CatchingUp);
    }

    L.px.disable();
    first->tick();  // the safe sweep queues the ten events and a checkpoint; the sink cannot deliver them
    REQUIRE(wait_for([&] { return L.sink->mode() == SinkMode::Degraded; }, 30s));
    CHECK(L.event_ids(StreamKind::Events).empty());  // nothing landed
    CHECK(L.event_ids(StreamKind::Registry).empty());
    CHECK(L.ckpt_field("safe") == std::optional<std::string>("100"));  // and the checkpoint did not move

    // The process dies: sweeper, driver, sink and the spill's owner are gone.
    first.reset();
    L.sink.reset();
    L.px.enable();

    L.start();  // a new process on the same spill directory
    CHECK(L.sink->status().spill_records == 0);  // the old spill was wiped, never replayed
    Process second(ch, *L.sink);
    second.drive_to_live();
    REQUIRE(L.sink->wait_idle(30s));
    INFO("fatal: " << L.sink->fatal_message());
    REQUIRE(L.sink->mode() == SinkMode::Connected);
    expect_streams_equal(L, expected);
    CHECK(L.ckpt_field("safe") == std::optional<std::string>("110"));
}

// Scenario 6: Redis is wiped (FLUSHALL). The running sink must never write again with its old token. The runbook (a new
// lease and a checkpoint re-initialised at contract_start - 1; here 100) re-sweeps and repopulates the stream. The
// `--backfill-contract` command that performs the runbook step is Phase 5; this test drives the same library calls.
TEST_CASE("redis sink integration 6: FLUSHALL fences the old token, a re-backfill repopulates the stream",
          "[integration][sink][redis_serial]") {
    Chain ch;
    const EventIds expected = canonical_ids(ch);

    Live L;
    const std::string old_token = L.token;
    L.start();
    L.sink->init_checkpoint(100, ch.manifest());
    REQUIRE(L.sink->wait_idle(30s));
    {
        Process p(ch, *L.sink);
        p.drive_to_live();
    }
    REQUIRE(L.sink->wait_idle(30s));
    expect_streams_equal(L, expected);

    L.admin.flushall();
    L.sink->emit(mk("after_flush", 111));  // the old sink's next write
    REQUIRE(wait_for([&] { return L.sink->mode() == SinkMode::Fenced; }, 30s));  // renew answers FENCED: epoch is gone too
    CHECK(L.sink->status().discarded_total >= 1);
    CHECK(L.event_ids(StreamKind::Events).empty());  // the old token wrote nothing after the wipe
    CHECK(L.event_ids(StreamKind::Registry).empty());
    CHECK(L.admin.exists(L.keys.ckpt()) == 0);

    // Runbook: a new process takes a new lease (fresh nonce) and re-initialises the checkpoint below contract_start.
    L.sink.reset();
    {
        RedisppConn conn(L.direct_url, L.keys);
        const auto token = try_acquire_lease(conn, L.keys, "hostA", "n2", L.lease_ttl_ms);
        REQUIRE(token.has_value());
        CHECK(*token != old_token);
        L.token = *token;
    }
    ScriptRunner runner(L.admin);
    CHECK(runner.run(emit_call(L.keys, mk("zombie", 112), EmitParams{604800, system_now_ms() - 48ull * 3600 * 1000, old_token})) ==
          std::optional<std::string>("FENCED"));  // an old token never resumes, even though the lease key exists again

    L.start();
    L.sink->init_checkpoint(100, ch.manifest());
    REQUIRE(L.sink->wait_idle(30s));
    Process again(ch, *L.sink);
    again.drive_to_live();
    REQUIRE(L.sink->wait_idle(30s));
    INFO("fatal: " << L.sink->fatal_message());
    REQUIRE(L.sink->mode() == SinkMode::Connected);
    expect_streams_equal(L, expected);
}
