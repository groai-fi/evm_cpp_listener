// Integration tests of RedisSink + RedisppConn against the compose Redis through toxiproxy (plan 4, slice 4d:
// scenarios 2, 4, 12, 13, 16). The sink runs its own thread, exactly as in production.
//
// REDIS_URL and TOXIPROXY_URL come from the process environment only (see toxiproxy.hpp). Missing or unreachable
// services FAIL the tests; they never skip them. Every test uses its own random shard id and its own proxy, so tests
// do not see each other's keys; scenario 12 flushes the server-wide script cache and is therefore [redis_serial].

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <exception>
#include <filesystem>
#include <functional>
#include <iostream>
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
#include "sink/redis_sink.hpp"
#include "sink/redispp_conn.hpp"
#include "sink/script_runner.hpp"
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
