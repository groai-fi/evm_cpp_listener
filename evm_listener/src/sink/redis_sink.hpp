#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include <boost/asio/awaitable.hpp>

#include "core/config.hpp"
#include "pipeline/sink.hpp"
#include "sink/keys.hpp"
#include "sink/lua_scripts.hpp"
#include "sink/redis_conn.hpp"
#include "sink/spill.hpp"

namespace evml::sink {

// Plan 4.4 / 4.8. Connected -> Degraded (Redis unreachable: items go to the spill) -> Replaying (reconnected, spill
// drains in order) -> Connected. Fenced and Failed are terminal: the host must build a new sink (after re-acquiring the
// lease) or restart the process.
enum class SinkMode : int { Connected, Degraded, Replaying, Fenced, Failed };
const char* to_string(SinkMode m);

struct RedisSinkOptions {
    using Clock = std::chrono::steady_clock;

    std::string token;  // lease token every write script is fenced on, or kNoFence (lease disabled, local only)
    std::uint64_t retention_ms{};
    std::uint32_t dedup_ttl_s{};
    std::uint32_t queue_capacity{};  // soft cap: above it saturated() is true (the queue still accepts, never drops)
    std::filesystem::path spill_dir;
    std::uint64_t spill_max_bytes{};
    std::uint64_t spill_segment_bytes{Spill::kDefaultSegmentBytes};
    std::uint32_t lease_ttl_ms{};
    std::uint32_t renew_ms{};
    std::size_t max_batch{256};                       // emits per pipeline (plan 4.4)
    std::chrono::milliseconds probe_min{250};         // reconnect backoff: full jitter in [0, min(probe_max, probe_min * 2^n)]
    std::chrono::milliseconds probe_max{30000};
    std::function<Clock::time_point()> steady_now;    // empty = Clock::now; tests inject a fake clock
    std::function<std::uint64_t()> wall_ms;           // empty = system clock; the listener clock for XADD MINID

    // Copies the [sink] and [lease] settings. `token` must be kNoFence exactly when `lease.enabled` is false.
    static RedisSinkOptions from_config(const core::SinkCfg& sink, const core::LeaseCfg& lease, std::string token);
};

struct SinkStatus {
    SinkMode mode{SinkMode::Connected};
    std::uint64_t queue_depth{};
    std::uint64_t spill_records{};
    std::uint64_t spill_bytes{};
    std::uint64_t pending_writes{};       // accepted and neither acked nor discarded (queue + current batch + spill)
    std::uint64_t dup_total{};            // emit.lua answered DUP
    std::uint64_t below_finalized_total{};  // emit.lua refused a tracked emit at or below ckpt.finalized (SinkInvariant)
    std::uint64_t discarded_total{};      // evml_spill_discarded_records_total: items dropped on Fenced / Failed
    std::uint64_t renew_failures_total{};
    std::uint64_t reconnects_total{};
};

namespace detail {
// What the pipeline hands to the sink, as the sink thread stores it (queue, current batch, spill).
struct EmitItem {
    pipeline::Emission e;
};
struct CkptItem {
    pipeline::CheckpointKind kind{};
    std::uint64_t block{};
};
struct CkptInitItem {
    std::uint64_t c0{};
    std::vector<std::string> manifest;
};
struct ContractsItem {
    std::vector<std::string> manifest;
};
struct FinalTrimItem {
    std::uint64_t block{};
};
using WriteItem = std::variant<EmitItem, CkptItem, CkptInitItem, ContractsItem, FinalTrimItem>;

// A read sits in the same queue as the writes, so it is a barrier: it runs only after every earlier write is acked.
struct ReadItem {
    std::function<void(RedisConn&)> run;  // performs the read and delivers the result; throws ConnError on transport failure
    std::function<void()> abandon;        // delivers nullopt
};
using QueueItem = std::variant<WriteItem, ReadItem>;
}  // namespace detail

// The Phase 4 sink (plan 4.4). Producers (the io thread) enqueue and return; one sink thread talks to Redis.
//
// Order, oldest first: current batch (`held_`) -> spill -> queue. A newer item is never sent around an older one that is
// unacked or spilled. Only consecutive emits are pipelined; every other write and every read is a barrier that is sent
// alone, after every earlier item has a successful reply (P1: a checkpoint can never reach Redis ahead of an event it
// covers). On an error, everything from the first non-success reply on is re-sent in order; what already landed
// collapses to DUP in emit.lua or, at worst, re-emits an alternating log/revert pair (I6 still holds).
//
// Thread model: emit/checkpoint/... and the status getters are called from other threads. Everything else is private to
// the sink thread, which runs `pump()` in a loop (start()). Unit tests do not start the thread; they call pump() directly.
class RedisSink final : public pipeline::Sink {
public:
    // `data` carries events/reads, `lease` carries renewals (a second connection, so a slow batch never starves a
    // renewal; P4). `keys` is the shard's key schema.
    RedisSink(RedisSinkOptions opts, Keys keys, std::unique_ptr<RedisConn> data, std::unique_ptr<RedisConn> lease);
    ~RedisSink() override;
    RedisSink(const RedisSink&) = delete;
    RedisSink& operator=(const RedisSink&) = delete;

    // pipeline::Sink
    bool saturated() const override;
    bool readable() const override;
    void emit(pipeline::Emission e) override;
    void checkpoint(pipeline::CheckpointKind kind, std::uint64_t block) override;
    void init_checkpoint(std::uint64_t c0, std::vector<std::string> manifest) override;
    void set_contracts(std::vector<std::string> manifest) override;
    void final_trim(std::uint64_t block) override;
    boost::asio::awaitable<std::optional<pipeline::Checkpoint>> load_checkpoint() override;
    boost::asio::awaitable<std::optional<std::vector<pipeline::ReleasedEntry>>> released_range(std::uint64_t from,
                                                                                               std::uint64_t to) override;

    // Starts / stops the sink thread. stop() resolves queued reads to nullopt; writes still queued are left (the next
    // leader re-sweeps from the Redis checkpoint). The destructor stops.
    void start();
    void stop();

    // One iteration of the sink loop: lease renewal if due, then at most one batch (or one read, or one reconnect
    // probe). Returns true if it did something. Public for tests and for the loop thread only.
    bool pump();

    // True once nothing is pending (every accepted write acked or discarded), or the sink is Fenced / Failed. False on
    // timeout. For tests and for the drain on SIGTERM.
    bool wait_idle(std::chrono::milliseconds timeout) const;

    SinkStatus status() const;
    SinkMode mode() const { return mode_.load(); }
    // The reason of the Failed state (empty otherwise).
    std::string fatal_message() const;
    // Steady-clock ms of the last sink-loop iteration; the process watchdog compares it with now (plan 4.4).
    std::uint64_t loop_heartbeat_ms() const { return heartbeat_ms_.load(); }

private:
    using Clock = RedisSinkOptions::Clock;
    using TimePoint = Clock::time_point;
    enum class LeaseResult { Ok, Resumed, Fenced };
    struct Verdict;

    TimePoint now() const { return opts_.steady_now(); }
    bool leasing() const { return opts_.token != kNoFence; }

    void enqueue(detail::WriteItem item);
    template <class T, class Op>
    boost::asio::awaitable<std::optional<T>> read(Op op);

    void run();
    TimePoint wake_time() const;

    bool service_lease();
    LeaseResult renew_lease();
    bool drain_terminal();
    bool queue_to_spill();
    bool try_reconnect();
    bool refill_from_queue();
    bool refill_from_spill();
    bool send_held();
    void execute_read(detail::ReadItem& item);
    ScriptCall to_call(const detail::WriteItem& item) const;
    Verdict classify(const detail::WriteItem& item, const CallOutcome& outcome);
    std::uint64_t discard_queue();

    void go_degraded(const std::string& reason);
    void schedule_next_probe();
    void handle_fenced();
    void enter_fenced();
    void fail(const std::string& reason);
    void refresh_spill_atomics();

    RedisSinkOptions opts_;
    Keys keys_;
    std::unique_ptr<RedisConn> data_;
    std::unique_ptr<RedisConn> lease_;
    Spill spill_;

    // Shared with producers.
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<detail::QueueItem> queue_;
    std::atomic<bool> stop_{false};
    std::thread thread_;

    // Sink thread only.
    std::deque<detail::WriteItem> held_;  // the current batch: popped from the spill or queue, not yet acked
    TimePoint next_renew_;
    TimePoint next_probe_;
    std::uint32_t probe_attempt_{0};
    std::uint32_t reload_streak_{0};  // consecutive NOSCRIPT batches (a flush racing every reload is not plausible)
    std::uint32_t fence_streak_{0};   // consecutive FENCED batches that renew accepted
    std::mt19937_64 rng_;

    // Readable from any thread.
    std::atomic<SinkMode> mode_{SinkMode::Connected};
    std::atomic<std::uint64_t> queue_depth_{0};
    std::atomic<std::uint64_t> pending_{0};
    std::atomic<bool> spill_saturated_{false};
    std::atomic<std::uint64_t> spill_records_{0};
    std::atomic<std::uint64_t> spill_bytes_{0};
    std::atomic<std::uint64_t> dup_total_{0};
    std::atomic<std::uint64_t> below_finalized_total_{0};
    std::atomic<std::uint64_t> discarded_total_{0};
    std::atomic<std::uint64_t> renew_failures_total_{0};
    std::atomic<std::uint64_t> reconnects_total_{0};
    std::atomic<std::uint64_t> heartbeat_ms_{0};
    mutable std::mutex fatal_mu_;
    std::string fatal_message_;
};

}  // namespace evml::sink
