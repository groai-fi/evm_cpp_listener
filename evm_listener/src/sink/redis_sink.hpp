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
/// @brief State machine modes governing sink connection and fallback behavior.
/// Invariants:
/// - Connected: Primary mode; writes pipeline directly to Redis, reads execute against data connection.
/// - Degraded: Redis unreachable; pipeline writes divert to local spill WAL; reconnect probes back off with jitter.
/// - Replaying: Reconnected; held batch and spill WAL are drained in strict FIFO order before accepting queue writes.
/// - Fenced / Failed: Terminal states. Fenced drops queued items to avoid dual-writer split-brain; Failed halts on unrecoverable error.
enum class SinkMode : int { Connected, Degraded, Replaying, Fenced, Failed };
const char* to_string(SinkMode m);

/// @brief Configuration settings controlling Redis batching, leasing, dedup, and disk spill.
/// Watch out:
/// - `renew_ms` must be strictly smaller than `lease_ttl_ms` to provide time for retries before expiration.
/// - `queue_capacity` acts as a soft threshold triggering `saturated()` backpressure, but does not drop elements.
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

    // Heartbeat (plan 4.7): `SET hb:<host_id> <payload> EX heartbeat_ttl_s` every heartbeat_ms on the lease connection,
    // once set_heartbeat() gave it a payload. 0 = disabled. from_config() leaves these at their defaults: the host wires
    // them from [ops].heartbeat_s and [shard].host_id.
    std::string host_id;
    std::uint32_t heartbeat_ms{0};
    std::uint32_t heartbeat_ttl_s{30};

    // Consumer-group lag probe (plan 4.10): leader only, on the lease connection, every consumer_lag_interval_ms.
    // Empty groups = disabled. from_config() copies the groups.
    std::vector<core::ConsumerGroupCfg> consumer_groups;
    std::uint32_t consumer_lag_interval_ms{60000};

    // Process watchdog (plan 60 5.1): when non-null, every sink-loop iteration also stores its steady-clock ms here. The
    // storage belongs to the Watchdog (process lifetime) and must outlive this sink; null = no watchdog.
    std::atomic<std::uint64_t>* loop_beat{nullptr};

    /// @brief Constructs and validates RedisSinkOptions from configuration sections.
    /// @throws std::invalid_argument If configuration violates fencing consistency or timeout constraints.
    static RedisSinkOptions from_config(const core::SinkCfg& sink, const core::LeaseCfg& lease, std::string token);
};

/// @brief Telemetry snapshot capturing sink state, backpressure, and error counters.
struct SinkStatus {
    SinkMode mode{SinkMode::Connected};
    std::uint64_t queue_depth{};
    std::uint64_t spill_records{};
    std::uint64_t spill_bytes{};
    std::uint64_t pending_writes{};       // accepted and neither acked nor discarded (queue + current batch + spill)
    std::uint64_t dup_total{};            // emit.lua answered DUP
    std::uint64_t below_finalized_total{};  // emit.lua refused a tracked emit at or below ckpt.finalized (SinkInvariant)
    std::uint64_t discarded_total{};      // evml_spill_discarded_records_total: dropped on Fenced (queue, batch, spill); on Failed only queued items (the held batch and spill stay in pending_writes)
    std::uint64_t renew_failures_total{};
    std::uint64_t heartbeat_failures_total{};
    std::uint64_t consumer_lag_failures_total{};
    std::uint64_t reconnects_total{};
    // Age of the oldest accepted write that is neither acked nor discarded (queue, current batch or spill); 0 when
    // nothing is pending. Measured on the injected steady clock. Plan 60 §5.3.3 `oldest_record_age_s`.
    std::uint64_t oldest_record_age_s{};
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

struct StampedWrite {
    WriteItem item;
    RedisSinkOptions::Clock::time_point enqueued_at{};
};

// A read sits in the same queue as the writes, so it is a barrier: it runs only after every earlier write is acked.
struct ReadItem {
    std::function<void(RedisConn&)> run;  // performs the read and delivers the result; throws ConnError on transport failure
    std::function<void()> abandon;        // delivers nullopt
};
using QueueItem = std::variant<StampedWrite, ReadItem>;
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
/// @brief Multi-threaded Redis stream sink with atomic Lua dedup, dual TCP connections, and disk spill WAL.
///
/// Invariants & Guarantees:
/// - Invariant I3 (Monotonic Checkpoints): Checkpoint writes (`CkptItem`) only move forward via Lua scripts.
/// - Invariant I4 (Atomic Deduplication): `emit.lua` checks dedup key, writes stream with `XADD MINID`, and sets dedup TTL.
/// - Invariant P1 (Ordered Delivery): Checkpoints and trims act as barriers; emits pipeline up to `max_batch`.
/// - Dual TCP Connections: `lease_` connection runs lease renewals and heartbeats independently so slow event pipelines cannot starve renewals.
class RedisSink final : public pipeline::Sink {
public:
    /// @brief Constructs RedisSink with dedicated data and lease connections.
    /// @param opts Configuration options for batching, leasing, dedup, and spill.
    /// @param keys Key schema for Redis streams, sets, and checkpoints.
    /// @param data Dedicated connection for pipelined event writes and state reads.
    /// @param lease Dedicated connection for leader lease renewal and heartbeat.
    RedisSink(RedisSinkOptions opts, Keys keys, std::unique_ptr<RedisConn> data, std::unique_ptr<RedisConn> lease);
    ~RedisSink() override;
    RedisSink(const RedisSink&) = delete;
    RedisSink& operator=(const RedisSink&) = delete;

    // pipeline::Sink
    /// @brief Returns true if queue depth or spill exceeds soft cap, or if sink entered a terminal state.
    bool saturated() const override;
    /// @brief Returns true only when connected and running, permitting read operations.
    bool readable() const override;
    /// @brief Enqueues an event emission to the sink queue; non-blocking for callers.
    void emit(pipeline::Emission e) override;
    /// @brief Enqueues a checkpoint update; acts as an execution barrier behind pending emits.
    void checkpoint(pipeline::CheckpointKind kind, std::uint64_t block) override;
    /// @brief Enqueues initial checkpoint write if no checkpoint exists yet.
    void init_checkpoint(std::uint64_t c0, std::vector<std::string> manifest) override;
    /// @brief Enqueues registered contracts manifest update.
    void set_contracts(std::vector<std::string> manifest) override;
    /// @brief Enqueues release of finalized blocks and stream trims.
    void final_trim(std::uint64_t block) override;
    /// @brief Coroutine reading current checkpoint from Redis; flushes earlier writes first.
    boost::asio::awaitable<std::optional<pipeline::Checkpoint>> load_checkpoint() override;
    /// @brief Coroutine reading released block records within [from, to] range.
    boost::asio::awaitable<std::optional<std::vector<pipeline::ReleasedEntry>>> released_range(std::uint64_t from,
                                                                                               std::uint64_t to) override;

    // Starts / stops the sink thread. stop() resolves queued reads to nullopt; writes still queued are left (the next
    // leader re-sweeps from the Redis checkpoint). The destructor stops.
    /// @brief Spawns background worker thread running pump() loop.
    void start();
    /// @brief Signals worker thread to stop, abandons pending reads, and joins worker thread.
    void stop();

    // Publishes the heartbeat payload (the host's JSON state; the content comes from the io thread). Thread-safe. The sink
    // thread writes the latest payload every heartbeat_ms.
    /// @brief Updates latest JSON heartbeat payload to write to Redis.
    void set_heartbeat(std::string payload);

    // The latest consumer-group lag snapshot, one entry per configured group (empty before the first probe, or when the
    // probe is disabled). Thread-safe. Phase 5 turns it into metrics and alerts.
    /// @brief Returns cached consumer lag metrics for configured consumer groups.
    std::vector<ConsumerLag> consumer_lag() const;

    // Graceful shutdown (plan 4.8 "Draining"): waits up to `budget` for every accepted write to be acked, stops the sink
    // thread, then releases the lease so a standby takes over without waiting for the TTL. If the flush did not finish
    // it reports how many writes were left: the next leader re-sweeps from the Redis checkpoint, so that is allowed.
    struct DrainResult {
        bool flushed{false};
        std::uint64_t unflushed{0};
        bool lease_released{false};
    };
    /// @brief Flushes pending writes up to budget timeout, joins thread, and releases leader lease.
    DrainResult drain(std::chrono::milliseconds budget);

    // One iteration of the sink loop: lease renewal if due, then at most one batch (or one read, or one reconnect
    // probe). Returns true if it did something. Public for tests and for the loop thread only.
    /// @brief Performs one iteration of the sink event loop (lease check, heartbeat, spill flush, batch emit).
    /// @return True if progress was made during this iteration.
    bool pump();

    // True once nothing is pending (every accepted write acked or discarded), or the sink is Fenced / Failed. False on
    // timeout. For tests and for the drain on SIGTERM.
    /// @brief Blocks until pending write queue is empty or sink enters terminal state.
    bool wait_idle(std::chrono::milliseconds timeout) const;

    /// @brief Returns current sink status and queue/spill metrics snapshot.
    SinkStatus status() const;
    /// @brief Returns current operational mode atomically.
    SinkMode mode() const { return mode_.load(); }
    // The reason of the Failed state (empty otherwise).
    /// @brief Returns error description if sink entered Failed mode.
    std::string fatal_message() const;
    // Steady-clock ms of the last sink-loop iteration; the process watchdog compares it with now (plan 4.4).
    /// @brief Timestamp of most recent pump() pass for watchdog liveness monitoring.
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
    bool service_heartbeat();
    bool service_consumer_lag();
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
    std::optional<TimePoint> oldest_pending_stamp_locked() const;

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

    // Shared with producers and status() readers under mu_.
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<detail::QueueItem> queue_;
    std::deque<detail::StampedWrite> held_;  // the current batch: popped from the spill or queue, not yet acked
    std::deque<TimePoint> spill_stamps_;     // enqueue times of records currently on disk in spill_, oldest first
    std::atomic<bool> stop_{false};
    std::thread thread_;

    // Sink thread only.
    TimePoint next_renew_;
    TimePoint next_heartbeat_;
    TimePoint next_lag_probe_;
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
    std::atomic<std::uint64_t> heartbeat_failures_total_{0};
    std::atomic<std::uint64_t> consumer_lag_failures_total_{0};
    std::atomic<std::uint64_t> reconnects_total_{0};
    std::atomic<std::uint64_t> heartbeat_ms_{0};
    mutable std::mutex fatal_mu_;
    std::string fatal_message_;
    mutable std::mutex hb_mu_;
    std::string hb_payload_;
    mutable std::mutex lag_mu_;
    std::vector<ConsumerLag> lag_snapshot_;
};

}  // namespace evml::sink
