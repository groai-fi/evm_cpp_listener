#include "sink/redis_sink.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <utility>

#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/json.hpp>

#include "core/error.hpp"
#include "core/log.hpp"
#include "sink/lease.hpp"

namespace evml::sink {

using namespace detail;

namespace {

template <class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

bool is_terminal(SinkMode m) { return m == SinkMode::Fenced || m == SinkMode::Failed; }

// "n|host|nonce" -> n (the lease term). The whole token if it has no '|' (a token is opaque to everything but lease.lua).
std::string term_of(const std::string& token) { return token.substr(0, token.find('|')); }

bool all_digits(const std::string& s) {
    return !s.empty() && std::all_of(s.begin(), s.end(), [](unsigned char c) { return c >= '0' && c <= '9'; });
}

// "1699999999999-0": what XADD returns for '*'.
bool looks_like_stream_id(const std::string& s) {
    const auto dash = s.find('-');
    return dash != std::string::npos && all_digits(s.substr(0, dash)) && all_digits(s.substr(dash + 1));
}

std::string describe(const WriteItem& item) {
    return std::visit(overloaded{[](const EmitItem& i) { return "emit " + i.e.event_id; },
                                 [](const CkptItem& i) {
                                     return std::string("ckpt ") +
                                            (i.kind == pipeline::CheckpointKind::Safe ? "safe " : "finalized ") +
                                            std::to_string(i.block);
                                 },
                                 [](const CkptInitItem& i) { return "ckpt init " + std::to_string(i.c0); },
                                 [](const ContractsItem&) { return std::string("ckpt contracts"); },
                                 [](const FinalTrimItem& i) { return "final_trim " + std::to_string(i.block); }},
                      item);
}

// ---- spill record codec: one JSON object per item; the spill's `kind` byte names the item type ----

enum : std::uint8_t { kEmit = 1, kCkpt = 2, kCkptInit = 3, kContracts = 4, kFinalTrim = 5 };

std::pair<std::uint8_t, std::string> encode(const WriteItem& item) {
    boost::json::object o;
    std::uint8_t kind = 0;
    std::visit(overloaded{[&](const EmitItem& i) {
                              kind = kEmit;
                              o["stream"] = core::to_string(i.e.stream);
                              o["type"] = pipeline::to_string(i.e.type);
                              o["id"] = i.e.event_id;
                              o["dedup"] = i.e.dedup_key;
                              o["fields"] = pack_fields(i.e.fields);
                              if (i.e.released_block) o["released"] = *i.e.released_block;
                          },
                          [&](const CkptItem& i) {
                              kind = kCkpt;
                              o["kind"] = i.kind == pipeline::CheckpointKind::Safe ? "safe" : "finalized";
                              o["block"] = i.block;
                          },
                          [&](const CkptInitItem& i) {
                              kind = kCkptInit;
                              o["c0"] = i.c0;
                              o["manifest"] = pack_manifest(i.manifest);
                          },
                          [&](const ContractsItem& i) {
                              kind = kContracts;
                              o["manifest"] = pack_manifest(i.manifest);
                          },
                          [&](const FinalTrimItem& i) {
                              kind = kFinalTrim;
                              o["block"] = i.block;
                          }},
               item);
    return {kind, boost::json::serialize(o)};
}

const boost::json::value& need(const boost::json::object& o, const char* key, const std::string& raw) {
    const auto* v = o.if_contains(key);
    if (!v) throw core::DecodeError(std::string("spill record: missing `") + key + "`: " + raw);
    return *v;
}

std::uint64_t to_u64(const boost::json::value& v, const char* key, const std::string& raw) {
    if (v.is_uint64()) return v.as_uint64();
    if (v.is_int64() && v.as_int64() >= 0) return static_cast<std::uint64_t>(v.as_int64());
    throw core::DecodeError(std::string("spill record: `") + key + "` is not an unsigned integer: " + raw);
}

std::string to_str(const boost::json::value& v, const char* key, const std::string& raw) {
    if (!v.is_string()) throw core::DecodeError(std::string("spill record: `") + key + "` is not a string: " + raw);
    return std::string(v.as_string());
}

// A record that does not decode is spill corruption: never skipped (plan 4.5).
WriteItem decode(const SpillRecord& rec) {
    boost::system::error_code ec;
    const boost::json::value v = boost::json::parse(rec.payload, ec);
    if (ec || !v.is_object()) throw core::DecodeError("spill record: not a JSON object: " + rec.payload);
    const auto& o = v.as_object();
    const std::string& raw = rec.payload;

    switch (rec.kind) {
        case kEmit: {
            EmitItem i;
            const std::string stream = to_str(need(o, "stream", raw), "stream", raw);
            if (stream == core::to_string(core::StreamKind::Events)) i.e.stream = core::StreamKind::Events;
            else if (stream == core::to_string(core::StreamKind::Registry)) i.e.stream = core::StreamKind::Registry;
            else throw core::DecodeError("spill record: unknown stream '" + stream + "': " + raw);
            const std::string type = to_str(need(o, "type", raw), "type", raw);
            if (type == pipeline::to_string(pipeline::EventType::Log)) i.e.type = pipeline::EventType::Log;
            else if (type == pipeline::to_string(pipeline::EventType::Revert)) i.e.type = pipeline::EventType::Revert;
            else throw core::DecodeError("spill record: unknown type '" + type + "': " + raw);
            i.e.event_id = to_str(need(o, "id", raw), "id", raw);
            i.e.dedup_key = to_str(need(o, "dedup", raw), "dedup", raw);
            i.e.fields = unpack_fields(to_str(need(o, "fields", raw), "fields", raw));
            if (const auto* released = o.if_contains("released")) i.e.released_block = to_u64(*released, "released", raw);
            return i;
        }
        case kCkpt: {
            CkptItem i;
            const std::string kind = to_str(need(o, "kind", raw), "kind", raw);
            if (kind == "safe") i.kind = pipeline::CheckpointKind::Safe;
            else if (kind == "finalized") i.kind = pipeline::CheckpointKind::Finalized;
            else throw core::DecodeError("spill record: unknown checkpoint kind '" + kind + "': " + raw);
            i.block = to_u64(need(o, "block", raw), "block", raw);
            return i;
        }
        case kCkptInit: {
            CkptInitItem i;
            i.c0 = to_u64(need(o, "c0", raw), "c0", raw);
            i.manifest = unpack_manifest(to_str(need(o, "manifest", raw), "manifest", raw));
            return i;
        }
        case kContracts: {
            ContractsItem i;
            i.manifest = unpack_manifest(to_str(need(o, "manifest", raw), "manifest", raw));
            return i;
        }
        case kFinalTrim: {
            FinalTrimItem i;
            i.block = to_u64(need(o, "block", raw), "block", raw);
            return i;
        }
        default: throw core::DecodeError("spill record: unknown kind " + std::to_string(rec.kind) + ": " + raw);
    }
}

RedisSinkOptions validated(RedisSinkOptions o) {
    const auto bad = [](const char* what) { throw std::invalid_argument(std::string("RedisSinkOptions: ") + what); };
    if (o.queue_capacity == 0) bad("queue_capacity must be > 0");
    if (o.max_batch == 0) bad("max_batch must be > 0");
    if (o.retention_ms == 0) bad("retention_ms must be > 0");
    if (o.dedup_ttl_s == 0) bad("dedup_ttl_s must be > 0");
    if (o.token.empty()) bad("token must not be empty (use kNoFence to disable fencing)");
    if (o.token != kNoFence) {
        if (o.lease_ttl_ms == 0 || o.renew_ms == 0) bad("lease_ttl_ms and renew_ms must be > 0 when a lease token is used");
        if (o.renew_ms >= o.lease_ttl_ms) bad("renew_ms must be below lease_ttl_ms");
    }
    if (o.heartbeat_ms > 0 && o.host_id.empty()) bad("host_id must not be empty when heartbeat_ms > 0");
    if (o.heartbeat_ms > 0 && static_cast<std::uint64_t>(o.heartbeat_ttl_s) * 1000 <= o.heartbeat_ms)
        bad("heartbeat_ttl_s must outlive heartbeat_ms (0 would never expire, a shorter TTL makes the key flap)");
    if (!o.consumer_groups.empty() && o.consumer_lag_interval_ms == 0)
        bad("consumer_lag_interval_ms must be > 0 when consumer_groups is not empty");
    if (!o.steady_now) o.steady_now = [] { return RedisSinkOptions::Clock::now(); };
    if (!o.wall_ms) {
        o.wall_ms = [] {
            return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                   std::chrono::system_clock::now().time_since_epoch())
                                                   .count());
        };
    }
    return o;
}

}  // namespace

const char* to_string(SinkMode m) {
    switch (m) {
        case SinkMode::Connected: return "connected";
        case SinkMode::Degraded: return "degraded";
        case SinkMode::Replaying: return "replaying";
        case SinkMode::Fenced: return "fenced";
        case SinkMode::Failed: return "failed";
    }
    throw std::logic_error("to_string: unknown SinkMode");
}

RedisSinkOptions RedisSinkOptions::from_config(const core::SinkCfg& sink, const core::LeaseCfg& lease, std::string token) {
    if (lease.enabled == (token == kNoFence))
        throw std::invalid_argument(lease.enabled ? "RedisSinkOptions: [lease] is enabled but the token disables fencing"
                                                  : "RedisSinkOptions: [lease] is disabled but a lease token was given");
    RedisSinkOptions o;
    o.token = std::move(token);
    o.retention_ms = sink.retention_ms;
    o.dedup_ttl_s = sink.dedup_ttl_s;
    o.queue_capacity = sink.queue_capacity;
    o.spill_dir = sink.spill_dir;
    o.spill_max_bytes = sink.spill_max_bytes;
    o.lease_ttl_ms = lease.ttl_ms;
    o.renew_ms = lease.renew_ms;
    o.consumer_groups = sink.consumer_groups;
    return o;
}

struct RedisSink::Verdict {
    enum class Kind { Ack, Fenced, NoScript, Transient, Fatal } kind{Kind::Ack};
    std::string message;
};

RedisSink::RedisSink(RedisSinkOptions opts, Keys keys, std::unique_ptr<RedisConn> data, std::unique_ptr<RedisConn> lease)
    : opts_(validated(std::move(opts))),
      keys_(std::move(keys)),
      data_(std::move(data)),
      lease_(std::move(lease)),
      spill_(opts_.spill_dir, opts_.spill_max_bytes, opts_.spill_segment_bytes),
      rng_(std::random_device{}()) {
    if (!data_ || !lease_) throw std::invalid_argument("RedisSink: both connections are required");
    next_renew_ = now() + std::chrono::milliseconds(opts_.renew_ms);  // the lease was acquired just now
    next_probe_ = now();
    next_heartbeat_ = now();
    next_lag_probe_ = now();
}

RedisSink::~RedisSink() { stop(); }

// ---- producer side (any thread) ----

bool RedisSink::saturated() const {
    return queue_depth_.load() >= opts_.queue_capacity || spill_saturated_.load() || is_terminal(mode_.load());
}

bool RedisSink::readable() const { return mode_.load() == SinkMode::Connected && !stop_.load(); }

/// @brief Enqueues write item into thread-safe buffer; drops writes if already terminal.
void RedisSink::enqueue(WriteItem item) {
    {
        std::lock_guard lk(mu_);
        // Terminal: the pipeline is being told to stop (saturated()); whatever it still hands over is counted, not hidden.
        // Drops write immediately in terminal state so callers see backpressure without hiding pipeline state.
        if (is_terminal(mode_.load())) {
            ++discarded_total_;
            return;
        }
        queue_.emplace_back(StampedWrite{std::move(item), now()});
        ++queue_depth_;
        ++pending_;
    }
    cv_.notify_one();
}

void RedisSink::emit(pipeline::Emission e) { enqueue(EmitItem{std::move(e)}); }
void RedisSink::checkpoint(pipeline::CheckpointKind kind, std::uint64_t block) { enqueue(CkptItem{kind, block}); }
void RedisSink::init_checkpoint(std::uint64_t c0, std::vector<std::string> manifest) {
    enqueue(CkptInitItem{c0, std::move(manifest)});
}
void RedisSink::set_contracts(std::vector<std::string> manifest) { enqueue(ContractsItem{std::move(manifest)}); }
void RedisSink::final_trim(std::uint64_t block) { enqueue(FinalTrimItem{block}); }

/// @brief Dispatches read operation as a queue barrier, guaranteeing earlier writes are acked first.
template <class T, class Op>
boost::asio::awaitable<std::optional<T>> RedisSink::read(Op op) {
    if (!readable()) co_return std::nullopt;  // degraded / replaying / terminal: nothing to wait for (rev 6, A)

    using Channel = boost::asio::experimental::concurrent_channel<void(boost::system::error_code, std::optional<T>)>;
    auto ex = co_await boost::asio::this_coro::executor;
    // Capacity 1 channel owned only on the caller coroutine so ~Channel() never runs on the sink thread after io_context exits.
    auto channel = std::make_shared<Channel>(ex, 1);

    using Executor = decltype(ex);
    struct State {
        std::mutex mu;
        Executor ex;
        std::weak_ptr<Channel> weak_ch;
        bool active{true};
        State(Executor e, std::weak_ptr<Channel> c) : ex(std::move(e)), weak_ch(std::move(c)) {}
    };
    auto state = std::make_shared<State>(ex, channel);
    struct Guard {
        std::shared_ptr<State> state;
        ~Guard() {
            std::lock_guard lk(state->mu);
            state->active = false;
            state->ex = nullptr;
        }
    } guard{state};

    ReadItem item;
    item.run = [state, op](RedisConn& conn) {
        {
            std::lock_guard lk(state->mu);
            if (!state->active) return;
        }
        std::optional<T> res(op(conn));
        std::lock_guard lk(state->mu);
        if (!state->active) return;
        state->active = false;
        boost::asio::post(state->ex, [weak_ch = state->weak_ch, res = std::move(res)]() mutable {
            if (auto ch = weak_ch.lock()) ch->try_send(boost::system::error_code{}, std::move(res));
        });
        state->ex = nullptr;
    };
    item.abandon = [state] {
        std::lock_guard lk(state->mu);
        if (!state->active) return;
        state->active = false;
        boost::asio::post(state->ex, [weak_ch = state->weak_ch] {
            if (auto ch = weak_ch.lock()) ch->try_send(boost::system::error_code{}, std::optional<T>{});
        });
        state->ex = nullptr;
    };

    {
        std::lock_guard lk(mu_);
        // Invalidate read if sink entered terminal mode or stopped before enqueueing.
        if (is_terminal(mode_.load()) || stop_.load()) co_return std::nullopt;
        queue_.emplace_back(std::move(item));
    }
    cv_.notify_one();
    co_return co_await channel->async_receive(boost::asio::use_awaitable);
}

boost::asio::awaitable<std::optional<pipeline::Checkpoint>> RedisSink::load_checkpoint() {
    co_return co_await read<pipeline::Checkpoint>([](RedisConn& c) { return c.read_checkpoint(); });
}

boost::asio::awaitable<std::optional<std::vector<pipeline::ReleasedEntry>>> RedisSink::released_range(std::uint64_t from,
                                                                                                      std::uint64_t to) {
    co_return co_await read<std::vector<pipeline::ReleasedEntry>>(
        [from, to](RedisConn& c) { return c.read_released(from, to); });
}

// ---- thread lifecycle ----

void RedisSink::start() {
    if (thread_.joinable()) throw std::logic_error("RedisSink::start: already started");
    thread_ = std::thread([this] { run(); });
}

void RedisSink::stop() {
    {
        std::lock_guard lk(mu_);
        stop_.store(true);
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();

    // Reads wait for an answer: resolve them. Writes stay queued; the next leader re-sweeps from the Redis checkpoint.
    std::vector<ReadItem> reads;
    {
        std::lock_guard lk(mu_);
        std::deque<QueueItem> keep;
        for (auto& qi : queue_) {
            if (auto* r = std::get_if<ReadItem>(&qi)) reads.push_back(std::move(*r));
            else keep.push_back(std::move(qi));
        }
        queue_.swap(keep);
    }
    for (auto& r : reads) r.abandon();
}

void RedisSink::set_heartbeat(std::string payload) {
    std::lock_guard lk(hb_mu_);
    hb_payload_ = std::move(payload);
}

/// @brief Flushes pending items up to timeout, joins sink worker, and releases leadership lease.
RedisSink::DrainResult RedisSink::drain(std::chrono::milliseconds budget) {
    // Wait for in-flight batches and spill items to finish writing to Redis.
    wait_idle(budget);
    stop();  // joins the sink thread: from here the lease connection is ours alone
    DrainResult r;
    r.unflushed = pending_.load();
    r.flushed = r.unflushed == 0;
    if (r.unflushed != 0)
        core::log::warn("sink", "drain budget ended with writes unflushed, the next leader re-sweeps",
                        {{"unflushed", std::to_string(r.unflushed)}});
    // Explicitly release leader lease so standby replica takes over immediately without waiting for TTL.
    if (leasing() && mode_.load() != SinkMode::Fenced) {
        try {
            r.lease_released = release_lease(*lease_, keys_, opts_.token);
        } catch (const ConnError& e) {
            core::log::warn("sink", "lease release failed, a standby waits for the TTL", {{"error", e.what()}});
        } catch (const std::exception& e) {
            core::log::error("sink", "lease release failed", {{"error", e.what()}});
        }
    }
    return r;
}

RedisSink::TimePoint RedisSink::wake_time() const {
    TimePoint t = now() + std::chrono::milliseconds(100);  // upper bound, so stop() and the heartbeat never wait long
    if (leasing()) t = std::min(t, next_renew_);
    if (mode_.load() == SinkMode::Degraded) t = std::min(t, next_probe_);
    if (opts_.heartbeat_ms > 0) t = std::min(t, next_heartbeat_);
    if (leasing() && !opts_.consumer_groups.empty()) t = std::min(t, next_lag_probe_);
    return t;
}

void RedisSink::run() {
    while (!stop_.load()) {
        const auto beat_ms = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(now().time_since_epoch()).count());
        heartbeat_ms_.store(beat_ms);
        if (opts_.loop_beat) opts_.loop_beat->store(beat_ms, std::memory_order_relaxed);
        if (pump()) continue;
        std::unique_lock lk(mu_);
        cv_.wait_until(lk, wake_time(), [this] { return stop_.load() || !queue_.empty(); });
    }
}

bool RedisSink::wait_idle(std::chrono::milliseconds timeout) const {
    const auto deadline = Clock::now() + timeout;
    while (true) {
        if (pending_.load() == 0 || is_terminal(mode_.load())) return true;
        if (Clock::now() >= deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

std::optional<RedisSink::TimePoint> RedisSink::oldest_pending_stamp_locked() const {
    if (!held_.empty()) return held_.front().enqueued_at;
    if (!spill_stamps_.empty()) return spill_stamps_.front();
    for (const auto& qi : queue_) {
        if (const auto* w = std::get_if<StampedWrite>(&qi)) return w->enqueued_at;
    }
    return std::nullopt;
}

SinkStatus RedisSink::status() const {
    SinkStatus s;
    s.mode = mode_.load();
    s.queue_depth = queue_depth_.load();
    s.spill_records = spill_records_.load();
    s.spill_bytes = spill_bytes_.load();
    s.pending_writes = pending_.load();
    {
        std::lock_guard lk(mu_);
        if (const auto oldest = oldest_pending_stamp_locked()) {
            const auto age = std::chrono::duration_cast<std::chrono::seconds>(now() - *oldest);
            s.oldest_record_age_s = age.count() > 0 ? static_cast<std::uint64_t>(age.count()) : 0;
        }
    }
    s.dup_total = dup_total_.load();
    s.below_finalized_total = below_finalized_total_.load();
    s.discarded_total = discarded_total_.load();
    s.renew_failures_total = renew_failures_total_.load();
    s.reconnects_total = reconnects_total_.load();
    s.heartbeat_failures_total = heartbeat_failures_total_.load();
    s.consumer_lag_failures_total = consumer_lag_failures_total_.load();
    return s;
}

std::string RedisSink::fatal_message() const {
    std::lock_guard lk(fatal_mu_);
    return fatal_message_;
}

// ---- sink thread ----

/// @brief Advances sink state machine: services lease, heartbeats, lag probes, and batch writes.
bool RedisSink::pump() {
    try {
        if (is_terminal(mode_.load())) return drain_terminal();

        // Renew leader lease before doing any write work to ensure fencing token remains valid.
        bool worked = service_lease();
        if (is_terminal(mode_.load())) return drain_terminal() || worked;
        worked |= service_heartbeat();
        worked |= service_consumer_lag();

        // While anything older sits outside the queue (Degraded: unsent; Replaying: spilled), the queue is not allowed
        // to be sent around it: it joins the spill, in order (plan 4.4). Reads in it resolve to nullopt.
        // Diverts all in-memory writes to local disk spill WAL while degraded or replaying.
        if (mode_.load() != SinkMode::Connected) worked |= queue_to_spill();

        switch (mode_.load()) {
            case SinkMode::Degraded: worked |= try_reconnect(); break;
            case SinkMode::Replaying:
                // Replay disk spill records in strict FIFO order until empty.
                if (held_.empty()) refill_from_spill();
                if (!held_.empty()) worked |= send_held();
                if (mode_.load() == SinkMode::Replaying && held_.empty() && spill_.empty()) {
                    mode_.store(SinkMode::Connected);
                    core::log::info("sink", "spill replayed, back to connected");
                    worked = true;
                }
                break;
            case SinkMode::Connected:
                if (held_.empty()) worked |= refill_from_queue();  // may also have served a read
                if (!held_.empty()) worked |= send_held();
                break;
            case SinkMode::Fenced:
            case SinkMode::Failed: break;  // reached through service_lease/send_held above; handled on the next pump
        }
        return worked;
    } catch (const std::exception& e) {
        fail(e.what());  // fail fast and visible: Failed, saturated, CRITICAL log. Never retried silently.
        return true;
    }
}

bool RedisSink::drain_terminal() {
    const std::uint64_t dropped = discard_queue();
    discarded_total_ += dropped;
    pending_ -= dropped;
    return dropped != 0;
}

std::uint64_t RedisSink::discard_queue() {
    std::deque<QueueItem> taken;
    {
        std::lock_guard lk(mu_);
        taken.swap(queue_);
    }
    std::uint64_t writes = 0;
    for (auto& qi : taken) {
        if (auto* r = std::get_if<ReadItem>(&qi)) {
            r->abandon();
        } else {
            ++writes;
            --queue_depth_;
        }
    }
    return writes;
}

/// @brief Spills pending in-memory write queue to disk WAL during Redis outage or replaying mode.
bool RedisSink::queue_to_spill() {
    std::deque<QueueItem> taken;
    {
        std::lock_guard lk(mu_);
        taken.swap(queue_);
        for (const auto& qi : taken) {
            if (const auto* w = std::get_if<StampedWrite>(&qi)) {
                spill_stamps_.push_back(w->enqueued_at);
            }
        }
    }
    if (taken.empty()) return false;

    std::size_t done = 0;
    try {
        for (; done < taken.size(); ++done) {
            auto& qi = taken[done];
            if (auto* r = std::get_if<ReadItem>(&qi)) {
                r->abandon();
                continue;
            }
            // Encodes write item into JSON and writes sequentially to disk spill WAL with CRC32C checksum.
            const auto [kind, payload] = encode(std::get<StampedWrite>(qi).item);
            spill_.append(kind, payload);
            --queue_depth_;
        }
    } catch (...) {
        // The spill failed: the sink goes Failed (pump catches). What was not moved is lost, so it is counted.
        std::uint64_t lost = 0;
        for (std::size_t i = done; i < taken.size(); ++i) {
            if (std::holds_alternative<StampedWrite>(taken[i])) ++lost;
            else std::get<ReadItem>(taken[i]).abandon();
        }
        {
            std::lock_guard lk(mu_);
            if (lost > spill_stamps_.size()) throw std::logic_error("RedisSink: spill stamps underflow");
            spill_stamps_.resize(spill_stamps_.size() - static_cast<std::size_t>(lost));
        }
        queue_depth_ -= lost;
        pending_ -= lost;
        discarded_total_ += lost;
        refresh_spill_atomics();
        throw;
    }
    refresh_spill_atomics();
    return true;
}

/// @brief Fills held_ batch from in-memory queue; enforces barrier isolation for checkpoints and trims.
bool RedisSink::refill_from_queue() {
    std::unique_lock lk(mu_);
    if (queue_.empty()) return false;

    if (auto* r = std::get_if<ReadItem>(&queue_.front())) {
        // held_ is empty, so every earlier write is acked: the read sees them all (rev 6, A).
        ReadItem item = std::move(*r);
        queue_.pop_front();
        lk.unlock();
        execute_read(item);
        return true;
    }
    while (!queue_.empty() && held_.size() < opts_.max_batch) {
        auto* w = std::get_if<StampedWrite>(&queue_.front());
        if (!w) break;  // a read ends the batch: it is a barrier
        // Non-emit items (checkpoints, manifest, trims) are barriers: they break batching and run alone.
        const bool barrier = !std::holds_alternative<EmitItem>(w->item);
        if (barrier && !held_.empty()) break;  // a barrier is sent alone
        held_.push_back(std::move(*w));
        queue_.pop_front();
        --queue_depth_;
        if (barrier) break;
    }
    return !held_.empty();
}

/// @brief Fills held_ batch from disk spill WAL in strict FIFO order, respecting barrier boundaries.
bool RedisSink::refill_from_spill() {
    while (held_.size() < opts_.max_batch) {
        const auto rec = spill_.peek();
        if (!rec) break;
        WriteItem item = decode(*rec);
        // Non-emit items in spill also act as barriers and must be processed singly.
        const bool barrier = !std::holds_alternative<EmitItem>(item);
        if (barrier && !held_.empty()) break;
        spill_.pop();
        {
            std::lock_guard lk(mu_);
            if (spill_stamps_.empty()) throw std::logic_error("RedisSink: spill record missing enqueue timestamp");
            const TimePoint ts = spill_stamps_.front();
            spill_stamps_.pop_front();
            held_.push_back(StampedWrite{std::move(item), ts});
        }
        if (barrier) break;
    }
    refresh_spill_atomics();
    return !held_.empty();
}

void RedisSink::execute_read(ReadItem& item) {
    try {
        item.run(*data_);
    } catch (const ConnError& e) {
        item.abandon();
        go_degraded(std::string("read failed: ") + e.what());
    } catch (...) {
        item.abandon();
        throw;  // schema violation or a bug: pump() fails the sink
    }
}

/// @brief Converts high-level WriteItem to corresponding Lua ScriptCall with fencing token and MINID.
ScriptCall RedisSink::to_call(const WriteItem& item) const {
    const std::uint64_t wall = opts_.wall_ms();
    // Calculates sliding retention MINID timestamp to prune expired stream entries on XADD.
    const std::uint64_t minid = wall > opts_.retention_ms ? wall - opts_.retention_ms : 0;
    return std::visit(
        overloaded{[&](const EmitItem& i) {
                       return emit_call(keys_, i.e, EmitParams{opts_.dedup_ttl_s, minid, opts_.token});
                   },
                   [&](const CkptItem& i) { return ckpt_advance_call(keys_, opts_.token, i.kind, i.block); },
                   [&](const CkptInitItem& i) { return ckpt_init_call(keys_, opts_.token, i.c0, i.manifest); },
                   [&](const ContractsItem& i) { return ckpt_contracts_call(keys_, opts_.token, i.manifest); },
                   [&](const FinalTrimItem& i) { return final_trim_call(keys_, opts_.token, i.block); }},
        item);
}

/// @brief Classifies Lua execution response into Ack, Fenced, Transient, NoScript, or Fatal.
RedisSink::Verdict RedisSink::classify(const WriteItem& item, const CallOutcome& outcome) {
    using K = Verdict::Kind;
    if (const auto* f = std::get_if<ScriptFail>(&outcome)) {
        switch (f->kind) {
            case FailKind::NoScript: return {K::NoScript, f->message};
            case FailKind::Transient: return {K::Transient, f->message};
            case FailKind::Fatal: return {K::Fatal, "script error on " + describe(item) + ": " + f->message};
        }
    }
    const auto& ok = std::get<ScriptOk>(outcome);
    if (!ok.value) return {K::Fatal, "nil reply to " + describe(item)};
    const std::string& r = *ok.value;
    if (r == "FENCED") return {K::Fenced, {}};

    const auto unexpected = [&] { return Verdict{K::Fatal, "unexpected reply '" + r + "' to " + describe(item)}; };
    return std::visit(
        overloaded{[&](const EmitItem& i) -> Verdict {
                       if (r == "DUP") {
                           // Deduplication hit (DUP): event was already committed in Redis; safely skip re-emission.
                           ++dup_total_;
                       } else if (r == "BELOW_FINALIZED") {
                           // The final sweep already covered this block; dropping it is correct (plan 4.2). Normal
                           // operation never produces it, so it is loud.
                           // Refuse emit below finalized block checkpoint to preserve monotonic finality invariant.
                           ++below_finalized_total_;
                           const std::string block = i.e.released_block ? std::to_string(*i.e.released_block) : "";
                           core::log::error("sink", "SinkInvariant: BELOW_FINALIZED, emit dropped",
                                            {{"event_id", i.e.event_id}, {"block", block}});
                       } else if (!looks_like_stream_id(r)) {
                           return unexpected();
                       }
                       return {};
                   },
                   [&](const CkptItem&) -> Verdict { return r == "OK" || r == "NOOP" ? Verdict{} : unexpected(); },
                   [&](const CkptInitItem&) -> Verdict {
                       if (r == "EXISTS") {
                           // A checkpoint appeared between load and init: the fence, not the driver, resolves races.
                           // Checkpoint exists already; leader election fence ensures no overwrites occur.
                           core::log::warn("sink", "init_checkpoint found an existing checkpoint, left untouched");
                           return {};
                       }
                       return r == "OK" ? Verdict{} : unexpected();
                   },
                   [&](const ContractsItem&) -> Verdict { return r == "OK" ? Verdict{} : unexpected(); },
                   [&](const FinalTrimItem&) -> Verdict { return all_digits(r) ? Verdict{} : unexpected(); }},
        item);
}

/// @brief Sends held batch over Redis pipeline, acknowledges in-order prefix, and manages script reloads/fences.
bool RedisSink::send_held() {
    std::vector<ScriptCall> calls;
    calls.reserve(held_.size());
    for (const auto& sw : held_) calls.push_back(to_call(sw.item));

    std::vector<CallOutcome> outcomes;
    try {
        // Dispatches entire batch over data connection in a single network round-trip.
        outcomes = data_->pipeline(calls);
    } catch (const ConnError& e) {
        // The outcome of every call is unknown: the whole batch stays held and is re-sent (dedup makes that safe).
        go_degraded(e.what());
        return true;
    }
    if (outcomes.size() != calls.size())
        throw std::logic_error("RedisConn::pipeline returned " + std::to_string(outcomes.size()) + " outcomes for " +
                               std::to_string(calls.size()) + " calls");

    // Everything from the first non-success on is unacked, even if Redis ran some of it (plan 4.4, P1).
    // Acks only contiguous prefix of successful replies; stops at first non-Ack to preserve sequence.
    std::size_t acked = 0;
    std::optional<Verdict> stop;
    for (; acked < outcomes.size(); ++acked) {
        Verdict v = classify(held_[acked].item, outcomes[acked]);
        if (v.kind != Verdict::Kind::Ack) {
            stop = std::move(v);
            break;
        }
    }
    // Erase acked prefix from held_ queue and decrement pending write counter.
    {
        std::lock_guard lk(mu_);
        held_.erase(held_.begin(), held_.begin() + static_cast<std::ptrdiff_t>(acked));
    }
    pending_ -= acked;
    if (acked > 0) {
        reload_streak_ = 0;
        fence_streak_ = 0;
    }
    if (!stop) return true;

    switch (stop->kind) {
        case Verdict::Kind::Ack: break;
        case Verdict::Kind::NoScript:
            if (++reload_streak_ > 2) {
                go_degraded("scripts keep disappearing right after a reload");
                break;
            }
            try {
                // Redis flushed script cache; reload all SHA1 Lua scripts and retry on next pump.
                data_->load_scripts();  // the next pump re-sends from the failed item
            } catch (const ConnError& e) {
                go_degraded(std::string("script reload failed: ") + e.what());
            }
            break;
        case Verdict::Kind::Fenced: handle_fenced(); break;
        case Verdict::Kind::Transient: go_degraded(stop->message); break;
        case Verdict::Kind::Fatal: throw std::runtime_error(stop->message);
    }
    return true;
}

// ---- lease ----

/// @brief Sends lease renewal script on lease connection; retries once if script needs reloading.
RedisSink::LeaseResult RedisSink::renew_lease() {
    const ScriptCall call = lease_renew_call(keys_, opts_.token, opts_.lease_ttl_ms);
    for (int attempt = 0; attempt < 2; ++attempt) {
        // Dispatches renewal over dedicated lease connection so high event load never starves renewal.
        const std::vector<CallOutcome> outcomes = lease_->pipeline({call});
        if (outcomes.size() != 1) throw std::logic_error("RedisConn::pipeline returned the wrong number of outcomes for renew");

        if (const auto* f = std::get_if<ScriptFail>(&outcomes.front())) {
            if (f->kind == FailKind::NoScript && attempt == 0) {
                // Redis script cache flushed; reload script on lease connection and retry immediately.
                lease_->load_scripts();
                continue;
            }
            if (f->kind == FailKind::Fatal) throw std::runtime_error("lease renew failed: " + f->message);
            throw ConnError("lease renew: " + f->message);
        }
        const auto& value = std::get<ScriptOk>(outcomes.front()).value;
        if (value == "OK") return LeaseResult::Ok;
        if (value == "RESUMED") return LeaseResult::Resumed;
        if (value == "FENCED") return LeaseResult::Fenced;
        throw std::runtime_error("lease renew: unexpected reply '" + value.value_or("<nil>") + "'");
    }
    throw ConnError("lease renew: scripts still missing after a reload");
}

bool RedisSink::service_lease() {
    if (!leasing() || now() < next_renew_) return false;
    next_renew_ = now() + std::chrono::milliseconds(opts_.renew_ms);
    try {
        if (renew_lease() == LeaseResult::Fenced) enter_fenced();
    } catch (const ConnError& e) {
        // Not fatal and not a demotion: if the key expires meanwhile, the next write answers FENCED and handle_fenced()
        // asks again (RESUMED when nobody took over).
        ++renew_failures_total_;
        core::log::warn("sink", "lease renew failed", {{"error", e.what()}});
    }
    return true;
}

std::vector<ConsumerLag> RedisSink::consumer_lag() const {
    std::lock_guard lk(lag_mu_);
    return lag_snapshot_;
}

bool RedisSink::service_consumer_lag() {
    // Plan 4.10: the leader only; a lease-less host has no leader election, so the probe is off there.
    if (!leasing() || opts_.consumer_groups.empty() || now() < next_lag_probe_) return false;
    // Advance first, like the heartbeat: wake_time() includes this deadline.
    next_lag_probe_ = now() + std::chrono::milliseconds(opts_.consumer_lag_interval_ms);
    if (mode_.load() == SinkMode::Degraded) return false;  // Redis is known to be down
    try {
        std::vector<ConsumerLag> snapshot;
        snapshot.reserve(opts_.consumer_groups.size());
        const std::uint64_t probed_at_ms = opts_.wall_ms();
        for (const auto& g : opts_.consumer_groups) {
            snapshot.push_back(evaluate_consumer_lag(g, lease_->read_group_facts(keys_.stream(g.stream), g.group)));
            snapshot.back().probed_at_ms = probed_at_ms;
        }
        std::lock_guard lk(lag_mu_);
        lag_snapshot_ = std::move(snapshot);
    } catch (const ConnError& e) {
        ++consumer_lag_failures_total_;  // the previous snapshot stays; probed_at_ms tells the host how old it is
        core::log::warn("sink", "consumer lag probe failed", {{"error", e.what()}});
    } catch (const std::exception& e) {
        // A reply we cannot read (permissions, wrong type, parse error). The probe is read-only and must never stop the
        // writer, but it is not silent: counted, and logged at ERROR.
        ++consumer_lag_failures_total_;
        core::log::error("sink", "consumer lag probe failed, the sink keeps writing", {{"error", e.what()}});
    }
    return true;
}

bool RedisSink::service_heartbeat() {
    if (opts_.heartbeat_ms == 0 || now() < next_heartbeat_) return false;
    // Advance first: wake_time() includes this deadline, so every early return below must leave it in the future.
    next_heartbeat_ = now() + std::chrono::milliseconds(opts_.heartbeat_ms);
    if (mode_.load() == SinkMode::Degraded) return false;  // Redis is known to be down; the probe handles recovery
    std::string payload;
    {
        std::lock_guard lk(hb_mu_);
        payload = hb_payload_;
    }
    if (payload.empty()) return false;
    try {
        lease_->set_with_ttl(keys_.hb(opts_.host_id), payload, opts_.heartbeat_ttl_s);
    } catch (const ConnError& e) {
        ++heartbeat_failures_total_;
        core::log::warn("sink", "heartbeat write failed", {{"error", e.what()}});
    } catch (const std::exception& e) {
        ++heartbeat_failures_total_;  // not silent, but a heartbeat must never stop the writer
        core::log::error("sink", "heartbeat write failed, the sink keeps writing", {{"error", e.what()}});
    }
    return true;
}

/// @brief Handles FENCED response from write script by verifying leadership lease status.
void RedisSink::handle_fenced() {
    if (!leasing()) throw std::runtime_error("a write script answered FENCED, but fencing is disabled (token 0 never fences)");
    if (++fence_streak_ > 1)
        throw std::runtime_error("a write was fenced again right after renew accepted the token: lease state is inconsistent");
    try {
        // Immediate renewal query verifies whether another host took leadership or token expired.
        const LeaseResult r = renew_lease();
        next_renew_ = now() + std::chrono::milliseconds(opts_.renew_ms);
        if (r == LeaseResult::Fenced) enter_fenced();
        // Ok / Resumed: the batch is still held; the next pump re-sends it from the fenced item.
    } catch (const ConnError& e) {
        go_degraded(std::string("renew after FENCED failed: ") + e.what());
    }
}

/// @brief Transitions to terminal Fenced mode, discarding queue and disk spill to avoid stale dual writes.
void RedisSink::enter_fenced() {
    // Drop all held batches, local disk spill WAL, and unhandled queue items to prevent split-brain writes.
    std::uint64_t dropped = 0;
    {
        std::lock_guard lk(mu_);
        dropped += held_.size();
        held_.clear();
        spill_stamps_.clear();
    }
    dropped += spill_.discard_all();
    refresh_spill_atomics();
    dropped += discard_queue();
    discarded_total_ += dropped;
    pending_ -= dropped;
    mode_.store(SinkMode::Fenced);
    const std::string term = term_of(opts_.token);
    const std::string discarded = std::to_string(dropped);
    core::log::warn("sink", "SinkFenced: another term holds the lease, queue and spill discarded",
                    {{"old_term", term}, {"discarded_records", discarded}});
}

// ---- outage lifecycle ----

void RedisSink::go_degraded(const std::string& reason) {
    reload_streak_ = 0;
    fence_streak_ = 0;
    if (mode_.load() == SinkMode::Degraded) {
        schedule_next_probe();  // a failed probe: back off further
        return;
    }
    mode_.store(SinkMode::Degraded);
    probe_attempt_ = 0;
    schedule_next_probe();
    core::log::warn("sink", "redis unavailable, spilling", {{"reason", reason}});
}

/// @brief Computes exponential backoff interval with full jitter for next reconnect probe.
void RedisSink::schedule_next_probe() {
    // Calculates exponential backoff capped at probe_max with full random jitter in [0, cap_ms].
    const double cap_ms = std::min<double>(static_cast<double>(opts_.probe_max.count()),
                                           static_cast<double>(opts_.probe_min.count()) * std::pow(2.0, std::min(probe_attempt_, 20u)));
    std::uniform_int_distribution<std::int64_t> jitter(0, static_cast<std::int64_t>(cap_ms));  // full jitter
    next_probe_ = now() + std::chrono::milliseconds(jitter(rng_));
    ++probe_attempt_;
}

/// @brief Attempts reconnect by verifying lease renewal first, reloading scripts, and transitioning to Replaying.
bool RedisSink::try_reconnect() {
    if (now() < next_probe_) return false;
    try {
        // First op after an outage is a renewal (plan 4.8): it detects a takeover before any write is attempted.
        // Verifies leader fencing status before attempting data writes to prevent writing under expired term.
        if (leasing()) {
            const LeaseResult r = renew_lease();
            next_renew_ = now() + std::chrono::milliseconds(opts_.renew_ms);
            if (r == LeaseResult::Fenced) {
                enter_fenced();
                return true;
            }
        }
        // Loads SHA1 Lua scripts into Redis script cache to confirm data connection health.
        data_->load_scripts();  // proves the data connection works, and restores scripts after a Redis restart
    } catch (const ConnError& e) {
        go_degraded(e.what());
        return true;
    }
    ++reconnects_total_;
    probe_attempt_ = 0;
    // Enter Replaying mode if backlog exists in held batch or disk spill WAL; else Connected.
    const bool backlog = !held_.empty() || !spill_.empty();
    mode_.store(backlog ? SinkMode::Replaying : SinkMode::Connected);
    core::log::info("sink", backlog ? "redis reachable again, replaying the spill" : "redis reachable again");
    return true;
}

void RedisSink::fail(const std::string& reason) {
    {
        std::lock_guard lk(fatal_mu_);
        if (fatal_message_.empty()) fatal_message_ = reason;
    }
    mode_.store(SinkMode::Failed);
    core::log::error("sink", "CRITICAL: sink failed, no further writes", {{"reason", reason}});
}

void RedisSink::refresh_spill_atomics() {
    spill_records_.store(spill_.records());
    spill_bytes_.store(spill_.bytes());
    spill_saturated_.store(spill_.saturated());
}

}  // namespace evml::sink
