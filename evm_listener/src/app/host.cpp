#include "app/host.hpp"

#include <algorithm>
#include <csignal>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/json.hpp>

#include "app/pool_config.hpp"
#include "app/pool_log_source.hpp"
#include "app/tools.hpp"
#include "chain/finality_gate.hpp"
#include "chain/reorg_detector.hpp"
#include "core/env.hpp"
#include "core/log.hpp"
#include "core/version.hpp"
#include "ops/alert_sender.hpp"
#include "ops/alerts.hpp"
#include "ops/deadman.hpp"
#include "ops/metrics.hpp"
#include "ops/ops_http_server.hpp"
#include "ops/snapshot.hpp"
#include "ops/watchdog.hpp"
#include "pipeline/pending_buffer.hpp"
#include "pipeline/sweeper.hpp"
#include "provider/provider_pool.hpp"
#include "rpc/codec.hpp"
#include "sink/keys.hpp"
#include "sink/lease.hpp"
#include "sink/redis_sink.hpp"
#include "sink/redis_startup.hpp"
#include "sink/redispp_conn.hpp"

namespace evml::app {

namespace {

namespace net = boost::asio;
namespace log = core::log;
using namespace std::chrono_literals;

constexpr auto kDrainBudget = 25s;      // plan 4.8: flush the sink before exit
constexpr auto kShutdownGrace = 300ms;  // lets aborted pool I/O unwind before the io loop stops
constexpr auto kOpsPeriod = 10s;        // plan 5.3.3: the alert engine evaluates a fresh snapshot this often
// A fork walk fetches at most this many blocks (call_to is paced at reconcile_rps, so a long walk would hold the
// provider's token bucket for minutes). A longer gap or deeper fork resyncs the ring and re-verifies instead.
constexpr std::size_t kMaxWalkBlocks = 16;
constexpr int kWalkAttempts = 3;  // restarts after an Inconsistent walk (a reorg mid-walk or a bad block)

std::atomic<bool> g_signal_stop{false};
extern "C" void on_signal(int) { g_signal_stop.store(true, std::memory_order_relaxed); }

std::uint64_t wall_ms() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

/// Everything that exists only while the pipeline runs. Members are declared in dependency order, so the reverse
/// destruction order tears down the driver first and the sink last.
struct Runtime {
    Runtime(const std::string& shard, std::size_t ring_size, std::size_t pending_max)
        : keys(shard), ring(ring_size), pending(pending_max) {}
    sink::Keys keys;
    std::unique_ptr<sink::RedisSink> sink;
    chain::FinalityGate gate;
    chain::ReorgDetector ring;  // fed by WS heads on the io thread; read by `source` (also io thread)
    pipeline::PendingBuffer pending;  // WS logs awaiting their sweep (cross-check only, never emitted); io thread
    std::shared_ptr<provider::ProviderPool> pool;
    std::unique_ptr<PoolLogSource> source;
    std::unique_ptr<pipeline::Sweeper> sweeper;
    std::unique_ptr<pipeline::PipelineDriver> driver;
};

/// A WS head and the provider that delivered it.
struct HeadEvent {
    chain::BlockRef ref;
    std::string provider;
};

class Host {
public:
    Host(const core::Config& cfg, const HostOptions& opt)
        : cfg_(cfg),
          opt_(opt),
          wd_([&] {
              ops::Watchdog::Options o;
              o.stall_ms = cfg.watchdog.process_stall_ms;
              return o;
          }()),
          engine_(ops::AlertThresholds::from_config(cfg)),
          beat_timer_(ioc_),
          sleep_timer_(ioc_),
          grace_timer_(ioc_),
          freeze_timer_(ioc_) {}

    int run();

private:
    // Where the process is, for /readyz and the worker thread. Written by the worker (Standby) and the io thread (Leader).
    enum class Phase : int { WaitingRedis, Standby, Leader };

    // ---- io thread ----
    void start_runtime(const std::optional<sink::StartupReport>& rep, const std::string& token);
    net::awaitable<void> pipeline_loop();
    void on_tag(const std::string& provider, provider::Tag tag, std::uint64_t number, const core::Hash32& hash);
    // WS heads -> the term's ReorgDetector (plan 5.H). All of these run on the io thread.
    void on_head(const rpc::Head& h, const std::string& provider);
    void handle_head(const HeadEvent& e);
    net::awaitable<void> walk_fork(std::uint64_t epoch, HeadEvent e);
    void resync_ring(const chain::BlockRef& head, const std::string& why);
    void bump_head_epoch();
    // plan 4.7: the payload the sink writes as `hb:<host_id>` every [ops].heartbeat_s. io thread.
    void publish_heartbeat(const pipeline::DriverStatus& st);
    // WS logs -> PendingBuffer, and the per-page cross-check against the sweep (plan 3.4, G3). io thread.
    void on_ws_log(const rpc::Log& log, const std::string& provider);
    void on_sweep_page(core::Finality kind, const pipeline::BlockRange& range, const std::vector<rpc::Log>& logs);
    // plan 5.3: every kOpsPeriod build the alert snapshot, send what the engine returns; every [ops].deadman_ping_s
    // feed the dead-man. io thread, from on_beat().
    void ops_tick();
    void schedule_beat();
    void on_beat();
    void request_exit(int code, const std::string& why);  // first reason wins
    void begin_shutdown(int code, const std::string& why);
    void demote(const std::string& why);
    void end_term();
    void finish();
    ops::OpsHttpServer::Routes routes();
    std::string state_name() const;
    const char* role_name() const;

    // ---- worker thread (Redis wait, lease acquire / standby loop) ----
    void launch_acquire(bool first);
    void acquire_thread_main(bool first);

    const core::Config& cfg_;
    HostOptions opt_;
    std::string redis_url_;

    net::io_context ioc_;
    ops::Watchdog wd_;
    ops::AlertEngine engine_;
    ops::SnapshotBuilder snapshots_;
    std::unique_ptr<ops::AlertSender> alerts_;  // null = webhook not configured (alerts are only logged)
    std::unique_ptr<ops::Deadman> deadman_;
    std::chrono::steady_clock::time_point next_ops_{}, next_deadman_{};  // io thread
    ops::Metrics metrics_;
    std::unique_ptr<ops::OpsHttpServer> ops_;
    net::steady_timer beat_timer_, sleep_timer_, grace_timer_, freeze_timer_;
    std::thread startup_;  // the acquire worker; one run per leadership term

    std::atomic<bool> stopping_{false};  // read by the worker thread too
    std::atomic<Phase> phase_{Phase::WaitingRedis};
    // Redis unreachable (plan 5.3.3): the acquire thread sets the first, under the mutex the second, while the startup
    // wait runs; the io thread reads both. 0 = not waiting.
    std::atomic<std::int64_t> redis_wait_since_ns_{0};  // steady_clock epoch ns of the first failed attempt
    std::mutex redis_wait_mu_;
    std::string redis_wait_family_;
    std::optional<ops::RedisMisconfig> redis_misconfig_;  // io thread; from the startup report
    std::optional<std::chrono::steady_clock::time_point> sink_degraded_since_;  // io thread
    bool demoting_{false};
    // Head handling: a walk in flight defers newer heads (the ring must not change under it); `head_epoch_` ends stale
    // walks when the term ends or the WS provider changes.
    bool walking_{false};
    std::optional<HeadEvent> deferred_head_;
    std::uint64_t head_epoch_{0};
    std::chrono::steady_clock::time_point next_heartbeat_{};  // io thread; reset per term so the first beat publishes
    std::uint64_t ws_missed_total_{0};  // io thread; metrics export is 5-F
    std::uint64_t ws_extra_total_{0};
    bool finished_{false};
    bool exit_set_{false};
    int exit_code_{0};
    pipeline::PipelineState last_state_{pipeline::PipelineState::Init};

    std::unique_ptr<Runtime> rt_;  // declared last: destroyed first, before the io_context it posts to
};

// ------------------------------------------------------------------------------------------------------------------
// Ops routes (minimal; full wiring in 5-F)
// ------------------------------------------------------------------------------------------------------------------

std::string Host::state_name() const {
    if (rt_ && rt_->driver) return pipeline::to_string(rt_->driver->status().state);
    return phase_.load() == Phase::Standby ? "standby" : "waiting_redis";
}

const char* Host::role_name() const {
    if (rt_ && rt_->driver) return "leader";
    return phase_.load() == Phase::Standby ? "standby" : "none";
}

ops::OpsHttpServer::Routes Host::routes() {
    ops::OpsHttpServer::Routes r;
    r.healthz = [this] {
        ops::OpsResponse o;
        if (const char* stale = wd_.stale_heartbeat()) {
            o.status = 503;
            o.body = std::string("stale heartbeat: ") + stale + "\n";
        } else {
            o.body = "ok\n";
        }
        return o;
    };
    r.readyz = [this] {
        ops::OpsResponse o;
        const std::string state = state_name();
        o.status = state == "live" || state == "standby" ? 200 : 503;  // plan 5.1: leader and Live, or a healthy standby
        o.content_type = "application/json";
        o.body = "{\"state\":\"" + state + "\",\"role\":\"" + role_name() + "\",\"lease\":\"" +
                 (cfg_.lease.enabled ? "enabled" : "disabled") + "\"}\n";
        return o;
    };
    r.metrics = [this] {
        ops::OpsResponse o;
        o.content_type = "text/plain; version=0.0.4; charset=utf-8";
        o.body = metrics_.serialize();
        return o;
    };
    return r;
}

// ------------------------------------------------------------------------------------------------------------------
// Lifecycle
// ------------------------------------------------------------------------------------------------------------------

int Host::run() {
    if (!opt_.stop) throw std::invalid_argument("HostOptions::stop is required");
    redis_url_ = core::env::require(cfg_.sink.redis_url_env);
    ops_ = std::make_unique<ops::OpsHttpServer>(ioc_, cfg_.ops.http_listen, routes());

    // plan 5.3: the alert webhook and the dead-man URL come from the environment only. Both are optional (local dev):
    // without them alerts are only logged and the dead-man is "disabled" (the Deadman constructor warns).
    {
        const auto hook = core::env::get(cfg_.ops.alert_webhook_env);
        const auto token = core::env::get(cfg_.ops.alert_webhook_token_env);
        if (!hook.empty() && !token.empty()) {
            ops::AlertSenderOptions ao;
            ao.url = hook;
            ao.token = token;
            ao.service = cfg_.ops.alert_service;
            ao.shard = cfg_.shard.id;
            ao.host = cfg_.shard.host_id;
            ao.user_agent = user_agent(cfg_);
            ao.seed = std::random_device{}();
            alerts_ = std::make_unique<ops::AlertSender>(ioc_.get_executor(), std::move(ao));
        } else {
            log::warn("host", "alert webhook not configured: alerts are only logged",
                      {{"url_env", cfg_.ops.alert_webhook_env}, {"token_env", cfg_.ops.alert_webhook_token_env}});
        }
        ops::DeadmanOptions dopt;
        dopt.url = core::env::get(cfg_.ops.deadman_ping_env);
        dopt.user_agent = user_agent(cfg_);
        deadman_ = std::make_unique<ops::Deadman>(ioc_.get_executor(), std::move(dopt));
    }

    wd_.start();
    log::info("host", "started",
              {{"version", core::version_string()},
               {"shard", cfg_.shard.id},
               {"host_id", cfg_.shard.host_id},
               {"ops_port", std::to_string(ops_->port())},
               {"lease", cfg_.lease.enabled ? "enabled" : "disabled"}});

    if (opt_.test_freeze_io_after) {
        freeze_timer_.expires_after(*opt_.test_freeze_io_after);
        freeze_timer_.async_wait([](const boost::system::error_code& ec) {
            if (ec) return;
            log::error("host", "TEST HOOK: freezing the io thread");
            std::this_thread::sleep_for(1h);
        });
    }
    schedule_beat();
    launch_acquire(true);

    try {
        ioc_.run();
    } catch (...) {
        stopping_ = true;
        if (startup_.joinable()) startup_.join();
        wd_.stop();
        throw;
    }

    if (startup_.joinable()) startup_.join();
    // The drain blocks the main thread for up to kDrainBudget; stop the watchdog first so it cannot abort us.
    wd_.stop();
    if (rt_ && rt_->sink) {
        wd_.set_sink_registered(false);
        const auto d = rt_->sink->drain(kDrainBudget);
        log::info("host", "sink drained",
                  {{"flushed", d.flushed ? "true" : "false"},
                   {"unflushed", std::to_string(d.unflushed)},
                   {"lease_released", d.lease_released ? "true" : "false"}});
    }
    log::info("host", "stopped", {{"exit_code", std::to_string(exit_code_)}});
    return exit_code_;
}

void Host::launch_acquire(bool first) {
    if (startup_.joinable()) startup_.join();  // the previous run has already posted its result and is exiting
    startup_ = std::thread([this, first] { acquire_thread_main(first); });
}

void Host::acquire_thread_main(bool first) {
    // Blocking waits (Redis readiness on the first run, the standby loop on every run) live off the io thread:
    // /healthz and the io beat keep working while Redis is down or another host leads.
    const auto stop = [this] { return stopping_.load() || opt_.stop->load(std::memory_order_relaxed); };
    const auto sleep = [stop](std::chrono::milliseconds d) {  // sliced so a stop request is noticed quickly
        const auto end = std::chrono::steady_clock::now() + d;
        while (std::chrono::steady_clock::now() < end && !stop()) std::this_thread::sleep_for(50ms);
    };
    try {
        std::optional<sink::StartupReport> report;
        if (first) {
            sink::StartupOptions so;
            so.require_noeviction = cfg_.sink.require_noeviction;
            so.on_waiting = [this](const std::string& error, const std::string& family, std::chrono::milliseconds waited) {
                log::warn("host", "redis unreachable, waiting",
                          {{"error", error}, {"address_family", family}, {"waited_ms", std::to_string(waited.count())}});
                {
                    std::lock_guard lk(redis_wait_mu_);
                    redis_wait_family_ = family;
                }
                const auto since = std::chrono::steady_clock::now() - waited;
                redis_wait_since_ns_.store(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(since.time_since_epoch()).count());
            };
            so.stop = stop;
            so.sleep = sleep;
            report = sink::await_redis_ready(redis_url_, so);
            redis_wait_since_ns_.store(0);  // reachable again (or told to stop)
            if (!report) return;  // stop requested while waiting; the beat sees the same flag and shuts down
        }

        std::string token;
        if (cfg_.lease.enabled) {
            phase_ = Phase::Standby;
            const sink::Keys keys(cfg_.shard.id);
            sink::RedisppConn conn(redis_url_, keys);
            sink::AcquireOptions ao;
            ao.host_id = cfg_.shard.host_id;
            ao.ttl_ms = cfg_.lease.ttl_ms;
            ao.poll = std::chrono::milliseconds(cfg_.lease.renew_ms);  // plan 5.H: the standby polls every renew_ms
            ao.stop = stop;
            ao.sleep = sleep;
            const auto t = sink::acquire_lease_blocking(conn, keys, ao);
            if (!t) return;  // stop requested while on standby
            token = *t;
        } else {
            token = sink::lease_disabled_token();
        }
        net::post(ioc_, [this, report, token] { start_runtime(report, token); });
    } catch (const sink::StartupFatal& e) {
        net::post(ioc_, [this, msg = std::string(e.what())] { begin_shutdown(kExitUsage, "redis startup check failed: " + msg); });
    } catch (const std::exception& e) {
        net::post(ioc_, [this, msg = std::string(e.what())] { begin_shutdown(kExitFailure, "redis startup failed: " + msg); });
    }
}

void Host::start_runtime(const std::optional<sink::StartupReport>& rep, const std::string& token) {
    if (stopping_) return;
    try {
        if (rep && rep->misconfigured) {
            redis_misconfig_ = ops::RedisMisconfig{
                rep->maxmemory_policy, rep->appendonly,
                std::to_string(rep->version.major) + "." + std::to_string(rep->version.minor) + "." +
                    std::to_string(rep->version.patch)};
        }
        if (rep) {
            for (const auto& w : rep->warnings) log::warn("host", "redis startup warning", {{"warning", w}});
            log::info("host", "redis ready",
                      {{"redis_version", std::to_string(rep->version.major) + "." + std::to_string(rep->version.minor) + "." +
                                             std::to_string(rep->version.patch)}});
        }

        bump_head_epoch();
        rt_ = std::make_unique<Runtime>(cfg_.shard.id, cfg_.chain.hash_ring_size, cfg_.chain.pending_max_logs);

        auto sopts = sink::RedisSinkOptions::from_config(cfg_.sink, cfg_.lease, token);
        sopts.loop_beat = wd_.sink_beat_slot();
        sopts.host_id = cfg_.shard.host_id;
        sopts.heartbeat_ms = cfg_.ops.heartbeat_s * 1000;
        // The key must outlive a few missed writes (the sink rejects a TTL <= the interval).
        sopts.heartbeat_ttl_s = std::max<std::uint32_t>(sopts.heartbeat_ttl_s, cfg_.ops.heartbeat_s * 3);
        next_heartbeat_ = {};
        rt_->sink = std::make_unique<sink::RedisSink>(std::move(sopts), rt_->keys,
                                                      std::make_unique<sink::RedisppConn>(redis_url_, rt_->keys),
                                                      std::make_unique<sink::RedisppConn>(redis_url_, rt_->keys));
        rt_->sink->start();
        wd_.set_sink_registered(true);

        provider::PoolHandlers h;
        h.on_transition = [](const provider::Transition& t, std::string_view why) {
            log::info("pool", "provider transition",
                      {{"provider", t.provider},
                       {"from", provider::to_string(t.from)},
                       {"to", provider::to_string(t.to)},
                       {"reason", why}});
        };
        h.on_tag = [this](const std::string& p, provider::Tag tag, std::uint64_t n, const core::Hash32& hash) {
            on_tag(p, tag, n, hash);
        };
        h.on_head = [this](const rpc::Head& head, const std::string& p) { on_head(head, p); };
        h.on_log = [this](const rpc::Log& log, const std::string& p) { on_ws_log(log, p); };
        // The pool reports only the new WS provider; the previous one is remembered here for the failover alert's `from`.
        auto prev_ws = std::make_shared<std::optional<std::string>>();
        h.on_ws_active_changed = [this, prev_ws](const std::optional<std::string>& to, std::string_view reason) {
            // A new WS provider re-seeds the ring (plan 3.6); any walk on the old provider is abandoned.
            if (!rt_) return;
            const std::optional<std::string> from = std::exchange(*prev_ws, to);
            // "failover" is ProviderBook's Forced decision, which always names a replacement; a move to no provider is
            // "none_available" and raises no Provider Failover event (the no-healthy-provider alerts cover it).
            if (reason == "failover" && to) {  // forced move off a failed provider: one alert event (plan 60 §5.3.3)
                snapshots_.push_event(ops::provider_failover_event(from, *to, reason));
            }
            rt_->ring.reset();
            bump_head_epoch();
        };
        rt_->pool = provider::ProviderPool::create(ioc_.get_executor(), make_pool_config(cfg_, user_agent(cfg_)), std::move(h));
        // block() reads timestamps and hashes from the head ring first and asks a provider only on a ring miss.
        rt_->source = std::make_unique<PoolLogSource>(rt_->pool, &rt_->ring);

        pipeline::SweeperOptions sw;
        sw.min_range = cfg_.backfill.min_range;
        sw.max_page_logs = std::max<std::size_t>(1, cfg_.sink.queue_capacity / 2);  // plan 3.5 G6
        sw.on_page = [this](core::Finality kind, const pipeline::BlockRange& range, const std::vector<rpc::Log>& logs) {
            on_sweep_page(kind, range, logs);
        };
        const pipeline::EventContext ctx{cfg_.shard.id, cfg_.shard.host_id, core::version_string()};
        rt_->sweeper = std::make_unique<pipeline::Sweeper>(cfg_, ctx, *rt_->source, *rt_->sink, wall_ms, sw);

        pipeline::DriverOptions dopt;
        dopt.seed = std::random_device{}();
        rt_->driver = std::make_unique<pipeline::PipelineDriver>(cfg_, *rt_->sweeper, *rt_->sink, *rt_->source, wall_ms, dopt);

        rt_->pool->start();
        phase_ = Phase::Leader;
        log::info("host", "leadership term started", {{"lease", cfg_.lease.enabled ? "enabled" : "disabled"}});
        if (opt_.on_event) opt_.on_event("leader");
        net::co_spawn(ioc_, pipeline_loop(), net::detached);
    } catch (const std::exception& e) {
        begin_shutdown(kExitFailure, std::string("pipeline startup failed: ") + e.what());
    }
}

net::awaitable<void> Host::pipeline_loop() {
    auto& driver = *rt_->driver;
    try {
        for (;;) {
            const auto r = co_await driver.tick();
            if (r.done || stopping_ || demoting_) break;
            if (r.wait.count() > 0) {
                sleep_timer_.expires_after(r.wait);
                boost::system::error_code ec;  // operation_aborted = a stop request cancelled the sleep
                co_await sleep_timer_.async_wait(net::redirect_error(net::use_awaitable, ec));
            }
        }
    } catch (const std::exception& e) {
        request_exit(kExitFailure, std::string("pipeline tick threw: ") + e.what());
    }
    const auto st = driver.status();
    if (st.state == pipeline::PipelineState::Failed) {
        request_exit(kExitUsage, "pipeline failed: " + st.fatal);
    }
    if (demoting_ && !stopping_) {
        // Tear the term down from a fresh handler: this coroutine still runs on the runtime being destroyed.
        net::post(ioc_, [this] { end_term(); });
        co_return;
    }
    finish();
}

void Host::on_tag(const std::string& provider, provider::Tag tag, std::uint64_t number, const core::Hash32& hash) {
    if (!rt_ || !rt_->driver || !rt_->pool) return;
    // Only the current getLogs provider's tags define the gates (plan 3.3); the WS cross-check also reads tags.
    const auto cur = rt_->pool->current_getlogs();
    if (!cur || *cur != provider) return;

    auto& gate = rt_->gate;
    const auto sig = tag == provider::Tag::Safe ? gate.on_safe(provider, number, hash) : gate.on_finalized(provider, number, hash);
    rt_->driver->set_gates(gate.safe(), gate.finalized());
    switch (sig) {
        case chain::GateSignal::None:
            break;
        case chain::GateSignal::SafeRegression:
        case chain::GateSignal::SafeHashMismatch:
            log::warn("gate", "safe gate signal", {{"signal", chain::to_string(sig)}, {"provider", provider}});
            rt_->driver->on_safe_reorg_signal(std::string(chain::to_string(sig)) + " from " + provider);
            break;
        case chain::GateSignal::FinalizedAnomaly:
            log::error("gate", "finalized anomaly: release frozen", {{"provider", provider}, {"reason", gate.frozen_reason()}});
            rt_->driver->on_gate_frozen(gate.frozen_reason());
            break;
    }
}

void Host::bump_head_epoch() {
    ++head_epoch_;
    walking_ = false;
    deferred_head_.reset();
}

void Host::on_ws_log(const rpc::Log& log, const std::string& provider) {
    if (!rt_ || !rt_->driver || demoting_ || stopping_) return;
    if (log.topics.empty() || cfg_.route(log.address, log.topics.front()) == nullptr) {
        log::warn("host", "ws log matches no configured route, not staged",
                  {{"provider", provider}, {"block", std::to_string(log.block_number)}});
        return;
    }
    rt_->pending.stage(log, provider);
}

void Host::on_sweep_page(core::Finality kind, const pipeline::BlockRange& range, const std::vector<rpc::Log>& logs) {
    if (!rt_) return;
    // `logs` holds one class (safe for a safe page, finalized for a final page), so only staged logs of that class
    // are in scope; the other class waits for its own sweep.
    const bool want_safe = kind == core::Finality::Safe;
    const auto in_scope = [this, want_safe](const rpc::Log& l) {
        const auto* route = cfg_.route(l.address, l.topics.at(0));  // stage() only admits routed logs
        return route != nullptr && (route->finality == core::Finality::Safe) == want_safe;
    };
    const auto cc = rt_->pending.reconcile(range.from, range.to, logs, in_scope);
    ws_missed_total_ += cc.ws_missed;
    ws_extra_total_ += cc.ws_extra;
    if (cc.ws_missed != 0 || cc.ws_extra != 0) {
        log::warn("host", "ws cross-check mismatch",
                  {{"kind", want_safe ? "safe" : "finalized"},
                   {"from", std::to_string(range.from)},
                   {"to", std::to_string(range.to)},
                   {"ws_missed", std::to_string(cc.ws_missed)},
                   {"ws_extra", std::to_string(cc.ws_extra)}});
    }
    if (opt_.on_cross_check) opt_.on_cross_check(ws_missed_total_, ws_extra_total_);
}

void Host::on_head(const rpc::Head& h, const std::string& provider) {
    if (!rt_ || !rt_->driver || demoting_ || stopping_) return;
    HeadEvent e{chain::BlockRef{h.number, h.hash, h.parent_hash, h.timestamp}, provider};
    if (walking_) {
        deferred_head_ = std::move(e);  // only the newest matters: the next walk or extension starts from it
        return;
    }
    handle_head(e);
}

void Host::handle_head(const HeadEvent& e) {
    auto& ring = rt_->ring;
    if (const auto top = ring.top(); top && e.ref.number > *top + kMaxWalkBlocks) {
        resync_ring(e.ref, "head gap of " + std::to_string(e.ref.number - *top) + " blocks");
        return;
    }
    if (ring.on_head(e.ref) != chain::HeadKind::NeedWalk) return;
    walking_ = true;
    net::co_spawn(ioc_, walk_fork(head_epoch_, e), net::detached);
}

// The ring cannot say whether the chain forked below C_s (a gap or a fork too deep to walk, or a provider that would
// not answer): start over from this head and let the driver re-verify the released window.
void Host::resync_ring(const chain::BlockRef& head, const std::string& why) {
    log::warn("host", "head ring resync", {{"why", why}, {"head", std::to_string(head.number)}});
    rt_->ring.reset();
    rt_->ring.on_head(head);
    rt_->pending.purge_from(0);  // the old branch is unknown: staged logs cannot be trusted
    rt_->driver->on_safe_reorg_signal("head ring resync: " + why);
}

// Walks the new chain's ancestry back to the ring via eth_getBlockByNumber on the provider that sent the head.
net::awaitable<void> Host::walk_fork(std::uint64_t epoch, HeadEvent e) {
    const auto alive = [this, epoch] { return epoch == head_epoch_ && rt_ && rt_->driver; };
    const auto pool = rt_->pool;  // keeps the pool alive while a call_to is in flight
    try {
        rpc::Decoder dec;
        std::optional<chain::ReorgInfo> info;
        std::string why;
        for (int attempt = 0; attempt < kWalkAttempts && !info && why.empty(); ++attempt) {
            auto walk = rt_->ring.begin_walk(e.ref);
            std::size_t fetched = 0;
            while (walk.state() == chain::ForkWalk::State::NeedBlock) {
                if (++fetched > kMaxWalkBlocks) {
                    why = "fork walk longer than " + std::to_string(kMaxWalkBlocks) + " blocks";
                    break;
                }
                const std::uint64_t n = walk.need();
                const auto o = co_await pool->call_to(e.provider, [n](std::uint64_t id) {
                    return rpc::build_get_block_by_number(id, core::to_hex_u64(n), false);
                });
                if (!alive()) co_return;
                if (!o.ok) {
                    why = "block " + std::to_string(n) + " from " + e.provider + " failed: " + o.error;
                    break;
                }
                const auto b = dec.parse_block(o.result_raw);  // DecodeError ends up in the catch below
                if (!b) {
                    why = e.provider + " has no block " + std::to_string(n);
                    break;
                }
                walk.feed(chain::BlockRef{b->number, b->hash, b->parent_hash, b->timestamp});
            }
            if (!why.empty()) break;
            switch (walk.state()) {
                case chain::ForkWalk::State::Done:
                    info = rt_->ring.apply(walk);
                    break;
                case chain::ForkWalk::State::BeyondRing:
                    why = "fork deeper than the head ring";
                    break;
                case chain::ForkWalk::State::Inconsistent:  // a reorg mid-walk or a bad block: start over
                case chain::ForkWalk::State::NeedBlock:
                    break;
            }
        }
        if (!info && why.empty()) why = "fork walk inconsistent after " + std::to_string(kWalkAttempts) + " attempts";

        walking_ = false;
        if (info) {
            const auto c_s = rt_->driver->status().c_s;
            log::info("host", "head chain resolved",
                      {{"fork_point", std::to_string(info->fork_point)},
                       {"depth", std::to_string(info->depth)},
                       {"old_top", std::to_string(info->old_top)},
                       {"c_s", std::to_string(c_s)}});
            // depth 0 = a gap that was only filled: nothing was replaced.
            if (info->depth > 0) rt_->pending.purge_from(info->fork_point + 1);  // orphaned branch's WS logs
            if (info->depth > 0 && chain::ReorgDetector::crosses_safe(*info, c_s)) {
                log::warn("host", "head fork below C_s",
                          {{"fork_point", std::to_string(info->fork_point)}, {"c_s", std::to_string(c_s)}});
                rt_->driver->on_safe_reorg_signal("head fork at " + std::to_string(info->fork_point) + " below C_s " +
                                                  std::to_string(c_s));
            }
        } else {
            resync_ring(e.ref, why);
        }
        if (deferred_head_) {
            HeadEvent next = std::move(*deferred_head_);
            deferred_head_.reset();
            handle_head(next);
        }
    } catch (const std::exception& ex) {
        if (!alive()) co_return;
        walking_ = false;
        deferred_head_.reset();
        log::error("host", "head walk threw", {{"error", ex.what()}});
        resync_ring(e.ref, std::string("head walk threw: ") + ex.what());
    }
}

void Host::schedule_beat() {
    beat_timer_.expires_after(opt_.beat_period);
    beat_timer_.async_wait([this](const boost::system::error_code& ec) {
        if (ec || finished_) return;
        on_beat();
        if (!finished_) schedule_beat();
    });
}

void Host::publish_heartbeat(const pipeline::DriverStatus& st) {
    const auto now = std::chrono::steady_clock::now();
    if (now < next_heartbeat_) return;
    next_heartbeat_ = now + std::chrono::seconds(cfg_.ops.heartbeat_s);
    // null = not known yet (no gate read, empty head ring, no healthy getLogs provider). `epoch_n` of plan 4.7 is
    // left out: the sink has no epoch read yet (5-E).
    const auto opt_u64 = [](const std::optional<std::uint64_t>& v) {
        return v ? boost::json::value(*v) : boost::json::value(nullptr);
    };
    const auto provider = rt_->pool->current_getlogs();
    boost::json::object o;
    o["ts"] = wall_ms();
    o["role"] = "leader";
    o["state"] = pipeline::to_string(st.state);
    o["head"] = opt_u64(rt_->ring.top());
    o["G_s"] = opt_u64(st.g_s);
    o["G_f"] = opt_u64(st.g_f);
    o["C_s"] = st.c_s;
    o["C_f"] = st.c_f;
    o["active_provider"] = provider ? boost::json::value(*provider) : boost::json::value(nullptr);
    o["version"] = core::version_string();
    rt_->sink->set_heartbeat(boost::json::serialize(o));
}

void Host::ops_tick() {
    const auto now = std::chrono::steady_clock::now();
    const bool leader = rt_ && rt_->driver && !demoting_;

    if (now >= next_ops_) {
        next_ops_ = now + kOpsPeriod;
        ops::SnapshotInputs in;
        in.now = now;
        in.wall_ms = wall_ms();
        in.leader = leader;
        in.chain = cfg_.chain.name;
        in.redis_misconfigured = redis_misconfig_;

        // Redis unreachable: the startup wait, or (as leader) a sink that lost its connection and is spilling.
        std::chrono::seconds down{0};
        if (const auto ns = redis_wait_since_ns_.load(); ns != 0) {
            const std::chrono::steady_clock::time_point since{
                std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::nanoseconds(ns))};
            down = std::chrono::duration_cast<std::chrono::seconds>(now - since);
            std::lock_guard lk(redis_wait_mu_);
            in.addr_family = redis_wait_family_;
        }
        if (rt_ && rt_->sink && rt_->sink->mode() == sink::SinkMode::Degraded) {
            if (!sink_degraded_since_) sink_degraded_since_ = now;
            down = std::max(down, std::chrono::duration_cast<std::chrono::seconds>(now - *sink_degraded_since_));
        } else {
            sink_degraded_since_.reset();
        }
        if (down.count() > 0) in.redis_unreachable_s = static_cast<std::uint32_t>(down.count());

        if (leader) {
            in.head = rt_->ring.top();
            in.driver = rt_->driver->status();
            in.sink = rt_->sink->status();
            in.sink_saturated = rt_->sink->saturated();
            in.spill_max_bytes = cfg_.sink.spill_max_bytes;
            in.retention_s = cfg_.sink.retention_ms / 1000;
            in.safe_late_total = rt_->sweeper->stats().late;
            in.ws_missed_total = ws_missed_total_;
            in.consumers = rt_->sink->consumer_lag();
            in.providers = rt_->pool->health();
            in.ms_since_head = rt_->pool->ms_since_head();
            const auto safe_of = [&](const std::optional<std::string>& name) -> std::optional<std::uint64_t> {
                if (!name) return std::nullopt;
                const auto t = rt_->pool->cached_tag(*name, provider::Tag::Safe);
                return t ? std::optional<std::uint64_t>(t->number) : std::nullopt;
            };
            const auto ws = rt_->pool->active_ws();
            const auto getlogs = rt_->pool->current_getlogs();
            in.ws_provider = ws.value_or(in.ms_since_head ? "none" : "");
            in.getlogs_provider = getlogs.value_or("");
            in.ws_safe = safe_of(ws);
            in.getlogs_safe = safe_of(getlogs);
            for (const auto& p : in.providers) in.provider_inconsistent_total[p.name] = rt_->pool->inconsistency_total(p.name);
        }
        for (const auto& a : engine_.evaluate(snapshots_.build(in))) {
            log::warn("alert", "alert raised",
                      {{"severity", ops::to_string(a.severity)}, {"title", a.title}, {"message", a.message}});
            if (alerts_) alerts_->send(a);
        }
    }

    if (deadman_ && now >= next_deadman_) {
        next_deadman_ = now + std::chrono::seconds(cfg_.ops.deadman_ping_s);
        ops::DeadmanFacts f;
        f.now = now;
        f.leader = leader;
        if (leader) {
            const auto st = rt_->driver->status();
            f.state_ok = st.state == pipeline::PipelineState::CatchingUp || st.state == pipeline::PipelineState::Live;
            f.sink_saturated = rt_->sink->saturated();
            f.c_s = st.c_s;
            f.g_s = st.g_s;
        }
        deadman_->tick(f);
    }
}

void Host::on_beat() {
    wd_.beat_io();
    ops::Watchdog::State s;
    s.role = rt_ && rt_->driver ? 1 : 0;  // 1 = leader (lease disabled counts as leader), 0 = standby / starting
    if (rt_ && rt_->sink) s.sink_mode = static_cast<std::uint64_t>(rt_->sink->mode());
    if (rt_ && rt_->driver) {
        const auto st = rt_->driver->status();
        s.driver_state = static_cast<std::uint64_t>(st.state);
        s.c_s = st.c_s;
        s.c_f = st.c_f;
        s.g_s = st.g_s.value_or(0);
        s.g_f = st.g_f.value_or(0);
        if (st.state != last_state_) {
            log::info("host", "pipeline state",
                      {{"from", pipeline::to_string(last_state_)},
                       {"to", pipeline::to_string(st.state)},
                       {"c_s", std::to_string(st.c_s)},
                       {"c_f", std::to_string(st.c_f)}});
            last_state_ = st.state;
        }
        publish_heartbeat(st);
        if (opt_.on_status) opt_.on_status(st);
    }
    wd_.publish_state(s);
    ops_tick();

    if (opt_.stop->load(std::memory_order_relaxed)) {
        begin_shutdown(kExitOk, "stop requested");
    } else if (rt_ && rt_->sink && rt_->sink->mode() == sink::SinkMode::Failed) {
        begin_shutdown(kExitFailure, "sink failed: " + rt_->sink->fatal_message());
    } else if (rt_ && rt_->sink && rt_->sink->mode() == sink::SinkMode::Fenced) {
        demote("sink fenced: another term holds the lease");
    }
}

// A fenced sink has already discarded its queue and spill (plan 4.8); the host ends the term and goes back to the
// standby loop. The driver and pool are stopped first, then the term's objects are destroyed.
void Host::demote(const std::string& why) {
    if (demoting_ || stopping_ || !rt_ || !rt_->driver) return;
    demoting_ = true;
    const auto st = rt_->sink->status();
    ops::AlertEvent fenced;
    fenced.kind = ops::AlertEvent::Kind::SinkFenced;
    fenced.message = why;
    fenced.details = {{"discarded_records", st.discarded_total}};
    snapshots_.push_event(std::move(fenced));
    log::warn("host", "demoting to standby",
              {{"reason", why}, {"discarded_records", std::to_string(st.discarded_total)}});
    rt_->driver->request_drain();
    sleep_timer_.cancel();  // pipeline_loop ends and posts end_term()
}

void Host::end_term() {
    if (stopping_ || !rt_) return;  // a shutdown overtook the demotion; finish() owns the teardown
    wd_.set_sink_registered(false);  // before the sink is destroyed
    bump_head_epoch();               // a walk still waiting on a provider must not touch the runtime we destroy
    rt_->pool->stop();
    // Destroying the sink joins its thread (bounded by the Redis io timeout, 2 s): the io beat resumes right after.
    rt_.reset();
    demoting_ = false;
    last_state_ = pipeline::PipelineState::Init;
    phase_ = Phase::Standby;
    log::info("host", "leadership term ended, back on standby");
    if (opt_.on_event) opt_.on_event("demoted");
    launch_acquire(false);
}

void Host::request_exit(int code, const std::string& why) {
    if (exit_set_) return;
    exit_set_ = true;
    exit_code_ = code;
    if (code == kExitOk) {
        log::info("host", "shutting down", {{"reason", why}});
    } else {
        log::error("host", "shutting down", {{"reason", why}, {"exit_code", std::to_string(code)}});
    }
}

void Host::begin_shutdown(int code, const std::string& why) {
    if (stopping_.exchange(true)) return;
    request_exit(code, why);
    if (rt_ && rt_->driver) {
        rt_->driver->request_drain();
        sleep_timer_.cancel();  // pipeline_loop ends and calls finish()
    } else {
        finish();
    }
}

void Host::finish() {
    if (finished_) return;
    finished_ = true;
    stopping_ = true;
    beat_timer_.cancel();
    freeze_timer_.cancel();
    sleep_timer_.cancel();
    if (ops_) ops_->stop();
    if (rt_ && rt_->pool) rt_->pool->stop();
    grace_timer_.expires_after(kShutdownGrace);
    grace_timer_.async_wait([this](const boost::system::error_code&) { ioc_.stop(); });
}

}  // namespace

int run_host(const core::Config& cfg, const HostOptions& opt) {
    Host host(cfg, opt);
    return host.run();
}

int run_listener(const core::Config& cfg, std::optional<std::chrono::seconds> test_freeze_io_after) {
    core::log::init("info");
    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);

    HostOptions opt;
    opt.stop = &g_signal_stop;
    opt.test_freeze_io_after = test_freeze_io_after;
    return run_host(cfg, opt);
}

}  // namespace evml::app
