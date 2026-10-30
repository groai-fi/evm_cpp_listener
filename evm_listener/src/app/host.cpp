#include "app/host.hpp"

#include <algorithm>
#include <csignal>
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

#include "app/host_telemetry.hpp"
#include "app/pool_config.hpp"
#include "app/pool_log_source.hpp"
#include "app/tools.hpp"
#include "app/ws_reconciler.hpp"
#include "chain/finality_gate.hpp"
#include "chain/reorg_detector.hpp"
#include "core/env.hpp"
#include "core/log.hpp"
#include "core/version.hpp"
#include "ops/ops_http_server.hpp"
#include "ops/watchdog.hpp"
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
constexpr std::size_t kMaxWalkBlocks = 16;
constexpr int kWalkAttempts = 3;

std::atomic<bool> g_signal_stop{false};
extern "C" void on_signal(int) { g_signal_stop.store(true, std::memory_order_relaxed); }

std::uint64_t wall_ms() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

struct HeadEvent {
    chain::BlockRef ref;
    std::string provider;
};

struct Runtime {
    Runtime(const core::Config& cfg, const std::string& shard, std::size_t ring_size, std::size_t pending_max)
        : keys(shard), ring(ring_size), ws_reconciler(cfg, pending_max) {}
    sink::Keys keys;
    std::unique_ptr<sink::RedisSink> sink;
    chain::FinalityGate gate;
    chain::ReorgDetector ring;
    WsReconciler ws_reconciler;
    std::shared_ptr<provider::ProviderPool> pool;
    std::unique_ptr<PoolLogSource> source;
    std::unique_ptr<pipeline::Sweeper> sweeper;
    std::unique_ptr<pipeline::PipelineDriver> driver;
    std::optional<std::uint64_t> last_g_s;
    std::optional<std::chrono::steady_clock::time_point> last_g_s_advance;
    bool walking{false};
    std::optional<HeadEvent> deferred_head;
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
          telemetry_(cfg),
          beat_timer_(ioc_),
          sleep_timer_(ioc_),
          grace_timer_(ioc_),
          freeze_timer_(ioc_) {}

    int run();

private:
    enum class Phase : int { WaitingRedis, Standby, Leader };

    void start_runtime(const std::optional<sink::StartupReport>& rep, const sink::LeaseGrant& grant);
    net::awaitable<void> pipeline_loop();
    net::awaitable<void> walk_fork(std::uint64_t epoch, HeadEvent e);
    void acquire_thread_main(bool first);

    std::string state_name() const {
        if (rt_ && rt_->driver && !demoting_) return pipeline::to_string(rt_->driver->status().state);
        return phase_.load() == Phase::Standby ? "standby" : "waiting_redis";
    }
    const char* role_name() const {
        if (rt_ && rt_->driver && !demoting_) return "leader";
        return phase_.load() == Phase::Standby ? "standby" : "none";
    }
    TelemetryContext build_telemetry_context() const {
        TelemetryContext ctx;
        ctx.state = state_name();
        ctx.role = role_name();
        ctx.leader = rt_ && rt_->driver && !demoting_;
        const auto now = std::chrono::steady_clock::now();
        ctx.gate_age_ok = ctx.leader && rt_->last_g_s_advance &&
                          (now - *rt_->last_g_s_advance) <= std::chrono::seconds(cfg_.watchdog.safe_gate_stuck_s);
        ctx.healthy_standby = phase_.load() == Phase::Standby && redis_wait_since_ns_.load() == 0;
        ctx.provider_failovers_total = provider_failovers_total_;
        ctx.ws_missed_total = ws_missed_total_;
        ctx.ws_extra_total = ws_extra_total_;
        if (rt_ && rt_->driver) {
            ctx.sink = rt_->sink.get();
            ctx.sweeper = rt_->sweeper.get();
            ctx.driver = rt_->driver.get();
            ctx.ws_reconciler = &rt_->ws_reconciler;
            ctx.pool = rt_->pool.get();
            ctx.ring = &rt_->ring;
        }
        return ctx;
    }
    ops::OpsHttpServer::Routes routes() {
        return {[this] { return telemetry_.build_healthz(wd_); },
                [this] { return telemetry_.build_readyz(build_telemetry_context()); },
                [this] { return telemetry_.build_metrics_response(build_telemetry_context()); }};
    }
    void launch_acquire(bool first) {
        if (startup_.joinable()) startup_.join();
        startup_ = std::thread([this, first] { acquire_thread_main(first); });
    }
    void bump_head_epoch() {
        ++head_epoch_;
        if (rt_) {
            rt_->walking = false;
            rt_->deferred_head.reset();
        }
    }
    void on_ws_log(const rpc::Log& log, const std::string& provider) {
        if (!rt_ || !rt_->driver || demoting_ || stopping_) return;
        rt_->ws_reconciler.on_ws_log(log, provider);
        if (log.removed || log.topics.empty()) return;
        const auto* route = cfg_.route(log.address, log.topics.front());
        if (!route || !route->fast_stream) return;
        const auto now_ms = wall_ms();
        const std::uint64_t ts = rt_->ring.timestamp_of(log.block_number, log.block_hash).value_or(0);
        const pipeline::EventContext ctx{cfg_.shard.id, cfg_.shard.host_id, core::version_string()};
        auto ev = pipeline::make_event(log, cfg_, ctx, pipeline::EventType::Log, ts, provider, now_ms, now_ms);
        ev.finality = core::Finality::Latest;
        rt_->sink->emit_fast(std::move(ev));
    }
    void on_sweep_page(core::Finality kind, const pipeline::BlockRange& range, const std::vector<rpc::Log>& logs) {
        if (!rt_) return;
        const auto cc = rt_->ws_reconciler.on_sweep_page(kind, range, logs);
        ws_missed_total_ += cc.ws_missed;
        ws_extra_total_ += cc.ws_extra;
        if (opt_.on_cross_check) opt_.on_cross_check(ws_missed_total_, ws_extra_total_);
    }
    void on_head(const rpc::Head& h, const std::string& provider) {
        if (!rt_ || !rt_->driver || demoting_ || stopping_) return;
        HeadEvent e{chain::BlockRef{h.number, h.hash, h.parent_hash, h.timestamp}, provider};
        if (rt_->walking) rt_->deferred_head = std::move(e);
        else handle_head(e);
    }
    void handle_head(const HeadEvent& e) {
        std::optional<std::uint64_t> live_c_s;
        if (rt_->driver && rt_->driver->status().state == pipeline::PipelineState::Live) live_c_s = rt_->driver->status().c_s;
        rt_->ws_reconciler.on_head(e.ref.number, live_c_s);
        if (const auto top = rt_->ring.top(); top && e.ref.number > *top + kMaxWalkBlocks) {
            resync_ring(e.ref, "head gap of " + std::to_string(e.ref.number - *top) + " blocks");
            return;
        }
        if (rt_->ring.on_head(e.ref) != chain::HeadKind::NeedWalk) return;
        rt_->walking = true;
        net::co_spawn(ioc_, walk_fork(head_epoch_, e), net::detached);
    }
    void resync_ring(const chain::BlockRef& head, const std::string& why) {
        log::warn("host", "head ring resync", {{"why", why}, {"head", std::to_string(head.number)}});
        rt_->ring.reset();
        rt_->ring.on_head(head);
        if (rt_->source) rt_->source->clear_cache();
        rt_->ws_reconciler.on_ring_resync(head.number);
        rt_->driver->on_safe_reorg_signal("head ring resync: " + why);
    }
    void on_tag(const std::string& provider, provider::Tag tag, std::uint64_t number, const core::Hash32& hash) {
        if (!rt_ || !rt_->driver || !rt_->pool) return;
        const auto cur = rt_->pool->current_getlogs();
        if (!cur || *cur != provider) return;

        auto& gate = rt_->gate;
        const auto sig = tag == provider::Tag::Safe ? gate.on_safe(provider, number, hash) : gate.on_finalized(provider, number, hash);
        if (const auto g_s = gate.safe(); g_s && (!rt_->last_g_s || *g_s != *rt_->last_g_s)) {
            rt_->last_g_s = g_s;
            rt_->last_g_s_advance = std::chrono::steady_clock::now();
        }
        rt_->driver->set_gates(gate.safe(), gate.finalized());
        if (sig == chain::GateSignal::SafeRegression || sig == chain::GateSignal::SafeHashMismatch) {
            log::warn("gate", "safe gate signal", {{"signal", chain::to_string(sig)}, {"provider", provider}});
            if (rt_->source) rt_->source->clear_cache();
            rt_->driver->on_safe_reorg_signal(std::string(chain::to_string(sig)) + " from " + provider);
        } else if (sig == chain::GateSignal::FinalizedAnomaly) {
            log::error("gate", "finalized anomaly: release frozen", {{"provider", provider}, {"reason", gate.frozen_reason()}});
            rt_->driver->on_gate_frozen(gate.frozen_reason());
        }
    }
    void schedule_beat() {
        beat_timer_.expires_after(opt_.beat_period);
        beat_timer_.async_wait([this](const boost::system::error_code& ec) {
            if (ec || finished_) return;
            on_beat();
            if (!finished_) schedule_beat();
        });
    }
    void publish_heartbeat(const pipeline::DriverStatus& st) {
        const auto now = std::chrono::steady_clock::now();
        if (now < next_heartbeat_) return;
        next_heartbeat_ = now + std::chrono::seconds(cfg_.ops.heartbeat_s);
        rt_->sink->set_heartbeat(
            HostTelemetry::format_heartbeat_json(wall_ms(), st, rt_->ring.top(), rt_->pool->current_getlogs(), epoch_n_));
    }
    void ops_tick() {
        std::string addr_family;
        {
            std::lock_guard lk(redis_wait_mu_);
            addr_family = redis_wait_family_;
        }
        telemetry_.ops_tick(build_telemetry_context(), std::chrono::steady_clock::now(), wall_ms(), redis_misconfig_,
                            redis_wait_since_ns_.load(), addr_family);
    }
    void on_beat() {
        wd_.beat_io();
        ops::Watchdog::State s;
        s.role = rt_ && rt_->driver ? 1 : 0;
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
                          {{"from", pipeline::to_string(last_state_)}, {"to", pipeline::to_string(st.state)},
                           {"c_s", std::to_string(st.c_s)}, {"c_f", std::to_string(st.c_f)}});
                last_state_ = st.state;
            }
            publish_heartbeat(st);
            if (opt_.on_status) opt_.on_status(st);
        }
        wd_.publish_state(s);
        ops_tick();

        if (opt_.stop->load(std::memory_order_relaxed)) begin_shutdown(kExitOk, "stop requested");
        else if (rt_ && rt_->sink && rt_->sink->mode() == sink::SinkMode::Failed)
            begin_shutdown(kExitFailure, "sink failed: " + rt_->sink->fatal_message());
        else if (rt_ && rt_->sink && rt_->sink->mode() == sink::SinkMode::Fenced)
            demote("sink fenced: another term holds the lease");
    }
    void demote(const std::string& why) {
        if (demoting_ || stopping_ || !rt_ || !rt_->driver) return;
        demoting_ = true;
        const auto st = rt_->sink->status();
        telemetry_.push_event(ops::sink_fenced_event(why, st.discarded_total));
        telemetry_.update_metrics(build_telemetry_context());
        log::warn("host", "demoting to standby", {{"reason", why}, {"discarded_records", std::to_string(st.discarded_total)}});
        rt_->driver->request_drain();
        if (rt_->pool) rt_->pool->stop();
        sleep_timer_.cancel();
    }
    void end_term() {
        if (stopping_ || !rt_) return;
        wd_.set_sink_registered(false);
        bump_head_epoch();
        rt_->pool->stop();
        rt_.reset();
        demoting_ = false;
        last_state_ = pipeline::PipelineState::Init;
        phase_ = Phase::Standby;
        telemetry_.update_metrics(build_telemetry_context());
        log::info("host", "leadership term ended, back on standby");
        if (opt_.on_event) opt_.on_event("demoted");
        launch_acquire(false);
    }
    void request_exit(int code, const std::string& why) {
        if (exit_set_) return;
        exit_set_ = true;
        exit_code_ = code;
        if (code == kExitOk) log::info("host", "shutting down", {{"reason", why}});
        else log::error("host", "shutting down", {{"reason", why}, {"exit_code", std::to_string(code)}});
    }
    void begin_shutdown(int code, const std::string& why) {
        if (stopping_.exchange(true)) return;
        request_exit(code, why);
        if (rt_ && rt_->driver) {
            rt_->driver->request_drain();
            if (rt_->pool) rt_->pool->stop();
            sleep_timer_.cancel();
        } else {
            finish();
        }
    }
    void finish() {
        if (finished_) return;
        finished_ = true;
        stopping_ = true;
        beat_timer_.cancel();
        freeze_timer_.cancel();
        sleep_timer_.cancel();
        if (ops_) ops_->stop();
        telemetry_.stop();
        if (rt_ && rt_->pool) rt_->pool->stop();
        grace_timer_.expires_after(kShutdownGrace);
        grace_timer_.async_wait([this](const boost::system::error_code&) { ioc_.stop(); });
    }

    const core::Config& cfg_;
    HostOptions opt_;
    std::string redis_url_;
    net::io_context ioc_;
    ops::Watchdog wd_;
    HostTelemetry telemetry_;
    std::unique_ptr<ops::OpsHttpServer> ops_;
    net::steady_timer beat_timer_, sleep_timer_, grace_timer_, freeze_timer_;
    std::thread startup_;
    std::atomic<bool> stopping_{false};
    std::atomic<Phase> phase_{Phase::WaitingRedis};
    std::atomic<std::int64_t> redis_wait_since_ns_{0};
    std::mutex redis_wait_mu_;
    std::string redis_wait_family_;
    std::optional<ops::RedisMisconfig> redis_misconfig_;
    std::optional<std::uint64_t> epoch_n_;
    bool demoting_{false};
    std::uint64_t head_epoch_{0};
    std::chrono::steady_clock::time_point next_heartbeat_{};
    std::uint64_t ws_missed_total_{0}, ws_extra_total_{0}, provider_failovers_total_{0};
    bool finished_{false}, exit_set_{false};
    int exit_code_{0};
    pipeline::PipelineState last_state_{pipeline::PipelineState::Init};
    std::unique_ptr<Runtime> rt_;  // declared last: destroyed first, before the io_context it posts to
};

int Host::run() {
    if (!opt_.stop) throw std::invalid_argument("HostOptions::stop is required");
    redis_url_ = core::env::require(cfg_.sink.redis_url_env);
    telemetry_.init_metrics(cfg_);
    ops_ = std::make_unique<ops::OpsHttpServer>(ioc_, cfg_.ops.http_listen, routes());
    if (opt_.on_ops_port) opt_.on_ops_port(ops_->port());
    telemetry_.init_ops_clients(ioc_.get_executor(), user_agent(cfg_));

    wd_.start();
    log::info("host", "started",
              {{"version", core::version_string()}, {"shard", cfg_.shard.id}, {"host_id", cfg_.shard.host_id},
               {"ops_port", std::to_string(ops_->port())}, {"lease", cfg_.lease.enabled ? "enabled" : "disabled"}});

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
    wd_.stop();
    if (rt_ && rt_->sink) {
        wd_.set_sink_registered(false);
        const auto d = rt_->sink->drain(kDrainBudget);
        log::info("host", "sink drained",
                  {{"flushed", d.flushed ? "true" : "false"}, {"unflushed", std::to_string(d.unflushed)},
                   {"lease_released", d.lease_released ? "true" : "false"}});
    }
    log::info("host", "stopped", {{"exit_code", std::to_string(exit_code_)}});
    return exit_code_;
}

void Host::acquire_thread_main(bool first) {
    const auto stop = [this] { return stopping_.load() || opt_.stop->load(std::memory_order_relaxed); };
    const auto sleep = [stop](std::chrono::milliseconds d) {
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
            redis_wait_since_ns_.store(0);
            if (!report) return;
            std::lock_guard lk(redis_wait_mu_);
            redis_wait_family_ = report->address_family;
        }

        sink::LeaseGrant grant;
        if (cfg_.lease.enabled) {
            phase_ = Phase::Standby;
            const sink::Keys keys(cfg_.shard.id);
            sink::RedisppConn conn(redis_url_, keys);
            sink::AcquireOptions ao;
            ao.host_id = cfg_.shard.host_id;
            ao.ttl_ms = cfg_.lease.ttl_ms;
            ao.poll = std::chrono::milliseconds(cfg_.lease.renew_ms);
            ao.stop = stop;
            ao.sleep = sleep;
            ao.on_waiting = [this](const std::string& /*error*/, std::chrono::milliseconds waited) {
                const auto since = std::chrono::steady_clock::now() - waited;
                redis_wait_since_ns_.store(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(since.time_since_epoch()).count());
            };
            ao.on_reachable = [this] { redis_wait_since_ns_.store(0); };
            const auto g = sink::acquire_lease_blocking(conn, keys, ao);
            redis_wait_since_ns_.store(0);
            if (!g) return;
            grant = std::move(*g);
        } else {
            grant.token = sink::lease_disabled_token();
        }
        net::post(ioc_, [this, report, grant = std::move(grant)] { start_runtime(report, grant); });
    } catch (const sink::StartupFatal& e) {
        net::post(ioc_, [this, msg = std::string(e.what())] { begin_shutdown(kExitUsage, "redis startup check failed: " + msg); });
    } catch (const std::exception& e) {
        net::post(ioc_, [this, msg = std::string(e.what())] { begin_shutdown(kExitFailure, "redis startup failed: " + msg); });
    }
}

void Host::start_runtime(const std::optional<sink::StartupReport>& rep, const sink::LeaseGrant& grant) {
    if (stopping_) return;
    epoch_n_ = grant.epoch_n;
    if (grant.old_host) telemetry_.push_event(ops::leader_changed_event(*grant.old_host, cfg_.shard.host_id));
    try {
        if (rep) {
            const auto ver = std::to_string(rep->version.major) + "." + std::to_string(rep->version.minor) + "." +
                             std::to_string(rep->version.patch);
            if (rep->misconfigured) redis_misconfig_ = ops::RedisMisconfig{rep->maxmemory_policy, rep->appendonly, ver};
            for (const auto& w : rep->warnings) log::warn("host", "redis startup warning", {{"warning", w}});
            log::info("host", "redis ready", {{"redis_version", ver}});
        }

        bump_head_epoch();
        rt_ = std::make_unique<Runtime>(cfg_, cfg_.shard.id, cfg_.chain.hash_ring_size, cfg_.chain.pending_max_logs);

        auto sopts = sink::RedisSinkOptions::from_config(cfg_.sink, cfg_.lease, grant.token);
        sopts.loop_beat = wd_.sink_beat_slot();
        sopts.host_id = cfg_.shard.host_id;
        sopts.heartbeat_ms = cfg_.ops.heartbeat_s * 1000;
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
                      {{"provider", t.provider}, {"from", provider::to_string(t.from)}, {"to", provider::to_string(t.to)}, {"reason", why}});
        };
        h.on_tag = [this](const std::string& p, provider::Tag tag, std::uint64_t n, const core::Hash32& hash) { on_tag(p, tag, n, hash); };
        h.on_head = [this](const rpc::Head& head, const std::string& p) { on_head(head, p); };
        h.on_log = [this](const rpc::Log& log, const std::string& p) { on_ws_log(log, p); };
        h.on_ws_active_changed = [this](const std::optional<std::string>& from, const std::optional<std::string>& to, std::string_view reason) {
            if (!rt_) return;
            log::info("pool", "ws active provider changed", {{"from", from.value_or("none")}, {"to", to.value_or("none")}, {"reason", reason}});
            if ((reason == "failover" || reason == "switch_back") && to) {
                if (reason == "failover") ++provider_failovers_total_;
                telemetry_.push_event(ops::provider_failover_event(from, *to, reason));
            }
            const auto c_s = rt_->driver ? std::optional<std::uint64_t>(rt_->driver->status().c_s) : std::nullopt;
            rt_->ws_reconciler.on_ws_provider_changed(to.has_value(), c_s);
            if (rt_->source) rt_->source->clear_cache();
            bump_head_epoch();
        };
        rt_->pool = provider::ProviderPool::create(ioc_.get_executor(), make_pool_config(cfg_, user_agent(cfg_)), std::move(h));
        rt_->source = std::make_unique<PoolLogSource>(rt_->pool, &rt_->ring);

        pipeline::SweeperOptions sw;
        sw.min_range = cfg_.backfill.min_range;
        sw.max_page_logs = std::max<std::size_t>(1, cfg_.sink.queue_capacity / 2);
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
            if (driver.status().state == pipeline::PipelineState::Live) {
                rt_->ws_reconciler.on_live_tick(driver.status().c_s, rt_->pool && rt_->pool->active_ws().has_value());
            }
            if (r.done || stopping_ || demoting_) break;
            if (r.wait.count() > 0) {
                sleep_timer_.expires_after(r.wait);
                boost::system::error_code ec;
                co_await sleep_timer_.async_wait(net::redirect_error(net::use_awaitable, ec));
            }
        }
    } catch (const std::exception& e) {
        request_exit(kExitFailure, std::string("pipeline tick threw: ") + e.what());
    }
    const auto st = driver.status();
    if (st.state == pipeline::PipelineState::Failed) request_exit(kExitUsage, "pipeline failed: " + st.fatal);
    if (demoting_ && !stopping_) {
        net::post(ioc_, [this] { end_term(); });
        co_return;
    }
    finish();
}

net::awaitable<void> Host::walk_fork(std::uint64_t epoch, HeadEvent e) {
    const auto alive = [this, epoch] { return epoch == head_epoch_ && rt_ && rt_->driver; };
    const auto pool = rt_->pool;
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
                const auto b = dec.parse_block(o.result_raw);
                if (!b) {
                    why = e.provider + " has no block " + std::to_string(n);
                    break;
                }
                walk.feed(chain::BlockRef{b->number, b->hash, b->parent_hash, b->timestamp});
            }
            if (!why.empty()) break;
            if (walk.state() == chain::ForkWalk::State::Done) info = rt_->ring.apply(walk);
            else if (walk.state() == chain::ForkWalk::State::BeyondRing) why = "fork deeper than the head ring";
        }
        if (!info && why.empty()) why = "fork walk inconsistent after " + std::to_string(kWalkAttempts) + " attempts";

        rt_->walking = false;
        if (info) {
            const auto c_s = rt_->driver->status().c_s;
            log::info("host", "head chain resolved",
                      {{"fork_point", std::to_string(info->fork_point)}, {"depth", std::to_string(info->depth)},
                       {"old_top", std::to_string(info->old_top)}, {"c_s", std::to_string(c_s)}});
            rt_->ws_reconciler.on_fork_resolved(*info, e.ref.number);
            if (info->depth > 0 && rt_->source) rt_->source->clear_cache();
            if (info->depth > 0 && chain::ReorgDetector::crosses_safe(*info, c_s)) {
                log::warn("host", "head fork below C_s", {{"fork_point", std::to_string(info->fork_point)}, {"c_s", std::to_string(c_s)}});
                rt_->driver->on_safe_reorg_signal("head fork at " + std::to_string(info->fork_point) + " below C_s " + std::to_string(c_s));
            }
        } else {
            resync_ring(e.ref, why);
        }
        if (rt_->deferred_head) {
            HeadEvent next = std::move(*rt_->deferred_head);
            rt_->deferred_head.reset();
            handle_head(next);
        }
    } catch (const std::exception& ex) {
        if (!alive()) co_return;
        rt_->walking = false;
        rt_->deferred_head.reset();
        log::error("host", "head walk threw", {{"error", ex.what()}});
        resync_ring(e.ref, std::string("head walk threw: ") + ex.what());
    }
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
