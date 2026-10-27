#include "app/host.hpp"

#include <algorithm>
#include <csignal>
#include <cstring>
#include <iostream>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "app/pool_config.hpp"
#include "app/pool_log_source.hpp"
#include "app/tools.hpp"
#include "chain/finality_gate.hpp"
#include "core/env.hpp"
#include "core/log.hpp"
#include "core/version.hpp"
#include "ops/metrics.hpp"
#include "ops/ops_http_server.hpp"
#include "ops/watchdog.hpp"
#include "pipeline/sweeper.hpp"
#include "provider/provider_pool.hpp"
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

std::atomic<bool> g_signal_stop{false};
extern "C" void on_signal(int) { g_signal_stop.store(true, std::memory_order_relaxed); }

std::uint64_t wall_ms() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

/// Everything that exists only while the pipeline runs. Members are declared in dependency order, so the reverse
/// destruction order tears down the driver first and the sink last.
struct Runtime {
    explicit Runtime(const std::string& shard) : keys(shard) {}
    sink::Keys keys;
    std::unique_ptr<sink::RedisSink> sink;
    chain::FinalityGate gate;
    std::shared_ptr<provider::ProviderPool> pool;
    std::unique_ptr<PoolLogSource> source;
    std::unique_ptr<pipeline::Sweeper> sweeper;
    std::unique_ptr<pipeline::PipelineDriver> driver;
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
          beat_timer_(ioc_),
          sleep_timer_(ioc_),
          grace_timer_(ioc_),
          freeze_timer_(ioc_) {}

    int run();

private:
    // ---- io thread ----
    void start_runtime(const sink::StartupReport& rep);
    net::awaitable<void> pipeline_loop();
    void on_tag(const std::string& provider, provider::Tag tag, std::uint64_t number, const core::Hash32& hash);
    void schedule_beat();
    void on_beat();
    void request_exit(int code, const std::string& why);  // first reason wins
    void begin_shutdown(int code, const std::string& why);
    void finish();
    ops::OpsHttpServer::Routes routes();
    std::string state_name() const;

    // ---- startup thread ----
    void startup_thread_main();

    const core::Config& cfg_;
    HostOptions opt_;
    std::string redis_url_;

    net::io_context ioc_;
    ops::Watchdog wd_;
    ops::Metrics metrics_;
    std::unique_ptr<ops::OpsHttpServer> ops_;
    net::steady_timer beat_timer_, sleep_timer_, grace_timer_, freeze_timer_;
    std::thread startup_;

    std::atomic<bool> stopping_{false};  // read by the startup thread too
    bool finished_{false};
    bool exit_set_{false};
    int exit_code_{0};
    pipeline::PipelineState last_state_{pipeline::PipelineState::Init};

    std::unique_ptr<Runtime> rt_;  // declared last: destroyed first, before the io_context it posts to
};

// ------------------------------------------------------------------------------------------------------------------
// Ops routes (minimal in D1; full wiring in 5-F)
// ------------------------------------------------------------------------------------------------------------------

std::string Host::state_name() const {
    if (rt_ && rt_->driver) return pipeline::to_string(rt_->driver->status().state);
    return "waiting_redis";
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
        o.status = state == "live" ? 200 : 503;
        o.content_type = "application/json";
        o.body = "{\"state\":\"" + state + "\",\"role\":\"" + (rt_ && rt_->driver ? "leader" : "none") +
                 "\",\"lease\":\"disabled\"}\n";
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
    if (cfg_.lease.enabled) {
        std::cerr << "[lease] enabled = true is not supported yet (slice 5-D2); set enabled = false for a single host\n";
        return kExitUsage;
    }
    redis_url_ = core::env::require(cfg_.sink.redis_url_env);
    ops_ = std::make_unique<ops::OpsHttpServer>(ioc_, cfg_.ops.http_listen, routes());

    wd_.start();
    log::info("host", "started",
              {{"version", core::version_string()},
               {"shard", cfg_.shard.id},
               {"host_id", cfg_.shard.host_id},
               {"ops_port", std::to_string(ops_->port())},
               {"lease", "disabled"}});

    if (opt_.test_freeze_io_after) {
        freeze_timer_.expires_after(*opt_.test_freeze_io_after);
        freeze_timer_.async_wait([](const boost::system::error_code& ec) {
            if (ec) return;
            log::error("host", "TEST HOOK: freezing the io thread");
            std::this_thread::sleep_for(1h);
        });
    }
    schedule_beat();
    startup_ = std::thread([this] { startup_thread_main(); });

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

void Host::startup_thread_main() {
    // Blocking Redis wait lives off the io thread: /healthz and the io beat keep working while Redis is down.
    sink::StartupOptions so;
    so.require_noeviction = cfg_.sink.require_noeviction;
    so.on_waiting = [](const std::string& error, const std::string& family, std::chrono::milliseconds waited) {
        log::warn("host", "redis unreachable, waiting",
                  {{"error", error}, {"address_family", family}, {"waited_ms", std::to_string(waited.count())}});
    };
    so.stop = [this] { return stopping_.load() || opt_.stop->load(std::memory_order_relaxed); };
    so.sleep = [this](std::chrono::milliseconds d) {  // sliced so a stop request is noticed quickly
        const auto end = std::chrono::steady_clock::now() + d;
        while (std::chrono::steady_clock::now() < end && !stopping_.load() && !opt_.stop->load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(50ms);
        }
    };
    try {
        const auto report = sink::await_redis_ready(redis_url_, so);
        if (report) net::post(ioc_, [this, r = *report] { start_runtime(r); });
        // nullopt: stop was requested while waiting; the beat sees the same flag and shuts down.
    } catch (const sink::StartupFatal& e) {
        net::post(ioc_, [this, msg = std::string(e.what())] { begin_shutdown(kExitUsage, "redis startup check failed: " + msg); });
    } catch (const std::exception& e) {
        net::post(ioc_, [this, msg = std::string(e.what())] { begin_shutdown(kExitFailure, "redis startup failed: " + msg); });
    }
}

void Host::start_runtime(const sink::StartupReport& rep) {
    if (stopping_) return;
    try {
        for (const auto& w : rep.warnings) log::warn("host", "redis startup warning", {{"warning", w}});
        log::info("host", "redis ready",
                  {{"redis_version", std::to_string(rep.version.major) + "." + std::to_string(rep.version.minor) + "." +
                                         std::to_string(rep.version.patch)}});

        rt_ = std::make_unique<Runtime>(cfg_.shard.id);

        auto sopts = sink::RedisSinkOptions::from_config(cfg_.sink, cfg_.lease, sink::lease_disabled_token());
        sopts.loop_beat = wd_.sink_beat_slot();
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
        rt_->pool = provider::ProviderPool::create(ioc_.get_executor(), make_pool_config(cfg_, user_agent(cfg_)), std::move(h));
        // Heads and the reorg ring arrive in 5-D4; until then block() has no ring and always asks a provider.
        rt_->source = std::make_unique<PoolLogSource>(rt_->pool, nullptr);

        pipeline::SweeperOptions sw;
        sw.min_range = cfg_.backfill.min_range;
        sw.max_page_logs = std::max<std::size_t>(1, cfg_.sink.queue_capacity / 2);  // plan 3.5 G6
        const pipeline::EventContext ctx{cfg_.shard.id, cfg_.shard.host_id, core::version_string()};
        rt_->sweeper = std::make_unique<pipeline::Sweeper>(cfg_, ctx, *rt_->source, *rt_->sink, wall_ms, sw);

        pipeline::DriverOptions dopt;
        dopt.seed = std::random_device{}();
        rt_->driver = std::make_unique<pipeline::PipelineDriver>(cfg_, *rt_->sweeper, *rt_->sink, *rt_->source, wall_ms, dopt);

        rt_->pool->start();
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
            if (r.done || stopping_) break;
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

void Host::schedule_beat() {
    beat_timer_.expires_after(opt_.beat_period);
    beat_timer_.async_wait([this](const boost::system::error_code& ec) {
        if (ec || finished_) return;
        on_beat();
        if (!finished_) schedule_beat();
    });
}

void Host::on_beat() {
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
                      {{"from", pipeline::to_string(last_state_)},
                       {"to", pipeline::to_string(st.state)},
                       {"c_s", std::to_string(st.c_s)},
                       {"c_f", std::to_string(st.c_f)}});
            last_state_ = st.state;
        }
        if (opt_.on_status) opt_.on_status(st);
    }
    wd_.publish_state(s);

    if (opt_.stop->load(std::memory_order_relaxed)) {
        begin_shutdown(kExitOk, "stop requested");
    } else if (rt_ && rt_->sink && rt_->sink->mode() == sink::SinkMode::Failed) {
        begin_shutdown(kExitFailure, "sink failed: " + rt_->sink->fatal_message());
    }
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
