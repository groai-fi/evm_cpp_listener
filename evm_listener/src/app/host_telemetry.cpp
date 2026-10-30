#include "app/host_telemetry.hpp"

#include <algorithm>
#include <random>
#include <utility>

#include <boost/json.hpp>

#include "core/env.hpp"
#include "core/log.hpp"
#include "core/version.hpp"

namespace evml::app {

namespace log = core::log;
using namespace std::chrono_literals;

namespace {
constexpr auto kOpsPeriod = 10s;  // plan 5.3.3: the alert engine evaluates a fresh snapshot this often
}  // namespace

HostTelemetry::HostTelemetry(const core::Config& cfg)
    : cfg_(cfg), engine_(ops::AlertThresholds::from_config(cfg)) {}

void HostTelemetry::init_ops_clients(boost::asio::any_io_executor ex, const std::string& user_agent) {
    const auto hook = core::env::get(cfg_.ops.alert_webhook_env);
    const auto token = core::env::get(cfg_.ops.alert_webhook_token_env);
    if (!hook.empty() && !token.empty()) {
        ops::AlertSenderOptions ao;
        ao.url = hook;
        ao.token = token;
        ao.service = cfg_.ops.alert_service;
        ao.shard = cfg_.shard.id;
        ao.host = cfg_.shard.host_id;
        ao.user_agent = user_agent;
        ao.seed = std::random_device{}();
        alerts_ = std::make_unique<ops::AlertSender>(ex, std::move(ao));
    } else {
        log::warn("host", "alert webhook not configured: alerts are only logged",
                  {{"url_env", cfg_.ops.alert_webhook_env}, {"token_env", cfg_.ops.alert_webhook_token_env}});
    }
    ops::DeadmanOptions dopt;
    dopt.url = core::env::get(cfg_.ops.deadman_ping_env);
    dopt.user_agent = user_agent;
    deadman_ = std::make_unique<ops::Deadman>(ex, std::move(dopt));
}

void HostTelemetry::stop() {
    if (alerts_) alerts_->stop();
    if (deadman_) deadman_->stop();
}

void HostTelemetry::push_event(ops::AlertEvent ev) { snapshots_.push_event(std::move(ev)); }

void HostTelemetry::ops_tick(const TelemetryContext& ctx, std::chrono::steady_clock::time_point now,
                             std::uint64_t ts_ms, const std::optional<ops::RedisMisconfig>& redis_misconfig,
                             std::int64_t redis_wait_since_ns, const std::string& redis_wait_family) {
    if (now >= next_ops_) {
        next_ops_ = now + kOpsPeriod;
        std::chrono::seconds down{0};
        if (redis_wait_since_ns != 0) {
            const std::chrono::steady_clock::time_point since{
                std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                    std::chrono::nanoseconds(redis_wait_since_ns))};
            down = std::chrono::duration_cast<std::chrono::seconds>(now - since);
        }
        if (ctx.sink && ctx.sink->mode() == sink::SinkMode::Degraded) {
            if (!sink_degraded_since_) sink_degraded_since_ = now;
            down = std::max(down, std::chrono::duration_cast<std::chrono::seconds>(now - *sink_degraded_since_));
        } else {
            sink_degraded_since_.reset();
        }
        const auto in = build_snapshot_inputs(ctx, now, ts_ms, redis_misconfig,
                                              static_cast<std::uint32_t>(down.count()), redis_wait_family);
        for (const auto& a : engine_.evaluate(snapshots_.build(in))) {
            if (!a.resolved) ++alerts_fired_total_[a.title];
            log::warn("alert", "alert raised",
                      {{"severity", ops::to_string(a.severity)}, {"title", a.title}, {"message", a.message}});
            if (alerts_) alerts_->send(a);
        }
        update_metrics(ctx);
    }

    if (deadman_ && now >= next_deadman_) {
        next_deadman_ = now + std::chrono::seconds(cfg_.ops.deadman_ping_s);
        ops::DeadmanFacts f;
        f.now = now;
        f.leader = ctx.leader;
        if (ctx.leader && ctx.driver && ctx.sink) {
            const auto st = ctx.driver->status();
            f.state_ok = st.state == pipeline::PipelineState::CatchingUp || st.state == pipeline::PipelineState::Live;
            f.sink_saturated = ctx.sink->saturated();
            f.c_s = st.c_s;
            f.g_s = st.g_s;
        }
        deadman_->tick(f);
    }
}

void HostTelemetry::set_sink_counters(const sink::SinkStatus& sk) {
    metrics_.counter_total("evml_lease_renew_failures_total", "Lease renew failures", {}, sk.renew_failures_total);
    metrics_.counter_total("evml_spill_discarded_records_total", "Spill/queue records discarded on fenced/failed sink", {},
                           sk.discarded_total);
    metrics_.counter_total("evml_sink_dup_total", "Duplicate emissions collapsed by Redis emit.lua", {}, sk.dup_total);
    metrics_.counter_total("evml_sink_below_finalized_total", "Tracked emissions refused at or below finalized checkpoint", {},
                           sk.below_finalized_total);
    metrics_.counter_total("evml_sink_heartbeat_failures_total", "Redis heartbeat write failures", {}, sk.heartbeat_failures_total);
    metrics_.counter_total("evml_sink_consumer_lag_failures_total", "Consumer-group lag probe failures", {},
                           sk.consumer_lag_failures_total);
    metrics_.counter_total("evml_sink_reconnects_total", "Redis sink reconnects", {}, sk.reconnects_total);
    metrics_.counter_total("evml_fast_emitted_total", "Fast-stream tip events emitted to Redis", {}, sk.fast_emitted_total);
    metrics_.counter_total("evml_fast_dropped_total",
                           "Fast-stream tip events dropped due to backpressure, outage, or fencing", {},
                           sk.fast_dropped_total);
}

void HostTelemetry::set_sweeper_counters(const pipeline::SweepStats& sw) {
    metrics_.counter_total("evml_safe_missed_total", "Safe-class logs missed by safe sweep and emitted late by final sweep", {},
                           sw.late);
    metrics_.counter_total("evml_sweep_emitted_safe_total", "Safe-sweep log emissions", {}, sw.emitted_safe);
    metrics_.counter_total("evml_sweep_emitted_final_total", "Final-sweep log emissions", {}, sw.emitted_final);
    metrics_.counter_total("evml_sweep_deduped_total", "Emissions skipped by in-process dedup layer", {}, sw.deduped);
    metrics_.counter_total("evml_reverts_emitted_total", "Revert events emitted", {}, sw.reverts);
    metrics_.counter_total("evml_sweep_matched_total", "Safe-released logs confirmed by final sweep", {}, sw.matched);
    metrics_.counter_total("evml_sweep_range_shrinks_total", "AIMD page shrinks due to RangeTooLarge", {}, sw.range_shrinks);
    metrics_.counter_total("evml_sweep_failure_shrinks_total", "AIMD page shrinks due to rate limit/timeout/5xx", {},
                           sw.failure_shrinks);
    metrics_.counter_total("evml_sweep_volume_shrinks_total", "AIMD page shrinks due to max_page_logs", {}, sw.volume_shrinks);
    metrics_.counter_total("evml_sweep_reverified_new_total", "New logs emitted during reverify_released", {}, sw.reverified_new);
    metrics_.counter_total("evml_sweep_address_splits_total", "Address-split fallback pages at min_range", {}, sw.address_splits);
}

void HostTelemetry::set_leader_gauges(const sink::SinkStatus* sk, const pipeline::DriverStatus* st,
                                      std::optional<std::uint64_t> head) {
    metrics_.gauge("evml_sink_queue_depth", "In-memory sink queue depth", {}, sk ? static_cast<double>(sk->queue_depth) : 0.0);
    metrics_.gauge("evml_sink_spill_records", "Records currently on disk in the spill buffer", {},
                   sk ? static_cast<double>(sk->spill_records) : 0.0);
    metrics_.gauge("evml_sink_spill_bytes", "Bytes currently on disk in the spill buffer", {},
                   sk ? static_cast<double>(sk->spill_bytes) : 0.0);
    metrics_.gauge("evml_sink_pending_writes", "Total unacknowledged sink writes (queue + batch + spill)", {},
                   sk ? static_cast<double>(sk->pending_writes) : 0.0);
    metrics_.gauge("evml_sink_oldest_record_age_seconds", "Age in seconds of oldest unacknowledged sink write", {},
                   sk ? static_cast<double>(sk->oldest_record_age_s) : 0.0);
    metrics_.gauge("evml_head_block", "Highest block number in the WS head ring", {},
                   static_cast<double>(head.value_or(0)));
    metrics_.gauge("evml_gate_block", "Latest finality gate block number", {{"class", "safe"}},
                   st ? static_cast<double>(st->g_s.value_or(0)) : 0.0);
    metrics_.gauge("evml_gate_block", "Latest finality gate block number", {{"class", "finalized"}},
                   st ? static_cast<double>(st->g_f.value_or(0)) : 0.0);
    metrics_.gauge("evml_checkpoint_block", "Latest sweep cursor block number", {{"class", "safe"}},
                   st ? static_cast<double>(st->c_s) : 0.0);
    metrics_.gauge("evml_checkpoint_block", "Latest sweep cursor block number", {{"class", "finalized"}},
                   st ? static_cast<double>(st->c_f) : 0.0);
}

void HostTelemetry::set_consumer_lag_gauges(const ops::Metrics::Labels& lbl, double undelivered_age_s,
                                            double pending_age_s, double lag_entries) {
    metrics_.gauge("evml_consumer_undelivered_age_seconds", "Age in seconds of oldest unread stream entry", lbl,
                   undelivered_age_s);
    metrics_.gauge("evml_consumer_pending_age_seconds",
                   "Age in seconds of oldest pending unacknowledged stream entry", lbl, pending_age_s);
    metrics_.gauge("evml_consumer_lag_entries", "Unread stream entries for consumer group (-1 when unknown)", lbl,
                   lag_entries);
}

void HostTelemetry::init_metrics(const core::Config& cfg) {
    metrics_.gauge("evml_role", "Current role of this host (1 = active)", {{"role", "leader"}}, 0.0);
    metrics_.gauge("evml_role", "Current role of this host (1 = active)", {{"role", "standby"}}, 0.0);
    metrics_.counter_total("evml_provider_failovers_total", "WS provider failovers", {}, 0);
    metrics_.counter_total("evml_alerts_fired_total", "Alerts fired by the alert engine", {{"alert", "Safe Gate Stuck"}}, 0);
    metrics_.counter_total("evml_reverify_total", "Completed safe-window reverifications", {{"result", "clean"}}, 0);
    metrics_.counter_total("evml_reverify_total", "Completed safe-window reverifications", {{"result", "reorg"}}, 0);
    metrics_.counter_total("evml_alert_send_failures_total", "Alert webhook send failures after retries", {}, 0);
    metrics_.counter_total("evml_deadman_ping_failures_total", "Dead-man switch ping failures", {}, 0);
    metrics_.counter_total("evml_ws_missed_logs_total", "Logs found by getLogs sweep that WS missed", {}, 0);
    metrics_.counter_total("evml_ws_extra_logs_total", "Staged WS logs not present in canonical getLogs sweep", {}, 0);
    metrics_.counter_total("evml_pending_evicted_logs_total", "Staged WS logs evicted at pending_max_logs cap", {}, 0);
    set_sink_counters({});
    set_sweeper_counters({});
    set_leader_gauges(nullptr, nullptr, std::nullopt);

    const auto init_provider_metrics = [this](const std::string& name) {
        metrics_.gauge("evml_provider_active", "Whether provider is currently active for WS or getLogs", {{"provider", name}}, 0.0);
        metrics_.counter_total("evml_provider_inconsistency_total", "Bad or inconsistent responses reported for provider",
                               {{"provider", name}}, 0);
        metrics_.counter_total("evml_getlogs_throttled_total", "eth_getLogs calls throttled by token bucket",
                               {{"provider", name}}, 0);
        for (const char* m : {"eth_getLogs", "eth_getBlockByNumber", "eth_blockNumber", "eth_chainId"}) {
            metrics_.counter_total("evml_rpc_requests_total", "Total HTTP JSON-RPC requests sent to provider",
                                   {{"provider", name}, {"method", m}}, 0);
        }
        for (const char* s : {"heads", "logs"}) {
            metrics_.counter_total("evml_ws_messages_total", "Total WebSocket subscription messages received from provider",
                                   {{"provider", name}, {"stream", s}}, 0);
        }
    };
    for (const auto& p : cfg.getlogs_chain) init_provider_metrics(p.name);
    for (const auto& p : cfg.ws_chain) init_provider_metrics(p.name);
    if (cfg.sink.consumer_groups.empty()) {
        set_consumer_lag_gauges({{"stream", "events"}, {"group", "none"}}, 0.0, 0.0, 0.0);
    } else {
        for (const auto& g : cfg.sink.consumer_groups) {
            set_consumer_lag_gauges({{"stream", core::to_string(g.stream)}, {"group", g.group}}, 0.0, 0.0, 0.0);
        }
    }
}

void HostTelemetry::update_metrics(const TelemetryContext& ctx) {
    metrics_.gauge("evml_role", "Current role of this host (1 = active)", {{"role", "leader"}}, ctx.role == "leader" ? 1.0 : 0.0);
    metrics_.gauge("evml_role", "Current role of this host (1 = active)", {{"role", "standby"}}, ctx.role == "standby" ? 1.0 : 0.0);
    metrics_.counter_total("evml_provider_failovers_total", "WS provider failovers", {}, ctx.provider_failovers_total);
    metrics_.counter_total("evml_alert_send_failures_total", "Alert webhook send failures after retries", {},
                           alerts_ ? alerts_->send_failures_total() : 0);
    metrics_.counter_total("evml_deadman_ping_failures_total", "Dead-man switch ping failures", {},
                           deadman_ ? deadman_->ping_failures_total() : 0);
    metrics_.counter_total("evml_ws_missed_logs_total", "Logs found by getLogs sweep that WS missed", {}, ctx.ws_missed_total);
    metrics_.counter_total("evml_ws_extra_logs_total", "Staged WS logs not present in canonical getLogs sweep", {}, ctx.ws_extra_total);
    for (const auto& [title, count] : alerts_fired_total_) {
        metrics_.counter_total("evml_alerts_fired_total", "Alerts fired by the alert engine", {{"alert", title}}, count);
    }

    if (ctx.driver) {
        const auto sk = ctx.sink->status();
        set_sink_counters(sk);
        set_sweeper_counters(ctx.sweeper->stats());

        const auto st = ctx.driver->status();
        metrics_.counter_total("evml_reverify_total", "Completed safe-window reverifications", {{"result", "clean"}}, st.reverify_clean);
        metrics_.counter_total("evml_reverify_total", "Completed safe-window reverifications", {{"result", "reorg"}}, st.safe_reorgs);
        metrics_.counter_total("evml_pending_evicted_logs_total", "Staged WS logs evicted at pending_max_logs cap", {},
                               ctx.ws_reconciler->evicted_total());

        for (const auto& p : ctx.pool->health()) {
            metrics_.counter_total("evml_provider_inconsistency_total", "Bad or inconsistent responses reported for provider",
                                   {{"provider", p.name}}, ctx.pool->inconsistency_total(p.name));
            metrics_.counter_total("evml_getlogs_throttled_total", "eth_getLogs calls throttled by token bucket",
                                   {{"provider", p.name}}, ctx.pool->throttled_total(p.name));
            for (const auto& [method, count] : ctx.pool->rpc_requests_total(p.name)) {
                metrics_.counter_total("evml_rpc_requests_total", "Total HTTP JSON-RPC requests sent to provider",
                                       {{"provider", p.name}, {"method", method}}, count);
            }
            for (const auto& [stream, count] : ctx.pool->ws_messages_total(p.name)) {
                metrics_.counter_total("evml_ws_messages_total", "Total WebSocket subscription messages received from provider",
                                       {{"provider", p.name}, {"stream", stream}}, count);
            }
        }

        if (ctx.leader) {
            set_leader_gauges(&sk, &st, ctx.ring->top());

            const auto ws = ctx.pool->active_ws();
            const auto getlogs = ctx.pool->current_getlogs();
            for (const auto& p : ctx.pool->health()) {
                const bool active = (ws && *ws == p.name) || (getlogs && *getlogs == p.name);
                metrics_.gauge("evml_provider_active", "Whether provider is currently active for WS or getLogs", {{"provider", p.name}},
                               active ? 1.0 : 0.0);
            }

            for (const auto& c : ctx.sink->consumer_lag()) {
                if (c.probed_at_ms == 0) continue;
                set_consumer_lag_gauges({{"stream", core::to_string(c.stream)}, {"group", c.group}},
                                        static_cast<double>(c.undelivered_age_s), static_cast<double>(c.pending_age_s),
                                        c.lag ? static_cast<double>(*c.lag) : -1.0);
            }
        }
    }

    if (!ctx.leader) {
        for (const auto& p : cfg_.getlogs_chain) {
            metrics_.gauge("evml_provider_active", "Whether provider is currently active for WS or getLogs",
                           {{"provider", p.name}}, 0.0);
        }
        for (const auto& p : cfg_.ws_chain) {
            metrics_.gauge("evml_provider_active", "Whether provider is currently active for WS or getLogs",
                           {{"provider", p.name}}, 0.0);
        }
        set_leader_gauges(nullptr, nullptr, std::nullopt);
    }
}

ops::OpsResponse HostTelemetry::build_healthz(const ops::Watchdog& wd) const {
    ops::OpsResponse o;
    if (const char* stale = wd.stale_heartbeat()) {
        o.status = 503;
        o.body = std::string("stale heartbeat: ") + stale + "\n";
    } else {
        o.body = "ok\n";
    }
    return o;
}

ops::OpsResponse HostTelemetry::build_readyz(const TelemetryContext& ctx) const {
    ops::OpsResponse o;
    o.content_type = "application/json";
    const bool saturated = ctx.leader && ctx.sink && ctx.sink->saturated();
    const bool ready = (ctx.leader && ctx.state == "live" && !saturated && ctx.gate_age_ok) || ctx.healthy_standby;
    o.status = ready ? 200 : 503;

    const auto opt_u64 = [](const std::optional<std::uint64_t>& v) {
        return v ? boost::json::value(*v) : boost::json::value(nullptr);
    };
    boost::json::object gates;
    boost::json::object cursors;
    if (ctx.leader && ctx.driver) {
        const auto st = ctx.driver->status();
        gates["safe"] = opt_u64(st.g_s);
        gates["finalized"] = opt_u64(st.g_f);
        cursors["safe"] = st.c_s;
        cursors["finalized"] = st.c_f;
    } else {
        gates["safe"] = nullptr;
        gates["finalized"] = nullptr;
        cursors["safe"] = nullptr;
        cursors["finalized"] = nullptr;
    }

    boost::json::object body;
    body["state"] = ctx.state;
    body["role"] = std::string(ctx.role);
    body["lease"] = cfg_.lease.enabled ? "enabled" : "disabled";
    body["deadman"] = (deadman_ && deadman_->enabled()) ? "enabled" : "disabled";
    body["consumer_lag"] = !cfg_.sink.consumer_groups.empty() ? "enabled" : "disabled";
    body["saturated"] = saturated;
    body["gates"] = std::move(gates);
    body["cursors"] = std::move(cursors);
    o.body = boost::json::serialize(body) + "\n";
    return o;
}

ops::OpsResponse HostTelemetry::build_metrics_response(const TelemetryContext& ctx) {
    ops::OpsResponse o;
    o.content_type = "text/plain; version=0.0.4; charset=utf-8";
    update_metrics(ctx);
    o.body = metrics_.serialize();
    return o;
}

ops::SnapshotInputs HostTelemetry::build_snapshot_inputs(
    const TelemetryContext& ctx, std::chrono::steady_clock::time_point now, std::uint64_t ts_ms,
    const std::optional<ops::RedisMisconfig>& redis_misconfig, std::uint32_t redis_unreachable_s,
    const std::string& addr_family) const {
    ops::SnapshotInputs in;
    in.now = now;
    in.wall_ms = ts_ms;
    in.leader = ctx.leader;
    in.chain = cfg_.chain.name;
    in.redis_misconfigured = redis_misconfig;
    if (redis_unreachable_s > 0) {
        in.redis_unreachable_s = redis_unreachable_s;
        in.addr_family = addr_family;
    }
    if (ctx.leader) {
        in.head = ctx.ring->top();
        in.driver = ctx.driver->status();
        in.sink = ctx.sink->status();
        in.sink_saturated = ctx.sink->saturated();
        in.spill_max_bytes = cfg_.sink.spill_max_bytes;
        in.retention_s = cfg_.sink.retention_ms / 1000;
        in.safe_late_total = ctx.sweeper->stats().late;
        in.safe_late_last_block = ctx.sweeper->stats().last_late_block;
        in.ws_missed_total = ctx.ws_missed_total;
        in.consumers = ctx.sink->consumer_lag();
        in.providers = ctx.pool->health();
        in.ms_since_head = ctx.pool->ms_since_head();
        const auto safe_of = [&](const std::optional<std::string>& name) -> std::optional<std::uint64_t> {
            if (!name) return std::nullopt;
            const auto t = ctx.pool->cached_tag(*name, provider::Tag::Safe);
            return t ? std::optional<std::uint64_t>(t->number) : std::nullopt;
        };
        const auto ws = ctx.pool->active_ws();
        const auto getlogs = ctx.pool->current_getlogs();
        in.ws_provider = ws.value_or(in.ms_since_head ? "none" : "");
        in.getlogs_provider = getlogs.value_or("");
        in.ws_safe = safe_of(ws);
        in.getlogs_safe = safe_of(getlogs);
        for (const auto& p : in.providers) in.provider_inconsistent_total[p.name] = ctx.pool->inconsistency_total(p.name);
    }
    return in;
}

std::string HostTelemetry::format_heartbeat_json(std::uint64_t ts_ms, const pipeline::DriverStatus& st,
                                                 std::optional<std::uint64_t> head,
                                                 const std::optional<std::string>& active_provider,
                                                 std::optional<std::uint64_t> epoch_n) {
    const auto opt_u64 = [](const std::optional<std::uint64_t>& v) {
        return v ? boost::json::value(*v) : boost::json::value(nullptr);
    };
    boost::json::object o;
    o["ts"] = ts_ms;
    o["role"] = "leader";
    o["state"] = pipeline::to_string(st.state);
    o["head"] = opt_u64(head);
    o["G_s"] = opt_u64(st.g_s);
    o["G_f"] = opt_u64(st.g_f);
    o["C_s"] = st.c_s;
    o["C_f"] = st.c_f;
    o["active_provider"] = active_provider ? boost::json::value(*active_provider) : boost::json::value(nullptr);
    o["epoch_n"] = opt_u64(epoch_n);
    o["version"] = core::version_string();
    return boost::json::serialize(o);
}

}  // namespace evml::app
