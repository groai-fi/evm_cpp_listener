#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <boost/asio/any_io_executor.hpp>

#include "app/ws_reconciler.hpp"
#include "chain/reorg_detector.hpp"
#include "core/config.hpp"
#include "ops/alert_sender.hpp"
#include "ops/alerts.hpp"
#include "ops/deadman.hpp"
#include "ops/metrics.hpp"
#include "ops/ops_http_server.hpp"
#include "ops/snapshot.hpp"
#include "ops/watchdog.hpp"
#include "pipeline/pipeline_driver.hpp"
#include "pipeline/sweeper.hpp"
#include "provider/provider_pool.hpp"
#include "sink/redis_sink.hpp"

namespace evml::app {

struct TelemetryContext {
    std::string state;
    std::string_view role{"none"};
    bool leader{false};
    bool gate_age_ok{false};
    bool healthy_standby{false};
    std::uint64_t provider_failovers_total{0};
    std::uint64_t ws_missed_total{0};
    std::uint64_t ws_extra_total{0};
    const sink::RedisSink* sink{nullptr};
    const pipeline::Sweeper* sweeper{nullptr};
    const pipeline::PipelineDriver* driver{nullptr};
    const WsReconciler* ws_reconciler{nullptr};
    const provider::ProviderPool* pool{nullptr};
    const chain::ReorgDetector* ring{nullptr};
};

// Owns Prometheus metric registrations/updates, HTTP ops route formatting (/healthz, /readyz, /metrics),
// alert/dead-man evaluation, and Redis `hb:<host_id>` JSON heartbeat serialization (plan 60 §4.7, §5.2, §5.3, §5.4).
class HostTelemetry {
public:
    explicit HostTelemetry(const core::Config& cfg);

    void init_metrics(const core::Config& cfg);
    void init_ops_clients(boost::asio::any_io_executor ex, const std::string& user_agent);
    void stop();

    void push_event(ops::AlertEvent ev);
    void ops_tick(const TelemetryContext& ctx, std::chrono::steady_clock::time_point now, std::uint64_t ts_ms,
                  const std::optional<ops::RedisMisconfig>& redis_misconfig, std::int64_t redis_wait_since_ns,
                  const std::string& redis_wait_family);
    void update_metrics(const TelemetryContext& ctx);

    ops::OpsResponse build_healthz(const ops::Watchdog& wd) const;
    ops::OpsResponse build_readyz(const TelemetryContext& ctx) const;
    ops::OpsResponse build_metrics_response(const TelemetryContext& ctx);

    ops::SnapshotInputs build_snapshot_inputs(const TelemetryContext& ctx, std::chrono::steady_clock::time_point now,
                                              std::uint64_t ts_ms,
                                              const std::optional<ops::RedisMisconfig>& redis_misconfig,
                                              std::uint32_t redis_unreachable_s,
                                              const std::string& addr_family) const;

    static std::string format_heartbeat_json(std::uint64_t ts_ms, const pipeline::DriverStatus& st,
                                             std::optional<std::uint64_t> head,
                                             const std::optional<std::string>& active_provider,
                                             std::optional<std::uint64_t> epoch_n);

private:
    void set_sink_counters(const sink::SinkStatus& sk);
    void set_sweeper_counters(const pipeline::SweepStats& sw);
    void set_leader_gauges(const sink::SinkStatus* sk, const pipeline::DriverStatus* st,
                           std::optional<std::uint64_t> head);
    void set_consumer_lag_gauges(const ops::Metrics::Labels& lbl, double undelivered_age_s, double pending_age_s,
                                 double lag_entries);

    const core::Config& cfg_;
    ops::Metrics metrics_;
    ops::AlertEngine engine_;
    ops::SnapshotBuilder snapshots_;
    std::unique_ptr<ops::AlertSender> alerts_;
    std::unique_ptr<ops::Deadman> deadman_;
    std::chrono::steady_clock::time_point next_ops_{}, next_deadman_{};
    std::optional<std::chrono::steady_clock::time_point> sink_degraded_since_;
    std::map<std::string, std::uint64_t> alerts_fired_total_;
};

}  // namespace evml::app
