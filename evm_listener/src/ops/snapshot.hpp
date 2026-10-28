#pragma once
// Builds the alert engine's OpsSnapshot from the facts the host already has (plan 60 §5.3.3, slice 5-E4a).
// Pure: no I/O, no clock, no threads; the host reads the live objects on the io thread and passes copies in.
//
// Besides copying values it turns running counters into events: a counter that went up since the last build is one
// alert event (Safe Reorg, Sink Invariant, WS Missed Logs). Counters restart per leadership term, so the baseline is
// dropped whenever the host is not leader. Host-local events (Sink Fenced, later Leader Changed) are queued with
// push_event() and delivered with the next build.
//
// Facts that have no source yet are left unset, so their rules stay silent rather than guess (see plan 60, 5-E4a).
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "ops/alerts.hpp"
#include "pipeline/pipeline_driver.hpp"
#include "provider/provider_pool.hpp"
#include "sink/consumer_lag.hpp"
#include "sink/redis_sink.hpp"

namespace evml::ops {

struct SnapshotInputs {
    AlertClock::time_point now{};
    std::uint64_t wall_ms{0};  // listener wall clock; ages a consumer-lag probe (ConsumerLag::probed_at_ms)
    bool leader{false};
    std::string chain;

    // Host-local facts: valid in any role.
    std::optional<std::uint32_t> redis_unreachable_s;  // set while Redis connect / PING keeps failing
    std::string addr_family;                           // "ipv6" | "ipv4" | "unresolved" | "" (not known)
    std::optional<RedisMisconfig> redis_misconfigured;

    // Leader facts: read only when `leader`.
    std::optional<std::uint64_t> head;  // top of the head ring
    pipeline::DriverStatus driver;
    sink::SinkStatus sink;
    bool sink_saturated{false};
    std::uint64_t spill_max_bytes{0};
    std::uint64_t retention_s{0};
    std::uint64_t safe_late_total{0};   // SweepStats::late
    std::uint64_t ws_missed_total{0};   // host counter of WS logs the sweep found and the WS missed
    std::vector<sink::ConsumerLag> consumers;
    std::optional<std::uint32_t> ms_since_head;  // ProviderPool::ms_since_head()
    std::string ws_provider;                     // active WS provider; empty = none
    std::string getlogs_provider;                // current getLogs provider; empty = none
    std::optional<std::uint64_t> ws_safe;        // that provider's last read `safe`
    std::optional<std::uint64_t> getlogs_safe;
    std::vector<provider::ProviderHealth> providers;                  // ProviderPool::health()
    std::map<std::string, std::uint64_t> provider_inconsistent_total;  // ProviderPool::inconsistency_total() per provider
};

class SnapshotBuilder {
public:
    /// @brief Queue a host-local event (Sink Fenced, ...) for the next build(). Io thread only.
    void push_event(AlertEvent e);

    /// @brief The snapshot for `in`. Counter increases since the previous leader build become events.
    OpsSnapshot build(const SnapshotInputs& in);

private:
    struct Counters {
        std::uint64_t safe_reorgs{0};
        std::uint64_t below_finalized{0};
        std::uint64_t ws_missed{0};
        std::map<std::string, std::uint64_t> inconsistent;
    };
    std::optional<Counters> prev_;  // nullopt until the first leader build of a term
    std::vector<AlertEvent> pending_;
};

}  // namespace evml::ops
