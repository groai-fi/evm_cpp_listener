#pragma once
// Prometheus metrics registry for /metrics (plan 60 §5.4, slice 5-B4). The sink, sweeper and driver expose running
// TOTALS, not events, so counters are set from a monotonic total: the registry adds only the difference since the last
// call. Names and label names are validated by prometheus-cpp (std::invalid_argument on a bad one).
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include <prometheus/counter.h>
#include <prometheus/family.h>
#include <prometheus/gauge.h>
#include <prometheus/registry.h>

namespace evml::ops {

class Metrics {
public:
    using Labels = std::map<std::string, std::string>;

    Metrics();

    /// @brief Brings the counter `name{labels}` up to `total`.
    /// Example: totals 5 then 8 add 3 (the counter reads 8). A total BELOW the previous one means the source object was
    /// rebuilt (a new sink per leadership term restarts at 0), so the new total is added as fresh increments and the
    /// counter keeps rising, as Prometheus expects.
    /// @throws std::logic_error if `name` was registered as a gauge, or with a different help text.
    void counter_total(const std::string& name, const std::string& help, const Labels& labels, std::uint64_t total);

    /// @brief Sets the gauge `name{labels}` to `value`.
    /// @throws std::logic_error if `name` was registered as a counter, or with a different help text.
    void gauge(const std::string& name, const std::string& help, const Labels& labels, double value);

    /// @brief Prometheus text exposition format (what `/metrics` returns).
    std::string serialize() const;

private:
    static std::string series_key(const std::string& name, const Labels& labels);

    mutable std::mutex mu_;
    std::shared_ptr<prometheus::Registry> registry_;
    std::map<std::string, std::pair<prometheus::Family<prometheus::Counter>*, std::string>> counters_;  // name -> (family, help)
    std::map<std::string, std::pair<prometheus::Family<prometheus::Gauge>*, std::string>> gauges_;
    std::map<std::string, std::uint64_t> last_total_;  // series key -> last total seen
};

}  // namespace evml::ops
