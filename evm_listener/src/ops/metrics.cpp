#include "ops/metrics.hpp"

#include <prometheus/text_serializer.h>

#include <stdexcept>

namespace evml::ops {

Metrics::Metrics() : registry_(std::make_unique<prometheus::Registry>()) {}

std::string Metrics::series_key(std::string_view name, const Labels& labels) {
    std::string key(name);
    for (const auto& [k, v] : labels) key += '\x1f' + k + '\x1e' + v;  // std::map keeps label order stable
    return key;
}

void Metrics::counter_total(std::string_view name, std::string_view help, const Labels& labels,
                            std::uint64_t total) {
    const std::string name_s(name);
    const std::string help_s(help);
    const std::lock_guard lk(mu_);
    if (gauges_.count(name_s)) throw std::logic_error("metric " + name_s + " is already registered as a gauge");
    auto it = counters_.find(name_s);
    if (it == counters_.end()) {
        auto& family = prometheus::BuildCounter().Name(name_s).Help(help_s).Register(*registry_);
        it = counters_.emplace(name_s, std::make_pair(&family, help_s)).first;
    } else if (it->second.second != help) {
        throw std::logic_error("metric " + name_s + " is already registered with a different help text");
    }
    auto& counter = it->second.first->Add(labels);
    auto& last = last_total_[series_key(name, labels)];
    const std::uint64_t delta = total >= last ? total - last : total;  // total < last: source restarted from 0
    if (delta > 0) counter.Increment(static_cast<double>(delta));
    last = total;
}

void Metrics::gauge(std::string_view name, std::string_view help, const Labels& labels, double value) {
    const std::string name_s(name);
    const std::string help_s(help);
    const std::lock_guard lk(mu_);
    if (counters_.count(name_s)) throw std::logic_error("metric " + name_s + " is already registered as a counter");
    auto it = gauges_.find(name_s);
    if (it == gauges_.end()) {
        auto& family = prometheus::BuildGauge().Name(name_s).Help(help_s).Register(*registry_);
        it = gauges_.emplace(name_s, std::make_pair(&family, help_s)).first;
    } else if (it->second.second != help) {
        throw std::logic_error("metric " + name_s + " is already registered with a different help text");
    }
    it->second.first->Add(labels).Set(value);
}

std::string Metrics::serialize() const {
    const std::lock_guard lk(mu_);
    return prometheus::TextSerializer().Serialize(registry_->Collect());
}

}  // namespace evml::ops
