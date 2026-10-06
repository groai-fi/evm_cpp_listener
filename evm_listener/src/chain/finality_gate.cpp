#include "chain/finality_gate.hpp"

#include <algorithm>

namespace evml::chain {

const char* to_string(GateSignal s) {
    switch (s) {
        case GateSignal::None: return "none";
        case GateSignal::SafeRegression: return "safe_regression";
        case GateSignal::SafeHashMismatch: return "safe_hash_mismatch";
        case GateSignal::FinalizedAnomaly: return "finalized_anomaly";
    }
    return "unknown";
}

GateSignal FinalityGate::on_safe(const std::string& provider, std::uint64_t number, const core::Hash32& hash) {
    GateSignal sig = GateSignal::None;
    const auto it = safe_by_provider_.find(provider);
    if (it != safe_by_provider_.end()) {
        if (number < it->second.number) {
            sig = GateSignal::SafeRegression;
        } else if (number == it->second.number && !(hash == it->second.hash)) {
            sig = GateSignal::SafeHashMismatch;
        }
    }
    safe_by_provider_[provider] = {number, hash};  // always track the provider's latest reality
    latest_safe_ = number;
    return sig;
}

GateSignal FinalityGate::on_finalized(const std::string& provider, std::uint64_t number, const core::Hash32& hash) {
    GateSignal sig = GateSignal::None;
    const auto it = finalized_by_provider_.find(provider);
    if (it != finalized_by_provider_.end()) {
        if (number < it->second.number) {
            sig = GateSignal::FinalizedAnomaly;
            frozen_reason_ = provider + ": finalized regressed " + std::to_string(it->second.number) + " -> " +
                             std::to_string(number);
        } else if (number == it->second.number && !(hash == it->second.hash)) {
            sig = GateSignal::FinalizedAnomaly;
            frozen_reason_ = provider + ": finalized block " + std::to_string(number) + " changed hash";
        }
    }
    if (sig == GateSignal::FinalizedAnomaly) frozen_ = true;
    finalized_by_provider_[provider] = {number, hash};
    latest_finalized_ = number;
    return sig;
}

std::optional<std::uint64_t> FinalityGate::finalized() const {
    if (!latest_finalized_) return std::nullopt;
    if (!latest_safe_) return latest_finalized_;
    return std::min(*latest_finalized_, *latest_safe_);
}

}  // namespace evml::chain
