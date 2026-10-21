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

// Inspects safe block tag updates; detects safe regressions and block hash mismatches from the same provider.
GateSignal FinalityGate::on_safe(const std::string& provider, std::uint64_t number, const core::Hash32& hash) {
    GateSignal sig = GateSignal::None;
    const auto it = safe_by_provider_.find(provider);
    if (it != safe_by_provider_.end()) {
        if (number < it->second.number) {
            // Safe block height regressed on same provider: potential L1 reorg or provider reorganization
            sig = GateSignal::SafeRegression;
        } else if (number == it->second.number && !(hash == it->second.hash)) {
            // Same safe block height returned different canonical hash: fork on safe tier
            sig = GateSignal::SafeHashMismatch;
        }
    }
    safe_by_provider_[provider] = {number, hash};  // always track the provider's latest reality
    latest_safe_ = number;
    return sig;
}

// Inspects finalized block tag updates; flags impossible regressions as fatal anomalies.
// Watch out: Permanently freezes the gate on regression or hash mismatch (requires operator investigation).
GateSignal FinalityGate::on_finalized(const std::string& provider, std::uint64_t number, const core::Hash32& hash) {
    GateSignal sig = GateSignal::None;
    const auto it = finalized_by_provider_.find(provider);
    if (it != finalized_by_provider_.end()) {
        if (number < it->second.number) {
            // Finalized block height regressed: catastrophic RPC bug or impossible L1 consensus failure
            sig = GateSignal::FinalizedAnomaly;
            frozen_reason_ = provider + ": finalized regressed " + std::to_string(it->second.number) + " -> " +
                             std::to_string(number);
        } else if (number == it->second.number && !(hash == it->second.hash)) {
            // Finalized block changed canonical hash: catastrophic violation of EVM finality
            sig = GateSignal::FinalizedAnomaly;
            frozen_reason_ = provider + ": finalized block " + std::to_string(number) + " changed hash";
        }
    }
    if (sig == GateSignal::FinalizedAnomaly) frozen_ = true; // Invariant I5: freeze release permanently
    finalized_by_provider_[provider] = {number, hash};
    latest_finalized_ = number;
    return sig;
}

// Invariant I2 (Dual Finality): returns latest finalized tag clamped to safe tag (G_f <= G_s).
std::optional<std::uint64_t> FinalityGate::finalized() const {
    if (!latest_finalized_) return std::nullopt;
    if (!latest_safe_) return latest_finalized_;
    return std::min(*latest_finalized_, *latest_safe_); // Clamp G_f to G_s
}

}  // namespace evml::chain
