#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "core/hex.hpp"

namespace evml::chain {

enum class GateSignal : std::uint8_t {
    None,
    SafeRegression,    // the SAME provider's `safe` went backwards: possible L1 reorg (plan 3.3/3.6)
    SafeHashMismatch,  // the SAME provider returned a different hash for the same `safe` number
    FinalizedAnomaly,  // `finalized` changed hash at the same height: CRITICAL, release frozen
};
[[nodiscard]] constexpr const char* to_string(GateSignal s) noexcept {
    switch (s) {
        case GateSignal::None: return "none";
        case GateSignal::SafeRegression: return "safe_regression";
        case GateSignal::SafeHashMismatch: return "safe_hash_mismatch";
        case GateSignal::FinalizedAnomaly: return "finalized_anomaly";
    }
    return "unknown";
}

// Tracks the two finality gates G_s (safe) and G_f (finalized) from the tags of the CURRENT getLogs provider
// (plan 3.3). Pure: no I/O, no clock. Single-threaded.
//
// Regressions are judged per provider: after a getLogs failover the new provider may legitimately report lower
// tags; that is not an anomaly (the sweeps simply wait until the tag passes their cursors, checkpoints never
// regress). A `finalized` hash mismatch at the same block height freezes the gate permanently (restart required):
// conflicting finalized hashes indicate a provider fault or catastrophic L1 event, whereas a lower finalized
// block number from the same provider is ignored as stale load-balanced backend node jitter so G_f stays monotonic.
// Pure state machine tracking consensus finality gates G_s (safe) and G_f (finalized) across providers (plan 3.3).
// Evaluates tag regressions per-provider to distinguish benign failover gaps from genuine chain reorgs.
// Invariants enforced: Invariant I2 (G_f <= G_s), Invariant I5 (freeze pipeline permanently on FinalizedAnomaly).
class FinalityGate {
public:
    // Evaluates incoming safe block tag from the specified provider.
    // Watch out: Returns SafeRegression or SafeHashMismatch if the same provider regresses; signals reorg.
    [[nodiscard]] GateSignal on_safe(std::string_view provider, std::uint64_t number, const core::Hash32& hash);

    // Evaluates incoming finalized block tag from the specified provider.
    // Watch out: Ignores stale lower heights from load-balanced nodes; returns FinalizedAnomaly and permanently
    // freezes release if a finalized block changes hash at the same height.
    [[nodiscard]] GateSignal on_finalized(std::string_view provider, std::uint64_t number, const core::Hash32& hash);

    // Latest observed safe block height (nullopt until first tag received).
    [[nodiscard]] std::optional<std::uint64_t> safe() const noexcept { return latest_safe_; }

    // Latest observed finalized block height, clamped to safe() so that G_f <= G_s strictly holds (Invariant I2).
    // Watch out: Finalized gate never advances beyond safe gate even if RPC reports higher finalized tag.
    [[nodiscard]] std::optional<std::uint64_t> finalized() const noexcept;

    // True if pipeline release is permanently frozen due to a FinalizedAnomaly.
    [[nodiscard]] bool frozen() const noexcept { return frozen_; }
    [[nodiscard]] const std::string& frozen_reason() const noexcept { return frozen_reason_; }

private:
    struct Seen {
        std::uint64_t number{};
        core::Hash32 hash;
    };
    std::unordered_map<std::string, Seen> safe_by_provider_;
    std::unordered_map<std::string, Seen> finalized_by_provider_;
    std::optional<std::uint64_t> latest_safe_, latest_finalized_;
    bool frozen_{false};
    std::string frozen_reason_;
};

}  // namespace evml::chain
