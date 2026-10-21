#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

#include "core/hex.hpp"

namespace evml::chain {

enum class GateSignal {
    None,
    SafeRegression,    // the SAME provider's `safe` went backwards: possible L1 reorg (plan 3.3/3.6)
    SafeHashMismatch,  // the SAME provider returned a different hash for the same `safe` number
    FinalizedAnomaly,  // `finalized` regressed or changed hash for the same provider: CRITICAL, release frozen
};
const char* to_string(GateSignal s);

// Tracks the two finality gates G_s (safe) and G_f (finalized) from the tags of the CURRENT getLogs provider
// (plan 3.3). Pure: no I/O, no clock. Single-threaded.
//
// Regressions are judged per provider: after a getLogs failover the new provider may legitimately report lower
// tags; that is not an anomaly (the sweeps simply wait until the tag passes their cursors, checkpoints never
// regress). A `finalized` anomaly freezes the gate permanently (restart required): finalized data moving is a
// provider fault or a catastrophic L1 event, and the pipeline must stop releasing and alert.
// Pure state machine tracking consensus finality gates G_s (safe) and G_f (finalized) across providers (plan 3.3).
// Evaluates tag regressions per-provider to distinguish benign failover gaps from genuine chain reorgs.
// Invariants enforced: Invariant I2 (G_f <= G_s), Invariant I5 (freeze pipeline permanently on FinalizedAnomaly).
class FinalityGate {
public:
    // Evaluates incoming safe block tag from the specified provider.
    // Watch out: Returns SafeRegression or SafeHashMismatch if the same provider regresses; signals reorg.
    GateSignal on_safe(const std::string& provider, std::uint64_t number, const core::Hash32& hash);

    // Evaluates incoming finalized block tag from the specified provider.
    // Watch out: Returns FinalizedAnomaly and permanently freezes release if finalized height or hash regresses.
    GateSignal on_finalized(const std::string& provider, std::uint64_t number, const core::Hash32& hash);

    // Latest observed safe block height (nullopt until first tag received).
    std::optional<std::uint64_t> safe() const { return latest_safe_; }

    // Latest observed finalized block height, clamped to safe() so that G_f <= G_s strictly holds (Invariant I2).
    // Watch out: Finalized gate never advances beyond safe gate even if RPC reports higher finalized tag.
    std::optional<std::uint64_t> finalized() const;

    // True if pipeline release is permanently frozen due to a FinalizedAnomaly.
    bool frozen() const { return frozen_; }
    const std::string& frozen_reason() const { return frozen_reason_; }

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
