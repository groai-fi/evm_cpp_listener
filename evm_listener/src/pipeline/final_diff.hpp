#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "pipeline/sink.hpp"
#include "rpc/codec.hpp"

namespace evml::pipeline {

struct FinalDiff {
    std::vector<std::string> matched;       // released and still canonical: nothing to do
    std::vector<rpc::Log> late;             // in the sweep only: the safe sweep missed it (safe_missed_total)
    std::vector<ReleasedEntry> vanished;    // released only: no longer canonical -> revert (SafeReorg)
};

// Reconciles previously released safe logs against canonical finalized sweep logs (plan 3.5).
// Enforces Invariant I6 (Revert Fidelity): identifies vanished events requiring explicit retraction.
// Watch out: An event whose blockHash changed across a reorg appears as both vanished and late.
FinalDiff diff_released(std::uint64_t chain_id, const std::vector<ReleasedEntry>& released,
                        const std::vector<rpc::Log>& sweep_logs);

}  // namespace evml::pipeline
