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

// Final-sweep diff for safe-class logs (plan 3.5): `released` are the members tracked for the swept range,
// `sweep_logs` the safe-class logs the finalized sweep returned for the same range. Identity is the event_id
// (chain:blockHash:logIndex), so a log that moved to another block hash shows up as vanished + late.
// `late` keeps the sweep order; `vanished` is ordered by (block, event_id).
FinalDiff diff_released(std::uint64_t chain_id, const std::vector<ReleasedEntry>& released,
                        const std::vector<rpc::Log>& sweep_logs);

}  // namespace evml::pipeline
