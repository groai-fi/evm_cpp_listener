#include "pipeline/log_validation.hpp"

#include <unordered_set>

namespace evml::pipeline {

// Audits each log in the response batch against security, routing, and uniqueness invariants.
std::optional<std::string> validate_logs(const std::vector<rpc::Log>& logs, std::uint64_t from_block,
                                         std::uint64_t to_block, const core::Config& cfg,
                                         std::optional<core::Finality> only_class) {
    std::unordered_set<std::string> seen;
    for (std::size_t i = 0; i < logs.size(); ++i) {
        const auto& l = logs[i];
        const auto where = [&](const std::string& what) {
            return "log #" + std::to_string(i) + " (block " + std::to_string(l.block_number) + ", index " +
                   std::to_string(l.log_index) + "): " + what;
        };
        // Invariant: eth_getLogs must never return logs marked removed:true
        if (l.removed) return where("removed=true in an eth_getLogs response");
        // Invariant I1: ensure provider returned only logs strictly within requested window
        if (l.block_number < from_block || l.block_number > to_block) {
            return where("outside requested range " + std::to_string(from_block) + ".." + std::to_string(to_block));
        }
        if (l.topics.empty()) return where("no topic0");
        // Verify contract address and topic0 exist in active dispatch routing table
        const auto* route = cfg.route(l.address, l.topics[0]);
        if (route == nullptr) return where("untracked (address, topic0) " + l.address.to_hex() + " " + l.topics[0].to_hex());
        // Verify log matches expected finality class (Safe vs Finalized)
        if (only_class && route->finality != *only_class) {
            return where(std::string("finality class ") + core::to_string(route->finality) + " not requested (" +
                         core::to_string(*only_class) + " only)");
        }
        // Invariant I4: check for duplicate (blockHash, logIndex) within the single batch
        if (!seen.insert(l.block_hash.to_hex() + ":" + std::to_string(l.log_index)).second) {
            return where("duplicate (blockHash, logIndex)");
        }
    }
    return std::nullopt; // All logs structurally valid and verified
}

}  // namespace evml::pipeline
