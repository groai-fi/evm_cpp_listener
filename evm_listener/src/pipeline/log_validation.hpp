#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "core/config.hpp"
#include "rpc/codec.hpp"

namespace evml::pipeline {

// Validates an eth_getLogs response against the query bounds and configured routing table.
// Checks: removed==false, block in [from, to], valid topic0, tracked route, finality class match, no duplicates.
// Returns std::nullopt if sound, or error description of the first violation found.
// Watch out: Returning any error causes Sweeper to discard the entire batch and penalize the provider.
std::optional<std::string> validate_logs(const std::vector<rpc::Log>& logs, std::uint64_t from_block,
                                         std::uint64_t to_block, const core::Config& cfg,
                                         std::optional<core::Finality> only_class);

}  // namespace evml::pipeline
