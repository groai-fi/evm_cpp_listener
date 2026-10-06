#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "core/config.hpp"
#include "rpc/codec.hpp"

namespace evml::pipeline {

// Validates an eth_getLogs response (plan 3.5) against the request. Returns nullopt when sound, else a precise
// description of the FIRST violation (the caller discards the whole response and retries on another provider).
// Checks, per log: removed == false; fromBlock <= block <= toBlock; has topic0; (address, topic0) is tracked;
// belongs to `only_class` when given (the safe sweep requests safe-class addresses only); and no duplicate
// (blockHash, logIndex) inside the response.
std::optional<std::string> validate_logs(const std::vector<rpc::Log>& logs, std::uint64_t from_block,
                                         std::uint64_t to_block, const core::Config& cfg,
                                         std::optional<core::Finality> only_class);

}  // namespace evml::pipeline
