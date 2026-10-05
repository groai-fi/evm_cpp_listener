#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include "core/config.hpp"
#include "core/hex.hpp"

namespace evml::app {

// Process exit codes: 0 ok, 1 runtime/check failure, 2 usage or configuration error.
inline constexpr int kExitOk = 0;
inline constexpr int kExitFailure = 1;
inline constexpr int kExitUsage = 2;

// Offline summary: shard, chain, enabled providers (redacted), notes, contracts, topic0 table, route count.
void print_config_summary(const core::Config& cfg);

// Live checks against the enabled getLogs providers (eth_chainId == cfg.chain.chain_id; every contract has code
// at `latest`). A chain-id mismatch is fatal; other provider failures are warnings as long as one provider works.
// Returns kExitOk or kExitFailure.
int check_config_online(const core::Config& cfg);

// Lowest block at which `address` has code, by binary search over eth_getCode with provider failover.
// Throws std::runtime_error if no provider can answer or the address has no code at `latest`.
std::uint64_t find_deploy_block(const core::Config& cfg, const core::Address20& address);

// Entry point used by main(): parses argv, loads .env, dispatches. Returns the process exit code.
int run_cli(int argc, char** argv);

}  // namespace evml::app
