#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include "core/config.hpp"
#include "core/hex.hpp"

namespace evml::app {

/// Process exit codes returned to the calling shell.
inline constexpr int kExitOk = 0;       ///< Success (clean execution, all checks passed).
inline constexpr int kExitFailure = 1;  ///< Runtime error, probe failure, or online check failure.
inline constexpr int kExitUsage = 2;    ///< Command-line syntax error or invalid configuration.

/// @brief Returns `[ops].http_user_agent` with the `{version}` placeholder replaced by the compiled semver.
std::string user_agent(const core::Config& cfg);

/// @brief Prints an offline structural summary of the loaded configuration to stdout.
///
/// Outputs shard ID, chain specs, enabled WS and getLogs providers (with secrets redacted),
/// configuration warnings/notes, registered contracts with event topics, and total route count.
/// Never attempts network communication.
///
/// @param cfg Validated core configuration.
void print_config_summary(const core::Config& cfg);

/// @brief Performs pre-flight live validation against configured RPC providers.
///
/// 1. Queries `eth_chainId` on each provider in the getLogs chain. Mismatch with `cfg.chain.chain_id`
///    is considered fatal.
/// 2. Queries `eth_getCode` at `latest` for all registered contracts. Contracts with no bytecode
///    fail pre-flight.
///
/// @param cfg Validated core configuration.
/// @return kExitOk if all contracts exist and at least one provider verified the chain ID; otherwise kExitFailure.
int check_config_online(const core::Config& cfg);

/// @brief Discovers the exact deployment block of a contract via binary search over `eth_getCode`.
///
/// Searches the range [0, head] using $\mathcal{O}(\log N)$ RPC calls across the getLogs failover chain.
///
/// @param cfg Validated core configuration.
/// @param address Contract address to inspect.
/// @return Lowest block number where bytecode exists at the target address.
/// @throws std::runtime_error If all RPC providers fail or the contract has no code at head.
std::uint64_t find_deploy_block(const core::Config& cfg, const core::Address20& address);

/// @brief Runs a live, end-to-end transport probe using real WebSocket and HTTP connections.
///
/// Streams new heads and logs via the ProviderPool for `seconds`, then executes a verification
/// `eth_getLogs` call across the failover chain and polls both `safe` and `finalized` block tags.
/// Redacts all URLs from stdout (prints provider names only).
///
/// @param cfg Validated core configuration.
/// @param seconds Duration in seconds to run the live probe.
/// @return kExitOk if heads were streamed and all verification queries succeeded; otherwise kExitFailure.
int probe_transport(const core::Config& cfg, unsigned seconds);

/// @brief Primary command-line interface entry point invoked from `main()`.
///
/// Dynamically resolves `.env` files, parses command-line arguments, loads TOML configuration,
/// dispatches to the requested sub-command, and catches top-level exceptions.
///
/// @param argc Argument count.
/// @param argv Argument vector.
/// @return Process exit code (kExitOk, kExitFailure, or kExitUsage).
int run_cli(int argc, char** argv);

}  // namespace evml::app
