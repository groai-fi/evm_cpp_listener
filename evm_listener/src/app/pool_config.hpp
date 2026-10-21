#pragma once

#include <string>

#include "core/config.hpp"
#include "provider/provider_pool.hpp"

namespace evml::app {

/// @brief Maps declarative application configuration to runtime ProviderPool topology.
///
/// Combines enabled WebSocket and HTTP endpoints (merging dual-role providers into unified endpoints),
/// constructs operator-defined failover orders, extracts composite log filters across all contracts,
/// and transfers watchdog timeouts, polling intervals, and health policies.
///
/// @param cfg Validated, immutable core configuration.
/// @param user_agent Evaluated User-Agent HTTP/WS header string with injected semver.
/// @return Fully populated PoolConfig ready for ProviderPool instantiation.
/// @throws std::invalid_argument If the resulting pool topology violates internal pool invariants.
provider::PoolConfig make_pool_config(const core::Config& cfg, const std::string& user_agent);

}  // namespace evml::app
