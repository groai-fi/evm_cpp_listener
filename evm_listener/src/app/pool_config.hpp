#pragma once

#include <string>

#include "core/config.hpp"
#include "provider/provider_pool.hpp"

namespace evml::app {

// Maps the validated Config onto the ProviderPool: enabled providers (a provider listed in both chains is one
// endpoint with both URLs), the two failover orders, the log filter of every tracked contract, and the redundancy,
// watchdog and polling settings. Throws std::invalid_argument (from the pool) only for an inconsistent result.
provider::PoolConfig make_pool_config(const core::Config& cfg, const std::string& user_agent);

}  // namespace evml::app
