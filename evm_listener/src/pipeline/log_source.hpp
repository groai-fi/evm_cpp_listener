#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/awaitable.hpp>

#include "core/config.hpp"
#include "core/hex.hpp"
#include "pipeline/aimd_pager.hpp"
#include "rpc/classify.hpp"
#include "rpc/codec.hpp"

namespace evml::pipeline {

struct LogResult {
    bool ok{false};
    std::vector<rpc::Log> logs;                 // when ok
    rpc::FailKind kind{rpc::FailKind::Retry};   // when !ok
    std::optional<std::uint32_t> range_limit;   // RangeTooLarge: the provider's stated limit, if any
    std::string provider;                       // who answered (or failed last)
    std::string error;                          // no secrets
};

struct BlockInfo {
    core::Hash32 hash;
    std::uint64_t timestamp{};
};

// Abstract provider interface decoupling pipeline sweeps from JSON-RPC transport and failover logic.
// All invocations execute sequentially within the Asio io thread.
class LogSource {
public:
    virtual ~LogSource() = default;

    // Queries logs across the inclusive block range with configured filter over the provider failover chain.
    // Watch out: Returns rpc::FailKind::RangeTooLarge if provider query limits are exceeded.
    virtual boost::asio::awaitable<LogResult> get_logs(BlockRange range, const core::LogFilter& filter) = 0;

    // Fetches canonical header hash and timestamp for block `number` to verify log canonicality.
    // Watch out: Returns nullopt on read timeout or transport drop; treated as transient retry.
    virtual boost::asio::awaitable<std::optional<BlockInfo>> block(std::uint64_t number) = 0;

    // Returns active getLogs provider name; used to look up provider-specific AIMD pager bounds.
    // Watch out: Returns nullopt when all providers in the chain are in cooldown or down.
    virtual std::optional<std::string> current_provider() const = 0;

    // Reports that `provider` returned data failing validation or blockHash checks (rev 5, G2).
    // Penalizes provider health score and steers subsequent queries to failover fallback.
    virtual void report_bad(const std::string& provider, const std::string& reason) = 0;

    // Clears any cached verified block headers (called on reorg signals or safe-window re-verification).
    virtual void clear_cache() {}
};

}  // namespace evml::pipeline
