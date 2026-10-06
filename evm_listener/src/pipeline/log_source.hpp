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

// What the sweeps need from the provider layer. The production adapter wraps ProviderPool::call (Phase 5 wiring);
// tests use a scripted fake. All calls are made from the io thread.
class LogSource {
public:
    virtual ~LogSource() = default;

    // eth_getLogs for the inclusive range with the given filter, over the getLogs provider chain.
    virtual boost::asio::awaitable<LogResult> get_logs(BlockRange range, const core::LogFilter& filter) = 0;

    // Canonical hash and timestamp of block `number` (nullopt: could not be read, counts as a retry).
    virtual boost::asio::awaitable<std::optional<BlockInfo>> block(std::uint64_t number) = 0;

    // The provider that the next get_logs will start on (sizes the first page); nullopt when none is usable.
    virtual std::optional<std::string> current_provider() const = 0;
};

}  // namespace evml::pipeline
