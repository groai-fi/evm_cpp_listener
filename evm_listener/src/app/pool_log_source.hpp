#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "chain/reorg_detector.hpp"
#include "pipeline/log_source.hpp"
#include "provider/provider_pool.hpp"
#include "rpc/codec.hpp"

namespace evml::app {

/// @brief Production pipeline::LogSource over a ProviderPool (plan 60 §5.4a).
/// Invariants:
/// - get_logs retries the next getLogs provider, never one already tried in the same call. RangeTooLarge returns at once.
/// - After report_bad(p), the NEXT get_logs / current_provider() steer away from p (the sweeper re-asks after a bad
///   page). If p is the only usable provider it is asked again, so the sweeper's max_bad_attempts ends in Stalled.
/// - block(n) is independent of the provider that returned the logs: it reads the head ring first (hit with a known
///   timestamp = no RPC), otherwise asks a provider other than the last logs provider. A hash that disagrees with the
///   ring books report_bad and returns nullopt (= Retry). No independent provider = nullopt, never the logs provider.
/// - Single-threaded: every call runs on the pool's io thread; `ring` is the same thread's ReorgDetector (may be null).
class PoolLogSource final : public pipeline::LogSource {
public:
    PoolLogSource(std::shared_ptr<provider::ProviderPool> pool, const chain::ReorgDetector* ring);

    boost::asio::awaitable<pipeline::LogResult> get_logs(pipeline::BlockRange range,
                                                         const core::LogFilter& filter) override;
    boost::asio::awaitable<std::optional<pipeline::BlockInfo>> block(std::uint64_t number) override;
    std::optional<std::string> current_provider() const override;
    void report_bad(const std::string& provider, const std::string& reason) override;
    void clear_cache() override;

    static constexpr std::size_t kMaxBlockCache = 4096;

private:
    void cache_block(std::uint64_t number, const pipeline::BlockInfo& info);

    std::shared_ptr<provider::ProviderPool> pool_;
    const chain::ReorgDetector* ring_;
    rpc::Decoder dec_;                      // used only synchronously (never across a co_await)
    std::unordered_set<std::string> avoid_;          // providers reported bad since the last get_logs
    std::unordered_set<std::string> logs_providers_; // every provider that answered get_logs for the current page
    std::optional<pipeline::BlockRange> last_logs_range_;
    std::unordered_map<std::uint64_t, pipeline::BlockInfo> block_cache_;
    std::deque<std::uint64_t> cache_order_;
};

}  // namespace evml::app
