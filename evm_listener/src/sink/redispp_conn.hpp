#pragma once

#include <array>
#include <chrono>
#include <string>
#include <string_view>

#include <sw/redis++/redis++.h>

#include "sink/keys.hpp"
#include "sink/redis_conn.hpp"

namespace evml::sink {

// RedisConn on one dedicated redis++ connection (pool of 1; the sink thread is its only user).
//
// Error mapping (the seam's contract, plan 4.4):
//   transport failure / timeout / closed connection        -> ConnError (outcome unknown: the sink re-sends)
//   OOM, LOADING, BUSY error reply                         -> ConnError for reads, ScriptFail::Transient in a pipeline
//   NOSCRIPT error reply                                   -> ScriptFail::NoScript
//   any other error reply, or a protocol error             -> ScriptFail::Fatal / a thrown exception (never retried)
//
// Both timeouts default to 2 s: redis++'s own default is "wait forever", which would wedge the sink thread on a
// half-open TCP connection.
/// @brief Concrete RedisConn implementation wrapping a single redis++ connection with bounded timeouts.
/// Watch out: Connection timeouts are enforced to prevent half-open TCP hangs.
class RedisppConn final : public RedisConn {
public:
    /// @brief Constructs RedisppConn configured with URL and bounded I/O timeouts.
    RedisppConn(std::string_view url, const Keys& keys,
                std::chrono::milliseconds io_timeout = std::chrono::milliseconds(2000),
                std::string_view client_name = "");

    /// @brief Executes batch of ScriptCalls via EVALSHA on redis++ pipeline.
    [[nodiscard]] std::vector<CallOutcome> pipeline(const std::vector<ScriptCall>& calls) override;
    /// @brief Pre-loads embedded Lua scripts into Redis and records SHA1 digests.
    void load_scripts() override;
    /// @brief Reads checkpoint hash fields (safe, finalized, contracts) via HGETALL.
    [[nodiscard]] pipeline::Checkpoint read_checkpoint() override;
    /// @brief Queries released members by block range via ZRANGEBYSCORE and batches HMGET for before-images.
    [[nodiscard]] std::vector<pipeline::ReleasedEntry> read_released(std::uint64_t from, std::uint64_t to) override;
    /// @brief Sets key with TTL seconds using redis.set EX.
    void set_with_ttl(std::string_view key, std::string_view value, std::uint32_t ttl_s) override;
    /// @brief Queries stream consumer group statistics using XINFO, XRANGE, XPENDING, and TIME.
    [[nodiscard]] GroupFacts read_group_facts(std::string_view stream_key, std::string_view group) override;
    /// @brief Collects runtime tenancy facts (`INFO keyspace`, bounded `SCAN`, `CLIENT LIST`).
    [[nodiscard]] TenancyFacts read_tenancy_facts() override;

    [[nodiscard]] core::Result<std::vector<CallOutcome>, ConnError> try_pipeline(
        const std::vector<ScriptCall>& calls) override;
    [[nodiscard]] core::Result<void, ConnError> try_load_scripts() override;
    [[nodiscard]] core::Result<pipeline::Checkpoint, ConnError> try_read_checkpoint() override;
    [[nodiscard]] core::Result<std::vector<pipeline::ReleasedEntry>, ConnError> try_read_released(
        std::uint64_t from, std::uint64_t to) override;
    [[nodiscard]] core::Result<void, ConnError> try_set_with_ttl(std::string_view key, std::string_view value,
                                                                 std::uint32_t ttl_s) override;
    [[nodiscard]] core::Result<GroupFacts, ConnError> try_read_group_facts(std::string_view stream_key,
                                                                           std::string_view group) override;
    [[nodiscard]] core::Result<TenancyFacts, ConnError> try_read_tenancy_facts() override;

private:
    sw::redis::Redis redis_;
    Keys keys_;
    std::array<std::string, kScriptCount> shas_;
    bool loaded_{false};
};

}  // namespace evml::sink
