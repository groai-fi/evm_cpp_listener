#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "core/error.hpp"
#include "pipeline/sink.hpp"
#include "sink/consumer_lag.hpp"
#include "sink/lua_scripts.hpp"
#include "sink/redis_startup.hpp"

namespace evml::sink {

// The transport failed (connect refused, timeout, closed, or the server is temporarily unable to serve: OOM, LOADING,
// BUSY). Whatever was in flight has an UNKNOWN outcome: it may or may not have landed. The sink re-sends it (dedup in
// emit.lua makes that safe) once the connection is back (plan 4.4, P1).
/// @brief Transport-level failure exception (timeout, disconnect, connection refused, or server busy).
struct ConnError : std::runtime_error {
    ConnError() : std::runtime_error("") {}
    using std::runtime_error::runtime_error;
};

// What Redis answered for one script call of a pipeline.
/// @brief Successful outcome of a Redis Lua script execution.
struct ScriptOk {
    std::optional<std::string> value;  // nullopt = nil reply
};

/// @brief Classification of script execution failures for error handling.
enum class FailKind : std::uint8_t {
    NoScript,   // the script cache was flushed (Redis restart, SCRIPT FLUSH): reload and re-send
    Transient,  // the server refused to run it now (OOM before execution, LOADING, BUSY): nothing was written; retry later
    Fatal,      // anything else (script error reply: an invariant violation, a bug): never retried
};

/// @brief Script error details with failure category and server message.
struct ScriptFail {
    FailKind kind{FailKind::Fatal};
    std::string message;  // the server's error text, for the log
};
using CallOutcome = std::variant<ScriptOk, ScriptFail>;

// One Redis connection as the sink uses it (plan 4.4 "transport seam"). Production: RedisppConn. Tests: a fake that
// injects errors and timeouts at chosen positions of a pipeline, so the P1 / A guarantees are proven without a Redis.
// The sink owns two instances: one for data, one for the lease (so a slow batch never starves a renewal, P4).
//
// Not thread-safe: each instance is used from the sink thread only.
/// @brief Abstract transport seam interface decoupling Redis communications from protocol drivers and unit tests.
class RedisConn {
public:
    RedisConn() = default;
    virtual ~RedisConn() = default;
    RedisConn(const RedisConn&) = delete;
    RedisConn& operator=(const RedisConn&) = delete;
    RedisConn(RedisConn&&) = delete;
    RedisConn& operator=(RedisConn&&) = delete;

    // Sends every call in one pipeline and returns exactly one outcome per call, in order. Calls after a failed one
    // may still have run (Redis keeps executing a pipeline), so the caller treats the first non-success and everything
    // after it as unacked. Throws ConnError when the transport failed: then no outcome is known.
    /// @brief Sends sequence of ScriptCall commands in a single pipelined network round-trip.
    /// @throws ConnError If transport fails before all responses are received.
    [[nodiscard]] virtual std::vector<CallOutcome> pipeline(const std::vector<ScriptCall>& calls) = 0;

    // (Re)loads every embedded script. Throws ConnError.
    /// @brief Loads all embedded Lua scripts via SCRIPT LOAD and caches SHA1 digests.
    virtual void load_scripts() = 0;

    // HGETALL of the checkpoint hash; every field empty on a fresh store. Throws ConnError on transport failure, and
    // core::DecodeError / std::runtime_error on stored data that violates the schema (never defaulted).
    /// @brief Reads current safe and finalized block checkpoints from Redis hash.
    [[nodiscard]] virtual pipeline::Checkpoint read_checkpoint() = 0;

    // Released members with score in [from, to], with their before images. A member without its before image is a
    // contract violation (std::runtime_error), never skipped. Throws ConnError on transport failure.
    /// @brief Reads released log members and before-images within block range [from, to].
    [[nodiscard]] virtual std::vector<pipeline::ReleasedEntry> read_released(std::uint64_t from, std::uint64_t to) = 0;

    // SET key value EX ttl_s (the heartbeat, plan 4.7). Throws ConnError on transport failure.
    /// @brief Writes key-value pair with expiration TTL (used for host heartbeats).
    virtual void set_with_ttl(std::string_view key, std::string_view value, std::uint32_t ttl_s) = 0;

    // Read-only facts about one consumer group of a stream (plan 4.10; XINFO / XRANGE / XPENDING / TIME). A missing
    // stream or group is reported in the result, not thrown. Throws ConnError on transport failure.
    /// @brief Collects consumer group metrics from Redis stream introspection commands.
    [[nodiscard]] virtual GroupFacts read_group_facts(std::string_view stream_key, std::string_view group) = 0;

    /// @brief Collects dedicated-Redis runtime tenancy facts (`INFO keyspace`, bounded `SCAN`, `CLIENT LIST`).
    [[nodiscard]] virtual TenancyFacts read_tenancy_facts() { return {}; }

    [[nodiscard]] virtual core::Result<std::vector<CallOutcome>, ConnError> try_pipeline(
        const std::vector<ScriptCall>& calls) {
        try {
            return pipeline(calls);
        } catch (const ConnError& e) {
            return e;
        }
    }

    [[nodiscard]] virtual core::Result<void, ConnError> try_load_scripts() {
        try {
            load_scripts();
            return {};
        } catch (const ConnError& e) {
            return e;
        }
    }

    [[nodiscard]] virtual core::Result<pipeline::Checkpoint, ConnError> try_read_checkpoint() {
        try {
            return read_checkpoint();
        } catch (const ConnError& e) {
            return e;
        }
    }

    [[nodiscard]] virtual core::Result<std::vector<pipeline::ReleasedEntry>, ConnError> try_read_released(
        std::uint64_t from, std::uint64_t to) {
        try {
            return read_released(from, to);
        } catch (const ConnError& e) {
            return e;
        }
    }

    [[nodiscard]] virtual core::Result<void, ConnError> try_set_with_ttl(std::string_view key, std::string_view value,
                                                                         std::uint32_t ttl_s) {
        try {
            set_with_ttl(key, value, ttl_s);
            return {};
        } catch (const ConnError& e) {
            return e;
        }
    }

    [[nodiscard]] virtual core::Result<GroupFacts, ConnError> try_read_group_facts(std::string_view stream_key,
                                                                                   std::string_view group) {
        try {
            return read_group_facts(stream_key, group);
        } catch (const ConnError& e) {
            return e;
        }
    }

    [[nodiscard]] virtual core::Result<TenancyFacts, ConnError> try_read_tenancy_facts() {
        try {
            return read_tenancy_facts();
        } catch (const ConnError& e) {
            return e;
        }
    }
};

}  // namespace evml::sink
