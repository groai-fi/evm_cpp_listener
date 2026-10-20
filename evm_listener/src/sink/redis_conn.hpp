#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

#include "pipeline/sink.hpp"
#include "sink/lua_scripts.hpp"

namespace evml::sink {

// The transport failed (connect refused, timeout, closed, or the server is temporarily unable to serve: OOM, LOADING,
// BUSY). Whatever was in flight has an UNKNOWN outcome: it may or may not have landed. The sink re-sends it (dedup in
// emit.lua makes that safe) once the connection is back (plan 4.4, P1).
struct ConnError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// What Redis answered for one script call of a pipeline.
struct ScriptOk {
    std::optional<std::string> value;  // nullopt = nil reply
};
enum class FailKind {
    NoScript,   // the script cache was flushed (Redis restart, SCRIPT FLUSH): reload and re-send
    Transient,  // the server refused to run it now (OOM before execution, LOADING, BUSY): nothing was written; retry later
    Fatal,      // anything else (script error reply: an invariant violation, a bug): never retried
};
struct ScriptFail {
    FailKind kind{FailKind::Fatal};
    std::string message;  // the server's error text, for the log
};
using CallOutcome = std::variant<ScriptOk, ScriptFail>;

// One Redis connection as the sink uses it (plan 4.4 "transport seam"). Production: RedisppConn. Tests: a fake that
// injects errors and timeouts at chosen positions of a batch, so the P1 / A guarantees are proven without a Redis.
// The sink owns two instances: one for data, one for the lease (so a slow batch never starves a renewal, P4).
//
// Not thread-safe: each instance is used from the sink thread only.
class RedisConn {
public:
    virtual ~RedisConn() = default;

    // Sends every call in one pipeline and returns exactly one outcome per call, in order. Calls after a failed one
    // may still have run (Redis keeps executing a pipeline), so the caller treats the first non-success and everything
    // after it as unacked. Throws ConnError when the transport failed: then no outcome is known.
    virtual std::vector<CallOutcome> pipeline(const std::vector<ScriptCall>& calls) = 0;

    // (Re)loads every embedded script. Throws ConnError.
    virtual void load_scripts() = 0;

    // HGETALL of the checkpoint hash; every field empty on a fresh store. Throws ConnError on transport failure, and
    // core::DecodeError / std::runtime_error on stored data that violates the schema (never defaulted).
    virtual pipeline::Checkpoint read_checkpoint() = 0;

    // Released members with score in [from, to], with their before images. A member without its before image is a
    // contract violation (std::runtime_error), never skipped. Throws ConnError on transport failure.
    virtual std::vector<pipeline::ReleasedEntry> read_released(std::uint64_t from, std::uint64_t to) = 0;
};

}  // namespace evml::sink
