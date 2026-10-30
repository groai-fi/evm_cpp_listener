#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "pipeline/sink.hpp"
#include "sink/keys.hpp"

namespace evml::sink {

// The embedded Lua scripts (plan 4.2). Each starts with the `#!lua` shebang and no allow-oom flag, so Redis >= 7.0
// refuses the whole script before it runs when over maxmemory.
/// @brief Identifiers for embedded Redis Lua scripts executing atomically.
/// Invariants:
/// - Invariant I3 (Monotonic Ckpt): Ckpt script guarantees stored checkpoint only increases.
/// - Invariant I4 (Deduplication): Emit script checks dedup key, writes stream with XADD MINID, then sets dedup key.
/// - Invariant I6 (Revert Fidelity): Releases store before-image for revert emission matching original log stream and fields.
enum class Script { Emit, Ckpt, FinalTrim, Lease, EmitFast };
inline constexpr std::size_t kScriptCount = 5;

const char* script_name(Script s);
const char* script_source(Script s);

// One EVALSHA to issue. Replies are always a string or nil (every script converts numbers with tostring).
/// @brief Parameter bundle passed to Redis EVALSHA containing script identity, KEYS, and ARGV.
struct ScriptCall {
    Script script{};
    std::vector<std::string> keys;
    std::vector<std::string> args;
};

// Fence token that disables the leader check (`[lease] enabled=false`, local only; plan 4.8).
inline constexpr const char* kNoFence = "0";

/// @brief Runtime parameters supplied to emit.lua.
struct EmitParams {
    std::uint32_t dedup_ttl_s{};
    std::uint64_t minid_ms{};  // XADD MINID ~ (now - retention), from the listener clock
    std::string token;         // lease token, or kNoFence
};

// emit.lua replies: a stream id (written), "DUP", "FENCED" or "BELOW_FINALIZED".
/// @brief Builds ScriptCall for emit.lua (atomic dedup, stream append, before-image caching, and fencing check).
ScriptCall emit_call(const Keys& k, const pipeline::Emission& e, const EmitParams& p);

// emit_fast.lua replies: a stream id (written) or "FENCED". Writes zero dedup/released keys.
/// @brief Builds ScriptCall for emit_fast.lua (fenced XADD MINID ~ on evml:{shard}:fast with zero dedup keys).
ScriptCall emit_fast_call(const Keys& k, const std::vector<pipeline::Field>& fields, std::uint64_t minid_ms,
                          const std::string& token);
ScriptCall emit_fast_call(const Keys& k, const pipeline::Event& e, std::uint64_t minid_ms, const std::string& token);

// ckpt.lua replies: "OK", "NOOP" (advance that would move backwards), "EXISTS" (init), "FENCED".
// An advance that would make finalized > safe is an error reply (I2: a bug, never retried silently).
/// @brief Builds ScriptCall to advance safe or finalized checkpoint monotonically.
ScriptCall ckpt_advance_call(const Keys& k, const std::string& token, pipeline::CheckpointKind kind, std::uint64_t block);
/// @brief Builds ScriptCall to initialize checkpoint hash when starting a fresh pipeline.
ScriptCall ckpt_init_call(const Keys& k, const std::string& token, std::uint64_t c0, const std::vector<std::string>& manifest);
/// @brief Builds ScriptCall to update tracked contract addresses manifest in checkpoint hash.
ScriptCall ckpt_contracts_call(const Keys& k, const std::string& token, const std::vector<std::string>& manifest);

// final_trim.lua replies: the number of trimmed members as a decimal string, or "FENCED".
/// @brief Builds ScriptCall to trim released zset members strictly below finalized block.
ScriptCall final_trim_call(const Keys& k, const std::string& token, std::uint64_t g_f);

// lease.lua replies. acquire: "<token>\n<prev_epoch>", or nil when a leader exists. renew: "OK", "RESUMED" or "FENCED".
// release: "OK" or "NOOP".
/// @brief Builds ScriptCall for leader lease acquisition.
ScriptCall lease_acquire_call(const Keys& k, const std::string& host, const std::string& nonce, std::uint32_t ttl_ms);
/// @brief Builds ScriptCall for periodic leader lease renewal.
ScriptCall lease_renew_call(const Keys& k, const std::string& token, std::uint32_t ttl_ms);
/// @brief Builds ScriptCall to voluntarily release leader lease on graceful shutdown.
ScriptCall lease_release_call(const Keys& k, const std::string& token);

// JSON codecs for the values stored in Redis. Malformed input throws core::DecodeError.
/// @brief Serializes log fields to JSON string: [["field1","val1"],["field2","val2"]].
std::string pack_fields(const std::vector<pipeline::Field>& fields);   // [[k,v],...]
/// @brief Deserializes log fields from JSON string; throws core::DecodeError on parse failure.
std::vector<pipeline::Field> unpack_fields(std::string_view json);
/// @brief Serializes contract address manifest array to JSON string.
std::string pack_manifest(const std::vector<std::string>& manifest);   // ckpt.contracts value: ["addr:start:class",...]
/// @brief Deserializes contract address manifest array from JSON string.
std::vector<std::string> unpack_manifest(std::string_view json);

// released_data value (the before image of a tracked emission): {"stream":"events","fields":[[k,v],...]}. The stream is
// part of it because it is not derivable from the fields, and a revert must go to the stream of the log it retracts.
/// @brief Serializes before-image of released log for revert reconstruction.
std::string pack_released(core::StreamKind stream, const std::vector<pipeline::Field>& fields);
/// @brief Unpacked before-image structure containing destination stream and original fields.
struct ReleasedImage {
    core::StreamKind stream{core::StreamKind::Events};
    std::vector<pipeline::Field> fields;
};
/// @brief Deserializes before-image JSON; throws core::DecodeError if structure is invalid.
ReleasedImage unpack_released(std::string_view json);

}  // namespace evml::sink
