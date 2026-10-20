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
enum class Script { Emit, Ckpt, FinalTrim, Lease };
inline constexpr std::size_t kScriptCount = 4;

const char* script_name(Script s);
const char* script_source(Script s);

// One EVALSHA to issue. Replies are always a string or nil (every script converts numbers with tostring).
struct ScriptCall {
    Script script{};
    std::vector<std::string> keys;
    std::vector<std::string> args;
};

// Fence token that disables the leader check (`[lease] enabled=false`, local only; plan 4.8).
inline constexpr const char* kNoFence = "0";

struct EmitParams {
    std::uint32_t dedup_ttl_s{};
    std::uint64_t minid_ms{};  // XADD MINID ~ (now - retention), from the listener clock
    std::string token;         // lease token, or kNoFence
};

// emit.lua replies: a stream id (written), "DUP", "FENCED" or "BELOW_FINALIZED".
ScriptCall emit_call(const Keys& k, const pipeline::Emission& e, const EmitParams& p);

// ckpt.lua replies: "OK", "NOOP" (advance that would move backwards), "EXISTS" (init), "FENCED".
// An advance that would make finalized > safe is an error reply (I2: a bug, never retried silently).
ScriptCall ckpt_advance_call(const Keys& k, const std::string& token, pipeline::CheckpointKind kind, std::uint64_t block);
ScriptCall ckpt_init_call(const Keys& k, const std::string& token, std::uint64_t c0, const std::vector<std::string>& manifest);
ScriptCall ckpt_contracts_call(const Keys& k, const std::string& token, const std::vector<std::string>& manifest);

// final_trim.lua replies: the number of trimmed members as a decimal string, or "FENCED".
ScriptCall final_trim_call(const Keys& k, const std::string& token, std::uint64_t g_f);

// lease.lua replies. acquire: the new token, or nil when a leader exists. renew: "OK", "RESUMED" or "FENCED".
// release: "OK" or "NOOP".
ScriptCall lease_acquire_call(const Keys& k, const std::string& host, const std::string& nonce, std::uint32_t ttl_ms);
ScriptCall lease_renew_call(const Keys& k, const std::string& token, std::uint32_t ttl_ms);
ScriptCall lease_release_call(const Keys& k, const std::string& token);

// JSON codecs for the values stored in Redis. Malformed input throws core::DecodeError.
std::string pack_fields(const std::vector<pipeline::Field>& fields);   // [[k,v],...]
std::vector<pipeline::Field> unpack_fields(std::string_view json);
std::string pack_manifest(const std::vector<std::string>& manifest);   // ckpt.contracts value: ["addr:start:class",...]
std::vector<std::string> unpack_manifest(std::string_view json);

// released_data value (the before image of a tracked emission): {"stream":"events","fields":[[k,v],...]}. The stream is
// part of it because it is not derivable from the fields, and a revert must go to the stream of the log it retracts.
std::string pack_released(core::StreamKind stream, const std::vector<pipeline::Field>& fields);
struct ReleasedImage {
    core::StreamKind stream{core::StreamKind::Events};
    std::vector<pipeline::Field> fields;
};
ReleasedImage unpack_released(std::string_view json);

}  // namespace evml::sink
