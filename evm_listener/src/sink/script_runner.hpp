#pragma once

#include <array>
#include <optional>
#include <string>

#include <sw/redis++/redis++.h>

#include "sink/lua_scripts.hpp"

namespace evml::sink {

// Loads the embedded scripts (SCRIPT LOAD) and runs ScriptCalls by EVALSHA. A NOSCRIPT reply (Redis restarted, or
// SCRIPT FLUSH) reloads every script and retries the call once; a second failure propagates. Every other Redis error
// propagates unchanged (redis++ maps OOM to sw::redis::OomError, other error replies to sw::redis::ReplyError).
//
// Not thread-safe: one runner per connection, used from one thread.
/// @brief Manages Redis Lua script SHA1 hashes and executes EVALSHA with automatic NOSCRIPT reload.
/// Guarantees:
/// - Fast EVALSHA path avoids sending script source on every call.
/// - Automatic NOSCRIPT recovery reloads all 4 scripts if Redis flushes its cache or restarts.
class ScriptRunner {
public:
    // Loads all scripts. Throws on connection failure or a script that Redis rejects (e.g. Redis < 7.0 and the shebang).
    /// @brief Loads all embedded Lua scripts via SCRIPT LOAD and caches their SHA1 digests.
    explicit ScriptRunner(sw::redis::Redis& redis);

    // The reply, or nullopt for a nil reply (lease acquire while a leader exists).
    /// @brief Executes script via EVALSHA; reloads all scripts once if Redis returns NOSCRIPT error.
    std::optional<std::string> run(const ScriptCall& call);

    // The SHA1 SCRIPT LOAD returned for `s`.
    /// @brief Returns cached SHA1 hash string for given script enum.
    const std::string& sha(Script s) const { return shas_[static_cast<std::size_t>(s)]; }

private:
    void load_all();
    std::optional<std::string> evalsha(const ScriptCall& call);

    sw::redis::Redis& redis_;
    std::array<std::string, kScriptCount> shas_;
};

}  // namespace evml::sink
