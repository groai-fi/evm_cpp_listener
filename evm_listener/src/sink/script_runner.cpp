#include "sink/script_runner.hpp"

namespace evml::sink {

ScriptRunner::ScriptRunner(sw::redis::Redis& redis) : redis_(redis) { load_all(); }

void ScriptRunner::load_all() {
    for (std::size_t i = 0; i < kScriptCount; ++i)
        shas_[i] = redis_.script_load(script_source(static_cast<Script>(i)));
}

std::optional<std::string> ScriptRunner::evalsha(const ScriptCall& call) {
    // Invokes Redis EVALSHA passing cached SHA1 digest without re-transmitting Lua script text.
    return redis_.evalsha<sw::redis::OptionalString>(shas_[static_cast<std::size_t>(call.script)], call.keys.begin(),
                                                     call.keys.end(), call.args.begin(), call.args.end());
}

std::optional<std::string> ScriptRunner::run(const ScriptCall& call) {
    try {
        return evalsha(call);
    } catch (const sw::redis::ReplyError& e) {
        // Catch NOSCRIPT error when Redis flushed script cache or restarted; re-throw any other error.
        if (std::string(e.what()).rfind("NOSCRIPT", 0) != 0) throw;
    }
    // Reload all script sources via SCRIPT LOAD and retry EVALSHA execution once.
    load_all();
    return evalsha(call);
}

}  // namespace evml::sink
