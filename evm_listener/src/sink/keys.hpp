#pragma once

#include <stdexcept>
#include <string>

#include "core/config.hpp"

namespace evml::sink {

// Redis key schema of one shard (plan 4.1). Every key shares the hash tag `{shard}`, so every Lua script is single-slot.
class Keys {
public:
    explicit Keys(const std::string& shard) : prefix_("evml:{" + shard + "}:") {
        if (shard.empty() || shard.find_first_of("{}") != std::string::npos)
            throw std::invalid_argument("Keys: shard id must be non-empty and contain no braces: '" + shard + "'");
    }

    std::string events() const { return prefix_ + "events"; }
    std::string registry() const { return prefix_ + "registry"; }
    std::string stream(core::StreamKind k) const { return k == core::StreamKind::Events ? events() : registry(); }
    std::string ckpt() const { return prefix_ + "ckpt"; }
    std::string released() const { return prefix_ + "released"; }
    std::string released_data() const { return prefix_ + "released_data"; }
    std::string leader() const { return prefix_ + "leader"; }
    std::string epoch() const { return prefix_ + "epoch"; }
    std::string hb(const std::string& host_id) const { return prefix_ + "hb:" + host_id; }
    // `dedup_key` is the Emission's "dedup:<event_id>".
    std::string dedup(const std::string& dedup_key) const { return prefix_ + dedup_key; }
    // Glob matching every key of the shard (tests use it to clean up their isolated shard).
    std::string pattern() const { return prefix_ + "*"; }

private:
    std::string prefix_;
};

}  // namespace evml::sink
