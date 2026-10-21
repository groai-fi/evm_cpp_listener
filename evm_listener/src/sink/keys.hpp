#pragma once

#include <stdexcept>
#include <string>

#include "core/config.hpp"

namespace evml::sink {

// Redis key schema of one shard (plan 4.1). Every key shares the hash tag `{shard}`, so every Lua script is single-slot.
/// @brief Redis key naming generator scoped by shard hashtag {shard}.
/// Guarantees:
/// - Cluster Compatibility: Every key contains the `{shard}` hashtag, guaranteeing all shard keys hash to the same cluster slot for atomic Lua execution.
class Keys {
public:
    /// @brief Initializes key prefix using shard name.
    /// @throws std::invalid_argument If shard name is empty or contains curly braces.
    explicit Keys(const std::string& shard) : prefix_("evml:{" + shard + "}:") {
        if (shard.empty() || shard.find_first_of("{}") != std::string::npos)
            throw std::invalid_argument("Keys: shard id must be non-empty and contain no braces: '" + shard + "'");
    }

    /// @brief Primary event stream key: evml:{shard}:events.
    std::string events() const { return prefix_ + "events"; }
    /// @brief Registry event stream key: evml:{shard}:registry.
    std::string registry() const { return prefix_ + "registry"; }
    /// @brief Resolves stream key for given StreamKind enum.
    std::string stream(core::StreamKind k) const { return k == core::StreamKind::Events ? events() : registry(); }
    /// @brief Checkpoint hash key: evml:{shard}:ckpt.
    std::string ckpt() const { return prefix_ + "ckpt"; }
    /// @brief Released log entries sorted set key: evml:{shard}:released.
    std::string released() const { return prefix_ + "released"; }
    /// @brief Released log before-image hash key: evml:{shard}:released_data.
    std::string released_data() const { return prefix_ + "released_data"; }
    /// @brief Leader lease key: evml:{shard}:leader.
    std::string leader() const { return prefix_ + "leader"; }
    /// @brief Leader epoch counter key: evml:{shard}:epoch.
    std::string epoch() const { return prefix_ + "epoch"; }
    /// @brief Heartbeat key for given host: evml:{shard}:hb:<host_id>.
    std::string hb(const std::string& host_id) const { return prefix_ + "hb:" + host_id; }
    // `dedup_key` is the Emission's "dedup:<event_id>".
    /// @brief Per-event deduplication key: evml:{shard}:dedup:<event_id>.
    std::string dedup(const std::string& dedup_key) const { return prefix_ + dedup_key; }
    // Glob matching every key of the shard (tests use it to clean up their isolated shard).
    /// @brief Glob pattern matching all shard keys: evml:{shard}:*.
    std::string pattern() const { return prefix_ + "*"; }

private:
    std::string prefix_;
};

}  // namespace evml::sink
