#pragma once

#include <stdexcept>
#include <string>
#include <string_view>

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
    explicit Keys(std::string_view shard) : prefix_("evml:{" + std::string(shard) + "}:") {
        if (shard.empty() || shard.find_first_of("{}") != std::string_view::npos)
            throw std::invalid_argument("Keys: shard id must be non-empty and contain no braces: '" +
                                        std::string(shard) + "'");
    }

    /// @brief Primary event stream key: evml:{shard}:events.
    [[nodiscard]] std::string events() const { return prefix_ + "events"; }
    /// @brief Registry event stream key: evml:{shard}:registry.
    [[nodiscard]] std::string registry() const { return prefix_ + "registry"; }
    /// @brief Low-latency WebSocket tip signal stream key: evml:{shard}:fast.
    [[nodiscard]] std::string fast() const { return prefix_ + "fast"; }
    /// @brief Resolves stream key for given StreamKind enum.
    [[nodiscard]] std::string stream(core::StreamKind k) const {
        return k == core::StreamKind::Events ? events() : registry();
    }
    /// @brief Checkpoint hash key: evml:{shard}:ckpt.
    [[nodiscard]] std::string ckpt() const { return prefix_ + "ckpt"; }
    /// @brief Released log entries sorted set key: evml:{shard}:released.
    [[nodiscard]] std::string released() const { return prefix_ + "released"; }
    /// @brief Released log before-image hash key: evml:{shard}:released_data.
    [[nodiscard]] std::string released_data() const { return prefix_ + "released_data"; }
    /// @brief Leader lease key: evml:{shard}:leader.
    [[nodiscard]] std::string leader() const { return prefix_ + "leader"; }
    /// @brief Leader epoch counter key: evml:{shard}:epoch.
    [[nodiscard]] std::string epoch() const { return prefix_ + "epoch"; }
    /// @brief Heartbeat key for given host: evml:{shard}:hb:<host_id>.
    [[nodiscard]] std::string hb(std::string_view host_id) const { return prefix_ + "hb:" + std::string(host_id); }
    /// @brief Dedicated-Redis tenant marker key: evml:{shard}:tenant.
    [[nodiscard]] std::string tenant() const { return prefix_ + "tenant"; }
    // `dedup_key` is the Emission's "dedup:<event_id>".
    /// @brief Per-event deduplication key: evml:{shard}:dedup:<event_id>.
    [[nodiscard]] std::string dedup(std::string_view dedup_key) const { return prefix_ + std::string(dedup_key); }
    // Glob matching every key of the shard (tests use it to clean up their isolated shard).
    /// @brief Glob pattern matching all shard keys: evml:{shard}:*.
    [[nodiscard]] std::string pattern() const { return prefix_ + "*"; }

private:
    std::string prefix_;
};

}  // namespace evml::sink
