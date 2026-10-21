#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "core/hex.hpp"

namespace evml::core {

/// @brief Finality level classification for chain events and sweeps.
enum class Finality {
    Safe,       // Subject to safe reorgs; emitted to safe stream and tracked in pending buffer
    Finalized,  // Irreversible canonical events; audited against safe emissions
    Depth       // Block depth confirmation (N blocks behind chain head)
};

/// @brief Destination Redis stream classification.
enum class StreamKind {
    Events,    // Fast-path operational and state events stream
    Registry   // Registry / contract lifecycle stream
};

/// @brief Sweep starting block policy on initial uncheckpointed deployment.
enum class StartPolicy {
    ContractStart,  // Sweeps from minimum contract start_block configured in TOML
    Gate            // Sweeps from active finality gate position (no deep historical backfill)
};

/// @brief Converts Finality enum to lowercase string.
const char* to_string(Finality f);
/// @brief Converts StreamKind enum to lowercase string.
const char* to_string(StreamKind s);

/// @brief Shard identification parameters determining Redis key hash tagging.
struct ShardCfg {
    std::string id;       // [a-z0-9_]+ : used inside the Redis hash tag {id} for multi-key atomicity
    std::string host_id;  // Host identifier from TOML, $EVML_HOST_ID, or machine hostname
};

/// @brief EVM chain parameters and poll frequencies.
struct ChainCfg {
    std::string name;                                        // Chain name (e.g. "base", "ethereum", "arbitrum")
    std::uint64_t chain_id{};                                // Canonical EVM chain ID (e.g. 8453 for Base)
    std::uint32_t block_time_ms{};                           // Average block production time
    Finality default_finality{Finality::Finalized};          // Default finality class for contracts omitting it
    std::uint32_t depth{};                                   // Block depth requirement when finality == Depth
    std::uint32_t hash_ring_size{};                          // Ring buffer size for block hash history
    StartPolicy start_policy{StartPolicy::ContractStart};    // Cold startup block positioning policy
    std::uint32_t safe_poll_ms{};                            // Safe finality polling interval
    std::uint32_t finalized_poll_ms{};                       // Finalized finality polling interval
};

/// @brief Active RPC provider credentials and capability configuration.
/// Security Notice: URLs may contain authentication tokens; never log unredacted.
struct Provider {
    std::string name;                   // Provider identifier name (e.g. "alchemy", "infura", "ankr")
    std::string ws_url;                 // Resolved WebSocket endpoint URL (empty if not enabled)
    std::string http_url;               // Resolved HTTP endpoint URL (empty if not enabled)
    std::uint32_t max_getlogs_range{};  // Provider-specific getLogs block window ceiling
};

/// @brief Multi-provider redundancy, prioritization, and failover policy parameters.
struct RedundancyCfg {
    std::string mode;                       // Redundancy mode ("failover" or "hedged")
    std::vector<std::string> ws_order;      // Prioritized provider sequence for WebSocket streams
    std::vector<std::string> getlogs_order; // Prioritized provider sequence for HTTP getLogs
    std::uint32_t failover_after{};         // Consecutive error threshold before failover
    std::uint32_t stale_after_blocks{};     // Maximum block divergence before marking provider stale
    std::uint32_t prefer_primary_after_s{}; // Stability duration required before failing back to primary
    std::uint32_t quota_cooldown_s{};       // Cooldown duration when monthly quota is exhausted
};

/// @brief Distributed Redis leader lease parameters.
struct LeaseCfg {
    bool enabled{};          // True if distributed leader election is enabled
    std::uint32_t ttl_ms{};  // Lease lock time-to-live in milliseconds
    std::uint32_t renew_ms{};// Heartbeat interval to renew active lease
};

/// @brief Tracked contract address, monitored events, and emission routing.
struct ContractCfg {
    std::string name;                          // Human-readable contract label
    Address20 address;                         // 20-byte contract address (lowercase hex)
    std::optional<std::uint64_t> start_block;  // Starting block height (nullopt => start at gate)
    Finality finality{Finality::Finalized};    // Required finality classification
    StreamKind stream{StreamKind::Events};     // Destination stream category
    std::vector<std::string> events;           // Canonical event signatures (e.g. "Transfer(address,address,uint256)")
    std::vector<Hash32> topics;                // Topic0 Keccak hashes corresponding to events
};

/// @brief Historical backfill pacing and timeout parameters.
struct BackfillCfg {
    std::uint32_t min_range{};           // Minimum block range floor during AIMD backoff
    std::uint32_t request_timeout_ms{};  // getLogs HTTP request deadline
};

/// @brief Liveness watchdog thresholds for silent node and pipeline stall detection.
struct WatchdogCfg {
    std::uint32_t head_stall_ms{};         // Max silence from WebSocket head feed before alert
    std::uint32_t subscribe_timeout_ms{};  // WebSocket subscription handshake deadline
    std::uint32_t progress_stall_ms{};     // Max silence without pipeline sweep progress
    std::uint32_t safe_gate_stuck_s{};     // Max duration safe gate can remain immobile
    std::uint32_t gate_lag_alert_s{};      // Max lag between finalized gate and safe head
    std::uint32_t process_stall_ms{};      // Maximum processing loop stall threshold
};

/// @brief Downstream consumer group configuration for lag tracking.
struct ConsumerGroupCfg {
    StreamKind stream{StreamKind::Events};  // Monitored stream
    std::string group;                      // Redis stream consumer group name
};

/// @brief Redis stream sink, retention, deduplication, and local spill configuration.
struct SinkCfg {
    std::string redis_url_env;                               // Environment variable name holding Redis connection URL
    std::uint64_t retention_ms{};                            // Maximum message retention window in milliseconds
    std::uint32_t dedup_ttl_s{};                             // Redis deduplication key TTL in seconds
    std::uint32_t queue_capacity{};                          // Maximum in-flight buffer capacity
    std::string spill_dir;                                   // Local filesystem path for disk spill files
    std::uint64_t spill_max_bytes{};                         // Maximum disk spill storage quota in bytes
    bool require_noeviction{};                               // Invariant: require maxmemory-policy == noeviction
    std::vector<ConsumerGroupCfg> consumer_groups;           // Downstream consumer groups to monitor
    std::uint32_t consumer_lag_warn_s{43200};                // Consumer lag warning threshold (default 12h)
    std::uint32_t consumer_lag_crit_s{86400};                // Consumer lag critical threshold (default 24h)
};

/// @brief Operational endpoints, metrics, healthchecks, and external alert webhooks.
struct OpsCfg {
    std::string http_listen;              // Bind address for metrics/health HTTP server
    std::uint32_t heartbeat_s{};          // Metric heartbeat emission interval in seconds
    std::string alert_webhook_env;        // Environment variable for alerting webhook URL (n8n/Slack)
    std::string alert_webhook_token_env;  // Environment variable for alert webhook auth bearer token
    std::string alert_service;            // Service label attached to alert payloads
    std::string deadman_ping_env;         // Environment variable for dead-man's switch ping URL
    std::uint32_t deadman_ping_s{};       // Dead-man switch ping interval in seconds
    std::string http_user_agent;          // User-Agent string sent in outbound HTTP/WS requests
};

/// @brief Route metadata for a matched (contract_address, topic0) event tuple.
struct EventRoute {
    std::string contract;                   // Emitting contract name
    std::string event;                      // Event name (prefix before '(')
    StreamKind stream{StreamKind::Events};  // Destination stream
    Finality finality{Finality::Finalized}; // Event finality requirement
};

/// @brief Composite key lookup for fast event routing: (contract_address, topic0).
struct RouteKey {
    Address20 address;
    Hash32 topic0;
    friend bool operator==(const RouteKey&, const RouteKey&) = default;
};

/// @brief Hash functor for composite RouteKey.
struct RouteKeyHash {
    std::size_t operator()(const RouteKey& k) const noexcept {
        return std::hash<Address20>{}(k.address) * 1000003u ^ std::hash<Hash32>{}(k.topic0);
    }
};

/// @brief Unified filter representation for eth_getLogs and eth_subscribe.
struct LogFilter {
    std::vector<Address20> addresses;  // Monitored contract addresses
    std::vector<Hash32> topic0s;       // Monitored event topic0 hashes
};

using EnvLookup = std::function<std::string(const std::string&)>;

/// @brief Comprehensive, validated runtime configuration for the evm_listener process.
/// Invariants:
/// - Fully Validated: All cross-section constraints, addresses, signatures, and provider URLs are verified.
/// - Fast Routing: O(1) event routing map from (address, topic0) to destination stream and finality.
/// - Security: Contains only resolved, valid providers; secrets are isolated to environment lookups.
struct Config {
    ShardCfg shard;
    ChainCfg chain;
    RedundancyCfg redundancy;
    LeaseCfg lease;
    std::vector<ContractCfg> contracts;
    BackfillCfg backfill;
    WatchdogCfg watchdog;
    SinkCfg sink;
    OpsCfg ops;

    std::vector<Provider> ws_chain;       // enabled WS providers in ws_order
    std::vector<Provider> getlogs_chain;  // enabled getLogs providers in getlogs_order

    // INFO/WARN lines produced while resolving (skipped providers, disabled dead-man switch). No secrets.
    std::vector<std::string> notes;

    std::unordered_map<RouteKey, EventRoute, RouteKeyHash> routes;

    /// @brief Fast O(1) event route lookup by (contract_address, topic0).
    /// @return Pointer to EventRoute if tracked, or nullptr if untracked.
    const EventRoute* route(const Address20& address, const Hash32& topic0) const;

    /// @brief Generates consolidated LogFilter spanning all tracked contracts.
    LogFilter filter() const;
    /// @brief Generates LogFilter filtered to contracts belonging to specified Finality class.
    LogFilter filter(Finality only) const;

    /// @brief Parses TOML configuration text using custom environment lookup.
    /// @param toml_text Raw TOML file content.
    /// @param origin Source origin label (file path) for diagnostic error reporting.
    /// @param env Environment resolver function.
    /// @return Validated Config object.
    /// @throws ConfigError with detailed explanation on syntax or constraint failure.
    static Config parse(std::string_view toml_text, const std::string& origin, const EnvLookup& env);
    /// @brief Parses TOML configuration text resolving variables against process environment.
    static Config parse(std::string_view toml_text, const std::string& origin);
    /// @brief Loads and parses a TOML configuration file from disk.
    static Config load_file(const std::filesystem::path& path);
};

}  // namespace evml::core
