#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "core/config.hpp"
#include "rpc/codec.hpp"

namespace evml::pipeline {

enum class EventType { Log, Revert };
const char* to_string(EventType t);

// One stream entry (plan 3.8, schema v=1). A revert carries the fields of the log it retracts.
struct Event {
    EventType type{EventType::Log};
    core::Finality finality{core::Finality::Finalized};
    core::StreamKind stream{core::StreamKind::Events};  // routing only, not a stream field
    std::string event_id;
    std::uint64_t chain_id{};
    std::string contract;
    core::Address20 address;
    std::string event;      // name, e.g. "Swap"
    std::string signature;  // canonical, e.g. "Swap(address,address,int256,...)"
    std::uint64_t block_number{};
    core::Hash32 block_hash;
    std::uint64_t block_timestamp{};
    core::Hash32 tx_hash;
    std::uint64_t tx_index{};
    std::uint64_t log_index{};
    std::vector<core::Hash32> topics;
    std::string data;
    std::string first_source;  // provider that delivered it first
    std::uint64_t observed_ms{};
    std::uint64_t emitted_ms{};
    std::string host;
    std::string shard;
    std::string listener_version;
};

// "{chain_id}:{blockHash}:{logIndex}": unique per canonical log; a reorg changes the blockHash and so the id.
std::string make_event_id(std::uint64_t chain_id, const core::Hash32& block_hash, std::uint64_t log_index);

// Dedup key "dedup:{event_id}:{type}" with type log|revert, so a revert is never swallowed by its log's key.
std::string dedup_key(const Event& e);

// Process-level constants of every event this instance emits.
struct EventContext {
    std::string shard;
    std::string host;
    std::string listener_version;
};

// Builds the Event for `log` from the config route table. Throws std::invalid_argument when the log has no topic0
// or (address, topic0) is not a tracked pair (callers validate responses first, so this is a contract violation).
Event make_event(const rpc::Log& log, const core::Config& cfg, const EventContext& ctx, EventType type,
                 std::uint64_t block_timestamp, std::string first_source, std::uint64_t observed_ms,
                 std::uint64_t emitted_ms);

using Field = std::pair<std::string, std::string>;
// Flat stream fields in schema order: v, type, finality, event_id, chain_id, contract, address, event, signature,
// block_number, block_hash, block_timestamp, tx_hash, tx_index, log_index, topics (JSON array), data, first_source,
// observed_ms, emitted_ms, host, shard, listener_version.
std::vector<Field> to_fields(const Event& e);

}  // namespace evml::pipeline
