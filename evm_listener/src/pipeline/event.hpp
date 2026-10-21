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

// Constructs unique deterministic event identifier "{chain_id}:{blockHash}:{logIndex}".
// Watch out: A chain reorg produces a different blockHash, yielding a distinct event_id for the new fork.
std::string make_event_id(std::uint64_t chain_id, const core::Hash32& block_hash, std::uint64_t log_index);

// Derives Redis deduplication key "dedup:{event_id}" (rev 5, N1).
// Watch out: Value stores the last emitted EventType (log|revert) to permit log -> revert -> log transitions.
std::string dedup_key(const Event& e);

// Process-level constants of every event this instance emits.
struct EventContext {
    std::string shard;
    std::string host;
    std::string listener_version;
};

// Enriches raw rpc::Log into a fully formed canonical Event with routing and signature metadata.
// Watch out: Throws std::invalid_argument if topic0 is missing or route is untracked (contract violation).
Event make_event(const rpc::Log& log, const core::Config& cfg, const EventContext& ctx, EventType type,
                 std::uint64_t block_timestamp, std::string first_source, std::uint64_t observed_ms,
                 std::uint64_t emitted_ms);

using Field = std::pair<std::string, std::string>;
// Serializes Event struct into Redis XADD field-value pairs according to schema v=1.
// Watch out: Topics are formatted as a JSON array string of 0x-prefixed 32-byte hex hashes.
std::vector<Field> to_fields(const Event& e);

}  // namespace evml::pipeline
