#include "pipeline/event.hpp"

#include <stdexcept>

namespace evml::pipeline {

const char* to_string(EventType t) { return t == EventType::Log ? "log" : "revert"; }

// Formats canonical event ID: {chain_id}:{blockHash}:{logIndex}.
std::string make_event_id(std::uint64_t chain_id, const core::Hash32& block_hash, std::uint64_t log_index) {
    return std::to_string(chain_id) + ":" + block_hash.to_hex() + ":" + std::to_string(log_index);
}

// Prefixes event_id with dedup namespace for Redis string storage.
std::string dedup_key(const Event& e) { return "dedup:" + e.event_id; }

// Enriches raw RPC log into structured Event entity according to schema v=1.
Event make_event(const rpc::Log& log, const core::Config& cfg, const EventContext& ctx, EventType type,
                 std::uint64_t block_timestamp, std::string first_source, std::uint64_t observed_ms,
                 std::uint64_t emitted_ms) {
    if (log.topics.empty()) throw std::invalid_argument("make_event: log without topic0 at block " + std::to_string(log.block_number));
    // Find dispatch rule for contract address and event signature topic0
    const core::EventRoute* route = cfg.route(log.address, log.topics[0]);
    if (route == nullptr) {
        throw std::invalid_argument("make_event: untracked (address, topic0) " + log.address.to_hex() + " " +
                                    log.topics[0].to_hex());
    }

    Event e;
    e.type = type;
    e.finality = route->finality;
    e.stream = route->stream;
    e.chain_id = cfg.chain.chain_id;
    e.contract = route->contract;
    e.address = log.address;
    e.event = route->event;
    // Map topic0 hash to human-readable canonical signature (e.g. Swap(...))
    for (const auto& c : cfg.contracts) {
        if (c.name != route->contract) continue;
        for (std::size_t i = 0; i < c.topics.size(); ++i) {
            if (c.topics[i] == log.topics[0]) e.signature = c.events[i];
        }
    }
    if (e.signature.empty()) throw std::logic_error("make_event: route without a configured signature: " + route->contract);
    e.block_number = log.block_number;
    e.block_hash = log.block_hash;
    e.block_timestamp = block_timestamp;
    e.tx_hash = log.tx_hash;
    e.tx_index = log.tx_index;
    e.log_index = log.log_index;
    e.topics = log.topics;
    e.data = log.data;
    e.event_id = make_event_id(e.chain_id, e.block_hash, e.log_index); // Unique event key
    e.first_source = std::move(first_source);
    e.observed_ms = observed_ms;
    e.emitted_ms = emitted_ms;
    e.host = ctx.host;
    e.shard = ctx.shard;
    e.listener_version = ctx.listener_version;
    return e;
}

std::vector<Field> to_fields(const Event& e) {
    std::string topics = "[";
    for (std::size_t i = 0; i < e.topics.size(); ++i) {
        if (i != 0) topics += ",";
        topics += "\"" + e.topics[i].to_hex() + "\"";  // lowercase hex: nothing to escape
    }
    topics += "]";

    const auto finality = std::string(core::to_string(e.finality));
    return {
        {"v", "1"},
        {"type", to_string(e.type)},
        {"finality", finality},
        {"event_id", e.event_id},
        {"chain_id", std::to_string(e.chain_id)},
        {"contract", e.contract},
        {"address", e.address.to_hex()},
        {"event", e.event},
        {"signature", e.signature},
        {"block_number", std::to_string(e.block_number)},
        {"block_hash", e.block_hash.to_hex()},
        {"block_timestamp", std::to_string(e.block_timestamp)},
        {"tx_hash", e.tx_hash.to_hex()},
        {"tx_index", std::to_string(e.tx_index)},
        {"log_index", std::to_string(e.log_index)},
        {"topics", topics},
        {"data", e.data},
        {"first_source", e.first_source},
        {"observed_ms", std::to_string(e.observed_ms)},
        {"emitted_ms", std::to_string(e.emitted_ms)},
        {"host", e.host},
        {"shard", e.shard},
        {"listener_version", e.listener_version},
    };
}

}  // namespace evml::pipeline
