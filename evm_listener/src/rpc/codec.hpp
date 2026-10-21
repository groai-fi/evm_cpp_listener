#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <simdjson.h>

#include "core/config.hpp"
#include "core/hex.hpp"

namespace evml::rpc {

using evml::core::Address20;
using evml::core::Hash32;

/// @brief JSON-RPC 2.0 error envelope.
struct RpcError {
    std::int64_t code{};   // Standard JSON-RPC error code or provider-specific code
    std::string message;   // Human-readable error description
    std::string data_raw;  // Raw JSON string of optional error.data field (for diagnostics)
};

/// @brief Inbound JSON-RPC response envelope parsed from WebSocket or HTTP.
struct Response {
    std::optional<std::uint64_t> id;  // Request ID (nullopt for top-level parse errors where "id": null)
    bool ok{};                        // True if "result" present, false if "error" present
    std::string result_raw;           // Raw unparsed JSON text of "result" field
    RpcError error;                   // Parsed error details when ok == false
};

/// @brief Dissected block header summary from newHeads or getBlockByNumber.
struct Head {
    std::uint64_t number{};  // Block height
    Hash32 hash;             // Canonical block hash
    Hash32 parent_hash;      // Parent block hash (used by ReorgDetector)
    std::uint64_t timestamp{};// Block timestamp in Unix seconds
};

/// @brief Dissected EVM log event corresponding to Solidity events.
struct Log {
    Address20 address;           // Emitting smart contract address
    std::vector<Hash32> topics;  // Indexed event topics (0 to 4 topics)
    std::string data;            // Non-indexed event payload hex ("0x" + even hex digits)
    std::uint64_t block_number{};// Block height where event was mined
    Hash32 block_hash;           // Block hash containing receipt
    Hash32 tx_hash;              // Originating transaction hash
    std::uint64_t tx_index{};    // Transaction index within the block
    std::uint64_t log_index{};   // Global log index within the block
    bool removed{};              // True if log was reverted/removed during chain reorg
};

/// @brief Asynchronous WebSocket push notification envelope.
struct Notification {
    std::string subscription;            // Subscription hex ID returned by eth_subscribe
    std::variant<Head, Log> payload;     // Dissected event payload (Head or Log)
};

/// @brief Inbound message union: either a direct request Response or an async Notification.
using Message = std::variant<Response, Notification>;

/// @brief Zero-copy, high-throughput JSON-RPC decoder utilizing simdjson On-Demand.
/// Concurrency & Threading:
/// - NOT thread-safe: Maintains internal reusable padded buffer and parser state.
/// - Allocate one Decoder instance per worker connection/session.
/// Error Handling:
/// - Throws evml::core::DecodeError naming the missing or malformed field.
/// - Never silently falls back or defaults missing required fields.
class Decoder {
public:
    /// @brief Dissects a raw JSON string into a Response or Notification.
    Message decode(std::string_view json);

    // Typed extractors over a Response::result_raw.
    /// @brief Parses an array of Log objects from eth_getLogs result JSON.
    std::vector<Log> parse_logs(std::string_view result_raw);
    /// @brief Parses a Head block header from eth_getBlockByNumber result JSON (null returns nullopt).
    std::optional<Head> parse_block(std::string_view result_raw);
    /// @brief Parses a hex quantity (e.g., "0x1b4") into an unsigned 64-bit integer.
    std::uint64_t parse_quantity(std::string_view result_raw);
    /// @brief Parses a raw string scalar from result JSON.
    std::string parse_string(std::string_view result_raw);

    /// @brief Attempts to extract RpcError from an HTTP error body.
    /// Supports standard {"error":{...}} and bare {"code":...,"message":...} formats (Alchemy 429).
    /// @return Extracted RpcError, or nullopt if body is not valid JSON-RPC error.
    std::optional<RpcError> try_parse_error_body(std::string_view body);

private:
    simdjson::padded_string_view load(std::string_view json);

    simdjson::ondemand::parser parser_;
    std::string buf_;
};

// Outbound builders (Boost.JSON). Ids are caller-supplied monotonic uint64.
/// @brief Builds "eth_blockNumber" JSON-RPC request string.
std::string build_block_number(std::uint64_t id);
/// @brief Builds "eth_chainId" JSON-RPC request string.
std::string build_chain_id(std::uint64_t id);
/// @brief Builds "eth_subscribe" request string for specified feed (e.g. "newHeads").
std::string build_subscribe(std::uint64_t id, std::string_view kind);
/// @brief Builds "eth_subscribe" request for logs matching the given filter.
/// @warning Throws std::invalid_argument if filter contains no addresses or topic0s.
std::string build_subscribe_logs(std::uint64_t id, const core::LogFilter& filter);
/// @brief Builds "eth_unsubscribe" request for an active subscription ID.
std::string build_unsubscribe(std::uint64_t id, std::string_view subscription_id);
/// @brief Builds "eth_getBlockByNumber" request (block tag or "0x..." hex number).
std::string build_get_block_by_number(std::uint64_t id, std::string_view block, bool full_tx);
/// @brief Builds "eth_getCode" request for bytecode inspection.
std::string build_get_code(std::uint64_t id, const Address20& address, std::string_view block);
/// @brief Builds "eth_getLogs" request spanning [from_block, to_block].
/// @warning Throws std::invalid_argument if from_block > to_block or filter is empty.
std::string build_get_logs(std::uint64_t id, std::uint64_t from_block, std::uint64_t to_block, const core::LogFilter& filter);

}  // namespace evml::rpc
