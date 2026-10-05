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

struct RpcError {
    std::int64_t code{};
    std::string message;
    std::string data_raw;  // raw JSON of error.data when present (diagnostics only)
};

struct Response {
    std::optional<std::uint64_t> id;  // nullopt for `"id": null` (e.g. parse errors)
    bool ok{};
    std::string result_raw;  // raw JSON of `result` when ok
    RpcError error;          // when !ok
};

struct Head {
    std::uint64_t number{};
    Hash32 hash;
    Hash32 parent_hash;
    std::uint64_t timestamp{};
};

struct Log {
    Address20 address;
    std::vector<Hash32> topics;  // 0..4
    std::string data;            // "0x" + even number of hex digits
    std::uint64_t block_number{};
    Hash32 block_hash;
    Hash32 tx_hash;
    std::uint64_t tx_index{};
    std::uint64_t log_index{};
    bool removed{};
};

struct Notification {
    std::string subscription;
    std::variant<Head, Log> payload;
};

using Message = std::variant<Response, Notification>;

// Inbound JSON-RPC decoding (simdjson On-Demand). One Decoder per connection: it owns a parser and a reusable
// padded buffer and is NOT thread-safe. Every malformed or missing field throws core::DecodeError naming the
// field; nothing is defaulted.
class Decoder {
public:
    Message decode(std::string_view json);

    // Typed extractors over a Response::result_raw.
    std::vector<Log> parse_logs(std::string_view result_raw);
    std::optional<Head> parse_block(std::string_view result_raw);  // `null` result -> nullopt
    std::uint64_t parse_quantity(std::string_view result_raw);
    std::string parse_string(std::string_view result_raw);

    // For HTTP error bodies. Accepts `{"error":{code,message[,data]}}` and the bare `{"code","message"}` shape
    // (Alchemy 429). nullopt means "body is not a recognised JSON-RPC error": callers fall back to HTTP status+text.
    std::optional<RpcError> try_parse_error_body(std::string_view body);

private:
    simdjson::padded_string_view load(std::string_view json);

    simdjson::ondemand::parser parser_;
    std::string buf_;
};

// Outbound builders (Boost.JSON). Ids are caller-supplied monotonic uint64.
std::string build_block_number(std::uint64_t id);
std::string build_chain_id(std::uint64_t id);
std::string build_subscribe(std::uint64_t id, std::string_view kind);  // e.g. "newHeads"
std::string build_unsubscribe(std::uint64_t id, std::string_view subscription_id);
std::string build_get_block_by_number(std::uint64_t id, std::string_view block /* tag or 0x.. */, bool full_tx);
std::string build_get_code(std::uint64_t id, const Address20& address, std::string_view block /* tag or 0x.. */);
// Throws std::invalid_argument for from > to or an empty address/topic list (programmer error).
std::string build_get_logs(std::uint64_t id, std::uint64_t from_block, std::uint64_t to_block, const core::LogFilter& filter);

}  // namespace evml::rpc
