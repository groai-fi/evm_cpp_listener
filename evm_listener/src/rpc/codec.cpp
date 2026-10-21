#include "rpc/codec.hpp"

#include <stdexcept>

#include <boost/json.hpp>

#include "core/error.hpp"

namespace evml::rpc {

using core::DecodeError;
namespace od = simdjson::ondemand;
namespace bj = boost::json;

namespace {

/// @brief Trims trailing whitespace from raw JSON scalar text returned by simdjson raw_json().
std::string raw_text(std::string_view raw) {
    const auto last = raw.find_last_not_of(" \t\r\n");
    return std::string(last == std::string_view::npos ? std::string_view{} : raw.substr(0, last + 1));
}

/// @brief Throws a standardized DecodeError identifying context, field name, and specific reason.
[[noreturn]] void fail_field(const char* ctx, const char* field, const std::string& why) {
    throw DecodeError(std::string(ctx) + ": field \"" + field + "\": " + why);
}

/// @brief Looks up a required field in a simdjson object, failing fast if absent or unreadable.
od::value need(od::object& obj, const char* field, const char* ctx) {
    // Unordered field lookup is faster than ordered scanning
    auto v = obj.find_field_unordered(field);
    if (v.error()) fail_field(ctx, field, std::string("missing or unreadable (") + simdjson::error_message(v.error()) + ")");
    return v.value();
}

/// @brief Extracts a string_view from a simdjson value, failing if type is not string.
std::string_view str_of(od::value v, const char* field, const char* ctx) {
    auto s = v.get_string();
    if (s.error()) fail_field(ctx, field, std::string("expected string (") + simdjson::error_message(s.error()) + ")");
    return s.value();
}

/// @brief Combines need() and str_of() to extract a required string field.
std::string_view need_str(od::object& obj, const char* field, const char* ctx) {
    return str_of(need(obj, field, ctx), field, ctx);
}

/// @brief Wraps downstream parsing exceptions with the offending field name.
template <class F>
auto with_field(const char* ctx, const char* field, F&& f) -> decltype(f()) {
    try {
        return f();
    } catch (const DecodeError& e) {
        fail_field(ctx, field, e.what());
    }
}

/// @brief Parses a required hex-encoded unsigned 64-bit integer quantity (e.g. "0x1a").
std::uint64_t need_quantity(od::object& obj, const char* field, const char* ctx) {
    const auto s = need_str(obj, field, ctx);
    return with_field(ctx, field, [&] { return core::parse_u64_hex(s); });
}

/// @brief Parses a required 32-byte hash from 0x-prefixed 64 hex characters.
Hash32 need_hash(od::object& obj, const char* field, const char* ctx) {
    const auto s = need_str(obj, field, ctx);
    return with_field(ctx, field, [&] { return Hash32::from_hex(s); });
}

/// @brief Extracts Head struct fields from a block object (newHeads or getBlockByNumber).
Head extract_head(od::object& o, const char* ctx) {
    Head h;
    h.number = need_quantity(o, "number", ctx);
    h.hash = need_hash(o, "hash", ctx);
    h.parent_hash = need_hash(o, "parentHash", ctx);
    h.timestamp = need_quantity(o, "timestamp", ctx);
    return h;
}

/// @brief Extracts Log struct fields from an event log object.
Log extract_log(od::object& o, const char* ctx) {
    Log l;
    {
        // Parse emitting contract address
        const auto s = need_str(o, "address", ctx);
        l.address = with_field(ctx, "address", [&] { return Address20::from_hex(s); });
    }
    {
        // Parse indexed topics array (0 to 4 topics allowed by EVM spec)
        auto arr_r = need(o, "topics", ctx).get_array();
        if (arr_r.error()) fail_field(ctx, "topics", "expected array");
        for (auto t : arr_r.value()) {
            auto ts = t.get_string();
            if (ts.error()) fail_field(ctx, "topics", "expected array of strings");
            if (l.topics.size() == 4) fail_field(ctx, "topics", "more than 4 topics");
            const std::string_view sv = ts.value();
            l.topics.push_back(with_field(ctx, "topics", [&] { return Hash32::from_hex(sv); }));
        }
    }
    {
        // Validate and extract non-indexed event payload hex
        const auto d = need_str(o, "data", ctx);
        const bool ok = d.size() >= 2 && d[0] == '0' && d[1] == 'x' && (d.size() % 2 == 0) &&
                        d.find_first_not_of("0123456789abcdefABCDEF", 2) == std::string_view::npos;
        if (!ok) fail_field(ctx, "data", "not 0x-prefixed even-length hex");
        l.data = std::string(d);
    }
    l.block_number = need_quantity(o, "blockNumber", ctx);
    l.block_hash = need_hash(o, "blockHash", ctx);
    l.tx_hash = need_hash(o, "transactionHash", ctx);
    l.tx_index = need_quantity(o, "transactionIndex", ctx);
    l.log_index = need_quantity(o, "logIndex", ctx);
    {
        // Parse chain reorganization removed flag
        auto b = need(o, "removed", ctx).get_bool();
        if (b.error()) fail_field(ctx, "removed", "expected boolean");
        l.removed = b.value();
    }
    return l;
}

/// @brief Extracts RpcError fields from a JSON-RPC error object.
RpcError extract_error(od::object& e, const char* ctx) {
    RpcError r;
    auto code = need(e, "code", ctx).get_int64();
    if (code.error()) fail_field(ctx, "code", "expected integer");
    r.code = code.value();
    r.message = std::string(need_str(e, "message", ctx));
    // Optional error.data field may contain contract revert reasons or rate limit hints
    if (auto d = e.find_field_unordered("data"); !d.error()) {
        auto raw = d.value().raw_json();
        if (!raw.error()) r.data_raw = raw_text(raw.value());
    }
    return r;
}

/// @brief Guards simdjson parsing block, mapping library exceptions to DecodeError.
template <class F>
auto guarded(const char* what, F&& f) -> decltype(f()) {
    try {
        return f();
    } catch (const simdjson::simdjson_error& e) {
        throw DecodeError(std::string("malformed JSON in ") + what + ": " + e.what());
    }
}

}  // namespace

/// @brief Copies input string into internal buffer with required SIMD padding for simdjson parser.
simdjson::padded_string_view Decoder::load(std::string_view json) {
    // Ensure sufficient capacity including SIMD padding
    buf_.reserve(json.size() + simdjson::SIMDJSON_PADDING);
    buf_.assign(json.data(), json.size());
    return simdjson::padded_string_view(buf_.data(), buf_.size(), buf_.capacity());
}

/// @brief Dissects an inbound JSON-RPC message into either a Response or a Notification.
Message Decoder::decode(std::string_view json) {
    return guarded("JSON-RPC message", [&]() -> Message {
        // Iterate on demand over SIMD-padded buffer
        auto doc = parser_.iterate(load(json));
        auto type = doc.type();
        if (type.error() || type.value() != od::json_type::object) throw DecodeError("JSON-RPC message: expected a JSON object");
        od::object obj = doc.get_object();

        // Branch 1: Message has "id" field -> RPC Response
        if (auto idv = obj.find_field_unordered("id"); !idv.error()) {
            const char* ctx = "response";
            Response r;
            od::value id = idv.value();
            auto is_null = id.is_null();
            if (is_null.error()) fail_field(ctx, "id", "unreadable");
            if (!is_null.value()) {
                // Non-null request ID
                auto u = id.get_uint64();
                if (u.error()) fail_field(ctx, "id", "expected unsigned integer");
                r.id = u.value();
            }
            // Check for error payload
            if (auto ev = obj.find_field_unordered("error"); !ev.error() && !ev.value().is_null().value()) {
                od::object eo = ev.value().get_object();
                r.ok = false;
                r.error = extract_error(eo, ctx);
            } else {
                // Successful result payload
                auto rv = need(obj, "result", ctx);
                auto raw = rv.raw_json();
                if (raw.error()) fail_field(ctx, "result", "unreadable");
                r.ok = true;
                r.result_raw = raw_text(raw.value());
            }
            return r;
        }

        // Branch 2: Message lacks "id" -> Async Notification (eth_subscription)
        const char* ctx = "notification";
        auto mv = obj.find_field_unordered("method");
        if (mv.error()) throw DecodeError("JSON-RPC message has neither \"id\" nor \"method\"");
        const std::string_view method = str_of(mv.value(), "method", ctx);
        if (method != "eth_subscription") throw DecodeError("unexpected notification method: " + std::string(method.substr(0, 64)));

        od::object params = need(obj, "params", ctx).get_object();
        Notification n;
        n.subscription = std::string(need_str(params, "subscription", ctx));
        od::object result = need(params, "result", ctx).get_object();
        // Route payload based on presence of "topics" (Log vs newHeads Head)
        if (!result.find_field_unordered("topics").error()) {
            n.payload = extract_log(result, "log notification");
        } else {
            n.payload = extract_head(result, "newHeads notification");
        }
        return n;
    });
}

/// @brief Parses an array of logs from eth_getLogs raw JSON result.
std::vector<Log> Decoder::parse_logs(std::string_view result_raw) {
    return guarded("eth_getLogs result", [&] {
        auto doc = parser_.iterate(load(result_raw));
        std::vector<Log> out;
        for (auto v : doc.get_array()) {
            od::object o = v.get_object();
            out.push_back(extract_log(o, "eth_getLogs log"));
        }
        return out;
    });
}

/// @brief Parses block header from eth_getBlockByNumber raw JSON result (null if not found).
std::optional<Head> Decoder::parse_block(std::string_view result_raw) {
    return guarded("eth_getBlockByNumber result", [&]() -> std::optional<Head> {
        auto doc = parser_.iterate(load(result_raw));
        // Return nullopt if block does not exist on node
        if (doc.is_null().value()) return std::nullopt;
        od::object o = doc.get_object();
        return extract_head(o, "block");
    });
}

/// @brief Parses quantity hex string into uint64_t.
std::uint64_t Decoder::parse_quantity(std::string_view result_raw) {
    return guarded("quantity result", [&] {
        auto doc = parser_.iterate(load(result_raw));
        const std::string_view s = doc.get_string();
        return with_field("quantity", "result", [&] { return core::parse_u64_hex(s); });
    });
}

/// @brief Parses string value from raw JSON result.
std::string Decoder::parse_string(std::string_view result_raw) {
    return guarded("string result", [&] {
        auto doc = parser_.iterate(load(result_raw));
        return std::string(doc.get_string().value());
    });
}

/// @brief Attempts to extract RpcError from HTTP response body.
std::optional<RpcError> Decoder::try_parse_error_body(std::string_view body) {
    try {
        auto doc = parser_.iterate(load(body));
        auto type = doc.type();
        if (type.error() || type.value() != od::json_type::object) return std::nullopt;
        od::object obj = doc.get_object();
        // Check for standard JSON-RPC 2.0 {"error": {...}} wrapper
        if (auto ev = obj.find_field_unordered("error"); !ev.error()) {
            if (ev.value().is_null().value()) return std::nullopt;
            od::object eo = ev.value().get_object();
            return extract_error(eo, "error body");
        }
        // Check for bare {"code": ..., "message": ...} format (Alchemy 429)
        if (obj.find_field_unordered("code").error() || obj.find_field_unordered("message").error()) return std::nullopt;
        return extract_error(obj, "error body");
    } catch (const simdjson::simdjson_error&) {
        return std::nullopt;  // documented: not a JSON-RPC error body
    } catch (const DecodeError&) {
        return std::nullopt;  // documented: shape recognised but unusable -> caller uses HTTP status + text
    }
}

namespace {

/// @brief Helper to construct standard JSON-RPC 2.0 request envelope.
std::string request(std::uint64_t id, const char* method, bj::array params) {
    bj::object o;
    o["jsonrpc"] = "2.0";
    o["id"] = id;
    o["method"] = method;
    o["params"] = std::move(params);
    return bj::serialize(o);
}

}  // namespace

/// @brief Outbound builder for eth_blockNumber.
std::string build_block_number(std::uint64_t id) { return request(id, "eth_blockNumber", {}); }

/// @brief Outbound builder for eth_chainId.
std::string build_chain_id(std::uint64_t id) { return request(id, "eth_chainId", {}); }

/// @brief Outbound builder for eth_subscribe.
std::string build_subscribe(std::uint64_t id, std::string_view kind) {
    return request(id, "eth_subscribe", bj::array{bj::string(kind)});
}

/// @brief Outbound builder for eth_subscribe logs filter.
std::string build_subscribe_logs(std::uint64_t id, const core::LogFilter& filter) {
    // Validate non-empty address and topic requirements
    if (filter.addresses.empty() || filter.topic0s.empty()) {
        throw std::invalid_argument("build_subscribe_logs: filter needs at least one address and one topic0");
    }
    bj::array addrs;
    for (const auto& a : filter.addresses) addrs.emplace_back(a.to_hex());
    bj::array topic0s;
    for (const auto& t : filter.topic0s) topic0s.emplace_back(t.to_hex());
    bj::object f;
    f["address"] = std::move(addrs);
    f["topics"] = bj::array{std::move(topic0s)};
    return request(id, "eth_subscribe", bj::array{bj::string("logs"), std::move(f)});
}

/// @brief Outbound builder for eth_unsubscribe.
std::string build_unsubscribe(std::uint64_t id, std::string_view subscription_id) {
    return request(id, "eth_unsubscribe", bj::array{bj::string(subscription_id)});
}

/// @brief Outbound builder for eth_getBlockByNumber.
std::string build_get_block_by_number(std::uint64_t id, std::string_view block, bool full_tx) {
    return request(id, "eth_getBlockByNumber", bj::array{bj::string(block), full_tx});
}

/// @brief Outbound builder for eth_getCode.
std::string build_get_code(std::uint64_t id, const Address20& address, std::string_view block) {
    return request(id, "eth_getCode", bj::array{bj::string(address.to_hex()), bj::string(block)});
}

/// @brief Outbound builder for eth_getLogs spanning block interval [from_block, to_block].
std::string build_get_logs(std::uint64_t id, std::uint64_t from_block, std::uint64_t to_block, const core::LogFilter& filter) {
    // Invariant I_1: contiguous range must be ordered
    if (from_block > to_block) throw std::invalid_argument("build_get_logs: from_block > to_block");
    if (filter.addresses.empty() || filter.topic0s.empty()) {
        throw std::invalid_argument("build_get_logs: filter needs at least one address and one topic0");
    }
    bj::array addrs;
    for (const auto& a : filter.addresses) addrs.emplace_back(a.to_hex());
    bj::array topic0s;
    for (const auto& t : filter.topic0s) topic0s.emplace_back(t.to_hex());
    bj::object f;
    f["fromBlock"] = core::to_hex_u64(from_block);
    f["toBlock"] = core::to_hex_u64(to_block);
    f["address"] = std::move(addrs);
    f["topics"] = bj::array{std::move(topic0s)};
    return request(id, "eth_getLogs", bj::array{std::move(f)});
}

}  // namespace evml::rpc
