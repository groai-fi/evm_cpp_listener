#include "rpc/codec.hpp"

#include <boost/json.hpp>
#include <stdexcept>

#include "core/error.hpp"

namespace evml::rpc {

using core::DecodeError;
namespace od = simdjson::ondemand;
namespace bj = boost::json;

namespace {

/// @brief Trims trailing whitespace from raw JSON scalar text returned by simdjson raw_json().
[[nodiscard]] std::string raw_text(std::string_view raw) {
    const auto last = raw.find_last_not_of(" \t\r\n");
    return std::string(last == std::string_view::npos ? std::string_view{} : raw.substr(0, last + 1));
}

/// @brief Formats a standardized field decode error identifying context, field name, and specific reason.
[[nodiscard]] core::Error fail_field(const char* ctx, const char* field, std::string_view why) {
    return core::Error{std::string(ctx) + ": field \"" + field + "\": " + std::string(why)};
}

/// @brief Formats a standardized malformed JSON error for a given context.
[[nodiscard]] core::Error fail_json(const char* what, simdjson::error_code ec) {
    return core::Error{std::string("malformed JSON in ") + what + ": " + simdjson::error_message(ec)};
}

/// @brief Looks up a required field in a simdjson object, failing fast if absent or unreadable.
[[nodiscard]] core::Result<od::value> need(od::object& obj, const char* field, const char* ctx) {
    // Unordered field lookup is faster than ordered scanning
    auto v = obj.find_field_unordered(field);
    if (v.error()) {
        return fail_field(ctx, field,
                          std::string("missing or unreadable (") + simdjson::error_message(v.error()) + ")");
    }
    return v.value_unsafe();
}

/// @brief Extracts a string_view from a simdjson value, failing if type is not string.
[[nodiscard]] core::Result<std::string_view> str_of(od::value v, const char* field, const char* ctx) {
    auto s = v.get_string();
    if (s.error()) {
        return fail_field(ctx, field, std::string("expected string (") + simdjson::error_message(s.error()) + ")");
    }
    return s.value_unsafe();
}

/// @brief Combines need() and str_of() to extract a required string field.
[[nodiscard]] core::Result<std::string_view> need_str(od::object& obj, const char* field, const char* ctx) {
    auto v = need(obj, field, ctx);
    if (v.has_error()) return v.error();
    return str_of(v.value(), field, ctx);
}

/// @brief Parses a required hex-encoded unsigned 64-bit integer quantity (e.g. "0x1a").
[[nodiscard]] core::Result<std::uint64_t> need_quantity(od::object& obj, const char* field, const char* ctx) {
    const auto s = need_str(obj, field, ctx);
    if (s.has_error()) return s.error();
    const auto q = core::try_parse_u64_hex(s.value());
    if (q.has_error()) return fail_field(ctx, field, q.error().message);
    return q.value();
}

/// @brief Parses a required 32-byte hash from 0x-prefixed 64 hex characters.
[[nodiscard]] core::Result<Hash32> need_hash(od::object& obj, const char* field, const char* ctx) {
    const auto s = need_str(obj, field, ctx);
    if (s.has_error()) return s.error();
    const auto h = Hash32::try_from_hex(s.value());
    if (h.has_error()) return fail_field(ctx, field, h.error().message);
    return h.value();
}

/// @brief Extracts Head struct fields from a block object (newHeads or getBlockByNumber).
[[nodiscard]] core::Result<Head> extract_head(od::object& o, const char* ctx) {
    Head h;
    const auto num = need_quantity(o, "number", ctx);
    if (num.has_error()) return num.error();
    h.number = num.value();

    const auto hash = need_hash(o, "hash", ctx);
    if (hash.has_error()) return hash.error();
    h.hash = hash.value();

    const auto parent = need_hash(o, "parentHash", ctx);
    if (parent.has_error()) return parent.error();
    h.parent_hash = parent.value();

    const auto ts = need_quantity(o, "timestamp", ctx);
    if (ts.has_error()) return ts.error();
    h.timestamp = ts.value();
    return h;
}

/// @brief Extracts Log struct fields from an event log object.
[[nodiscard]] core::Result<Log> extract_log(od::object& o, const char* ctx) {
    Log l;
    {
        // Parse emitting contract address
        const auto s = need_str(o, "address", ctx);
        if (s.has_error()) return s.error();
        const auto addr = Address20::try_from_hex(s.value());
        if (addr.has_error()) return fail_field(ctx, "address", addr.error().message);
        l.address = addr.value();
    }
    {
        // Parse indexed topics array (0 to 4 topics allowed by EVM spec)
        auto tv = need(o, "topics", ctx);
        if (tv.has_error()) return tv.error();
        auto arr_r = tv.value().get_array();
        if (arr_r.error()) return fail_field(ctx, "topics", "expected array");
        for (auto t : arr_r.value_unsafe()) {
            if (t.error()) return fail_field(ctx, "topics", simdjson::error_message(t.error()));
            auto ts = t.value_unsafe().get_string();
            if (ts.error()) return fail_field(ctx, "topics", "expected array of strings");
            if (l.topics.size() == 4) return fail_field(ctx, "topics", "more than 4 topics");
            const auto h = Hash32::try_from_hex(ts.value_unsafe());
            if (h.has_error()) return fail_field(ctx, "topics", h.error().message);
            l.topics.push_back(h.value());
        }
    }
    {
        // Validate and extract non-indexed event payload hex
        const auto ds = need_str(o, "data", ctx);
        if (ds.has_error()) return ds.error();
        const std::string_view d = ds.value();
        const bool ok = d.size() >= 2 && d[0] == '0' && d[1] == 'x' && (d.size() % 2 == 0) &&
                        d.find_first_not_of("0123456789abcdefABCDEF", 2) == std::string_view::npos;
        if (!ok) return fail_field(ctx, "data", "not 0x-prefixed even-length hex");
        l.data = std::string(d);
    }
    const auto bn = need_quantity(o, "blockNumber", ctx);
    if (bn.has_error()) return bn.error();
    l.block_number = bn.value();

    const auto bh = need_hash(o, "blockHash", ctx);
    if (bh.has_error()) return bh.error();
    l.block_hash = bh.value();

    const auto th = need_hash(o, "transactionHash", ctx);
    if (th.has_error()) return th.error();
    l.tx_hash = th.value();

    const auto ti = need_quantity(o, "transactionIndex", ctx);
    if (ti.has_error()) return ti.error();
    l.tx_index = ti.value();

    const auto li = need_quantity(o, "logIndex", ctx);
    if (li.has_error()) return li.error();
    l.log_index = li.value();
    {
        // Parse chain reorganization removed flag
        auto rv = need(o, "removed", ctx);
        if (rv.has_error()) return rv.error();
        auto b = rv.value().get_bool();
        if (b.error()) return fail_field(ctx, "removed", "expected boolean");
        l.removed = b.value_unsafe();
    }
    return l;
}

/// @brief Extracts RpcError fields from a JSON-RPC error object.
[[nodiscard]] core::Result<RpcError> extract_error(od::object& e, const char* ctx) {
    RpcError r;
    auto cv = need(e, "code", ctx);
    if (cv.has_error()) return cv.error();
    auto code = cv.value().get_int64();
    if (code.error()) return fail_field(ctx, "code", "expected integer");
    r.code = code.value_unsafe();
    const auto msg = need_str(e, "message", ctx);
    if (msg.has_error()) return msg.error();
    r.message = std::string(msg.value());
    // Optional error.data field may contain contract revert reasons or rate limit hints
    if (auto d = e.find_field_unordered("data"); !d.error()) {
        auto raw = d.value_unsafe().raw_json();
        if (!raw.error()) r.data_raw = raw_text(raw.value_unsafe());
    }
    return r;
}

}  // namespace

/// @brief Copies input string into internal buffer with required SIMD padding for simdjson parser.
simdjson::padded_string_view Decoder::load(std::string_view json) {
    // Ensure sufficient capacity including SIMD padding
    buf_.reserve(json.size() + simdjson::SIMDJSON_PADDING);
    buf_.assign(json.data(), json.size());
    return simdjson::padded_string_view(buf_.data(), buf_.size(), buf_.capacity());
}

/// @brief Dissects an inbound JSON-RPC message into either a Response or a Notification without throwing.
core::Result<Message> Decoder::try_decode(std::string_view json) {
    // Iterate on demand over SIMD-padded buffer
    auto doc_r = parser_.iterate(load(json));
    if (doc_r.error()) return fail_json("JSON-RPC message", doc_r.error());
    od::document& doc = doc_r.value_unsafe();
    auto type = doc.type();
    if (type.error() || type.value_unsafe() != od::json_type::object) {
        return core::Error{"JSON-RPC message: expected a JSON object"};
    }
    auto obj_r = doc.get_object();
    if (obj_r.error()) return fail_json("JSON-RPC message", obj_r.error());
    od::object obj = obj_r.value_unsafe();

    // Branch 1: Message has "id" field -> RPC Response
    if (auto idv = obj.find_field_unordered("id"); !idv.error()) {
        const char* ctx = "response";
        Response r;
        od::value id = idv.value_unsafe();
        auto is_null = id.is_null();
        if (is_null.error()) return fail_field(ctx, "id", "unreadable");
        if (!is_null.value_unsafe()) {
            // Non-null request ID
            auto u = id.get_uint64();
            if (u.error()) return fail_field(ctx, "id", "expected unsigned integer");
            r.id = u.value_unsafe();
        }
        // Check for error payload
        auto ev = obj.find_field_unordered("error");
        bool has_error = false;
        if (!ev.error()) {
            auto enull = ev.value_unsafe().is_null();
            if (enull.error()) return fail_field(ctx, "error", "unreadable");
            has_error = !enull.value_unsafe();
        }
        if (has_error) {
            auto eo_r = ev.value_unsafe().get_object();
            if (eo_r.error()) return fail_field(ctx, "error", "expected object");
            od::object eo = eo_r.value_unsafe();
            auto err = extract_error(eo, ctx);
            if (err.has_error()) return err.error();
            r.ok = false;
            r.error = std::move(err).value();
        } else {
            // Successful result payload
            auto rv = need(obj, "result", ctx);
            if (rv.has_error()) return rv.error();
            auto raw = rv.value().raw_json();
            if (raw.error()) return fail_field(ctx, "result", "unreadable");
            r.ok = true;
            r.result_raw = raw_text(raw.value_unsafe());
        }
        return Message{std::move(r)};
    }

    // Branch 2: Message lacks "id" -> Async Notification (eth_subscription)
    const char* ctx = "notification";
    auto mv = obj.find_field_unordered("method");
    if (mv.error()) return core::Error{"JSON-RPC message has neither \"id\" nor \"method\""};
    const auto method_r = str_of(mv.value_unsafe(), "method", ctx);
    if (method_r.has_error()) return method_r.error();
    const std::string_view method = method_r.value();
    if (method != "eth_subscription") {
        return core::Error{"unexpected notification method: " + std::string(method.substr(0, 64))};
    }

    auto pv = need(obj, "params", ctx);
    if (pv.has_error()) return pv.error();
    auto params_r = pv.value().get_object();
    if (params_r.error()) return fail_field(ctx, "params", "expected object");
    od::object params = params_r.value_unsafe();

    const auto sub_r = need_str(params, "subscription", ctx);
    if (sub_r.has_error()) return sub_r.error();
    Notification n;
    n.subscription = std::string(sub_r.value());

    auto res_v = need(params, "result", ctx);
    if (res_v.has_error()) return res_v.error();
    auto res_r = res_v.value().get_object();
    if (res_r.error()) return fail_field(ctx, "result", "expected object");
    od::object result = res_r.value_unsafe();
    // Route payload based on presence of "topics" (Log vs newHeads Head)
    if (!result.find_field_unordered("topics").error()) {
        auto lg = extract_log(result, "log notification");
        if (lg.has_error()) return lg.error();
        n.payload = std::move(lg).value();
    } else {
        auto hd = extract_head(result, "newHeads notification");
        if (hd.has_error()) return hd.error();
        n.payload = std::move(hd).value();
    }
    return Message{std::move(n)};
}

/// @brief Dissects an inbound JSON-RPC message into either a Response or a Notification.
Message Decoder::decode(std::string_view json) {
    auto r = try_decode(json);
    if (r.has_error()) throw DecodeError(r.error().message);
    return std::move(r).value();
}

/// @brief Parses an array of logs from eth_getLogs raw JSON result without throwing.
core::Result<std::vector<Log>> Decoder::try_parse_logs(std::string_view result_raw) {
    auto doc_r = parser_.iterate(load(result_raw));
    if (doc_r.error()) return fail_json("eth_getLogs result", doc_r.error());
    auto arr_r = doc_r.value_unsafe().get_array();
    if (arr_r.error()) return fail_json("eth_getLogs result", arr_r.error());
    std::vector<Log> out;
    for (auto v : arr_r.value_unsafe()) {
        if (v.error()) return fail_json("eth_getLogs result", v.error());
        auto o_r = v.value_unsafe().get_object();
        if (o_r.error()) return fail_json("eth_getLogs result", o_r.error());
        od::object o = o_r.value_unsafe();
        auto lg = extract_log(o, "eth_getLogs log");
        if (lg.has_error()) return lg.error();
        out.push_back(std::move(lg).value());
    }
    return out;
}

/// @brief Parses an array of logs from eth_getLogs raw JSON result.
std::vector<Log> Decoder::parse_logs(std::string_view result_raw) {
    auto r = try_parse_logs(result_raw);
    if (r.has_error()) throw DecodeError(r.error().message);
    return std::move(r).value();
}

/// @brief Parses block header from eth_getBlockByNumber raw JSON result without throwing.
core::Result<std::optional<Head>> Decoder::try_parse_block(std::string_view result_raw) {
    auto doc_r = parser_.iterate(load(result_raw));
    if (doc_r.error()) return fail_json("eth_getBlockByNumber result", doc_r.error());
    od::document& doc = doc_r.value_unsafe();
    // Return nullopt if block does not exist on node
    auto is_null = doc.is_null();
    if (is_null.error()) return fail_json("eth_getBlockByNumber result", is_null.error());
    if (is_null.value_unsafe()) return std::optional<Head>{std::nullopt};
    auto o_r = doc.get_object();
    if (o_r.error()) return fail_json("eth_getBlockByNumber result", o_r.error());
    od::object o = o_r.value_unsafe();
    auto hd = extract_head(o, "block");
    if (hd.has_error()) return hd.error();
    return std::optional<Head>{std::move(hd).value()};
}

/// @brief Parses block header from eth_getBlockByNumber raw JSON result (null if not found).
std::optional<Head> Decoder::parse_block(std::string_view result_raw) {
    auto r = try_parse_block(result_raw);
    if (r.has_error()) throw DecodeError(r.error().message);
    return std::move(r).value();
}

/// @brief Parses quantity hex string into uint64_t without throwing.
core::Result<std::uint64_t> Decoder::try_parse_quantity(std::string_view result_raw) {
    auto doc_r = parser_.iterate(load(result_raw));
    if (doc_r.error()) return fail_json("quantity result", doc_r.error());
    auto s_r = doc_r.value_unsafe().get_string();
    if (s_r.error()) return fail_json("quantity result", s_r.error());
    const auto q = core::try_parse_u64_hex(s_r.value_unsafe());
    if (q.has_error()) return fail_field("quantity", "result", q.error().message);
    return q.value();
}

/// @brief Parses quantity hex string into uint64_t.
std::uint64_t Decoder::parse_quantity(std::string_view result_raw) {
    auto r = try_parse_quantity(result_raw);
    if (r.has_error()) throw DecodeError(r.error().message);
    return r.value();
}

/// @brief Parses string value from raw JSON result without throwing.
core::Result<std::string> Decoder::try_parse_string(std::string_view result_raw) {
    auto doc_r = parser_.iterate(load(result_raw));
    if (doc_r.error()) return fail_json("string result", doc_r.error());
    auto s_r = doc_r.value_unsafe().get_string();
    if (s_r.error()) return fail_json("string result", s_r.error());
    return std::string(s_r.value_unsafe());
}

/// @brief Parses string value from raw JSON result.
std::string Decoder::parse_string(std::string_view result_raw) {
    auto r = try_parse_string(result_raw);
    if (r.has_error()) throw DecodeError(r.error().message);
    return std::move(r).value();
}

/// @brief Attempts to extract RpcError from HTTP response body.
std::optional<RpcError> Decoder::try_parse_error_body(std::string_view body) {
    auto doc_r = parser_.iterate(load(body));
    if (doc_r.error()) return std::nullopt;
    od::document& doc = doc_r.value_unsafe();
    auto type = doc.type();
    if (type.error() || type.value_unsafe() != od::json_type::object) return std::nullopt;
    auto obj_r = doc.get_object();
    if (obj_r.error()) return std::nullopt;
    od::object obj = obj_r.value_unsafe();
    // Check for standard JSON-RPC 2.0 {"error": {...}} wrapper
    if (auto ev = obj.find_field_unordered("error"); !ev.error()) {
        auto is_null = ev.value_unsafe().is_null();
        if (is_null.error() || is_null.value_unsafe()) return std::nullopt;
        auto eo_r = ev.value_unsafe().get_object();
        if (eo_r.error()) return std::nullopt;
        od::object eo = eo_r.value_unsafe();
        auto err = extract_error(eo, "error body");
        if (err.has_error()) return std::nullopt;
        return std::move(err).value();
    }
    // Check for bare {"code": ..., "message": ...} format (Alchemy 429)
    if (obj.find_field_unordered("code").error() || obj.find_field_unordered("message").error()) return std::nullopt;
    auto err = extract_error(obj, "error body");
    if (err.has_error()) return std::nullopt;
    return std::move(err).value();
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
std::string build_block_number(std::uint64_t id) {
    return request(id, "eth_blockNumber", {});
}

/// @brief Outbound builder for eth_chainId.
std::string build_chain_id(std::uint64_t id) {
    return request(id, "eth_chainId", {});
}

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
    for (const auto& a : filter.addresses)
        addrs.emplace_back(a.to_hex());
    bj::array topic0s;
    for (const auto& t : filter.topic0s)
        topic0s.emplace_back(t.to_hex());
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
std::string build_get_logs(std::uint64_t id, std::uint64_t from_block, std::uint64_t to_block,
                           const core::LogFilter& filter) {
    // Invariant I_1: contiguous range must be ordered
    if (from_block > to_block) throw std::invalid_argument("build_get_logs: from_block > to_block");
    if (filter.addresses.empty() || filter.topic0s.empty()) {
        throw std::invalid_argument("build_get_logs: filter needs at least one address and one topic0");
    }
    bj::array addrs;
    for (const auto& a : filter.addresses)
        addrs.emplace_back(a.to_hex());
    bj::array topic0s;
    for (const auto& t : filter.topic0s)
        topic0s.emplace_back(t.to_hex());
    bj::object f;
    f["fromBlock"] = core::to_hex_u64(from_block);
    f["toBlock"] = core::to_hex_u64(to_block);
    f["address"] = std::move(addrs);
    f["topics"] = bj::array{std::move(topic0s)};
    return request(id, "eth_getLogs", bj::array{std::move(f)});
}

}  // namespace evml::rpc
