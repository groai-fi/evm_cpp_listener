#include "sink/lua_scripts.hpp"

#include <boost/json.hpp>
#include <stdexcept>

#include "core/error.hpp"
#include "sink/lua_sources.hpp"

namespace evml::sink {

namespace {

std::string num(std::uint64_t v) {
    return std::to_string(v);
}

boost::json::array fields_to_json(const std::vector<pipeline::Field>& fields) {
    boost::json::array arr;
    for (const auto& [name, value] : fields)
        arr.push_back(boost::json::array{name, value});
    return arr;
}

// `what` names the caller in error messages; `raw` is the full stored value, quoted for diagnosis.
core::Result<std::vector<pipeline::Field>> fields_from_json(const boost::json::value& v, const char* what,
                                                            std::string_view raw) {
    if (!v.is_array()) return core::Error(std::string(what) + ": not a JSON array: " + std::string(raw));

    std::vector<pipeline::Field> out;
    for (const auto& item : v.as_array()) {
        if (!item.is_array() || item.as_array().size() != 2 || !item.as_array()[0].is_string() ||
            !item.as_array()[1].is_string())
            return core::Error(std::string(what) + ": element is not a [name, value] string pair: " + std::string(raw));
        out.emplace_back(std::string(item.as_array()[0].as_string()), std::string(item.as_array()[1].as_string()));
    }
    return out;
}

core::Result<boost::json::value> parse_json(std::string_view json, const char* what) {
    boost::system::error_code ec;
    boost::json::value v = boost::json::parse(json, ec);
    if (ec) return core::Error(std::string(what) + ": invalid JSON (" + ec.message() + "): " + std::string(json));
    return v;
}

}  // namespace

const char* script_source(Script s) noexcept {
    switch (s) {
        case Script::Emit:
            return kEmitLua;
        case Script::Ckpt:
            return kCkptLua;
        case Script::FinalTrim:
            return kFinalTrimLua;
        case Script::Lease:
            return kLeaseLua;
        case Script::EmitFast:
            return kEmitFastLua;
    }
    return kEmitLua;
}

/// @brief Builds Redis EVALSHA call payload for emit.lua atomically executing deduplication and stream append.
ScriptCall emit_call(const Keys& k, const pipeline::Emission& e, const EmitParams& p) {
    if (e.fields.empty()) throw std::invalid_argument("emit_call: emission " + e.event_id + " has no fields");
    if (e.dedup_key.rfind("dedup:", 0) != 0)
        throw std::invalid_argument("emit_call: emission " + e.event_id + " has a malformed dedup key '" + e.dedup_key +
                                    "'");

    ScriptCall c;
    c.script = Script::Emit;
    // Keys array: dedup key, stream, released set, released before-data hash, leader lease, checkpoint hash.
    c.keys = {k.dedup(e.dedup_key), k.stream(e.stream), k.released(), k.released_data(), k.leader(), k.ckpt()};
    // Args array: dedup TTL, minid retention, event type, released block, event id, before-image JSON, fencing token.
    c.args = {num(p.dedup_ttl_s),
              num(p.minid_ms),
              pipeline::to_string(e.type),
              e.released_block ? num(*e.released_block) : std::string(),
              e.event_id,
              e.released_block ? pack_released(e.stream, e.fields) : std::string(),
              p.token};
    // Append alternating field names and values for Redis stream payload.
    for (const auto& [name, value] : e.fields) {
        c.args.push_back(name);
        c.args.push_back(value);
    }
    return c;
}

ScriptCall emit_fast_call(const Keys& k, const std::vector<pipeline::Field>& fields, std::uint64_t minid_ms,
                          std::string_view token) {
    if (fields.empty()) throw std::invalid_argument("emit_fast_call: event has no fields");

    ScriptCall c;
    c.script = Script::EmitFast;
    c.keys = {k.fast(), k.leader()};
    c.args = {num(minid_ms), std::string(token)};
    for (const auto& [name, value] : fields) {
        c.args.push_back(name);
        c.args.push_back(value);
    }
    return c;
}

ScriptCall emit_fast_call(const Keys& k, const pipeline::Event& e, std::uint64_t minid_ms, std::string_view token) {
    return emit_fast_call(k, pipeline::to_fields(e), minid_ms, token);
}

/// @brief Builds call to advance safe or finalized checkpoint hash field.
ScriptCall ckpt_advance_call(const Keys& k, std::string_view token, pipeline::CheckpointKind kind,
                             std::uint64_t block) {
    const char* field = kind == pipeline::CheckpointKind::Safe ? "safe" : "finalized";
    // Checkpoint script verifies fencing token and enforces monotonic advancement.
    return {Script::Ckpt, {k.ckpt(), k.leader()}, {std::string(token), "advance", field, num(block)}};
}

/// @brief Builds call to initialize checkpoint with starting block c0 and tracked contract manifest.
ScriptCall ckpt_init_call(const Keys& k, std::string_view token, std::uint64_t c0,
                          const std::vector<std::string>& manifest) {
    return {Script::Ckpt, {k.ckpt(), k.leader()}, {std::string(token), "init", num(c0), pack_manifest(manifest)}};
}

/// @brief Builds call to update contract manifest list in checkpoint hash.
ScriptCall ckpt_contracts_call(const Keys& k, std::string_view token, const std::vector<std::string>& manifest) {
    return {Script::Ckpt, {k.ckpt(), k.leader()}, {std::string(token), "contracts", pack_manifest(manifest)}};
}

/// @brief Builds call to trim released zset and associated before-image data below finalized block g_f.
ScriptCall final_trim_call(const Keys& k, std::string_view token, std::uint64_t g_f) {
    return {Script::FinalTrim, {k.released(), k.released_data(), k.leader()}, {std::string(token), num(g_f)}};
}

/// @brief Builds call to acquire leader lease token using random nonce and TTL.
ScriptCall lease_acquire_call(const Keys& k, std::string_view host, std::string_view nonce, std::uint32_t ttl_ms) {
    return {Script::Lease, {k.leader(), k.epoch()}, {"acquire", std::string(host), std::string(nonce), num(ttl_ms)}};
}

/// @brief Builds call to extend existing leader lease token TTL.
ScriptCall lease_renew_call(const Keys& k, std::string_view token, std::uint32_t ttl_ms) {
    return {Script::Lease, {k.leader(), k.epoch()}, {"renew", std::string(token), num(ttl_ms)}};
}

/// @brief Builds call to release leader lease token voluntarily on graceful shutdown.
ScriptCall lease_release_call(const Keys& k, std::string_view token) {
    return {Script::Lease, {k.leader(), k.epoch()}, {"release", std::string(token)}};
}

std::string pack_fields(const std::vector<pipeline::Field>& fields) {
    return boost::json::serialize(fields_to_json(fields));
}

core::Result<std::vector<pipeline::Field>> try_unpack_fields(std::string_view json) noexcept {
    try {
        const auto v = parse_json(json, "unpack_fields");
        if (!v) return v.error();
        return fields_from_json(*v, "unpack_fields", json);
    } catch (...) {
        return core::Error("unpack_fields: exception");
    }
}

std::vector<pipeline::Field> unpack_fields(std::string_view json) {
    auto r = try_unpack_fields(json);
    if (!r) throw core::DecodeError(r.error().what());
    return std::move(*r);
}

std::string pack_released(core::StreamKind stream, const std::vector<pipeline::Field>& fields) {
    boost::json::object o;
    o["stream"] = core::to_string(stream);
    o["fields"] = fields_to_json(fields);
    return boost::json::serialize(o);
}

core::Result<ReleasedImage> try_unpack_released(std::string_view json) noexcept {
    try {
        const auto v_res = parse_json(json, "unpack_released");
        if (!v_res) return v_res.error();
        const boost::json::value& v = *v_res;
        if (!v.is_object()) return core::Error("unpack_released: not a JSON object: " + std::string(json));
        const auto& o = v.as_object();
        const auto* stream = o.if_contains("stream");
        const auto* fields = o.if_contains("fields");
        if (!stream || !stream->is_string() || !fields) {
            return core::Error("unpack_released: needs a string `stream` and a `fields` array: " + std::string(json));
        }

        ReleasedImage out;
        if (stream->as_string() == core::to_string(core::StreamKind::Events)) {
            out.stream = core::StreamKind::Events;
        } else if (stream->as_string() == core::to_string(core::StreamKind::Registry)) {
            out.stream = core::StreamKind::Registry;
        } else {
            return core::Error("unpack_released: unknown stream '" + std::string(stream->as_string()) +
                               "': " + std::string(json));
        }
        auto fields_res = fields_from_json(*fields, "unpack_released", json);
        if (!fields_res) return fields_res.error();
        out.fields = std::move(*fields_res);
        return out;
    } catch (...) {
        return core::Error("unpack_released: exception");
    }
}

ReleasedImage unpack_released(std::string_view json) {
    auto r = try_unpack_released(json);
    if (!r) throw core::DecodeError(r.error().what());
    return std::move(*r);
}

std::string pack_manifest(const std::vector<std::string>& manifest) {
    boost::json::array arr;
    for (const auto& s : manifest)
        arr.push_back(boost::json::string(s));
    return boost::json::serialize(arr);
}

core::Result<std::vector<std::string>> try_unpack_manifest(std::string_view json) noexcept {
    try {
        boost::system::error_code ec;
        const boost::json::value v = boost::json::parse(json, ec);
        if (ec) return core::Error("unpack_manifest: invalid JSON (" + ec.message() + "): " + std::string(json));
        if (!v.is_array()) return core::Error("unpack_manifest: not a JSON array: " + std::string(json));

        std::vector<std::string> out;
        for (const auto& item : v.as_array()) {
            if (!item.is_string()) return core::Error("unpack_manifest: element is not a string: " + std::string(json));
            out.emplace_back(item.as_string());
        }
        return out;
    } catch (...) {
        return core::Error("unpack_manifest: exception");
    }
}

std::vector<std::string> unpack_manifest(std::string_view json) {
    auto r = try_unpack_manifest(json);
    if (!r) throw core::DecodeError(r.error().what());
    return std::move(*r);
}

}  // namespace evml::sink
