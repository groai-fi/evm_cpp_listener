#include "sink/lua_scripts.hpp"

#include <stdexcept>

#include <boost/json.hpp>

#include "core/error.hpp"

namespace evml::sink {

namespace {

// The `#!lua` shebang must be the very first bytes of each script. No `flags=allow-oom`: see lua_scripts.hpp.
constexpr const char* kEmitLua = R"lua(#!lua
-- emit.lua (plan 4.2)
-- KEYS: 1 dedup:<event_id>  2 stream  3 released  4 released_data  5 leader  6 ckpt
-- ARGV: 1 ttl_s  2 minid_ms  3 type ('log'|'revert')  4 released_score (block, or "")
--       5 event_id  6 packed_fields ("" unless released_score set)  7 token  8.. field,value...
if ARGV[7] ~= '0' and redis.call('GET', KEYS[5]) ~= ARGV[7] then return 'FENCED' end
local last = redis.call('GET', KEYS[1])
if last == ARGV[3] then return 'DUP' end
if ARGV[3] == 'revert' and not last then return 'DUP' end
if ARGV[4] ~= '' then
  local cf = redis.call('HGET', KEYS[6], 'finalized')
  if cf and tonumber(ARGV[4]) <= tonumber(cf) then return 'BELOW_FINALIZED' end
end
local id = redis.call('XADD', KEYS[2], 'MINID', '~', ARGV[2], '*', unpack(ARGV, 8))
if ARGV[4] ~= '' then
  redis.call('ZADD', KEYS[3], ARGV[4], ARGV[5])
  redis.call('HSET', KEYS[4], ARGV[5], ARGV[6])
end
if ARGV[3] == 'revert' then
  redis.call('ZREM', KEYS[3], ARGV[5])
  redis.call('HDEL', KEYS[4], ARGV[5])
end
-- Dedup state is written LAST: Redis does not roll back a failed script, so a failure above leaves dedup unset and
-- the retry re-emits (a duplicate, allowed), never a silent loss.
redis.call('SET', KEYS[1], ARGV[3], 'EX', ARGV[1])
return id
)lua";

constexpr const char* kCkptLua = R"lua(#!lua
-- ckpt.lua (plan 4.2)
-- KEYS: 1 ckpt  2 leader
-- ARGV: 1 token  2 mode  3.. mode arguments
if ARGV[1] ~= '0' and redis.call('GET', KEYS[2]) ~= ARGV[1] then return 'FENCED' end
local mode = ARGV[2]
if mode == 'advance' then
  -- ARGV: 3 field ('safe'|'finalized')  4 value
  local field = ARGV[3]
  local v = tonumber(ARGV[4])
  if (field ~= 'safe' and field ~= 'finalized') or not v then
    return redis.error_reply('ERR ckpt advance: bad field or value')
  end
  local cur = tonumber(redis.call('HGET', KEYS[1], field))
  if cur and v <= cur then return 'NOOP' end
  local s = tonumber(redis.call('HGET', KEYS[1], 'safe'))
  local f = tonumber(redis.call('HGET', KEYS[1], 'finalized'))
  if field == 'safe' then s = v else f = v end
  -- Checked before writing: Redis does not roll back, and I2 violations must never reach the store.
  if f and (not s or f > s) then return redis.error_reply('ERR ckpt advance: finalized > safe (I2)') end
  redis.call('HSET', KEYS[1], field, ARGV[4])
  return 'OK'
elseif mode == 'init' then
  -- ARGV: 3 c0  4 contracts_json
  if redis.call('EXISTS', KEYS[1]) == 1 then return 'EXISTS' end
  redis.call('HSET', KEYS[1], 'safe', ARGV[3], 'finalized', ARGV[3], 'contracts', ARGV[4])
  return 'OK'
elseif mode == 'contracts' then
  -- ARGV: 3 contracts_json
  redis.call('HSET', KEYS[1], 'contracts', ARGV[3])
  return 'OK'
end
return redis.error_reply('ERR ckpt: unknown mode')
)lua";

constexpr const char* kFinalTrimLua = R"lua(#!lua
-- final_trim.lua (plan 4.2)
-- KEYS: 1 released  2 released_data  3 leader
-- ARGV: 1 token  2 g_f
if ARGV[1] ~= '0' and redis.call('GET', KEYS[3]) ~= ARGV[1] then return 'FENCED' end
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])
for i = 1, #ids, 1000 do
  redis.call('HDEL', KEYS[2], unpack(ids, i, math.min(i + 999, #ids)))
end
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])
return tostring(#ids)
)lua";

constexpr const char* kLeaseLua = R"lua(#!lua
-- lease.lua (plan 4.2, 4.8)
-- KEYS: 1 leader  2 epoch
-- ARGV: 1 mode  2.. mode arguments
local mode = ARGV[1]
if mode == 'acquire' then
  -- ARGV: 2 host  3 nonce  4 ttl_ms
  if redis.call('EXISTS', KEYS[1]) == 1 then return false end
  local e = redis.call('GET', KEYS[2])
  local n = 1
  if e then n = 1 + (tonumber(string.match(e, '^(%d+)|')) or 0) end
  local token = n .. '|' .. ARGV[2] .. '|' .. ARGV[3]
  redis.call('SET', KEYS[2], token)
  redis.call('SET', KEYS[1], token, 'PX', ARGV[4])
  return token
elseif mode == 'renew' then
  -- ARGV: 2 token  3 ttl_ms
  local cur = redis.call('GET', KEYS[1])
  if cur == ARGV[2] then
    redis.call('PEXPIRE', KEYS[1], ARGV[3])
    return 'OK'
  end
  -- Nobody took over during a blip: the epoch still names us, so resume.
  if not cur and redis.call('GET', KEYS[2]) == ARGV[2] then
    redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
    return 'RESUMED'
  end
  return 'FENCED'
elseif mode == 'release' then
  -- ARGV: 2 token
  if redis.call('GET', KEYS[1]) == ARGV[2] then
    redis.call('DEL', KEYS[1])
    return 'OK'
  end
  return 'NOOP'
end
return redis.error_reply('ERR lease: unknown mode')
)lua";

std::string num(std::uint64_t v) { return std::to_string(v); }

}  // namespace

const char* script_name(Script s) {
    switch (s) {
        case Script::Emit: return "emit";
        case Script::Ckpt: return "ckpt";
        case Script::FinalTrim: return "final_trim";
        case Script::Lease: return "lease";
    }
    throw std::logic_error("script_name: unknown script");
}

const char* script_source(Script s) {
    switch (s) {
        case Script::Emit: return kEmitLua;
        case Script::Ckpt: return kCkptLua;
        case Script::FinalTrim: return kFinalTrimLua;
        case Script::Lease: return kLeaseLua;
    }
    throw std::logic_error("script_source: unknown script");
}

ScriptCall emit_call(const Keys& k, const pipeline::Emission& e, const EmitParams& p) {
    if (e.fields.empty()) throw std::invalid_argument("emit_call: emission " + e.event_id + " has no fields");
    if (e.dedup_key.rfind("dedup:", 0) != 0)
        throw std::invalid_argument("emit_call: emission " + e.event_id + " has a malformed dedup key '" + e.dedup_key + "'");

    ScriptCall c;
    c.script = Script::Emit;
    c.keys = {k.dedup(e.dedup_key), k.stream(e.stream), k.released(), k.released_data(), k.leader(), k.ckpt()};
    c.args = {num(p.dedup_ttl_s),
              num(p.minid_ms),
              pipeline::to_string(e.type),
              e.released_block ? num(*e.released_block) : std::string(),
              e.event_id,
              e.released_block ? pack_fields(e.fields) : std::string(),
              p.token};
    for (const auto& [name, value] : e.fields) {
        c.args.push_back(name);
        c.args.push_back(value);
    }
    return c;
}

ScriptCall ckpt_advance_call(const Keys& k, const std::string& token, pipeline::CheckpointKind kind, std::uint64_t block) {
    const char* field = kind == pipeline::CheckpointKind::Safe ? "safe" : "finalized";
    return {Script::Ckpt, {k.ckpt(), k.leader()}, {token, "advance", field, num(block)}};
}

ScriptCall ckpt_init_call(const Keys& k, const std::string& token, std::uint64_t c0, const std::vector<std::string>& manifest) {
    return {Script::Ckpt, {k.ckpt(), k.leader()}, {token, "init", num(c0), pack_manifest(manifest)}};
}

ScriptCall ckpt_contracts_call(const Keys& k, const std::string& token, const std::vector<std::string>& manifest) {
    return {Script::Ckpt, {k.ckpt(), k.leader()}, {token, "contracts", pack_manifest(manifest)}};
}

ScriptCall final_trim_call(const Keys& k, const std::string& token, std::uint64_t g_f) {
    return {Script::FinalTrim, {k.released(), k.released_data(), k.leader()}, {token, num(g_f)}};
}

ScriptCall lease_acquire_call(const Keys& k, const std::string& host, const std::string& nonce, std::uint32_t ttl_ms) {
    return {Script::Lease, {k.leader(), k.epoch()}, {"acquire", host, nonce, num(ttl_ms)}};
}

ScriptCall lease_renew_call(const Keys& k, const std::string& token, std::uint32_t ttl_ms) {
    return {Script::Lease, {k.leader(), k.epoch()}, {"renew", token, num(ttl_ms)}};
}

ScriptCall lease_release_call(const Keys& k, const std::string& token) {
    return {Script::Lease, {k.leader(), k.epoch()}, {"release", token}};
}

std::string pack_fields(const std::vector<pipeline::Field>& fields) {
    boost::json::array arr;
    for (const auto& [name, value] : fields) arr.push_back(boost::json::array{name, value});
    return boost::json::serialize(arr);
}

std::vector<pipeline::Field> unpack_fields(std::string_view json) {
    boost::system::error_code ec;
    const boost::json::value v = boost::json::parse(json, ec);
    if (ec) throw core::DecodeError("unpack_fields: invalid JSON (" + ec.message() + "): " + std::string(json));
    if (!v.is_array()) throw core::DecodeError("unpack_fields: not a JSON array: " + std::string(json));

    std::vector<pipeline::Field> out;
    for (const auto& item : v.as_array()) {
        if (!item.is_array() || item.as_array().size() != 2 || !item.as_array()[0].is_string() ||
            !item.as_array()[1].is_string())
            throw core::DecodeError("unpack_fields: element is not a [name, value] string pair: " + std::string(json));
        out.emplace_back(std::string(item.as_array()[0].as_string()), std::string(item.as_array()[1].as_string()));
    }
    return out;
}

std::string pack_manifest(const std::vector<std::string>& manifest) {
    boost::json::array arr;
    for (const auto& s : manifest) arr.push_back(boost::json::string(s));
    return boost::json::serialize(arr);
}

std::vector<std::string> unpack_manifest(std::string_view json) {
    boost::system::error_code ec;
    const boost::json::value v = boost::json::parse(json, ec);
    if (ec) throw core::DecodeError("unpack_manifest: invalid JSON (" + ec.message() + "): " + std::string(json));
    if (!v.is_array()) throw core::DecodeError("unpack_manifest: not a JSON array: " + std::string(json));

    std::vector<std::string> out;
    for (const auto& item : v.as_array()) {
        if (!item.is_string()) throw core::DecodeError("unpack_manifest: element is not a string: " + std::string(json));
        out.emplace_back(item.as_string());
    }
    return out;
}

}  // namespace evml::sink
