#!lua
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
