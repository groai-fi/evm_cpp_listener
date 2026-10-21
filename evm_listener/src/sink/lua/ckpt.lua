#!lua
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
