#!lua
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
