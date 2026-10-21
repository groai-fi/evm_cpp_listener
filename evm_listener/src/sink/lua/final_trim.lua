#!lua
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
