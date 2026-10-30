#!lua
-- emit_fast.lua
-- KEYS: 1 fast_stream  2 leader
-- ARGV: 1 minid_ms  2 token  3.. field,value...
if ARGV[2] ~= '0' and redis.call('GET', KEYS[2]) ~= ARGV[2] then return 'FENCED' end
return redis.call('XADD', KEYS[1], 'MINID', '~', ARGV[1], '*', unpack(ARGV, 3))

