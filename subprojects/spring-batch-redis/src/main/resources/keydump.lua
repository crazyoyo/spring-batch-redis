local key = KEYS[1]
local ttl = redis.call('PTTL', key)
if ttl >= 0 then
  local e = redis.call('TIME')
  e = e[1] * 1000 + e[2] / 1000
  ttl = e + ttl
end
local dump = redis.call('DUMP', key)
return { key, ttl, dump }