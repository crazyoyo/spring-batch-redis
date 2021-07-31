-- Returns a key's absolute TTL in milliseconds

local ttl = redis.call('PTTL', KEYS[1])
if ttl < 0 then
  return ttl
else
  local e = redis.call('TIME')
  e = e[1] * 1000 + e[2] / 1000
  return e + ttl
end
