-- Returns a key's absolute TTL in milliseconds

local key = KEYS[1]
local ttl = redis.call('PTTL', key)
if ttl >= 0 then
  local e = redis.call('TIME')
  e = e[1] * 1000 + e[2] / 1000
  ttl = e + ttl
end
local type = redis.call('TYPE', key)['ok']
local value = nil
if type == 'hash' then
  value = redis.call('HGETALL', key)
elseif type == 'ReJSON-RL' then
  value = redis.call('JSON.GET', key)
elseif type == 'list' then
  value = redis.call('LRANGE', key, 0, -1)
elseif type == 'set' then
  value = redis.call('SMEMBERS', key)
elseif type == 'stream' then
  value = redis.call('XRANGE', key, '-', '+')
elseif type == 'string' then
  value = redis.call('GET', key)
elseif type == 'TSDB-TYPE' then
  value = redis.call('TS.RANGE', key, '-', '+')
elseif type == 'zset' then
  value = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
end
return { key, ttl, type, value }
