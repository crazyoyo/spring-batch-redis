local key = KEYS[1]
local type = redis.call('TYPE', key)['ok']
if type == nil or type == 'none' then
  return {key, type}
end
local ttl = redis.call('PTTL', key)
if ttl == -2 then
  return {key, type, ttl}
end
if ttl >= 0 then
  local e = redis.call('TIME')
  e = e[1] * 1000 + e[2] / 1000
  ttl = e + ttl
end
local mem = 0
local memlimit = tonumber(ARGV[1])
if memlimit ~= 0 then
  local samples = tonumber(ARGV[2])
  mem = redis.call('MEMORY', 'USAGE', key, 'SAMPLES', samples)
  if memlimit > 0 and mem > memlimit then
    return { key, type, ttl, mem }
  end
end
local value = nil
if ARGV[3] == 'DUMP' then
  value = redis.call('DUMP', key)
elseif type == 'hash' then
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
return { key, type, ttl, mem, value }