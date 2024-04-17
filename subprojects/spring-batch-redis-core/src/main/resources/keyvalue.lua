local key = KEYS[1]
local mode = ARGV[1]
local memlimit = tonumber(ARGV[2])
local samples = tonumber(ARGV[3])
local ttl = redis.call('PTTL', key)
if ttl == -2 then
  return { key, ttl }
elseif ttl >= 0 then
  local e = redis.call('TIME')
  local time = e[1]*1000 + e[2]/1000
  ttl = time + ttl
end
local type = redis.call('TYPE', key)['ok']
local mem = 0
if memlimit > 0 then
  mem = redis.call('MEMORY', 'USAGE', key, 'SAMPLES', samples)
  if mem > memlimit then
    return { key, ttl, type, mem }
  end
end
local value = nil
if mode == 'dump' then
  value = redis.call('DUMP', key)
elseif mode == 'struct' then
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
end
return { key, ttl, type, mem, value }