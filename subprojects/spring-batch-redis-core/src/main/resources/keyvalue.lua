local function unix_ms ()
  local now = redis.call('TIME')
  return tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
end

local function absttl (key)
  local ttl = redis.call('PTTL', key)
  if ttl < 0 then
    return ttl
  end
  return unix_ms() + ttl
end

local function struct_value (key, type)
  if type == 'hash' then
    return redis.call('HGETALL', key)
  end
  if type == 'ReJSON-RL' then
    return redis.call('JSON.GET', key)
  end
  if type == 'list' then
    return redis.call('LRANGE', key, 0, -1)
  end
  if type == 'set' then
    return redis.call('SMEMBERS', key)
  end
  if type == 'stream' then
    return redis.call('XRANGE', key, '-', '+')
  end
  if type == 'string' then
    return redis.call('GET', key)
  end
  if type == 'TSDB-TYPE' then
    return redis.call('TS.RANGE', key, '-', '+')
  end
  if type == 'zset' then
    return redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
  end
  return nil
end

local function value (mode, key, type)
  if mode == 'dump' then
    return redis.call('DUMP', key)
  end
  return struct_value(key, type)
end

local key = KEYS[1]
local mode = ARGV[1]
local memlimit = tonumber(ARGV[2])
local samples = tonumber(ARGV[3])
local ttl = absttl(key)
if ttl == -2 then
  return { key, ttl }
end
local type = redis.call('TYPE', key)['ok']
local mem = 0
if memlimit > 0 then
  mem = redis.call('MEMORY', 'USAGE', key, 'SAMPLES', samples)
  if mem > memlimit then
    return { key, ttl, type, mem }
  end
end
local value = value(mode, key, type)
return { key, ttl, type, mem, value }