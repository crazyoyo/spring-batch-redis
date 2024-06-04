local function time ()
  local now = redis.call('TIME')
  return tonumber(now[1]) * 1000
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

local key = KEYS[1]
local time = time()
local ttl = redis.call('PTTL', key)
local type = nil
local mem = 0
local value = nil
if ttl ~= -2 then
  type = redis.call('TYPE', key)['ok']
  local memlimit = tonumber(ARGV[2])  
  if memlimit > 0 then
    local samples = tonumber(ARGV[3])
    mem = redis.call('MEMORY', 'USAGE', key, 'SAMPLES', samples)
  end
  if mem <= memlimit then
    local mode = ARGV[1]
    if mode == 'DUMP' then
      value = redis.call('DUMP', key)
    elseif mode == 'STRUCT' then
      value = struct_value(key, type)
    end
  end
end
return { key, time, ttl, type, mem, value }