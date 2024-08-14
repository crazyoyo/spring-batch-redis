local function now ()
  local time = redis.call('TIME')
  return tonumber(time[1]) * 1000
end

local function struct (key, type)
  if type == 'hash' then
    return redis.call('HGETALL', key)
  elseif type == 'ReJSON-RL' or type == 'json' then
    return redis.call('JSON.GET', key)
  elseif type == 'list' then
    return redis.call('LRANGE', key, 0, -1)
  elseif type == 'set' then
    return redis.call('SMEMBERS', key)
  elseif type == 'stream' then
    return redis.call('XRANGE', key, '-', '+')
  elseif type == 'string' then
    return redis.call('GET', key)
  elseif type == 'TSDB-TYPE' then
    return redis.call('TS.RANGE', key, '-', '+')
  elseif type == 'zset' then
    return redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
  end
  return nil
end

--[[
    KEYS:
      1. Key for which to fetch TTL, mem usage, type, and value
    ARGV:
      1. mode: value format
           DUMP  : DUMP <key>
           STRUCT: data-structure specific command (string -> GET, hash -> HGETALL, ...)
           else  : no value
      2. limit: criteria on key memory usage
           -1 : no mem usage, no limit
            0 : mem usage, no limit
            1+: mem usage, limit
      3. samples: number of sampled nested values (MEMORY USAGE <key> SAMPLES <samples>) 
--]]
local key = KEYS[1]
local mode = ARGV[1]
local limit = tonumber(ARGV[2])
local samples = tonumber(ARGV[3])
local ttl = redis.call('PTTL', key)
if ttl >= 0 then
  ttl = now() + ttl
end
local mem = 0
local type
local value
if ttl ~= -2 then
  if limit >= 0 then
    mem = redis.call('MEMORY', 'USAGE', key, 'SAMPLES', samples)
  end
  type = redis.call('TYPE', key)['ok']
  if limit <= 0 or mem <= limit then
    if mode == 'DUMP' then
      value = redis.call('DUMP', key)
    elseif mode == 'STRUCT' then
      value = struct(key, type)
    end
  end
end
return { key, ttl, mem, type, value }