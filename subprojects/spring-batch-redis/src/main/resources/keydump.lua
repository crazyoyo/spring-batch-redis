local key = KEYS[1]
local type = redis.call('TYPE', key)['ok']
local ttl = redis.call('PTTL', key)
if ttl >= 0 then
  local e = redis.call('TIME')
  e = e[1] * 1000 + e[2] / 1000
  ttl = e + ttl
end
local mem = 0
if #ARGV == 2 then
  mem = redis.call('MEMORY', 'USAGE', key, 'SAMPLES', tonumber(ARGV[2]))
  local memlimit = tonumber(ARGV[1])
  if memlimit > 0 and mem > memlimit then
    return { key, ttl, type, mem }
  end
end
local dump = redis.call('DUMP', key)
return { key, ttl, type, mem, dump }