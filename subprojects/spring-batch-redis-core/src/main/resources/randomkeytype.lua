-- Returns a random key and its type

local key = redis.call('RANDOMKEY')
local type = redis.call('TYPE', key)['ok']
return { key, type }