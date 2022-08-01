-- refresh.lua: => Arguments: [value, ttl]
-- refresh.lua refreshes provided keys's ttls if all their values match the input. 

-- Check all keys values matches provided input.
local values = redis.call("mget", unpack(KEYS))
for i, _ in ipairs(KEYS) do
	if values[i] ~= ARGV[1] then
		return redis.error_reply("redislock: not obtained")
	end
end

-- Update keys ttls.
for _, key in ipairs(KEYS) do
	if redis.call("pexpire", key, ARGV[2]) ~= 1 then
		return redis.error_reply("redislock: failed to pexpire " + key)
	end
end

return redis.status_reply("OK")