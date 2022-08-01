-- release.lua: => Arguments: [value]
-- Release.lua deletes provided keys if all their values match the input. 

-- Check all keys values matches provided input.
local values = redis.call("mget", unpack(KEYS))
for i, _ in ipairs(KEYS) do
	if values[i] ~= ARGV[1] then
		return redis.error_reply("redislock: lock not held")
	end
end

-- Delete keys.
for _, key in ipairs(KEYS) do
	if redis.call("del", key) ~= 1 then
		return redis.error_reply("redislock: failed to del " + key)
	end
end

return redis.status_reply("OK")