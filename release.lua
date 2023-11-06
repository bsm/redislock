-- release.lua: => Arguments: [value]
-- Release.lua deletes provided keys if all their values match the input. 

-- Check all keys values matches provided input.
local values = redis.call("mget", unpack(KEYS))
for i, _ in ipairs(KEYS) do
	if values[i] ~= ARGV[1] then
		return false
	end
end

-- Delete keys.
for _, key in ipairs(KEYS) do
	redis.call("del", key)
end

return redis.status_reply("OK")