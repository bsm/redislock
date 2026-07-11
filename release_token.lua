-- release_token.lua: => Arguments: [token]
-- release_token.lua deletes provided keys if their current value is prefixed
-- by the given token, ignoring any metadata suffix. Unlike release.lua, the
-- caller does not need to know the original value (token+metadata).

local offset = #ARGV[1]

-- Check all keys' values are prefixed with the provided token.
for _, key in ipairs(KEYS) do
	if redis.call("getrange", key, 0, offset-1) ~= ARGV[1] then
		return false
	end
end

-- Delete keys.
redis.call("del", unpack(KEYS))

return redis.status_reply("OK")