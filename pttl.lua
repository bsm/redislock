-- pttl.lua: => Arguments: [value]
-- pttl.lua returns provided keys's ttls if all their values match the input. 

-- Check all keys values matches provided input.
local values = redis.call("mget", unpack(KEYS))
for i, _ in ipairs(KEYS) do
	if values[i] ~= ARGV[1] then
		return false
	end
end

-- Find and return shortest TTL among keys.
local minTTL = 0
for _, key in ipairs(KEYS) do
	local ttl = redis.call("pttl", key)
	-- Note: ttl < 0 probably means the key no longer exists.
	if ttl > 0 and (minTTL == 0 or ttl < minTTL) then
		minTTL = ttl
	end
end
return minTTL