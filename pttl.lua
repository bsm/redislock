local values = redis.call("mget", unpack(KEYS))
local ttl = -3
for i, _ in ipairs(KEYS) do
	if values[i] == ARGV[1] then
		ttl = redis.call("pttl", KEYS[i])
    else
        return -3
	end
end
return ttl