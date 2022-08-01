local values = redis.call("mget", unpack(KEYS))
local delCount = 0
for i, _ in ipairs(values) do
	if values[i] == ARGV[1] then
		if redis.call("del", KEYS[i]) == 1 then
			delCount = delCount + 1
		end
	end
end
return delCount