local values = redis.call("mget", unpack(KEYS))
local pexpireCount = 0
for i, _ in ipairs(values) do
	if values[i] == ARGV[1] then
		if redis.call("pexpire", KEYS[i], ARGV[2]) == 1 then
			pexpireCount = pexpireCount + 1
		end
	end
end
return pexpireCount