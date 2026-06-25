-- obtain.lua: arguments => [value, tokenLen, ttl, fenceKey]
-- Obtain.lua try to set provided keys's with value and ttl if they do not exists.
-- Keys can be overriden if they already exists and the correct value+tokenLen is provided. 
-- When fenceKey is set, returns a fencing token instead of "OK".

local function pexpire(ttl)
	-- Update keys ttls.
	for _, key in ipairs(KEYS) do
		redis.call("pexpire", key, ttl)
	end
end

-- canOverrideLock check either or not the provided token match
-- previously set lock's tokens.
local function canOverrideKeys() 
	local offset = tonumber(ARGV[2])

	for _, key in ipairs(KEYS) do
		if redis.call("getrange", key, 0, offset-1) ~= string.sub(ARGV[1], 1, offset) then
			return false
		end
	end
	return true
end

local fenceKey = ARGV[4]

-- reply returns the fencing token, advancing it only on a fresh acquisition,
-- or "OK" when fencing is disabled.
local function reply(fresh)
	if fenceKey == nil or fenceKey == "" then
		return redis.status_reply("OK")
	end
	if fresh then
		return redis.call("incr", fenceKey)
	end
	return tonumber(redis.call("get", fenceKey) or "0")
end

-- Prepare mset arguments.
local setArgs = {}
for _, key in ipairs(KEYS) do
	table.insert(setArgs, key)
	table.insert(setArgs, ARGV[1])
end

if redis.call("msetnx", unpack(setArgs)) ~= 1 then
	if canOverrideKeys() == false then
		return false
	end
	redis.call("mset", unpack(setArgs))
	pexpire(ARGV[3])
	return reply(false)
end

pexpire(ARGV[3])
return reply(true)
