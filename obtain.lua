-- obtain.lua: arguments => [value, tokenLen, ttl, fenced]
-- Obtain.lua try to set provided keys's with value and ttl if they do not exists.
-- Keys can be overriden if they already exists and the correct value+tokenLen is provided.
-- When fenced is "1", the last KEYS entry is a fence key and obtain returns a
-- fencing token instead of "OK".

local fenced = tonumber(ARGV[4]) == 1

-- Lock keys are KEYS[1..lockCount]; the fence key, if any, is the last entry.
local lockCount = #KEYS
if fenced then
	lockCount = lockCount - 1
end

local function pexpire(ttl)
	-- Update keys ttls.
	for i = 1, lockCount do
		redis.call("pexpire", KEYS[i], ttl)
	end
end

-- canOverrideLock check either or not the provided token match
-- previously set lock's tokens.
local function canOverrideKeys()
	local offset = tonumber(ARGV[2])

	for i = 1, lockCount do
		if redis.call("getrange", KEYS[i], 0, offset-1) ~= string.sub(ARGV[1], 1, offset) then
			return false
		end
	end
	return true
end

-- reply returns the fencing token, advancing it only on a fresh acquisition,
-- or "OK" when fencing is disabled.
local function reply(fresh)
	if not fenced then
		return redis.status_reply("OK")
	end
	local fenceKey = KEYS[#KEYS]
	if fresh then
		return redis.call("incr", fenceKey)
	end
	return tonumber(redis.call("get", fenceKey) or "0")
end

-- Prepare mset arguments.
local setArgs = {}
for i = 1, lockCount do
	table.insert(setArgs, KEYS[i])
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
