package redislock

import "github.com/redis/go-redis/v9"

var (
	luaReleaseMultiple = redis.NewScript(`
	local num_key = table.getn(KEYS)
	local res
	for i = 1, num_key do
		if redis.call("get", KEYS[i]) == ARGV[1] then
			res = redis.call("del", KEYS[i]) 
		else 
			return 0 
		end
	end

	return res
	`)

	luaRefreshMultiple = redis.NewScript(`
	local num_key = table.getn(KEYS)
	local res
	for i = 1, num_key do
		if redis.call("get", KEYS[i]) == ARGV[1] then
			res = redis.call("pexpire", KEYS[1], ARGV[2]) 
		else 
			return 0 
		end
	end

	return res
	`)

	luaPTTLMultiple = redis.NewScript(`
	local num_key = table.getn(KEYS)

	for i = 1, num_key do
		if redis.call("get", KEYS[i]) == ARGV[1] then
			return redis.call("pttl", KEYS[1]) 
		else 
			return -3
		end
	end
	`)
)
