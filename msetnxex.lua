local setExCount = 0
if redis.call("msetnx", unpack(KEYS)) == 1 then
    for i, key in ipairs(KEYS) do
        if i % 2 == 1 then
            if redis.call("pexpire", key, ARGV[1]) == 1 then
                setExCount = setExCount + 1               
            end
        end
    end
end
return setExCount