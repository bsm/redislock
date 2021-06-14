## v0.7.1

- Make `RetryStrategy` thread-safe to allow re-using `Options` [#31](https://github.com/bsm/redislock/pull/31) [calvinxiao](https://github.com/calvinxiao)
- Improve test-performance, perform race testing on CI [#33](https://github.com/bsm/redislock/pull/33)

# v0.7.0

- Replace Options.Context with explicit ctx parameter [#25](https://github.com/bsm/redislock/pull/25)

# v0.6.0

- Allow to customise retry deadlines through context [#22](https://github.com/bsm/redislock/pull/22)
- Migrate to `github.com/go-redis/redis/v8` [#15](https://github.com/bsm/redislock/pull/15)

# v0.5.0

- Migrate to `github.com/go-redis/redis/v7` [#11](https://github.com/bsm/redislock/pull/11)
