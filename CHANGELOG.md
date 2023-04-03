## v0.9.3

- Feature: allow custom lock tokens [#66](https://github.com/bsm/redislock/pull/66)

## v0.9.2

- Feature: better handling of nil lock.Release() [#68](https://github.com/bsm/redislock/pull/68)

## v0.9.1

- Fix: reset backoff ticker for exponential backoff [#58](https://github.com/bsm/redislock/pull/58)

# v0.9.0

- Chore: use updated go-redis import path [#55](https://github.com/bsm/redislock/pull/55)
- Chore: use redis.v9 GA [#57](https://github.com/bsm/redislock/pull/57)

## v0.8.2

- Fix: use ticker instead of resetting timer [#52](https://github.com/bsm/redislock/pull/52)

## v0.8.1

- Fix: compatibility with redis.v9 [#50](https://github.com/bsm/redislock/pull/50)

# v0.8.0

- Prepare for go-redis/redis v9 release [#49](https://github.com/bsm/redislock/pull/49)

## v0.7.2

- Allow custom deadlines, use stdlib testing package [#35](https://github.com/bsm/redislock/pull/35)
- Reduce deps [#38](https://github.com/bsm/redislock/pull/38)

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
