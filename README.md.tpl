# redislock

[![Test](https://github.com/bsm/redislock/actions/workflows/test.yml/badge.svg)](https://github.com/bsm/redislock/actions/workflows/test.yml)
[![GoDoc](https://godoc.org/github.com/bsm/redislock?status.png)](http://godoc.org/github.com/bsm/redislock)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Simplified distributed locking implementation using [Redis](http://redis.io/topics/distlock).
For more information, please see examples.

## Documentation

Full documentation is available on [GoDoc](http://godoc.org/github.com/bsm/redislock)

## Examples

```go
import (
  "context"
  "fmt"
  "log"
  "time"

  "github.com/bsm/redislock"
  "github.com/redis/go-redis/v9"
)

func main() {{ "Example" | code }}
```

### External watchdog

`redislock` deliberately does not bundle a built-in watchdog goroutine: refresh
cadence and error handling are application concerns (log and continue, retry,
cancel the protected work, page someone, etc.). If you need a long-running lock
that outlives its initial TTL, drop a small ticker next to your work and let it
call `Refresh` for you:

```go
func watchdog() {{ "ExampleLock_Refresh_watchdog" | code }}
```

A few notes:

- Pick `interval ≈ ttl/3` so a single transient redis blip still leaves room to
  retry before the lock expires.
- A `Refresh` error of `redislock.ErrNotObtained` means the lock was lost
  (expired or stolen) and is usually fatal for the protected work; cancel the
  work context so the caller stops.
- `Release` stays with the caller, so ownership is explicit.
