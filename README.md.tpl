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

### Fencing tokens

Set `Options.Fence` to mint a **fencing token** with the lock: a strictly
increasing value, incremented atomically on each new acquisition and returned by
`Lock.FenceToken`. Stamp every write to the protected resource with the token and
have the resource reject any write carrying an older token. A lock holder that
pauses (GC, scheduling) long enough to lose the lock without noticing is then
fenced out — its writes carry a stale token and are refused. This is the
mitigation described in Martin Kleppmann's
[How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html).

```go
func fence() {{ "ExampleClient_Obtain_fence" | code }}
```

A few notes:

- Enforcement is the resource's job: `redislock` mints and returns the token, but
  the check (`reject if incoming < highest applied`) must happen atomically at the
  resource, which is the only place the comparison and the write can be made one
  operation.
- The token is stored at `<key>:fence` (the first key, for multi-key locks) and
  persists across release so it keeps increasing; it is never reset by `Release`.
- The token is only as monotonic as the underlying Redis. On a single instance it
  is strict; on a Sentinel/Cluster failover that loses the `INCR`, it can regress.
  For strict cross-failover monotonicity, source the token from a linearizable
  store instead.

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
