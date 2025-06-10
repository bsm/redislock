# redislock

[![Test](https://github.com/bsm/redislock/actions/workflows/test.yml/badge.svg)](https://github.com/bsm/redislock/actions/workflows/test.yml)
[![GoDoc](https://godoc.org/github.com/bsm/redislock?status.png)](http://godoc.org/github.com/bsm/redislock)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Simplified distributed locking implementation using [Redis](http://redis.io/topics/distlock).
For more information, please see examples.

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

func main() {
	// Connect to redis.
	client := redis.NewClient(&redis.Options{
		Network:	"tcp",
		Addr:		"127.0.0.1:6379",
	})
	defer client.Close()

	// Create a new lock client.
	locker := redislock.New(client)

	ctx := context.Background()

	// Try to obtain lock.
	lock, err := locker.Obtain(ctx, "my-key", 100*time.Millisecond, nil)
	if err == redislock.ErrNotObtained {
		fmt.Println("Could not obtain lock!")
		// It default 0 times backoff retrive, next step should be return or continue in a loop.
		return
	} else if err != nil {
		log.Fatalln(err)
	}

	// Don't forget to defer Release.
	defer lock.Release(ctx)
	fmt.Println("I have a lock!")

	// Sleep and check the remaining TTL.
	time.Sleep(50 * time.Millisecond)
	if ttl, err := lock.TTL(ctx); err != nil {
		log.Fatalln(err)
	} else if ttl > 0 {
		fmt.Println("Yay, I still have my lock!")
	}

	// Extend my lock.
	if err := lock.Refresh(ctx, 100*time.Millisecond, nil); err != nil {
		log.Fatalln(err)
	}

	// Sleep a little longer, then check.
	time.Sleep(100 * time.Millisecond)
	if ttl, err := lock.TTL(ctx); err != nil {
		log.Fatalln(err)
	} else if ttl == 0 {
		fmt.Println("Now, my lock has expired!")
	}

}
```

## Watchdog

If you want a long lock, you can use watchdog to refresh in background atomatically.
Set an interval for watchdog shorter than TTL, it would refresh the lock before expiration,
therefore your lock won't be released until you release it explicitly.

```go
lock, err := locker.Obtain(ctx, "my-key", 100*time.Millisecond, &redislock.Options{
    Watchdog: redislock.NewTickWatchdog(50*time.Millisecond),
})
```

## Stats

Sometimes you need statistics for monitoring, telemetry, debugging, etc.

```go
stats := redislock.GetStats()
```

If you want prometheus metrics, see [redislock-prometheus](https://github.com/WqyJh/redislock-prometheus).


## Documentation

Full documentation is available on [GoDoc](http://godoc.org/github.com/bsm/redislock)
