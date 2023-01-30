package lock_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/iSerganov/redislock/v1/lock"
	"github.com/redis/go-redis/v9"
)

func Example() {
	// Connect to redis.
	client := redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    "127.0.0.1:6379",
	})
	defer client.Close()

	// Create a new lock client.
	locker := lock.New(client)

	ctx := context.Background()

	// Try to obtain lock.
	l, err := locker.Obtain(ctx, "my-key", 100*time.Millisecond, nil)
	if errors.Is(err, &lock.ErrNotObtained{}) {
		fmt.Println("Could not obtain lock!")
	} else if err != nil {
		log.Fatalln(err)
	}

	// Don't forget to defer Release.
	defer l.Release(ctx)
	fmt.Println("I have a lock!")

	// Sleep and check the remaining TTL.
	time.Sleep(50 * time.Millisecond)
	if ttl, err := l.TTL(ctx); err != nil {
		log.Fatalln(err)
	} else if ttl > 0 {
		fmt.Println("Yay, I still have my lock!")
	}

	// Extend my lock.
	if err := l.Refresh(ctx, 100*time.Millisecond, nil); err != nil {
		log.Fatalln(err)
	}

	// Sleep a little longer, then check.
	time.Sleep(100 * time.Millisecond)
	if ttl, err := l.TTL(ctx); err != nil {
		log.Fatalln(err)
	} else if ttl == 0 {
		fmt.Println("Now, my lock has expired!")
	}

	// Output:
	// I have a lock!
	// Yay, I still have my lock!
	// Now, my lock has expired!
}

func ExampleClient_Obtain_retry() {
	client := redis.NewClient(&redis.Options{Network: "tcp", Addr: "127.0.0.1:6379"})
	defer client.Close()

	locker := lock.New(client)

	ctx := context.Background()

	// Retry every 100ms, for up-to 3x
	backoff := lock.LimitRetry(lock.LinearBackoff(100*time.Millisecond), 3)

	// Obtain lock with retry
	l, err := locker.Obtain(ctx, "my-key", time.Second, &lock.Options{
		RetryStrategy: backoff,
	})
	if errors.Is(err, &lock.ErrNotObtained{}) {
		fmt.Println("Could not obtain lock!")
	} else if err != nil {
		log.Fatalln(err)
	}
	defer l.Release(ctx)

	fmt.Println("I have a lock!")
}

func ExampleClient_Obtain_customDeadline() {
	client := redis.NewClient(&redis.Options{Network: "tcp", Addr: "127.0.0.1:6379"})
	defer client.Close()

	locker := lock.New(client)

	// Retry every 500ms, for up-to a minute
	backoff := lock.LinearBackoff(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()

	// Obtain lock with retry + custom deadline
	l, err := locker.Obtain(ctx, "my-key", time.Second, &lock.Options{
		RetryStrategy: backoff,
	})
	if errors.Is(err, &lock.ErrNotObtained{}) {
		fmt.Println("Could not obtain lock!")
	} else if err != nil {
		log.Fatalln(err)
	}
	defer l.Release(context.Background())

	fmt.Println("I have a lock!")
}
