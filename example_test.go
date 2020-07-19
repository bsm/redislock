package redislock_test

import (
	"fmt"
	"log"
	"time"

	"github.com/bsm/redislock"
	"github.com/garyburd/redigo/redis"
)

func Example() {
	// Connect to redis.
	client := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			// TODO: connect to database bsaed on the client id
			return redis.Dial("tcp", ":6379",
				redis.DialDatabase(1))
		},
	}

	// Create a new lock client.
	locker := redislock.NewMin(client)

	// Try to obtain lock.
	lock, err := locker.Obtain("my-key", 100*time.Millisecond, nil)
	if err == redislock.ErrNotObtained {
		fmt.Println("Could not obtain lock!")
	} else if err != nil {
		log.Fatalln(err)
	}

	// Don't forget to defer Release.
	defer lock.Release()
	fmt.Println("I have a lock!")

	// Sleep and check the remaining TTL.
	time.Sleep(50 * time.Millisecond)
	if ttl, err := lock.TTL(); err != nil {
		log.Fatalln(err)
	} else if ttl > 0 {
		fmt.Println("Yay, I still have my lock!")
	}

	// Extend my lock.
	if err := lock.Refresh(100*time.Millisecond, nil); err != nil {
		log.Fatalln(err)
	}

	// Sleep a little longer, then check.
	time.Sleep(100 * time.Millisecond)
	if ttl, err := lock.TTL(); err != nil {
		log.Fatalln(err)
	} else if ttl == 0 {
		fmt.Println("Now, my lock has expired!")
	}

	// Output:
	// I have a lock!
	// Yay, I still have my lock!
	// Now, my lock has expired!
}
