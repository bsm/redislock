package redislock_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis"

	"github.com/bsm/redislock"
)

// more user and perUserLoop will give redis a large pressure
const userNumber = 400
const perUserLoop = 4

func TestRetryStrategy(t *testing.T) {
	redisClient = redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    "127.0.0.1:6379", DB: 9,
	})

	t.Run("linear", func(t *testing.T) {
		bench(t, redisClient, "linear")
	})

	t.Run("step", func(t *testing.T) {
		bench(t, redisClient, "step")
	})
}

func bench(t *testing.T, client redislock.RedisClient, flag string) {
	wg := &sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < userNumber; i++ {
		go userDoSomethingFast(t, wg, client, i, flag)
	}
	wg.Wait()
	dt := time.Now().Sub(start)
	t.Logf("total cost:%s\n", dt)
}

func userDoSomethingFast(t *testing.T, wg *sync.WaitGroup, client redislock.RedisClient, index int, flag string) {
	userKey := fmt.Sprintf("__bsm_user_lock_%d", index)
	for i := 0; i < perUserLoop; i++ {
		go doWithLock(t, wg, client, userKey, flag)
	}
}

func doWithLock(t *testing.T, wg *sync.WaitGroup, client redislock.RedisClient, key, flag string) {
	wg.Add(1)
	defer wg.Done()
	var lock *redislock.Lock
	var err error
	switch flag {
	case "linear":
		lock, err = redislock.Obtain(client, key, time.Hour, &redislock.Options{RetryStrategy: redislock.NewLinearRetry(100 * time.Millisecond)})
	case "step":
		lock, err = redislock.Obtain(client, key, time.Hour, &redislock.Options{RetryStrategy: redislock.NewStepRetry()})
	}
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 10)
	lock.Release()
}
