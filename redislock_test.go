package redislock_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

var redisOpts = &redis.Options{
	Network: "tcp",
	Addr:    "127.0.0.1:6379",
	DB:      9,
}

func TestClient(t *testing.T) {
	lockKey := getLockKey()
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc, lockKey)

	// init client
	client := New(rc)

	// obtain
	lock, err := client.Obtain(ctx, lockKey, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(ctx)

	if exp, got := 22, len(lock.Token()); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}

	// check TTL
	assertTTL(t, lock, time.Hour)

	// try to obtain again
	_, err = client.Obtain(ctx, lockKey, time.Hour, nil)
	if exp, got := ErrNotObtained, err; !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}

	// manually unlock
	if err := lock.Release(ctx); err != nil {
		t.Fatal(err)
	}

	// lock again
	lock, err = client.Obtain(ctx, lockKey, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(ctx)
}

func TestObtain(t *testing.T) {
	lockKey := getLockKey()
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc, lockKey)

	lock := quickObtain(t, rc, lockKey, time.Hour)
	if err := lock.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestObtain_metadata(t *testing.T) {
	lockKey := getLockKey()
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc, lockKey)

	meta := "my-data"
	lock, err := Obtain(ctx, rc, lockKey, time.Hour, &Options{Metadata: meta})
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(ctx)

	if exp, got := meta, lock.Metadata(); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestObtain_custom_token(t *testing.T) {
	lockKey := getLockKey()
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc, lockKey)

	// obtain lock
	lock1, err := Obtain(ctx, rc, lockKey, time.Hour, &Options{Token: "foo", Metadata: "bar"})
	if err != nil {
		t.Fatal(err)
	}
	defer lock1.Release(ctx)

	if exp, got := "foo", lock1.Token(); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
	if exp, got := "bar", lock1.Metadata(); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}

	// try to obtain again
	_, err = Obtain(ctx, rc, lockKey, time.Hour, nil)
	if exp, got := ErrNotObtained, err; !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}

	// allow to re-obtain lock if token is known
	lock2, err := Obtain(ctx, rc, lockKey, time.Hour, &Options{Token: "foo", Metadata: "baz"})
	if err != nil {
		t.Fatal(err)
	}
	defer lock2.Release(ctx)

	if exp, got := "foo", lock2.Token(); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
	if exp, got := "baz", lock2.Metadata(); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
}

func TestObtain_retry_success(t *testing.T) {
	lockKey := getLockKey()
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc, lockKey)

	// obtain for 20ms
	lock1 := quickObtain(t, rc, lockKey, 20*time.Millisecond)
	defer lock1.Release(ctx)

	// lock again with linar retry - 3x for 20ms
	lock2, err := Obtain(ctx, rc, lockKey, time.Hour, &Options{
		RetryStrategy: LimitRetry(LinearBackoff(20*time.Millisecond), 3),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lock2.Release(ctx)
}

func TestObtain_retry_failure(t *testing.T) {
	lockKey := getLockKey()
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc, lockKey)

	// obtain for 50ms
	lock1 := quickObtain(t, rc, lockKey, 50*time.Millisecond)
	defer lock1.Release(ctx)

	// lock again with linar retry - 2x for 5ms
	_, err := Obtain(ctx, rc, lockKey, time.Hour, &Options{
		RetryStrategy: LimitRetry(LinearBackoff(5*time.Millisecond), 2),
	})
	if exp, got := ErrNotObtained, err; !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestObtain_concurrent(t *testing.T) {
	lockKey := getLockKey()
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc, lockKey)

	numLocks := int32(0)
	numThreads := 100
	wg := new(sync.WaitGroup)
	errs := make(chan error, numThreads)
	for i := 0; i < numThreads; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			wait := rand.Int63n(int64(10 * time.Millisecond))
			time.Sleep(time.Duration(wait))

			_, err := Obtain(ctx, rc, lockKey, time.Minute, nil)
			if err == ErrNotObtained {
				return
			} else if err != nil {
				errs <- err
			} else {
				atomic.AddInt32(&numLocks, 1)
			}
		}()
	}
	wg.Wait()

	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if exp, got := 1, int(numLocks); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestLock_Refresh(t *testing.T) {
	lockKey := getLockKey()
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc, lockKey)

	lock := quickObtain(t, rc, lockKey, time.Hour)
	defer lock.Release(ctx)

	// check TTL
	assertTTL(t, lock, time.Hour)

	// update TTL
	if err := lock.Refresh(ctx, time.Minute, nil); err != nil {
		t.Fatal(err)
	}

	// check TTL again
	assertTTL(t, lock, time.Minute)
}

func TestLock_Refresh_expired(t *testing.T) {
	lockKey := getLockKey()
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc, lockKey)

	lock := quickObtain(t, rc, lockKey, 5*time.Millisecond)
	defer lock.Release(ctx)

	// try releasing
	time.Sleep(10 * time.Millisecond)
	if exp, got := ErrNotObtained, lock.Refresh(ctx, time.Minute, nil); !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestLock_Release_expired(t *testing.T) {
	lockKey := getLockKey()
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc, lockKey)

	lock := quickObtain(t, rc, lockKey, 5*time.Millisecond)
	defer lock.Release(ctx)

	// try releasing
	time.Sleep(10 * time.Millisecond)
	if exp, got := ErrLockNotHeld, lock.Release(ctx); !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestLock_Release_not_own(t *testing.T) {
	lockKey := getLockKey()
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc, lockKey)

	lock := quickObtain(t, rc, lockKey, time.Hour)
	defer lock.Release(ctx)

	// manually transfer ownership
	if err := rc.Set(ctx, lockKey, "ABCD", 0).Err(); err != nil {
		t.Fatal(err)
	}

	cmd := rc.Get(ctx, lockKey)
	v, err := cmd.Result()
	if err != nil {
		t.Fatal(err)
	}
	if v != "ABCD" {
		t.Fatalf("expected %v, got %v", "ABCD", v)
	}

	// try releasing
	if exp, got := ErrLockNotHeld, lock.Release(ctx); !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestLock_Release_not_held(t *testing.T) {
	lockKey := getLockKey()
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc, lockKey)

	lock1 := quickObtain(t, rc, lockKey, time.Hour)
	defer lock1.Release(ctx)

	lock2, err := Obtain(context.Background(), rc, lockKey, time.Minute, nil)
	if exp, got := ErrNotObtained, err; !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := (*Lock)(nil), lock2; exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := ErrLockNotHeld, lock2.Release(ctx); !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestLock_ObtainMulti(t *testing.T) {
	lockKeys := []string{
		getLockKey() + "_MultiLock_1",
		getLockKey() + "_MultiLock_2",
		getLockKey() + "_MultiLock_3",
		getLockKey() + "_MultiLock_4",
	}
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc, lockKeys...)

	lockKey1 := lockKeys[0]
	lockKey2 := lockKeys[1]
	lockKey3 := lockKeys[2]
	lockKey4 := lockKeys[3]

	// 1. Obtain lock 1 and 2
	lock12, err := ObtainMulti(ctx, rc, []string{lockKey1, lockKey2}, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Obtain lock 3 and 4
	lock34, err := ObtainMulti(ctx, rc, []string{lockKey3, lockKey4}, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}

	// 3. Try to obtain lock 2 and 3
	_, err = ObtainMulti(ctx, rc, []string{lockKey2, lockKey3}, time.Hour, nil)
	// Expect it to fail since lock 2 and 3 are already locked.
	if !errors.Is(err, ErrNotObtained) {
		t.Fatalf("expected ErrNotObtained, got %s.", err)
	}

	// 4. Release lock 1 and 2
	lock12.Release(ctx)

	// 5. Obtain lock 1
	lock1, err := ObtainMulti(ctx, rc, []string{lockKey1}, time.Hour, nil)
	// Expected to succeed since lock 1 was released (along with lock 2)
	if err != nil {
		t.Fatal(err)
	}
	defer lock1.Release(ctx)

	// 6. Try to obtain lock 2 and 3 (again)
	_, err = ObtainMulti(ctx, rc, []string{lockKey2, lockKey3}, time.Hour, nil)
	// Expect it to fail since lock 3 is still locked.
	if !errors.Is(err, ErrNotObtained) {
		t.Fatalf("expected ErrNotObtained, got %s.", err)
	}

	// 7. Release lock 3 and 4
	lock34.Release(ctx)

	// 8. Try to obtain lock 2 and 3 (again)
	lock23, err := ObtainMulti(ctx, rc, []string{lockKey2, lockKey3}, time.Hour, nil)
	// Expect it to succeed since lock 2 and 3 are available.
	if err != nil {
		t.Fatal(err)
	}

	defer lock23.Release(ctx)
}

func getLockKey() string {
	return fmt.Sprintf("__bsm_redislock_%s_%d__", getCallingFunctionName(1), time.Now().UnixNano())
}

func quickObtain(t *testing.T, rc *redis.Client, lockKey string, ttl time.Duration) *Lock {
	t.Helper()

	lock, err := Obtain(context.Background(), rc, lockKey, ttl, nil)
	if err != nil {
		t.Fatal(err)
	}
	return lock
}

func assertTTL(t *testing.T, lock *Lock, exp time.Duration) {
	t.Helper()

	ttl, err := lock.TTL(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	delta := ttl - exp
	if delta < 0 {
		delta = 1 - delta
	}
	if delta > time.Second {
		t.Fatalf("expected ~%v, got %v", exp, ttl)
	}
}

func teardown(t *testing.T, rc *redis.Client, lockKeys ...string) {
	t.Helper()

	for _, lockKey := range lockKeys {
		if err := rc.Del(context.Background(), lockKey).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
}

func getCallingFunctionName(skipFrameCount int) string {
	fpc, _, _, _ := runtime.Caller(1 + skipFrameCount)
	funcName := "unknown"
	fun := runtime.FuncForPC(fpc)
	if fun != nil {
		_, funcName = filepath.Split(fun.Name())
	}
	return funcName
}
