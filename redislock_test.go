package redislock_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

func TestClient(t *testing.T) {
	rc := redisConnect(t)
	lockKey := rc.lockKey()

	// obtain
	client := New(rc)
	lock, err := client.Obtain(t.Context(), lockKey, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(t.Context())

	if exp, got := 22, len(lock.Token()); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}

	// check TTL
	assertTTL(t, lock, time.Hour)

	// try to obtain again
	_, err = client.Obtain(t.Context(), lockKey, time.Hour, nil)
	if exp, got := ErrNotObtained, err; !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}

	// manually unlock
	if err := lock.Release(t.Context()); err != nil {
		t.Fatal(err)
	}

	// lock again
	lock, err = client.Obtain(t.Context(), lockKey, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(t.Context())
}

func TestObtain(t *testing.T) {
	lock := quickObtain(t, time.Hour)
	if err := lock.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestObtain_metadata(t *testing.T) {
	rc := redisConnect(t)

	meta := "my-data"
	lock, err := Obtain(t.Context(), rc, rc.lockKey(), time.Hour, &Options{Metadata: meta})
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(t.Context())

	if exp, got := meta, lock.Metadata(); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestObtain_custom_token(t *testing.T) {
	rc := redisConnect(t)
	lockKey := rc.lockKey()

	// obtain lock
	lock1, err := Obtain(t.Context(), rc, lockKey, time.Hour, &Options{Token: "foo", Metadata: "bar"})
	if err != nil {
		t.Fatal(err)
	}
	defer lock1.Release(t.Context())

	if exp, got := "foo", lock1.Token(); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
	if exp, got := "bar", lock1.Metadata(); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}

	// try to obtain again
	_, err = Obtain(t.Context(), rc, lockKey, time.Hour, nil)
	if exp, got := ErrNotObtained, err; !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}

	// allow to re-obtain lock if token is known
	lock2, err := Obtain(t.Context(), rc, lockKey, time.Hour, &Options{Token: "foo", Metadata: "baz"})
	if err != nil {
		t.Fatal(err)
	}
	defer lock2.Release(t.Context())

	if exp, got := "foo", lock2.Token(); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
	if exp, got := "baz", lock2.Metadata(); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
}

func TestObtain_retry_success(t *testing.T) {
	rc := redisConnect(t)
	lockKey := rc.lockKey()

	// obtain for 20ms
	lock1 := quickObtain(t, 20*time.Millisecond)
	defer lock1.Release(t.Context())

	// lock again with linar retry - 3x for 20ms
	lock2, err := Obtain(t.Context(), rc, lockKey, time.Hour, &Options{
		RetryStrategy: LimitRetry(LinearBackoff(20*time.Millisecond), 3),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lock2.Release(t.Context())
}

func TestObtain_retry_failure(t *testing.T) {
	rc := redisConnect(t)
	lockKey := rc.lockKey()

	// obtain for 50ms
	lock1, err := Obtain(t.Context(), rc, lockKey, 50*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock1.Release(t.Context())

	// lock again with linar retry - 2x for 5ms
	_, err = Obtain(t.Context(), rc, lockKey, time.Hour, &Options{
		RetryStrategy: LimitRetry(LinearBackoff(5*time.Millisecond), 2),
	})
	if exp, got := ErrNotObtained, err; !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestObtain_retry_context_deadline(t *testing.T) {
	rc := redisConnect(t)
	lockKey := rc.lockKey()

	lock1, err := Obtain(t.Context(), rc, lockKey, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock1.Release(t.Context())

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	_, err = Obtain(ctx, rc, lockKey, time.Hour, &Options{
		RetryStrategy: LinearBackoff(5 * time.Millisecond),
	})
	if !errors.Is(err, ErrNotObtained) {
		t.Fatalf("expected error to wrap ErrNotObtained, got %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected error to wrap context.DeadlineExceeded, got %v", err)
	}
}

func TestObtain_concurrent(t *testing.T) {
	rc := redisConnect(t)
	lockKey := rc.lockKey()

	numLocks := int32(0)
	numThreads := 100
	wg := new(sync.WaitGroup)
	errs := make(chan error, numThreads)
	for range numThreads {
		wg.Go(func() {
			wait := rand.Int63n(int64(10 * time.Millisecond))
			time.Sleep(time.Duration(wait))

			_, err := Obtain(t.Context(), rc, lockKey, time.Minute, nil)
			if err == ErrNotObtained {
				return
			} else if err != nil {
				errs <- err
			} else {
				atomic.AddInt32(&numLocks, 1)
			}
		})
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
	lock := quickObtain(t, time.Hour)
	defer lock.Release(t.Context())

	// check TTL
	assertTTL(t, lock, time.Hour)

	// update TTL
	if err := lock.Refresh(t.Context(), time.Minute, nil); err != nil {
		t.Fatal(err)
	}

	// check TTL again
	assertTTL(t, lock, time.Minute)
}

func TestLock_Refresh_retry_success(t *testing.T) {
	lock := quickObtain(t, time.Hour)
	defer lock.Release(t.Context())

	// refresh with linear retry - 3x for 20ms
	if err := lock.Refresh(t.Context(), time.Minute, &Options{
		RetryStrategy: LimitRetry(LinearBackoff(20*time.Millisecond), 3),
	}); err != nil {
		t.Fatal(err)
	}
	assertTTL(t, lock, time.Minute)
}

func TestLock_Refresh_retry_failure(t *testing.T) {
	lock := quickObtain(t, 5*time.Millisecond)
	defer lock.Release(t.Context())

	// let the lock expire
	time.Sleep(10 * time.Millisecond)

	// refresh with linear retry - 2x for 100ms; should still return quickly
	// because a lost lock is a terminal failure rather than a retryable one.
	backoff := 100 * time.Millisecond
	start := time.Now()
	err := lock.Refresh(t.Context(), time.Hour, &Options{
		RetryStrategy: LimitRetry(LinearBackoff(backoff), 2),
	})
	if exp, got := ErrNotObtained, err; !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if elapsed := time.Since(start); elapsed >= backoff {
		t.Fatalf("expected Refresh on a lost lock to return without retrying, took %v", elapsed)
	}
}

func TestLock_Refresh_retry_transient_error(t *testing.T) {
	rc := redisConnect(t)
	flaky := &flakyScripter{Client: rc.Client}
	client := New(flaky)

	lock, err := client.Obtain(t.Context(), rc.lockKey(), time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(t.Context())

	// fail the next 2 refresh attempts; the 3rd should succeed.
	flaky.fails.Store(2)
	if err := lock.Refresh(t.Context(), time.Minute, &Options{
		RetryStrategy: LimitRetry(LinearBackoff(5*time.Millisecond), 5),
	}); err != nil {
		t.Fatalf("expected refresh to recover, got %v", err)
	}
	if remaining := flaky.fails.Load(); remaining != 0 {
		t.Fatalf("expected all injected failures to be consumed, %d remaining", remaining)
	}
	assertTTL(t, lock, time.Minute)
}

func TestLock_Refresh_retry_transient_error_exhausted(t *testing.T) {
	rc := redisConnect(t)
	flaky := &flakyScripter{Client: rc.Client}
	client := New(flaky)

	lock, err := client.Obtain(t.Context(), rc.lockKey(), time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(t.Context())

	// keep failing past the retry limit; we should see the injected error
	// rather than ErrNotObtained.
	flaky.fails.Store(10)
	err = lock.Refresh(t.Context(), time.Minute, &Options{
		RetryStrategy: LimitRetry(LinearBackoff(5*time.Millisecond), 2),
	})
	if !errors.Is(err, errFlaky) {
		t.Fatalf("expected %v, got %v", errFlaky, err)
	}
}

func TestLock_Refresh_expired(t *testing.T) {
	lock := quickObtain(t, 5*time.Millisecond)
	defer lock.Release(t.Context())

	// try releasing
	time.Sleep(10 * time.Millisecond)
	if exp, got := ErrNotObtained, lock.Refresh(t.Context(), time.Minute, nil); !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestLock_Release_expired(t *testing.T) {
	lock := quickObtain(t, 5*time.Millisecond)
	defer lock.Release(t.Context())

	// try releasing
	time.Sleep(10 * time.Millisecond)
	if exp, got := ErrLockNotHeld, lock.Release(t.Context()); !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestLock_Release_not_own(t *testing.T) {
	rc := redisConnect(t)
	lockKey := rc.lockKey()

	lock, err := Obtain(t.Context(), rc, lockKey, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(t.Context())

	// manually transfer ownership
	if err := rc.Set(t.Context(), lockKey, "ABCD", 0).Err(); err != nil {
		t.Fatal(err)
	}

	cmd := rc.Get(t.Context(), lockKey)
	v, err := cmd.Result()
	if err != nil {
		t.Fatal(err)
	}
	if v != "ABCD" {
		t.Fatalf("expected %v, got %v", "ABCD", v)
	}

	// try releasing
	if exp, got := ErrLockNotHeld, lock.Release(t.Context()); !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestLock_Release_not_held(t *testing.T) {
	rc := redisConnect(t)
	lockKey := rc.lockKey()

	lock1, err := Obtain(t.Context(), rc, lockKey, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock1.Release(t.Context())

	lock2, err := Obtain(t.Context(), rc, lockKey, time.Minute, nil)
	if exp, got := ErrNotObtained, err; !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := (*Lock)(nil), lock2; exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := ErrLockNotHeld, lock2.Release(t.Context()); !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestLock_ObtainMulti(t *testing.T) {
	rc := redisConnect(t)
	lockKey1 := rc.lockKey("_MultiLock_1")
	lockKey2 := rc.lockKey("_MultiLock_2")
	lockKey3 := rc.lockKey("_MultiLock_3")
	lockKey4 := rc.lockKey("_MultiLock_4")

	// 1. Obtain lock 1 and 2
	lock12, err := ObtainMulti(t.Context(), rc, []string{lockKey1, lockKey2}, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Obtain lock 3 and 4
	lock34, err := ObtainMulti(t.Context(), rc, []string{lockKey3, lockKey4}, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}

	// 3. Try to obtain lock 2 and 3
	_, err = ObtainMulti(t.Context(), rc, []string{lockKey2, lockKey3}, time.Hour, nil)
	// Expect it to fail since lock 2 and 3 are already locked.
	if !errors.Is(err, ErrNotObtained) {
		t.Fatalf("expected ErrNotObtained, got %s.", err)
	}

	// 4. Release lock 1 and 2
	lock12.Release(t.Context())

	// 5. Obtain lock 1
	lock1, err := ObtainMulti(t.Context(), rc, []string{lockKey1}, time.Hour, nil)
	// Expected to succeed since lock 1 was released (along with lock 2)
	if err != nil {
		t.Fatal(err)
	}
	defer lock1.Release(t.Context())

	// 6. Try to obtain lock 2 and 3 (again)
	_, err = ObtainMulti(t.Context(), rc, []string{lockKey2, lockKey3}, time.Hour, nil)
	// Expect it to fail since lock 3 is still locked.
	if !errors.Is(err, ErrNotObtained) {
		t.Fatalf("expected ErrNotObtained, got %s.", err)
	}

	// 7. Release lock 3 and 4
	lock34.Release(t.Context())

	// 8. Try to obtain lock 2 and 3 (again)
	lock23, err := ObtainMulti(t.Context(), rc, []string{lockKey2, lockKey3}, time.Hour, nil)
	// Expect it to succeed since lock 2 and 3 are available.
	if err != nil {
		t.Fatal(err)
	}

	defer lock23.Release(t.Context())
}

// ----------------------------------------------------------------------------

var lockInc = new(atomic.Int64)
var redisOpts = &redis.Options{
	Network: "tcp",
	Addr:    "127.0.0.1:6379",
	DB:      9,
}

type testRedis struct {
	*redis.Client
	keys []string
}

func (rc *testRedis) lockKey(suffix ...string) string {
	key := fmt.Sprintf("__bsm_redislock_%d__", lockInc.Add(1))
	if len(suffix) > 0 {
		key += suffix[0]
	}
	rc.keys = append(rc.keys, key)
	return key
}

func redisConnect(t *testing.T) *testRedis {
	t.Helper()

	rc := &testRedis{Client: redis.NewClient(redisOpts)}
	t.Cleanup(func() {
		// t.Context() is canceled before Cleanup runs, so use a fresh context
		ctx := context.Background()
		if len(rc.keys) > 0 {
			_ = rc.Del(ctx, rc.keys...).Err()
		}
		_ = rc.Close()
	})

	return rc
}

func quickObtain(t *testing.T, ttl time.Duration) *Lock {
	t.Helper()

	rc := redisConnect(t)
	lock, err := Obtain(t.Context(), rc, rc.lockKey(), ttl, nil)
	if err != nil {
		t.Fatal(err)
	}
	return lock
}

func assertTTL(t *testing.T, lock *Lock, exp time.Duration) {
	t.Helper()

	ttl, err := lock.TTL(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	delta := ttl - exp
	if delta < 0 {
		delta = -delta
	}
	if delta > time.Second {
		t.Fatalf("expected ~%v, got %v", exp, ttl)
	}
}

var errFlaky = errors.New("flaky scripter: injected failure")

// flakyScripter wraps a redis.Client and fails the next `fails` script
// invocations before delegating to the wrapped client.
type flakyScripter struct {
	*redis.Client
	fails atomic.Int32
}

func (f *flakyScripter) shouldFail() bool {
	for {
		n := f.fails.Load()
		if n <= 0 {
			return false
		}
		if f.fails.CompareAndSwap(n, n-1) {
			return true
		}
	}
}

func (f *flakyScripter) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	if f.shouldFail() {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(errFlaky)
		return cmd
	}
	return f.Client.Eval(ctx, script, keys, args...)
}

func (f *flakyScripter) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	if f.shouldFail() {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(errFlaky)
		return cmd
	}
	return f.Client.EvalSha(ctx, sha1, keys, args...)
}
