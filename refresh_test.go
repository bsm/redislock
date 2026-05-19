package redislock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const loadingErr = "LOADING Redis is loading the dataset in memory"

func setupMiniRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close(); mr.Close() })
	return mr, rc
}

func setLock(t *testing.T, rc *redis.Client, keys []string, val string, ttl time.Duration) *Lock {
	t.Helper()
	for _, k := range keys {
		if err := rc.Set(t.Context(), k, val, ttl).Err(); err != nil {
			t.Fatal(err)
		}
	}
	return &Lock{Client: &Client{client: rc}, keys: keys, value: val, tokenLen: len(val)}
}

func assertTTL(t *testing.T, rc *redis.Client, key string, lo, hi time.Duration) {
	t.Helper()
	d, err := rc.PTTL(t.Context(), key).Result()
	if err != nil || d < lo || d > hi {
		t.Fatalf("key %s ttl %v, want [%v,%v], err=%v", key, d, lo, hi, err)
	}
}

func refreshCtx(t *testing.T, ttl time.Duration) context.Context {
	if ttl > 0 {
		return t.Context()
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	t.Cleanup(cancel)
	return ctx
}

// setRedisErrors enables miniredis error injection. errFor > 0 auto-clears after that
// duration; errFor < 0 stays until cleanup. The returned func stops any timer and
// clears the error — call it before the Miniredis instance is closed.
func setRedisErrors(mr *miniredis.Miniredis, errFor time.Duration) (cleanup func()) {
	mr.SetError(loadingErr)
	var timer *time.Timer
	if errFor > 0 {
		timer = time.AfterFunc(errFor, func() { mr.SetError("") })
	}
	return func() {
		if timer != nil {
			timer.Stop()
		}
		mr.SetError("")
	}
}

type refreshEnv struct {
	t  *testing.T
	mr *miniredis.Miniredis
	rc *redis.Client
}

type refreshCase struct {
	name    string
	setup   func(refreshEnv) *Lock
	ttl     time.Duration
	opt     *Options
	errFor  time.Duration // 0=off, >0=brief, <0=persistent
	wantErr func(error) bool
	verify  func(refreshEnv)
}

func wantOK(err error) bool          { return err == nil }
func wantNotObtained(err error) bool { return errors.Is(err, ErrNotObtained) }
func wantLoading(err error) bool {
	return err != nil && strings.Contains(err.Error(), "LOADING")
}

func runRefreshCase(t *testing.T, tc refreshCase) {
	t.Helper()
	mr, rc := setupMiniRedis(t)
	env := refreshEnv{t: t, mr: mr, rc: rc}

	lock := tc.setup(env)
	if tc.errFor != 0 {
		defer setRedisErrors(mr, tc.errFor)()
	}

	err := lock.Refresh(refreshCtx(t, tc.ttl), tc.ttl, tc.opt)

	want := tc.wantErr
	if want == nil {
		want = wantOK
	}
	if !want(err) {
		t.Fatalf("%s: refresh error %v", tc.name, err)
	}
	if wantOK(err) && tc.verify != nil {
		tc.verify(env)
	}
}

func setupFreshLock(e refreshEnv) *Lock {
	return setLock(e.t, e.rc, []string{"k"}, "v", time.Hour)
}

func setupExpiredLock(e refreshEnv) *Lock {
	l := setLock(e.t, e.rc, []string{"k"}, "v", time.Millisecond)
	e.mr.FastForward(10 * time.Millisecond)
	return l
}

func verifyTTL(key string, lo, hi time.Duration) func(refreshEnv) {
	return func(e refreshEnv) { assertTTL(e.t, e.rc, key, lo, hi) }
}

func verifyKeyGone(forward time.Duration) func(refreshEnv) {
	return func(e refreshEnv) {
		e.mr.FastForward(forward)
		n, err := e.rc.Exists(e.t.Context(), "k").Result()
		if err != nil || n != 0 {
			e.t.Fatalf("key still exists: n=%d err=%v", n, err)
		}
	}
}

func TestRefresh(t *testing.T) {
	linear1ms := LinearBackoff(time.Millisecond)
	retry3 := &Options{RetryStrategy: LimitRetry(linear1ms, 3)}
	retry5 := &Options{RetryStrategy: LimitRetry(linear1ms, 5)}
	retry2 := &Options{RetryStrategy: LimitRetry(linear1ms, 2)}
	backoff200ms := &Options{RetryStrategy: LinearBackoff(200 * time.Millisecond)}

	cases := []refreshCase{
		{name: "nil receiver", setup: func(e refreshEnv) *Lock { return nil }, ttl: time.Minute, wantErr: wantNotObtained},
		{
			name: "success sets TTL", setup: setupFreshLock, ttl: time.Minute,
			verify: verifyTTL("k", 59*time.Second, time.Minute),
		},
		{
			name:  "extends TTL",
			setup: func(e refreshEnv) *Lock { return setLock(e.t, e.rc, []string{"k"}, "v", time.Minute) },
			ttl:   time.Hour, opt: &Options{}, verify: verifyTTL("k", 59*time.Minute, time.Hour),
		},
		{name: "expired lock", setup: setupExpiredLock, ttl: time.Minute, wantErr: wantNotObtained},
		{name: "expired with retry", setup: setupExpiredLock, ttl: time.Minute, opt: retry3, wantErr: wantNotObtained},
		{
			name: "value mismatch",
			setup: func(e refreshEnv) *Lock {
				l := setLock(e.t, e.rc, []string{"k"}, "v", time.Hour)
				e.rc.Set(e.t.Context(), "k", "other", time.Hour)
				return l
			},
			ttl: time.Minute, wantErr: wantNotObtained,
		},
		{
			name: "deleted key",
			setup: func(e refreshEnv) *Lock {
				l := setLock(e.t, e.rc, []string{"k"}, "v", time.Hour)
				e.rc.Del(e.t.Context(), "k")
				return l
			},
			ttl: time.Minute, wantErr: wantNotObtained,
		},
		{
			name: "sequential refreshes", setup: setupFreshLock, ttl: 30 * time.Second,
			verify: func(e refreshEnv) {
				verifyTTL("k", 29*time.Second, 30*time.Second)(e)
				l := &Lock{Client: &Client{client: e.rc}, keys: []string{"k"}, value: "v", tokenLen: 1}
				if err := l.Refresh(e.t.Context(), 2*time.Minute, nil); err != nil {
					e.t.Fatal(err)
				}
				verifyTTL("k", 119*time.Second, 2*time.Minute)(e)
			},
		},
		{
			name: "preserves value", setup: func(e refreshEnv) *Lock {
				return setLock(e.t, e.rc, []string{"k"}, "tok+meta", time.Hour)
			},
			ttl: time.Minute,
			verify: func(e refreshEnv) {
				v, err := e.rc.Get(e.t.Context(), "k").Result()
				if err != nil || v != "tok+meta" {
					e.t.Fatalf("got %q err=%v", v, err)
				}
			},
		},
		{name: "small TTL expires", setup: setupFreshLock, ttl: time.Millisecond, verify: verifyKeyGone(10 * time.Millisecond)},
		{name: "zero TTL expires", setup: setupFreshLock, ttl: 0, verify: verifyKeyGone(time.Millisecond)},
		{
			name: "multi-key missing",
			setup: func(e refreshEnv) *Lock {
				l := setLock(e.t, e.rc, []string{"a", "b"}, "v", time.Hour)
				e.rc.Del(e.t.Context(), "b")
				return l
			},
			ttl: time.Minute, wantErr: wantNotObtained,
		},
		{
			name: "multi-key wrong value",
			setup: func(e refreshEnv) *Lock {
				l := setLock(e.t, e.rc, []string{"a", "b"}, "v", time.Hour)
				e.rc.Set(e.t.Context(), "a", "x", time.Hour)
				return l
			},
			ttl: time.Minute, wantErr: wantNotObtained,
		},
		{
			name: "ctx deadline during backoff", setup: setupExpiredLock,
			ttl: time.Minute, opt: backoff200ms, wantErr: wantNotObtained,
		},
		{
			name: "recovers from transient errors", setup: setupFreshLock,
			ttl: time.Minute, opt: retry5, errFor: 5 * time.Millisecond,
			verify: verifyTTL("k", 59*time.Second, time.Minute),
		},
		{
			name: "exhausts retries returns last error", setup: setupFreshLock,
			ttl: time.Minute, opt: retry2, errFor: -1, wantErr: wantLoading,
		},
		{
			name: "NoRetry returns redis error", setup: setupFreshLock,
			ttl: time.Minute, opt: &Options{RetryStrategy: NoRetry()}, errFor: -1, wantErr: wantLoading,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { runRefreshCase(t, tc) })
	}

	t.Run("ctx cancelled during retry", func(t *testing.T) {
		mr, rc := setupMiniRedis(t)
		lock := setLock(t, rc, []string{"k"}, "v", time.Hour)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mr.SetError(loadingErr)
		timer := time.AfterFunc(3*time.Millisecond, func() { cancel(); mr.SetError("") })
		defer func() {
			timer.Stop()
			mr.SetError("")
		}()
		err := lock.Refresh(ctx, time.Minute, &Options{RetryStrategy: linear1ms})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("NoRetry calls NextBackoff once on transient failure", func(t *testing.T) {
		mr, rc := setupMiniRedis(t)
		lock := setLock(t, rc, []string{"k"}, "v", time.Hour)
		defer setRedisErrors(mr, -1)()
		calls := 0
		spy := &spyRetry{inner: NoRetry(), onCall: func() { calls++ }}
		err := lock.Refresh(t.Context(), time.Minute, &Options{RetryStrategy: spy})
		if calls != 1 || !wantLoading(err) {
			t.Fatalf("calls=%d err=%v", calls, err)
		}
	})

	t.Run("success never calls NextBackoff", func(t *testing.T) {
		_, rc := setupMiniRedis(t)
		lock := setLock(t, rc, []string{"k"}, "v", time.Hour)
		calls := 0
		spy := &spyRetry{inner: linear1ms, onCall: func() { calls++ }}
		err := lock.Refresh(t.Context(), time.Minute, &Options{RetryStrategy: spy})
		if err != nil || calls != 0 {
			t.Fatalf("calls=%d err=%v", calls, err)
		}
	})
}

type spyRetry struct {
	inner  RetryStrategy
	onCall func()
}

func (s *spyRetry) NextBackoff() time.Duration {
	s.onCall()
	return s.inner.NextBackoff()
}
