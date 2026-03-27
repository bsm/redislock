package redislock

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupMiniRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close(); mr.Close() })
	return mr, rc
}

func setLock(t *testing.T, rc *redis.Client, keys []string, value string, ttl time.Duration) *Lock {
	t.Helper()
	for _, k := range keys {
		if err := rc.Set(context.Background(), k, value, ttl).Err(); err != nil {
			t.Fatalf("SET %q: %v", k, err)
		}
	}
	return &Lock{Client: &Client{client: rc}, keys: keys, value: value, tokenLen: len(value)}
}

func assertTTLRange(t *testing.T, rc *redis.Client, key string, lo, hi time.Duration) {
	t.Helper()
	d, err := rc.PTTL(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	if d < lo || d > hi {
		t.Fatalf("key %s: expected TTL in [%v, %v], got %v", key, lo, hi, d)
	}
}

func assertKeyGone(t *testing.T, rc *redis.Client, key string) {
	t.Helper()
	n, err := rc.Exists(context.Background(), key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected key %s to be gone", key)
	}
}

func TestRefresh(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T, mr *miniredis.Miniredis, rc *redis.Client) *Lock
		ttl     time.Duration
		opt     *Options
		wantErr error
		verify  func(t *testing.T, mr *miniredis.Miniredis, rc *redis.Client)
	}{
		{
			name:    "nil receiver",
			setup:   func(*testing.T, *miniredis.Miniredis, *redis.Client) *Lock { return nil },
			ttl:     time.Minute,
			wantErr: ErrNotObtained,
		},
		{
			name: "success with nil options sets new TTL",
			setup: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) *Lock {
				return setLock(t, rc, []string{"k"}, "v", time.Hour)
			},
			ttl: time.Minute,
			verify: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) {
				assertTTLRange(t, rc, "k", 59*time.Second, time.Minute)
			},
		},
		{
			name: "extends TTL",
			setup: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) *Lock {
				return setLock(t, rc, []string{"k"}, "v", time.Minute)
			},
			ttl: time.Hour, opt: &Options{},
			verify: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) {
				assertTTLRange(t, rc, "k", 59*time.Minute, time.Hour)
			},
		},
		{
			name: "expired lock",
			setup: func(t *testing.T, mr *miniredis.Miniredis, rc *redis.Client) *Lock {
				l := setLock(t, rc, []string{"k"}, "v", time.Millisecond)
				mr.FastForward(10 * time.Millisecond)
				return l
			},
			ttl: time.Minute, wantErr: ErrNotObtained,
		},
		{
			name: "value mismatch (taken by someone else)",
			setup: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) *Lock {
				l := setLock(t, rc, []string{"k"}, "v", time.Hour)
				rc.Set(context.Background(), "k", "other", time.Hour)
				return l
			},
			ttl: time.Minute, wantErr: ErrNotObtained,
		},
		{
			name: "deleted key",
			setup: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) *Lock {
				l := setLock(t, rc, []string{"k"}, "v", time.Hour)
				rc.Del(context.Background(), "k")
				return l
			},
			ttl: time.Minute, wantErr: ErrNotObtained,
		},
		{
			name: "expired lock with NoRetry",
			setup: func(t *testing.T, mr *miniredis.Miniredis, rc *redis.Client) *Lock {
				l := setLock(t, rc, []string{"k"}, "v", time.Millisecond)
				mr.FastForward(10 * time.Millisecond)
				return l
			},
			ttl: time.Minute, opt: &Options{RetryStrategy: NoRetry()}, wantErr: ErrNotObtained,
		},
		{
			name: "expired lock with retry strategy still fails",
			setup: func(t *testing.T, mr *miniredis.Miniredis, rc *redis.Client) *Lock {
				l := setLock(t, rc, []string{"k"}, "v", time.Millisecond)
				mr.FastForward(10 * time.Millisecond)
				return l
			},
			ttl: time.Minute, opt: &Options{RetryStrategy: LimitRetry(LinearBackoff(time.Millisecond), 3)}, wantErr: ErrNotObtained,
		},
		{
			name: "sequential refreshes",
			setup: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) *Lock {
				return setLock(t, rc, []string{"k"}, "v", time.Hour)
			},
			ttl: 30 * time.Second,
			verify: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) {
				assertTTLRange(t, rc, "k", 29*time.Second, 30*time.Second)
				l := &Lock{Client: &Client{client: rc}, keys: []string{"k"}, value: "v", tokenLen: 1}
				if err := l.Refresh(context.Background(), 2*time.Minute, nil); err != nil {
					t.Fatal(err)
				}
				assertTTLRange(t, rc, "k", 119*time.Second, 2*time.Minute)
			},
		},
		{
			name: "preserves value",
			setup: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) *Lock {
				return setLock(t, rc, []string{"k"}, "tok+meta", time.Hour)
			},
			ttl: time.Minute,
			verify: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) {
				v, _ := rc.Get(context.Background(), "k").Result()
				if v != "tok+meta" {
					t.Fatalf("expected %q, got %q", "tok+meta", v)
				}
			},
		},
		{
			name: "small TTL expires quickly",
			setup: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) *Lock {
				return setLock(t, rc, []string{"k"}, "v", time.Hour)
			},
			ttl: time.Millisecond,
			verify: func(t *testing.T, mr *miniredis.Miniredis, rc *redis.Client) {
				mr.FastForward(10 * time.Millisecond)
				assertKeyGone(t, rc, "k")
			},
		},
		{
			name: "zero TTL expires immediately",
			setup: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) *Lock {
				return setLock(t, rc, []string{"k"}, "v", time.Hour)
			},
			ttl: 0,
			verify: func(t *testing.T, mr *miniredis.Miniredis, rc *redis.Client) {
				mr.FastForward(time.Millisecond)
				assertKeyGone(t, rc, "k")
			},
		},
		{
			name: "multi-key fails when one key missing",
			setup: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) *Lock {
				l := setLock(t, rc, []string{"a", "b"}, "v", time.Hour)
				rc.Del(context.Background(), "b")
				return l
			},
			ttl: time.Minute, wantErr: ErrNotObtained,
		},
		{
			name: "multi-key fails when one key has wrong value",
			setup: func(t *testing.T, _ *miniredis.Miniredis, rc *redis.Client) *Lock {
				l := setLock(t, rc, []string{"a", "b"}, "v", time.Hour)
				rc.Set(context.Background(), "a", "x", time.Hour)
				return l
			},
			ttl: time.Minute, wantErr: ErrNotObtained,
		},
		{
			name: "context deadline during retry backoff",
			setup: func(t *testing.T, mr *miniredis.Miniredis, rc *redis.Client) *Lock {
				l := setLock(t, rc, []string{"k"}, "v", time.Millisecond)
				mr.FastForward(10 * time.Millisecond)
				return l
			},
			ttl: time.Minute, opt: &Options{RetryStrategy: LinearBackoff(200 * time.Millisecond)}, wantErr: ErrNotObtained,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mr, rc := setupMiniRedis(t)
			lock := tc.setup(t, mr, rc)
			err := lock.Refresh(context.Background(), tc.ttl, tc.opt)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("expected %v, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.verify != nil {
				tc.verify(t, mr, rc)
			}
		})
	}
}

// interceptingScripter wraps a real RedisClient, calling hook before each
// Eval/EvalSha. If hook returns a non-nil *redis.Cmd, that is returned
// instead of delegating to the real client.
type interceptingScripter struct {
	real RedisClient
	hook func(ctx context.Context) *redis.Cmd
}

func (s *interceptingScripter) eval(ctx context.Context, fn func() *redis.Cmd) *redis.Cmd {
	if cmd := s.hook(ctx); cmd != nil {
		return cmd
	}
	return fn()
}
func (s *interceptingScripter) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return s.eval(ctx, func() *redis.Cmd { return s.real.Eval(ctx, script, keys, args...) })
}
func (s *interceptingScripter) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return s.eval(ctx, func() *redis.Cmd { return s.real.EvalSha(ctx, sha1, keys, args...) })
}
func (s *interceptingScripter) EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return s.eval(ctx, func() *redis.Cmd { return s.real.EvalRO(ctx, script, keys, args...) })
}
func (s *interceptingScripter) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return s.eval(ctx, func() *redis.Cmd { return s.real.EvalShaRO(ctx, sha1, keys, args...) })
}
func (s *interceptingScripter) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	return s.real.ScriptExists(ctx, hashes...)
}
func (s *interceptingScripter) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	return s.real.ScriptLoad(ctx, script)
}

func errCmd(ctx context.Context, err error) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetErr(err)
	return cmd
}

func failNTimes(n int32, injectedErr error) func(context.Context) *redis.Cmd {
	var count int32
	return func(ctx context.Context) *redis.Cmd {
		if atomic.AddInt32(&count, 1) <= n {
			return errCmd(ctx, injectedErr)
		}
		return nil
	}
}

func lockWithClient(client RedisClient, key, value string) *Lock {
	return &Lock{Client: &Client{client: client}, keys: []string{key}, value: value, tokenLen: len(value)}
}

func TestRefresh_TransientErrors(t *testing.T) {
	loadingErr := errors.New("LOADING Redis is loading the dataset in memory")

	t.Run("succeeds after transient errors", func(t *testing.T) {
		_, rc := setupMiniRedis(t)
		rc.Set(context.Background(), "k", "v", time.Hour)
		w := &interceptingScripter{real: rc, hook: failNTimes(2, loadingErr)}
		err := lockWithClient(w, "k", "v").Refresh(context.Background(), time.Minute, &Options{
			RetryStrategy: LimitRetry(LinearBackoff(time.Millisecond), 5),
		})
		if err != nil {
			t.Fatalf("expected success, got %v", err)
		}
		assertTTLRange(t, rc, "k", 59*time.Second, time.Minute)
	})

	t.Run("exhausts retries", func(t *testing.T) {
		_, rc := setupMiniRedis(t)
		rc.Set(context.Background(), "k", "v", time.Hour)
		w := &interceptingScripter{real: rc, hook: failNTimes(100, loadingErr)}
		err := lockWithClient(w, "k", "v").Refresh(context.Background(), time.Minute, &Options{
			RetryStrategy: LimitRetry(LinearBackoff(time.Millisecond), 2),
		})
		if !errors.Is(err, ErrNotObtained) {
			t.Fatalf("expected ErrNotObtained, got %v", err)
		}
	})

	t.Run("non-retryable error returned directly", func(t *testing.T) {
		_, rc := setupMiniRedis(t)
		rc.Set(context.Background(), "k", "v", time.Hour)
		permErr := errors.New("NOPERM user has no permissions to run 'evalsha' command")
		w := &interceptingScripter{real: rc, hook: failNTimes(1, permErr)}
		err := lockWithClient(w, "k", "v").Refresh(context.Background(), time.Minute, &Options{
			RetryStrategy: LinearBackoff(time.Millisecond),
		})
		if err == nil || err.Error() != permErr.Error() {
			t.Fatalf("expected permission error, got %v", err)
		}
	})

	t.Run("context cancelled during retry attempt", func(t *testing.T) {
		_, rc := setupMiniRedis(t)
		rc.Set(context.Background(), "k", "v", time.Hour)
		ctx, cancel := context.WithCancel(context.Background())
		var count int32
		w := &interceptingScripter{real: rc, hook: func(c context.Context) *redis.Cmd {
			if atomic.AddInt32(&count, 1) == 2 {
				cancel()
				return errCmd(c, loadingErr)
			}
			return nil
		}}
		err := lockWithClient(w, "k", "v").Refresh(ctx, time.Minute, &Options{
			RetryStrategy: LinearBackoff(time.Millisecond),
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})
}

func TestRefresh_RetryCallCount(t *testing.T) {
	t.Run("NoRetry calls NextBackoff once on failure", func(t *testing.T) {
		mr, rc := setupMiniRedis(t)
		setLock(t, rc, []string{"k"}, "v", time.Millisecond)
		mr.FastForward(10 * time.Millisecond)
		n := 0
		spy := &spyRetry{inner: NoRetry(), onCall: func() { n++ }}
		_ = (&Lock{Client: &Client{client: rc}, keys: []string{"k"}, value: "v", tokenLen: 1}).
			Refresh(context.Background(), time.Minute, &Options{RetryStrategy: spy})
		if n != 1 {
			t.Fatalf("expected 1 call, got %d", n)
		}
	})

	t.Run("success never calls NextBackoff", func(t *testing.T) {
		_, rc := setupMiniRedis(t)
		setLock(t, rc, []string{"k"}, "v", time.Hour)
		n := 0
		spy := &spyRetry{inner: LinearBackoff(time.Millisecond), onCall: func() { n++ }}
		_ = (&Lock{Client: &Client{client: rc}, keys: []string{"k"}, value: "v", tokenLen: 1}).
			Refresh(context.Background(), time.Minute, &Options{RetryStrategy: spy})
		if n != 0 {
			t.Fatalf("expected 0 calls, got %d", n)
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
