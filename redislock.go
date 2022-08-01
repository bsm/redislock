package redislock

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

//go:embed release.lua
var luaReleaseScript string

//go:embed refresh.lua
var luaRefeshScript string

//go:embed pttl.lua
var luaPTTLScript string

//go:embed msetnxex.lua
var luaMSetNXEXScript string

var (
	luaRefresh  = redis.NewScript(luaRefeshScript)
	luaRelease  = redis.NewScript(luaReleaseScript)
	luaPTTL     = redis.NewScript(luaPTTLScript)
	luaMSetNXEX = redis.NewScript(luaMSetNXEXScript)
)

var (
	// ErrNotObtained is returned when a lock cannot be obtained.
	ErrNotObtained = errors.New("redislock: not obtained")

	// ErrLockNotHeld is returned when trying to release an inactive lock.
	ErrLockNotHeld = errors.New("redislock: lock not held")
)

// RedisClient is a minimal client interface.
type RedisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	MSetNX(ctx context.Context, values ...interface{}) *redis.BoolCmd
	TTL(ctx context.Context, key string) *redis.DurationCmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(ctx context.Context, scripts ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
}

// Client wraps a redis client.
type Client struct {
	client RedisClient
	tmp    []byte
	tmpMu  sync.Mutex
}

// New creates a new Client instance with a custom namespace.
func New(client RedisClient) *Client {
	return &Client{client: client}
}

// Obtain tries to obtain a new lock using a key with the given TTL.
// May return ErrNotObtained if not successful.
func (c *Client) Obtain(ctx context.Context, key string, ttl time.Duration, opt *Options) (*Lock, error) {
	return c.ObtainMany(ctx, []string{key}, ttl, opt)
}

// Obtain tries to obtain a new locks using keys with the given TTL.
// If any of requested key are already locked, no additional keys are
// locked and ErrNotObtained is returned.
// May return ErrNotObtained if not successful.
func (c *Client) ObtainMany(ctx context.Context, keys []string, ttl time.Duration, opt *Options) (*Lock, error) {
	// Create a random token
	token, err := c.randomToken()
	if err != nil {
		return nil, err
	}

	value := token + opt.getMetadata()
	retry := opt.getRetryStrategy()

	// make sure we don't retry forever
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Now().Add(ttl))
		defer cancel()
	}

	var timer *time.Timer
	for i := range keys {
		keys[i] = "redislock_" + keys[i]
	}
	for {
		ok, err := c.obtain(ctx, keys, value, ttl)
		if err != nil {
			return nil, err
		} else if ok {
			return &Lock{client: c, keys: keys, value: value}, nil
		}

		backoff := retry.NextBackoff()
		if backoff < 1 {
			return nil, ErrNotObtained
		}

		if timer == nil {
			timer = time.NewTimer(backoff)
			defer timer.Stop()
		} else {
			timer.Reset(backoff)
		}

		select {
		case <-ctx.Done():
			return nil, ErrNotObtained
		case <-timer.C:
		}
	}
}

func (c *Client) obtain(ctx context.Context, keys []string, value string, ttl time.Duration) (bool, error) {
	setArgs := []string{}
	for _, k := range keys {
		setArgs = append(setArgs, k, value)
	}
	res, err := luaMSetNXEX.Run(ctx, c.client, setArgs, ttl.Milliseconds()).Result()
	if err != nil {
		return false, fmt.Errorf("msetnx failed: %s", err)
	}
	return res == int64(len(keys)), nil
}

func (c *Client) randomToken() (string, error) {
	c.tmpMu.Lock()
	defer c.tmpMu.Unlock()

	if len(c.tmp) == 0 {
		c.tmp = make([]byte, 16)
	}

	if _, err := io.ReadFull(rand.Reader, c.tmp); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(c.tmp), nil
}

// --------------------------------------------------------------------

// Lock represents an obtained, distributed lock.
type Lock struct {
	client *Client
	keys   []string
	value  string
}

// Obtain is a short-cut for New(...).Obtain(...).
func Obtain(ctx context.Context, client RedisClient, key string, ttl time.Duration, opt *Options) (*Lock, error) {
	return New(client).Obtain(ctx, key, ttl, opt)
}

// Obtain is a short-cut for New(...).ObtainMany(...).
func ObtainMany(ctx context.Context, client RedisClient, keys []string, ttl time.Duration, opt *Options) (*Lock, error) {
	return New(client).ObtainMany(ctx, keys, ttl, opt)
}

// Key returns the redis key used by the lock.
func (l *Lock) Keys() []string {
	return l.keys
}

// Token returns the token value set by the lock.
func (l *Lock) Token() string {
	return l.value[:22]
}

// Metadata returns the metadata of the lock.
func (l *Lock) Metadata() string {
	return l.value[22:]
}

// TTL returns the remaining time-to-live. Returns 0 if the lock has expired.
func (l *Lock) TTL(ctx context.Context) (time.Duration, error) {
	res, err := luaPTTL.Run(ctx, l.client.client, l.keys, l.value).Result()
	// fmt.Printf("luaPTTL: %v, %v\n", res, err)
	if err == redis.Nil {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	if num := res.(int64); num > 0 {
		return time.Duration(num) * time.Millisecond, nil
	}
	return 0, nil
}

// Refresh extends the lock with a new TTL.
// May return ErrNotObtained if refresh is unsuccessful.
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration, opt *Options) error {
	res, err := luaRefresh.Run(ctx, l.client.client, l.keys, l.value, ttl.Milliseconds()).Result()
	// fmt.Printf("luaRefresh: %v, %v\n", res, err)
	if err != nil {
		return err
	}
	if refreshCount := res.(int64); refreshCount == int64(len(l.keys)) {
		return nil
	}
	return ErrNotObtained
}

// Release manually releases the lock.
// May return ErrLockNotHeld.
func (l *Lock) Release(ctx context.Context) error {
	res, err := luaRelease.Run(ctx, l.client.client, l.keys, l.value).Result()
	// fmt.Printf("luaRelease: %v, %v\n", res, err)
	if err == redis.Nil {
		return ErrLockNotHeld
	} else if err != nil {
		return err
	}

	if releaseCount := res.(int64); releaseCount != int64(len(l.keys)) {
		return ErrLockNotHeld
	}
	return nil
}

// --------------------------------------------------------------------

// Options describe the options for the lock
type Options struct {
	// RetryStrategy allows to customise the lock retry strategy.
	// Default: do not retry
	RetryStrategy RetryStrategy

	// Metadata string is appended to the lock token.
	Metadata string
}

func (o *Options) getMetadata() string {
	if o != nil {
		return o.Metadata
	}
	return ""
}

func (o *Options) getRetryStrategy() RetryStrategy {
	if o != nil && o.RetryStrategy != nil {
		return o.RetryStrategy
	}
	return NoRetry()
}

// --------------------------------------------------------------------

// RetryStrategy allows to customise the lock retry strategy.
type RetryStrategy interface {
	// NextBackoff returns the next backoff duration.
	NextBackoff() time.Duration
}

type linearBackoff time.Duration

// LinearBackoff allows retries regularly with customized intervals
func LinearBackoff(backoff time.Duration) RetryStrategy {
	return linearBackoff(backoff)
}

// NoRetry acquire the lock only once.
func NoRetry() RetryStrategy {
	return linearBackoff(0)
}

func (r linearBackoff) NextBackoff() time.Duration {
	return time.Duration(r)
}

type limitedRetry struct {
	s   RetryStrategy
	cnt int64
	max int64
}

// LimitRetry limits the number of retries to max attempts.
func LimitRetry(s RetryStrategy, max int) RetryStrategy {
	return &limitedRetry{s: s, max: int64(max)}
}

func (r *limitedRetry) NextBackoff() time.Duration {
	if atomic.LoadInt64(&r.cnt) >= r.max {
		return 0
	}
	atomic.AddInt64(&r.cnt, 1)
	return r.s.NextBackoff()
}

type exponentialBackoff struct {
	cnt uint64

	min, max time.Duration
}

// ExponentialBackoff strategy is an optimization strategy with a retry time of 2**n milliseconds (n means number of times).
// You can set a minimum and maximum value, the recommended minimum value is not less than 16ms.
func ExponentialBackoff(min, max time.Duration) RetryStrategy {
	return &exponentialBackoff{min: min, max: max}
}

func (r *exponentialBackoff) NextBackoff() time.Duration {
	cnt := atomic.AddUint64(&r.cnt, 1)

	ms := 2 << 25
	if cnt < 25 {
		ms = 2 << cnt
	}

	if d := time.Duration(ms) * time.Millisecond; d < r.min {
		return r.min
	} else if r.max != 0 && d > r.max {
		return r.max
	} else {
		return d
	}
}
