package redislock

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	luaRefresh = redis.NewScript(1, `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)
	luaRelease = redis.NewScript(1, `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
	luaPTTL    = redis.NewScript(1, `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pttl", KEYS[1]) else return -3 end`)
)

var (
	// ErrNotObtained is returned when a lock cannot be obtained.
	ErrNotObtained = errors.New("redislock: not obtained")

	// ErrLockNotHeld is returned when trying to release an inactive lock.
	ErrLockNotHeld = errors.New("redislock: lock not held")
)

// // RedisClient is a minimal client interface.
// type RedisClient interface {
// 	SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd
// 	Eval(script string, keys []string, args ...interface{}) *redis.Cmd
// 	EvalSha(sha1 string, keys []string, args ...interface{}) *redis.Cmd
// 	ScriptExists(scripts ...string) *redis.BoolSliceCmd
// 	ScriptLoad(script string) *redis.StringCmd
// }

// type RedisClientMinimal interface {
// 	SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd
// 	Eval(script string, keys []string, args ...interface{}) *redis.Cmd
// 	EvalSha(sha1 string, keys []string, args ...interface{}) *redis.Cmd
// 	ScriptExists(scripts ...string) *redis.BoolSliceCmd
// 	ScriptLoad(script string) *redis.StringCmd
// }

// // Client wraps a redis client.
// type Client struct {
// 	client RedisClient
// 	tmp    []byte
// 	tmpMu  sync.Mutex
// }

type ClientMin struct {
	pool  *redis.Pool
	tmp   []byte
	tmpMu sync.Mutex
}

// // New creates a new Client instance with a custom namespace.
// func New(client RedisClient) *Client {
// 	return &Client{client: client}
// }

func NewMin(pool *redis.Pool) *ClientMin {
	return &ClientMin{pool: pool}
}

// Obtain tries to obtain a new lock using a key with the given TTL.
// May return ErrNotObtained if not successful.
func (c *ClientMin) Obtain(key string, ttl time.Duration, opt *Options) (*LockMin, error) {
	// Create a random token
	token, err := c.randomToken()
	if err != nil {
		return nil, err
	}

	value := token + opt.getMetadata()
	ctx := opt.getContext()
	retry := opt.getRetryStrategy()

	var timer *time.Timer
	for deadline := time.Now().Add(ttl); time.Now().Before(deadline); {

		ok, err := c.obtainmin(key, value, ttl)
		if err != nil {
			return nil, err
		} else if ok {
			return &LockMin{client: c, key: key, value: value}, nil
		}

		backoff := retry.NextBackoff()
		if backoff < 1 {
			break
		}

		if timer == nil {
			timer = time.NewTimer(backoff)
			defer timer.Stop()
		} else {
			timer.Reset(backoff)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	return nil, ErrNotObtained
}

// func (c *Client) obtain(key, value string, ttl time.Duration) (bool, error) {
// 	return c.client.SetNX(key, value, ttl).Result()
// }

func (c *ClientMin) obtainmin(key, value string, ttl time.Duration) (bool, error) {
	con := c.pool.Get()
	defer con.Close()
	_, err := redis.String(con.Do("SET", key, value, "PX", ttl.Milliseconds(), "NX"))
	//Redigo returns nil so that means lock is not obtained so mask and return error
	if err == redis.ErrNil {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (c *ClientMin) randomToken() (string, error) {
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
// type Lock struct {
// 	client *Client
// 	key    string
// 	value  string
// }

type LockMin struct {
	client *ClientMin
	key    string
	value  string
}

// Obtain is a short-cut for New(...).Obtain(...).
// func Obtain(client RedisClient, key string, ttl time.Duration, opt *Options) (*Lock, error) {
// 	return New(client).Obtain(key, ttl, opt)
// }

// Obtain is a short-cut for New(...).Obtain(...).
func Obtain(pool *redis.Pool, key string, ttl time.Duration, opt *Options) (*LockMin, error) {
	return NewMin(pool).Obtain(key, ttl, opt)
}

// Key returns the redis key used by the lock.
func (l *LockMin) Key() string {
	return l.key
}

// Token returns the token value set by the lock.
func (l *LockMin) Token() string {
	return l.value[:22]
}

// Metadata returns the metadata of the lock.
func (l *LockMin) Metadata() string {
	return l.value[22:]
}

// TTL returns the remaining time-to-live. Returns 0 if the lock has expired.
// func (l *Lock) TTL() (time.Duration, error) {
// 	res, err := luaPTTL.Run(l.client.client, []string{l.key}, l.value).Result()
// 	if err == redis.Nil {
// 		return 0, nil
// 	} else if err != nil {
// 		return 0, err
// 	}

// 	if num := res.(int64); num > 0 {
// 		return time.Duration(num) * time.Millisecond, nil
// 	}
// 	return 0, nil
// }

func (l *LockMin) TTL() (time.Duration, error) {
	con := l.client.pool.Get()
	defer con.Close()

	res, err := redis.Int64(luaPTTL.Do(con, l.key, l.value))
	if err == redis.ErrNil {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	// if num := res.(int64); num > 0 {
	// 	return time.Duration(num) * time.Millisecond, nil
	// }
	if res > 0 {
		return time.Duration(res) * time.Millisecond, nil
	}
	return 0, nil
}

// Refresh extends the lock with a new TTL.
// May return ErrNotObtained if refresh is unsuccessful.
// func (l *Lock) Refresh(ttl time.Duration, opt *Options) error {
// 	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
// 	status, err := luaRefresh.Run(l.client.client, []string{l.key}, l.value, ttlVal).Result()
// 	if err != nil {
// 		return err
// 	} else if status == int64(1) {
// 		return nil
// 	}
// 	return ErrNotObtained
// }

func (l *LockMin) Refresh(ttl time.Duration, opt *Options) error {
	con := l.client.pool.Get()
	defer con.Close()

	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	status, err := redis.Int64(luaRefresh.Do(con, l.key, l.value, ttlVal))
	if err != nil {
		return err
	} else if status == 1 {
		return nil
	}
	return ErrNotObtained
}

// Release manually releases the lock.
// May return ErrLockNotHeld.
// func (l *Lock) Release() error {
// 	res, err := luaRelease.Run(l.client.client, []string{l.key}, l.value).Result()
// 	if err == redis.Nil {
// 		return ErrLockNotHeld
// 	} else if err != nil {
// 		return err
// 	}

// 	if i, ok := res.(int64); !ok || i != 1 {
// 		return ErrLockNotHeld
// 	}
// 	return nil
// }

func (l *LockMin) Release() error {
	con := l.client.pool.Get()
	defer con.Close()

	res, err := redis.Int64(luaRelease.Do(con, l.key, l.value))
	if err == redis.ErrNil {
		return ErrLockNotHeld
	} else if err != nil {
		return err
	}

	// if i, ok := res.(int64); !ok || i != 1 {
	// 	return ErrLockNotHeld
	// }
	if res != 1 {
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

	// Optional context for Obtain timeout and cancellation control.
	Context context.Context
}

func (o *Options) getMetadata() string {
	if o != nil {
		return o.Metadata
	}
	return ""
}

func (o *Options) getContext() context.Context {
	if o != nil && o.Context != nil {
		return o.Context
	}
	return context.Background()
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
	s RetryStrategy

	cnt, max int
}

// LimitRetry limits the number of retries to max attempts.
func LimitRetry(s RetryStrategy, max int) RetryStrategy {
	return &limitedRetry{s: s, max: max}
}

func (r *limitedRetry) NextBackoff() time.Duration {
	if r.cnt >= r.max {
		return 0
	}
	r.cnt++
	return r.s.NextBackoff()
}

type exponentialBackoff struct {
	cnt uint

	min, max time.Duration
}

// ExponentialBackoff strategy is an optimization strategy with a retry time of 2**n milliseconds (n means number of times).
// You can set a minimum and maximum value, the recommended minimum value is not less than 16ms.
func ExponentialBackoff(min, max time.Duration) RetryStrategy {
	return &exponentialBackoff{min: min, max: max}
}

func (r *exponentialBackoff) NextBackoff() time.Duration {
	r.cnt++

	ms := 2 << 25
	if r.cnt < 25 {
		ms = 2 << r.cnt
	}

	if d := time.Duration(ms) * time.Millisecond; d < r.min {
		return r.min
	} else if r.max != 0 && d > r.max {
		return r.max
	} else {
		return d
	}
}
