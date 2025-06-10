package redislock

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/base64"
	"errors"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed release.lua
var luaReleaseScript string

//go:embed refresh.lua
var luaRefreshScript string

//go:embed pttl.lua
var luaPTTLScript string

//go:embed obtain.lua
var luaObtainScript string

var (
	luaRefresh = redis.NewScript(luaRefreshScript)
	luaRelease = redis.NewScript(luaReleaseScript)
	luaPTTL    = redis.NewScript(luaPTTLScript)
	luaObtain  = redis.NewScript(luaObtainScript)
)

var (
	// ErrNotObtained is returned when a lock cannot be obtained.
	ErrNotObtained = errors.New("redislock: not obtained")

	// ErrLockNotHeld is returned when trying to release an inactive lock.
	ErrLockNotHeld = errors.New("redislock: lock not held")
)

// RedisClient is a minimal client interface.
type RedisClient interface {
	redis.Scripter
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
	return c.ObtainMulti(ctx, []string{key}, ttl, opt)
}

// ObtainMulti tries to obtain new locks using keys with the given TTL.
// If any of requested key are already locked, no additional keys are
// locked and ErrNotObtained is returned.
// May return ErrNotObtained if not successful.
func (c *Client) ObtainMulti(ctx context.Context, keys []string, ttl time.Duration, opt *Options) (_ *Lock, err error) {
	defer func() {
		recordStatus(&stats.Obtain, err)
	}()

	token := opt.getToken()
	// Create a random token
	if token == "" {
		var err error
		if token, err = c.randomToken(); err != nil {
			return nil, err
		}
	}

	value := token + opt.getMetadata()
	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	retry := opt.getRetryStrategy()

	var ticker *time.Ticker
	for {
		ok, err := c.obtain(ctx, keys, value, len(token), ttlVal)
		if err != nil {
			return nil, err
		} else if ok {
			lock := &Lock{Client: c, keys: keys, value: value, tokenLen: len(token)}
			watchdog := opt.getWatchdog()
			if watchdog != nil {
				go watchdog.Start(ctx, lock, ttl)
			}
			return lock, nil
		}

		backoff := retry.NextBackoff()
		if backoff < 1 {
			return nil, ErrNotObtained
		}

		if ticker == nil {
			ticker = time.NewTicker(backoff)
			defer ticker.Stop()
		} else {
			ticker.Reset(backoff)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Client) obtain(ctx context.Context, keys []string, value string, tokenLen int, ttlVal string) (bool, error) {
	_, err := luaObtain.Run(ctx, c.client, keys, value, tokenLen, ttlVal).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	return true, nil
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
	*Client
	keys     []string
	value    string
	tokenLen int
	opt      *Options
}

// Obtain is a short-cut for New(...).Obtain(...).
func Obtain(ctx context.Context, client RedisClient, key string, ttl time.Duration, opt *Options) (*Lock, error) {
	return New(client).Obtain(ctx, key, ttl, opt)
}

// ObtainMulti is a short-cut for New(...).ObtainMulti(...).
func ObtainMulti(ctx context.Context, client RedisClient, keys []string, ttl time.Duration, opt *Options) (*Lock, error) {
	return New(client).ObtainMulti(ctx, keys, ttl, opt)
}

// Key returns the redis key used by the lock.
// If the lock hold multiple key, only the first is returned.
func (l *Lock) Key() string {
	return l.keys[0]
}

// Keys returns the redis keys used by the lock.
func (l *Lock) Keys() []string {
	return l.keys
}

// Token returns the token value set by the lock.
func (l *Lock) Token() string {
	return l.value[:l.tokenLen]
}

// Metadata returns the metadata of the lock.
func (l *Lock) Metadata() string {
	return l.value[l.tokenLen:]
}

// TTL returns the remaining time-to-live. Returns 0 if the lock has expired.
// In case lock is holding multiple keys, TTL returns the min ttl among those keys.
func (l *Lock) TTL(ctx context.Context) (time.Duration, error) {
	if l == nil {
		return 0, ErrLockNotHeld
	}
	res, err := luaPTTL.Run(ctx, l.client, l.keys, l.value).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	if num := res.(int64); num > 0 {
		return time.Duration(num) * time.Millisecond, nil
	}
	return 0, nil
}

// Refresh extends the lock with a new TTL.
// May return ErrNotObtained if refresh is unsuccessful.
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration, opt *Options) (err error) {
	defer func() {
		recordStatus(&stats.Refresh, err)
	}()

	if l == nil {
		return ErrNotObtained
	}

	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	_, err = luaRefresh.Run(ctx, l.client, l.keys, l.value, ttlVal).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrNotObtained
		}
		return err
	}
	return nil
}

// Release manually releases the lock.
// May return ErrLockNotHeld.
func (l *Lock) Release(ctx context.Context) (err error) {
	defer func() {
		recordStatus(&stats.Release, err)
	}()

	if l == nil {
		return ErrLockNotHeld
	}

	dog := l.opt.getWatchdog()
	if dog != nil {
		dog.Stop()
	}

	_, err = luaRelease.Run(ctx, l.client, l.keys, l.value).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrLockNotHeld
		}
		return err
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

	// Token is a unique value that is used to identify the lock. By default, a random tokens are generated. Use this
	// option to provide a custom token instead.
	Token string

	// Watchdog allows to refresh atomatically.
	Watchdog Watchdog
}

func (o *Options) getMetadata() string {
	if o != nil {
		return o.Metadata
	}
	return ""
}

func (o *Options) getToken() string {
	if o != nil {
		return o.Token
	}
	return ""
}

func (o *Options) getWatchdog() Watchdog {
	if o != nil && o.Watchdog != nil {
		return o.Watchdog
	}
	return nil
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

func isCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func recordStatus(s *Status, err error) {
	if err == nil {
		atomic.AddInt64(&s.Success, 1)
	}
	if err == ErrNotObtained {
		atomic.AddInt64(&s.Failed, 1)
	} else if isCanceled(err) {
		atomic.AddInt64(&s.Cancel, 1)
	} else {
		atomic.AddInt64(&s.Error, 1)
	}
}

// --------------------------------------------------------------------

// Watchdog allows to refresh atomatically.
type Watchdog interface {
	// Start starts the watchdog.
	Start(ctx context.Context, lock *Lock, ttl time.Duration)
	// Stop stops and waits the watchdog.
	Stop()
}

// TickWatchdog refreshes the lock at regular intervals.
type TickWatchdog struct {
	ctx      context.Context
	cancel   context.CancelFunc
	interval time.Duration
	ch       chan struct{}
}

// NewTickWatchdog creates a new watchdog that refreshes the lock at regular intervals.
func NewTickWatchdog(interval time.Duration) *TickWatchdog {
	return &TickWatchdog{interval: interval, ch: make(chan struct{})}
}

// Start starts the watchdog.
func (w *TickWatchdog) Start(ctx context.Context, lock *Lock, ttl time.Duration) {
	defer close(w.ch)
	atomic.AddInt64(&stats.Watchdog, 1)

	w.ctx, w.cancel = context.WithCancel(ctx)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			atomic.AddInt64(&stats.WatchdogDone, 1)
			return
		case <-ticker.C:
			atomic.AddInt64(&stats.WatchdogTick, 1)

			err := lock.Refresh(w.ctx, ttl, nil)
			if err != nil {
				if err == ErrNotObtained {
					return
				}
				// continue on other errors
			}
		}
	}
}

// Stop stops and waits the watchdog.
func (w *TickWatchdog) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	<-w.ch
}
