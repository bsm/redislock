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
func (c *Client) ObtainMulti(ctx context.Context, keys []string, ttl time.Duration, opt *Options) (*Lock, error) {
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

	if err := withRetry(ctx, ttl, opt.getRetryStrategy(), func(ctx context.Context) (bool, error) {
		ok, err := c.obtain(ctx, keys, value, len(token), ttlVal)
		if err != nil {
			// any non-nil error from obtain is terminal (transient redis
			// errors are unlikely to clear within a lock TTL and retrying a
			// broken server is futile).
			return true, err
		}
		if ok {
			return true, nil
		}
		// lock is held by someone else; retryable.
		return false, nil
	}); err != nil {
		return nil, err
	}
	return &Lock{Client: c, keys: keys, value: value, tokenLen: len(token)}, nil
}

// withRetry runs attempt repeatedly until it signals it is done, the retry
// strategy is exhausted, or ctx is done. The attempt function returns
// (done, err):
//   - done=true: stop and return err (success when err is nil, otherwise a
//     terminal failure).
//   - done=false, err=nil: retryable, no error to surface; on exhaustion
//     withRetry returns ErrNotObtained.
//   - done=false, err!=nil: retryable transient error; on exhaustion
//     withRetry returns this last error, and on ctx cancellation it returns
//     the last error joined with ctx.Err(). If no transient error was seen,
//     ctx cancellation returns ErrNotObtained joined with ctx.Err().
//
// If ctx has no deadline one is derived from ttl as a final safety cap, so a
// caller passing an unbounded retry strategy and a background context cannot
// loop forever.
func withRetry(ctx context.Context, ttl time.Duration, retry RetryStrategy, attempt func(context.Context) (bool, error)) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Now().Add(ttl))
		defer cancel()
	}

	var ticker *time.Ticker
	var lastErr error
	for {
		done, err := attempt(ctx)
		if done {
			return err
		}
		if err != nil {
			lastErr = err
		}

		backoff := retry.NextBackoff()
		if backoff < 1 {
			if lastErr != nil {
				return lastErr
			}
			return ErrNotObtained
		}

		if ticker == nil {
			ticker = time.NewTicker(backoff)
			defer ticker.Stop()
		} else {
			ticker.Reset(backoff)
		}

		select {
		case <-ctx.Done():
			err := lastErr
			if err == nil {
				err = ErrNotObtained
			}
			return errors.Join(err, ctx.Err())
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
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration, opt *Options) error {
	if l == nil {
		return ErrNotObtained
	}
	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)

	return withRetry(ctx, ttl, opt.getRetryStrategy(), func(ctx context.Context) (bool, error) {
		_, err := luaRefresh.Run(ctx, l.client, l.keys, l.value, ttlVal).Result()
		if err == nil {
			return true, nil
		}
		if errors.Is(err, redis.Nil) {
			// the lock is no longer held by us; retrying cannot recover it.
			return true, ErrNotObtained
		}
		// transient error; allow the retry strategy to try again.
		return false, err
	})
}

// Release manually releases the lock.
// May return ErrLockNotHeld.
func (l *Lock) Release(ctx context.Context) error {
	if l == nil {
		return ErrLockNotHeld
	}
	_, err := luaRelease.Run(ctx, l.client, l.keys, l.value).Result()
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

	// Metadata string.
	Metadata string

	// Token is a unique value that is used to identify the lock. By default, a random tokens are generated. Use this
	// option to provide a custom token instead.
	Token string
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

func (o *Options) getRetryStrategy() RetryStrategy {
	if o != nil && o.RetryStrategy != nil {
		return o.RetryStrategy
	}
	return NoRetry()
}
