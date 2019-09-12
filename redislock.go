package redislock

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

var (
	luaRefresh = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)
	luaRelease = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
	luaPTTL    = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pttl", KEYS[1]) else return -3 end`)
)

var (
	// ErrNotObtained is returned when a lock cannot be obtained.
	ErrNotObtained = errors.New("redislock: not obtained")

	// ErrLockNotHeld is returned when trying to release an inactive lock.
	ErrLockNotHeld = errors.New("redislock: lock not held")
)

// RedisClient is a minimal client interface.
type RedisClient interface {
	SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Eval(script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(scripts ...string) *redis.BoolSliceCmd
	ScriptLoad(script string) *redis.StringCmd
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
func (c *Client) Obtain(key string, ttl time.Duration, opt *Options) (*Lock, error) {
	// Create a random token
	token, err := c.randomToken()
	if err != nil {
		return nil, err
	}
	if opt == nil {
		opt = &Options{}
	}
	opt.init()

	value := token + opt.getMetadata()
	ctx := opt.getContext()

	var timer *time.Timer
	for deadline := time.Now().Add(ttl); time.Now().Before(deadline); {
		backoff := opt.NextBackoff()
		if backoff < 1 {
			break
		}

		ok, err := c.obtain(key, value, ttl)
		if err != nil {
			return nil, err
		} else if ok {
			return &Lock{client: c, key: key, value: value}, nil
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

func (c *Client) obtain(key, value string, ttl time.Duration) (bool, error) {
	ok, err := c.client.SetNX(key, value, ttl).Result()
	if err == redis.Nil {
		err = nil
	}
	return ok, err
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
	key    string
	value  string
}

// Obtain is a short-cut for New(...).Obtain(...).
func Obtain(client RedisClient, key string, ttl time.Duration, opt *Options) (*Lock, error) {
	return New(client).Obtain(key, ttl, opt)
}

// Key returns the redis key used by the lock.
func (l *Lock) Key() string {
	return l.key
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
func (l *Lock) TTL() (time.Duration, error) {
	res, err := luaPTTL.Run(l.client.client, []string{l.key}, l.value).Result()
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
func (l *Lock) Refresh(ttl time.Duration, opt *Options) error {
	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	status, err := luaRefresh.Run(l.client.client, []string{l.key}, l.value, ttlVal).Result()
	if err != nil {
		return err
	} else if status == int64(1) {
		return nil
	}
	return ErrNotObtained
}

// Release manually releases the lock.
// May return ErrLockNotHeld.
func (l *Lock) Release() error {
	res, err := luaRelease.Run(l.client.client, []string{l.key}, l.value).Result()
	if err == redis.Nil {
		return ErrLockNotHeld
	} else if err != nil {
		return err
	}

	if i, ok := res.(int64); !ok || i != 1 {
		return ErrLockNotHeld
	}
	return nil
}

// --------------------------------------------------------------------
