package redislock

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/fanliao/go-promise"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"io"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	luaRefresh  = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)
	luaRelease  = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
	luaPTTL     = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pttl", KEYS[1]) else return -3 end`)
	luaAcquire  = redis.NewScript(`if (redis.call('exists', KEYS[1]) == 0) then redis.call('hset', KEYS[1], ARGV[2], 1); redis.call('pexpire', KEYS[1], ARGV[1]); return 0; end; if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then redis.call('hincrby', KEYS[1], ARGV[2], 1); redis.call('pexpire', KEYS[1], ARGV[1]); return 0; end; return redis.call('pttl', KEYS[1]);`)
	luaExpire   = redis.NewScript(`if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then return redis.call('pexpire', KEYS[1], ARGV[1]) else return 0 end`)
	luaRelease2 = redis.NewScript(`if (redis.call('hexists', KEYS[1], ARGV[2]) == 0) then  return 0; end; local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); if (counter > 0) then redis.call('pexpire', KEYS[1], ARGV[1]); return counter; else redis.call('del', KEYS[1]); local rid = redis.call('lpop', KEYS[2]); if (rid) then redis.call('publish', KEYS[3], rid); end; end; return 0;`)
)

var (
	// DefaultPubSubTimeout its type is int, and the unit is milliseconds，and it can be used to set the timeout period for subscribing to channel
	DefaultPubSubTimeout = 5000

	// The unique identifier used to store the watchDog, if it already exists, you don’t need to open a new one (to prevent recursive locking and open multiple watchDogs), it will be deleted when unlocked
	hasWatchDog = make(map[string]*promise.Future)
)

var (
	// ErrNotObtained is returned when a lock cannot be obtained.
	ErrNotObtained = errors.New("redislock: not obtained")

	// ErrLockNotHeld is returned when trying to release an inactive lock.
	ErrLockNotHeld = errors.New("redislock: lock not held")
)

// RedisClient is a minimal client interface.
type RedisClient interface {
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
	Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd
	RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	HGetAll(ctx context.Context, key string) *redis.StringStringMapCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
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
	expire time.Duration
}

// New creates a new Client instance with a custom namespace.
func New(client RedisClient) *Client {
	return &Client{client: client}
}

// Obtain tries to obtain a new lock using a key with the given TTL.
// May return ErrNotObtained if not successful.
func (c *Client) Obtain(ctx context.Context, key string, ttl time.Duration, opt *Options) (*Lock, error) {
	// Create a random token
	token, err := c.randomToken()
	if err != nil {
		return nil, err
	}

	value := token + opt.getMetadata()
	retry := opt.getRetryStrategy()

	deadlinectx, cancel := context.WithDeadline(ctx, time.Now().Add(ttl))
	defer cancel()

	var timer *time.Timer
	for {
		ok, err := c.obtain(deadlinectx, key, value, ttl)
		if err != nil {
			return nil, err
		} else if ok {
			return &Lock{client: c, key: key, value: value}, nil
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
		case <-deadlinectx.Done():
			return nil, ErrNotObtained
		case <-timer.C:
		}
	}
}

// TryLock can set the timeout period and whether to enable the automatic renewal of the daemon thread
func (c *Client) TryLock(ctx context.Context, key string, expiryTime time.Duration, waitTime time.Duration, isNeedScheduled bool, opt *Options) (*Lock, error) {
	c.expire = expiryTime
	field, success := c.getAndCheckUUID(ctx, key)
	if !success {
		return nil, errors.New("get uuid error")
	}

	value := field + "-" + strconv.Itoa(getGoroutineId())
	retry := opt.getRetryStrategy()

	ttl, err := c.tryAcquire(ctx, key, value, expiryTime, isNeedScheduled)
	if err != nil {
		return nil, err
	}
	if ttl == 0 {
		return &Lock{client: c, key: key, value: value}, nil
	}

	// Publish and subscribe to reduce waiting time
	c.pubsub(ctx, key)

	// CAS
	return c.cas(ctx, key, value, expiryTime, waitTime, isNeedScheduled, retry)
}

func (c *Client) obtain(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, value, ttl).Result()
}

func (c *Client) tryAcquire(ctx context.Context, key, value string, releaseTime time.Duration, isNeedScheduled bool) (int64, error) {
	cmd := luaAcquire.Run(ctx, c.client, []string{key}, int(releaseTime/time.Millisecond), value)
	ttl, err := cmd.Int64()
	if err != nil {
		// int64 is not important
		return -500, err
	}

	// Successfully locked, open WatchDog
	if isNeedScheduled && ttl == 0 {
		c.watchDog(ctx, key, key, releaseTime)
	}

	return ttl, nil
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
func Obtain(ctx context.Context, client RedisClient, key string, ttl time.Duration, opt *Options) (*Lock, error) {
	return New(client).Obtain(ctx, key, ttl, opt)
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
func (l *Lock) TTL(ctx context.Context) (time.Duration, error) {
	res, err := luaPTTL.Run(ctx, l.client.client, []string{l.key}, l.value).Result()
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
	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	status, err := luaRefresh.Run(ctx, l.client.client, []string{l.key}, l.value, ttlVal).Result()
	if err != nil {
		return err
	} else if status == int64(1) {
		return nil
	}
	return ErrNotObtained
}

// Release manually releases the lock.
// May return ErrLockNotHeld.
func (l *Lock) Release(ctx context.Context) error {
	res, err := luaRelease.Run(ctx, l.client.client, []string{l.key}, l.value).Result()
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

// ReleaseWithTryLock controls the unlocking operation of TryLock
// If there is no lock, it will be ignored.
func (l *Lock) ReleaseWithTryLock(ctx context.Context) (bool, error) {
	field, success := l.client.getAndCheckUUID(ctx, l.key)
	if !success {
		return false, fmt.Errorf(field)
	}
	field = field + "-" + strconv.Itoa(getGoroutineId())
	cmd := luaRelease2.Run(ctx, l.client.client, []string{l.key, l.key + "-list", l.key + "-pub"}, int(l.client.expire/time.Millisecond), field)

	// Return 0 to indicate successful unlocking or no need to unlock, return value greater than 0 indicates that there is a recursive lock -1, and reset the time, the return value indicates the number of layers of remaining locks
	res, err := cmd.Int64()
	if err != nil {
		return false, err
	} else if res > 0 {
	} else {
		// If the unlock is successful or does not need to be unlocked, close the thread
		if f, ok := hasWatchDog[field]; ok {
			err = f.Cancel()
			if err != nil {
				return false, err
			}
		}
	}

	return true, nil
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

type ttlBackoff struct {
	open bool
}

// TtlBackoff strategy can determine the spin waiting time according to the remaining time of the occupied lock.
// You can set true or false to use the ttl strategy
func TtlBackoff(isOpen bool) RetryStrategy {
	return &ttlBackoff{open: isOpen}
}

func (r *ttlBackoff) NextBackoff() time.Duration {
	if r.open {
		return -987654321
	} else {
		return -1
	}
}

// Return the first 36 bits of a field (not including the thread ID), which is used as the hash key
func (c *Client) getAndCheckUUID(ctx context.Context, lockKey string) (string, bool) {
	// Get the goroutineId of an existing lock, used to determine whether a new uid needs to be generated
	hMap := c.client.HGetAll(ctx, lockKey).Val()
	var lockField = ""
	for key, _ := range hMap {
		lockField = key
	}

	// If get a "", it means that the lock can be used.
	if lockField != "" {
		if len(lockField) < 38 {
			// Prevent the thread from having other locks, resulting in subscript out-of-bounds exception
			return "Attempt to lock failed, the current thread is occupied by [" + lockField + "]", false
		}
		// The last few digits of the field represent the thread id, to determine whether they are consistent
		if lockField[37:] != strconv.Itoa(getGoroutineId()) {
			// If inconsistent, generate a new field
			lockField = uuid.New().String()
		} else {
			// If they are consistent, directly assign the first 36 bits to the field
			lockField = lockField[:36]
		}
	} else {
		lockField = uuid.New().String()
	}

	return lockField, true
}

// Return the goroutine's id
func getGoroutineId() int {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("panic recover:panic info:%+v", err)
		}
	}()

	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

// Guard thread (extend the expiration time)
func (c *Client) watchDog(ctx context.Context, key, field string, releaseTime time.Duration) {
	if _, ok := hasWatchDog[field]; ok {
		return
	}

	f := promise.Start(func(canceller promise.Canceller) {
		var count = 0
		for {
			time.Sleep(10 * time.Millisecond)
			if canceller.IsCancelled() {
				log.Println(field, "'s watchdog is closed, count = ", count)
				return
			}
			time.Sleep(releaseTime / 3)
			log.Println(field, " open a watchdog")
			cmd := luaExpire.Run(ctx, c.client, []string{key}, int(releaseTime/time.Millisecond), field)
			res, err := cmd.Int64()
			if err != nil {
				log.Println(field, "'s watchdog has err", err)
				return
			}
			if res == 1 {
				count += 1
				continue
			} else {
				log.Println(field, "'s watchdog is closed, count = ", count)
				return
			}
		}
	}).OnComplete(func(v interface{}) {
		// It completes the asynchronous operation by itself and ends the life of the guard thread
		delete(hasWatchDog, field)
	}).OnCancel(func() {
		// It has been cancelled by Release() before executing this function
		delete(hasWatchDog, field)
	})
	hasWatchDog[field] = f
}

// Use go-promise for subscription operation. By default, it is not your turn to lock in 5 seconds, enter spin to lock
func (c *Client) pubsub(ctx context.Context, key string) {
	routineId := strconv.Itoa(getGoroutineId())
	uid := uuid.New().String() + "-" + routineId
	// Push the thread id to the message queue and queue
	c.client.RPush(ctx, key+"-list", uid)
	c.client.Expire(ctx, key+"-list", time.Duration(DefaultPubSubTimeout)*time.Millisecond)

	// Subscribe to the channel, block the thread waiting for the message
	f := promise.Start(func() {
		pub := c.client.Subscribe(ctx, key+"-pub")
		defer pub.Unsubscribe(ctx, key+"-pub")
		defer pub.Close()
		//log.Println(uid, "is listening the redis channel")
		for msgs := range pub.Channel() {
			msg := msgs.Payload
			if msg == uid {
				break
			}
		}
	})
	_, err, _ := f.GetOrTimeout(uint(DefaultPubSubTimeout))
	if err != nil {
		log.Fatal(err)
		return
	}
}

func (c *Client) cas(ctx context.Context, key string, value string, expiryTime, waitTime time.Duration, isNeedScheduled bool, retry RetryStrategy) (*Lock, error) {
	deadlinectx, cancel := context.WithDeadline(ctx, time.Now().Add(waitTime))
	defer cancel()

	var timer *time.Timer
	for {
		ttl, err := c.tryAcquire(deadlinectx, key, value, expiryTime, isNeedScheduled)
		if err != nil {
			return nil, err
		} else if ttl == 0 {
			return &Lock{client: c, key: key, value: value}, nil
		}
		backoff := retry.NextBackoff()
		if backoff < 1 {
			if backoff != -987654321 {
				return nil, ErrNotObtained
			} else {
				if ttl < 3000 {
					backoff = time.Duration(ttl)
				} else {
					backoff = time.Duration(ttl / 3)
				}
			}
		}
		if timer == nil {
			timer = time.NewTimer(backoff * time.Microsecond)
			defer timer.Stop()
		} else {
			timer.Reset(backoff)
		}

		select {
		case <-deadlinectx.Done():
			return nil, ErrNotObtained
		case <-timer.C:
		}
	}
}
