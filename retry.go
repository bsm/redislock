package redislock

import (
	"sync/atomic"
	"time"
)

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
	cnt atomic.Int64
	max int64
}

// LimitRetry limits the number of retries to max attempts.
func LimitRetry(s RetryStrategy, max int) RetryStrategy {
	return &limitedRetry{s: s, max: int64(max)}
}

func (r *limitedRetry) NextBackoff() time.Duration {
	if r.cnt.Load() >= r.max {
		return 0
	}
	r.cnt.Add(1)
	return r.s.NextBackoff()
}

type exponentialBackoff struct {
	cnt atomic.Uint64

	min, max time.Duration
}

// ExponentialBackoff returns a strategy that doubles the wait between retries,
// computed as 2**(n+1) milliseconds where n is the attempt count (so the
// first wait is 4ms, the second 8ms, the third 16ms, and so on, capped at
// 2**26 ms once n reaches 25).
//
// The min and max arguments clamp the returned duration:
//   - if the computed value is below min, min is returned; pass 0 to disable
//     the lower bound. The first few attempts produce sub-16ms waits, so
//     callers that want a sensible floor should pass min >= 16ms.
//   - if max is non-zero and the computed value exceeds max, max is returned.
//     Passing 0 means no upper bound, which lets the wait grow into hours
//     after enough attempts; combine with LimitRetry or a ctx deadline if
//     that is undesirable.
func ExponentialBackoff(min, max time.Duration) RetryStrategy {
	return &exponentialBackoff{min: min, max: max}
}

func (r *exponentialBackoff) NextBackoff() time.Duration {
	cnt := r.cnt.Add(1)

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
