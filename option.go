package redislock

import (
	"context"
	"time"
)

// Options describe the options for the lock
type Options struct {
	// RetryStrategy allows to customise the lock retry strategy.
	// if RetryStrategy is nil,it acquire the lock only once.
	RetryStrategy

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

func (o *Options) init() {
	if o.RetryStrategy != nil {
		return
	}
	o.RetryStrategy = NewLimitRetry(0, defaultRetryDuration)
}

const defaultRetryDuration = 100 * time.Millisecond

// RetryStrategy allows to customise the lock retry strategy.
type RetryStrategy interface {
	// NextBackoff returns the next backoff duration.
	NextBackoff() time.Duration
}

type linearRetry struct {
	backoff time.Duration
}

func (r *linearRetry) NextBackoff() time.Duration {
	return r.backoff
}

// NewLinearRetry allows retries regularly with customized intervals
func NewLinearRetry(backoff time.Duration) RetryStrategy {
	return &linearRetry{backoff: backoff}
}

type limitRetry struct {
	*linearRetry

	count      int
	limitCount int
}

func (r *limitRetry) NextBackoff() time.Duration {
	if r.count > r.limitCount {
		return 0
	}
	r.count++
	return r.backoff
}

// NewLimitRetry limitRetry will limit number of retries
func NewLimitRetry(limit int, backoff time.Duration) RetryStrategy {
	return &limitRetry{linearRetry: &linearRetry{backoff: backoff}, limitCount: limit}
}

const (
	minStepRetryDuration = 16 * time.Millisecond
	maxStepRetryDuration = time.Second
)

type stepRetry struct {
	count int
}

func (r *stepRetry) NextBackoff() time.Duration {
	r.count++
	if r.count <= 4 {
		// 2ms maybe too fast
		return minStepRetryDuration
	}
	if r.count >= 10 {
		return maxStepRetryDuration
	}
	return time.Duration(2<<r.count) * time.Millisecond
}

// NewStepRetry will retry at following intervals: 16ms,16ms,16ms,16ms,64ms,128ms,256ms,512ms,1024ms,1s,1s,1s ...
func NewStepRetry() RetryStrategy {
	return &stepRetry{}
}
