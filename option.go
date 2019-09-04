package redislock

import (
	"time"
)

const defaultRetryDuration = 100 * time.Millisecond

//RetryStrategy describe the lock retry strategy,100ms is slow for some high performance application
type RetryStrategy interface {
	//Next
	Next() bool
	//GetRetryBackoff
	GetRetryBackoff() time.Duration
}

//NormalRetry every 100ms retry acquire lock
type NormalRetry struct {
	backoff time.Duration
}

func (r *NormalRetry) Next() bool {
	return true
}

func (r *NormalRetry) GetRetryBackoff() time.Duration {
	return r.backoff
}

func NewNormalRetry(backoff time.Duration) RetryStrategy {
	return &NormalRetry{backoff: backoff}
}

type LimitRetry struct {
	*NormalRetry

	count      int
	limitCount int
}

func (r *LimitRetry) Next() bool {
	if r.count > r.limitCount {
		return false
	}
	r.count++
	return r.NormalRetry.Next()
}

func NewLimitRetry(limit int, backoff time.Duration) RetryStrategy {
	return &LimitRetry{NormalRetry: &NormalRetry{backoff: backoff}, limitCount: limit}
}

type StepRetry struct {
	count int
}

func (r *StepRetry) Next() bool {
	r.count++
	return true
}

func (r *StepRetry) GetRetryBackoff() time.Duration {
	if r.count <= 4 {
		//2ms maybe too fast
		return 16 * time.Millisecond
	}
	if r.count >= 10 {
		//
		return defaultRetryDuration
	}
	return time.Duration(r.count*r.count) * time.Millisecond
}

func NewStepRetry() RetryStrategy {
	return &StepRetry{}
}
