package redislock

import (
	"time"
)

const defaultRetryDuration = 100 * time.Millisecond

//RetryStrategy describe the lock retry strategy,100ms is slow for some high performance application
type RetryStrategy interface {
	//SetTTL set total expire timeout
	SetTTL(ttl time.Duration)
	//Next
	Next() bool
	//GetRetryBackoff
	GetRetryBackoff() time.Duration
}

//NormalRetry every 100ms retry acquire lock
type NormalRetry struct {
	ttl      time.Duration
	usedTime time.Duration
	backoff  time.Duration
}

func (r *NormalRetry) SetTTL(ttl time.Duration) {
	r.ttl = ttl
}

func (r *NormalRetry) Next() bool {
	if r.usedTime >= r.ttl {
		return false
	}
	r.usedTime += r.backoff
	return true
}

func (r *NormalRetry) GetRetryBackoff() time.Duration {
	return r.backoff
}

func NewNormalRetry(backoff time.Duration) RetryStrategy {
	return &NormalRetry{backoff: backoff}
}

type LimitRetry struct {
	NormalRetry

	count      int
	limitCount int
}

func (r *LimitRetry) Next() bool {
	if r.count >= r.limitCount {
		return false
	}
	r.count++
	return r.NormalRetry.Next()
}

func NewLimitRetry(limit int, backoff time.Duration) RetryStrategy {
	return &LimitRetry{NormalRetry: NormalRetry{backoff: backoff}, limitCount: limit}
}

type StepRetry struct {
	ttl      time.Duration
	usedTime time.Duration
	count    int
}

func (r *StepRetry) SetTTL(ttl time.Duration) {
	r.ttl = ttl
}

func (r *StepRetry) Next() bool {
	if r.usedTime >= r.ttl {
		return false
	}
	r.usedTime += r.GetRetryBackoff()
	r.count++
	return true
}

func (r *StepRetry) GetRetryBackoff() time.Duration {
	if r.count < 3 {
		//2ms maybe too fast
		return 4 * time.Millisecond
	}
	if r.count > 10 {
		//
		return defaultRetryDuration
	}
	return time.Duration(r.count*r.count) * time.Millisecond
}

func NewStepRetry() RetryStrategy {
	return &StepRetry{}
}
