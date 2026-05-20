package redislock_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/bsm/redislock"
)

func TestNoRetry(t *testing.T) {
	retry := NoRetry()
	for i, exp := range []time.Duration{0, 0, 0} {
		if got := retry.NextBackoff(); exp != got {
			t.Fatalf("expected %d to be %v, got %v", i, exp, got)
		}
	}
}

func TestLinearBackoff(t *testing.T) {
	retry := LinearBackoff(time.Second)
	for i, exp := range []time.Duration{
		time.Second,
		time.Second,
		time.Second,
	} {
		if got := retry.NextBackoff(); exp != got {
			t.Fatalf("expected %d to be %v, got %v", i, exp, got)
		}
	}
}

func TestExponentialBackoff(t *testing.T) {
	retry := ExponentialBackoff(10*time.Millisecond, 300*time.Millisecond)
	for i, exp := range []time.Duration{
		10 * time.Millisecond,
		10 * time.Millisecond,
		16 * time.Millisecond,
		32 * time.Millisecond,
		64 * time.Millisecond,
		128 * time.Millisecond,
		256 * time.Millisecond,
		300 * time.Millisecond,
		300 * time.Millisecond,
	} {
		if got := retry.NextBackoff(); exp != got {
			t.Fatalf("expected %d to be %v, got %v", i, exp, got)
		}
	}
}

func TestLimitRetry(t *testing.T) {
	retry := LimitRetry(LinearBackoff(time.Second), 2)
	for i, exp := range []time.Duration{
		time.Second,
		time.Second,
		0,
	} {
		if got := retry.NextBackoff(); exp != got {
			t.Fatalf("expected %d to be %v, got %v", i, exp, got)
		}
	}
}

func TestLimitRetry_concurrent(t *testing.T) {
	const (
		max     = 100
		callers = 64
	)

	var allowed atomic.Int64
	retry := LimitRetry(LinearBackoff(time.Millisecond), max)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range callers {
		wg.Go(func() {
			<-start
			for range max {
				if retry.NextBackoff() > 0 {
					allowed.Add(1)
				}
			}
		})
	}
	close(start)
	wg.Wait()

	if got := allowed.Load(); got != max {
		t.Fatalf("expected exactly %d allowed retries, got %d", max, got)
	}
}
