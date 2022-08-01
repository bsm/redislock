package redislock_test

import (
	"testing"
	"time"

	. "github.com/Ohbibi/redislock"
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
