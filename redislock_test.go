package redislock_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/bsm/ginkgo"
	. "github.com/bsm/gomega"
	. "github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
)

const lockKey = "__bsm_redislock_unit_test__"

var _ = Describe("Client", func() {
	var subject *Client
	var ctx = context.Background()

	BeforeEach(func() {
		subject = New(redisClient)
	})

	AfterEach(func() {
		Expect(redisClient.Del(ctx, lockKey).Err()).To(Succeed())
	})

	It("obtains once with TTL", func() {
		lock1, err := subject.Obtain(ctx, lockKey, time.Hour, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(lock1.Token()).To(HaveLen(22))
		Expect(lock1.TTL(ctx)).To(BeNumerically("~", time.Hour, time.Second))
		defer lock1.Release(ctx)

		_, err = subject.Obtain(ctx, lockKey, time.Hour, nil)
		Expect(err).To(Equal(ErrNotObtained))
		Expect(lock1.Release(ctx)).To(Succeed())

		lock2, err := subject.Obtain(ctx, lockKey, time.Minute, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(lock2.TTL(ctx)).To(BeNumerically("~", time.Minute, time.Second))
		Expect(lock2.Release(ctx)).To(Succeed())
	})

	It("obtains through short-cut", func() {
		lock, err := Obtain(ctx, redisClient, lockKey, time.Hour, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(lock.Release(ctx)).To(Succeed())
	})

	It("supports custom metadata", func() {
		lock, err := Obtain(ctx, redisClient, lockKey, time.Hour, &Options{Metadata: "my-data"})
		Expect(err).NotTo(HaveOccurred())
		Expect(lock.Metadata()).To(Equal("my-data"))
		Expect(lock.Release(ctx)).To(Succeed())
	})

	It("refreshes", func() {
		lock, err := Obtain(ctx, redisClient, lockKey, time.Minute, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(lock.TTL(ctx)).To(BeNumerically("~", time.Minute, time.Second))
		Expect(lock.Refresh(ctx, time.Hour, nil)).To(Succeed())
		Expect(lock.TTL(ctx)).To(BeNumerically("~", time.Hour, time.Second))
		Expect(lock.Release(ctx)).To(Succeed())
	})

	It("fails to release if expired", func() {
		lock, err := Obtain(ctx, redisClient, lockKey, time.Millisecond, nil)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(5 * time.Millisecond)
		Expect(lock.Release(ctx)).To(MatchError(ErrLockNotHeld))
	})

	It("fails to release if ontained by someone else", func() {
		lock, err := Obtain(ctx, redisClient, lockKey, time.Minute, nil)
		Expect(err).NotTo(HaveOccurred())

		Expect(redisClient.Set(ctx, lockKey, "ABCD", 0).Err()).NotTo(HaveOccurred())
		Expect(lock.Release(ctx)).To(MatchError(ErrLockNotHeld))
	})

	It("fails to refresh if expired", func() {
		lock, err := Obtain(ctx, redisClient, lockKey, time.Millisecond, nil)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(5 * time.Millisecond)
		Expect(lock.Refresh(ctx, time.Hour, nil)).To(MatchError(ErrNotObtained))
	})

	It("retries if enabled", func() {
		// retry, succeed
		Expect(redisClient.Set(ctx, lockKey, "ABCD", 0).Err()).NotTo(HaveOccurred())
		Expect(redisClient.PExpire(ctx, lockKey, 20*time.Millisecond).Err()).NotTo(HaveOccurred())

		lock, err := Obtain(ctx, redisClient, lockKey, time.Hour, &Options{
			RetryStrategy: LimitRetry(LinearBackoff(100*time.Millisecond), 3),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(lock.Release(ctx)).To(Succeed())

		// no retry, fail
		Expect(redisClient.Set(ctx, lockKey, "ABCD", 0).Err()).NotTo(HaveOccurred())
		Expect(redisClient.PExpire(ctx, lockKey, 50*time.Millisecond).Err()).NotTo(HaveOccurred())

		_, err = Obtain(ctx, redisClient, lockKey, time.Hour, nil)
		Expect(err).To(MatchError(ErrNotObtained))

		// retry 2x, give up & fail
		Expect(redisClient.Set(ctx, lockKey, "ABCD", 0).Err()).NotTo(HaveOccurred())
		Expect(redisClient.PExpire(ctx, lockKey, 50*time.Millisecond).Err()).NotTo(HaveOccurred())

		_, err = Obtain(ctx, redisClient, lockKey, time.Hour, &Options{
			RetryStrategy: LimitRetry(LinearBackoff(time.Millisecond), 2),
		})
		Expect(err).To(MatchError(ErrNotObtained))
	})

	It("prevents multiple locks (fuzzing)", func() {
		numLocks := int32(0)
		wg := new(sync.WaitGroup)
		for i := 0; i < 100; i++ {
			wg.Add(1)

			go func() {
				defer GinkgoRecover()
				defer wg.Done()

				wait := rand.Int63n(int64(10 * time.Millisecond))
				time.Sleep(time.Duration(wait))

				_, err := subject.Obtain(ctx, lockKey, time.Minute, nil)
				if err == ErrNotObtained {
					return
				}
				Expect(err).NotTo(HaveOccurred())
				atomic.AddInt32(&numLocks, 1)
			}()
		}
		wg.Wait()
		Expect(numLocks).To(Equal(int32(1)))
	})

	It("test the ttl strategy", func() {
		numLocks := int32(0)
		numUnLocks := int32(0)
		wg := new(sync.WaitGroup)
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				strategy := TtlBackoff(true)
				opt := Options{RetryStrategy: strategy}
				lock, err := subject.TryObtain(context.Background(), lockKey, 5*time.Second, 10*time.Second, &opt)
				if err != nil {
					return
				}
				if lock.Key() != "" {
					atomic.AddInt32(&numLocks, 1)
				} else {
					atomic.AddInt32(&numUnLocks, 1)
				}
				lock.ReleaseWithTryLock(ctx)
			}()
		}
		wg.Wait()
		Expect(numLocks + numUnLocks).To(Equal(int32(100)))
	})

	It("test the ttl strategy with guard", func() {
		numLocks := int32(0)
		numUnLocks := int32(0)
		wg := new(sync.WaitGroup)
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				strategy := TtlBackoff(true)
				opt := Options{RetryStrategy: strategy}
				lock, err := subject.TryObtainWithGuard(context.Background(), lockKey, 10*time.Second, &opt)
				if err != nil {
					return
				}
				if lock.Key() != "" {
					atomic.AddInt32(&numLocks, 1)
				} else {
					atomic.AddInt32(&numUnLocks, 1)
				}
				lock.ReleaseWithTryLock(ctx)
			}()
		}
		wg.Wait()
		Expect(numLocks + numUnLocks).To(Equal(int32(100)))
	})
})

var _ = Describe("RetryStrategy", func() {
	It("supports no-retry", func() {
		subject := NoRetry()
		Expect(subject.NextBackoff()).To(Equal(time.Duration(0)))
	})

	It("supports linear backoff", func() {
		subject := LinearBackoff(time.Second)
		Expect(subject.NextBackoff()).To(Equal(time.Second))
		Expect(subject.NextBackoff()).To(Equal(time.Second))
		Expect(subject).To(beThreadSafe{})
	})

	It("supports limits", func() {
		subject := LimitRetry(LinearBackoff(time.Second), 2)
		Expect(subject.NextBackoff()).To(Equal(time.Second))
		Expect(subject.NextBackoff()).To(Equal(time.Second))
		Expect(subject.NextBackoff()).To(Equal(time.Duration(0)))
		Expect(subject).To(beThreadSafe{})
	})

	It("supports exponential backoff", func() {
		subject := ExponentialBackoff(10*time.Millisecond, 300*time.Millisecond)
		Expect(subject.NextBackoff()).To(Equal(10 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(10 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(16 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(32 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(64 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(128 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(256 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(300 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(300 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(300 * time.Millisecond))
		Expect(subject).To(beThreadSafe{})
	})

	It("supports ttl backoff", func() {
		subject := TtlBackoff(true)
		Expect(subject.NextBackoff()).To(Equal(time.Duration(-987654321)))
		Expect(subject).To(beThreadSafe{})
	})
})

// --------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "redislock")
}

var redisClient *redis.Client

var _ = BeforeSuite(func() {
	redisClient = redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    "127.0.0.1:6379", DB: 9,
	})
	Expect(redisClient.Ping(context.Background()).Err()).To(Succeed())
})

var _ = AfterSuite(func() {
	Expect(redisClient.Close()).To(Succeed())
})

type beThreadSafe struct{}

func (beThreadSafe) Match(actual interface{}) (bool, error) {
	strategy, ok := actual.(RetryStrategy)
	if !ok {
		return false, fmt.Errorf("beThreadSafe matcher expects a RetryStrategy")
	}

	wg := new(sync.WaitGroup)
	for i := 0; i < 1000; i++ {
		wg.Add(1)

		go func() {
			defer GinkgoRecover()
			defer wg.Done()

			strategy.NextBackoff()
		}()
	}
	wg.Wait()

	return true, nil
}

func (beThreadSafe) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected\n\t%T\nto be thread-safe", actual)
}

func (beThreadSafe) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected\n\t%T\nnot to be thread-safe", actual)
}
