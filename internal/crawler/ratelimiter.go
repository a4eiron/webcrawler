package crawler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Bucket struct {
	b          chan struct{}
	cap        int
	refillRate float64

	// avoid mutexes if possible
	lastSeen atomic.Int64
}

type TokenBucketRLimiter struct {
	cap        int
	refillRate float64
	buckets    sync.Map
	cancel     context.CancelFunc
}

func (b *Bucket) allow() bool {
	now := time.Now()

	last := time.Unix(0, b.lastSeen.Load())

	elapsed := now.Sub(last).Seconds()

	tokensToAdd := int(elapsed * b.refillRate)

	space := b.cap - len(b.b)
	tokensToAdd = min(tokensToAdd, space)

	for range tokensToAdd {
		// fmt.Println("  tokensToAdd:", tokensToAdd)
		b.b <- struct{}{}
	}

	select {
	case <-b.b:
		b.lastSeen.Store(now.UnixNano())
		return true
	default:
		return false
	}
}

func (rl *TokenBucketRLimiter) cleanup(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rl.buckets.Range(func(key, value any) bool {
				ip := key.(string)
				b := value.(*Bucket)
				if time.Since(time.Unix(0, b.lastSeen.Load())) > 1*time.Hour {
					rl.buckets.Delete(ip)
				}
				return true
			})
		case <-ctx.Done():
			return
		}
	}
}

func (rl *TokenBucketRLimiter) Allow(ip string) bool {

	val, ok := rl.buckets.Load(ip)
	if !ok {
		newBucket := NewBucket(rl.cap, rl.refillRate)
		actual, _ := rl.buckets.LoadOrStore(ip, newBucket)
		val = actual
	}

	bucket, ok := val.(*Bucket)
	if !ok {
		panic("invalid type stored in buckets map")
	}

	return bucket.allow()
}

func (rl *TokenBucketRLimiter) Close() {
	rl.cancel()
}

func NewBucket(cap int, refillRate float64) *Bucket {
	bucket := &Bucket{
		b:          make(chan struct{}, cap),
		cap:        cap,
		refillRate: refillRate,
	}

	bucket.lastSeen.Store(time.Now().UnixNano())

	for range cap {
		bucket.b <- struct{}{}
	}
	return bucket
}

func NewTokentBucketRLimiter(cap int, refillRate float64) *TokenBucketRLimiter {
	ctx, cancel := context.WithCancel(context.Background())

	rl := &TokenBucketRLimiter{
		cap:        cap,
		refillRate: refillRate,
		buckets:    sync.Map{},
		cancel:     cancel,
	}

	go rl.cleanup(ctx)

	return rl
}
