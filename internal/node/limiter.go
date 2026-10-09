package node

import (
	"sync"
	"time"
)

// tokenBucket implements a thread-safe token bucket rate limiter for bandwidth throttling.
type tokenBucket struct {
	mu       sync.Mutex
	rate     int64     // bytes per second (0 = unlimited)
	capacity int64     // max burst in bytes
	tokens   int64     // current tokens available
	last     time.Time // last refill time
}

func newTokenBucket(rate int64) *tokenBucket {
	if rate <= 0 {
		return &tokenBucket{}
	}
	// Burst capacity: allow up to 1 second worth of tokens, minimum 64 KB.
	capacity := rate
	if capacity < 64*1024 {
		capacity = 64 * 1024
	}
	return &tokenBucket{
		rate:     rate,
		capacity: capacity,
		tokens:   capacity,
		last:     time.Now(),
	}
}

// update changes the rate in bytes per second.
func (tb *tokenBucket) update(rate int64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.rate = rate
	if rate <= 0 {
		tb.tokens = 0
		tb.capacity = 0
		return
	}
	capacity := rate
	if capacity < 64*1024 {
		capacity = 64 * 1024
	}
	tb.capacity = capacity
	if tb.tokens > capacity {
		tb.tokens = capacity
	}
}

// wait consumes n bytes from the bucket, sleeping if necessary to comply with the rate limit.
func (tb *tokenBucket) wait(n int) {
	if tb == nil || n <= 0 {
		return
	}
	for {
		tb.mu.Lock()
		rate := tb.rate
		if rate <= 0 {
			tb.mu.Unlock()
			return
		}
		now := time.Now()
		elapsed := now.Sub(tb.last)
		if elapsed > 0 {
			// Refill tokens based on elapsed time.
			added := int64(elapsed.Seconds() * float64(rate))
			if added > 0 {
				tb.tokens += added
				if tb.tokens > tb.capacity {
					tb.tokens = tb.capacity
				}
				tb.last = now
			}
		}

		req := int64(n)
		if tb.tokens >= req {
			tb.tokens -= req
			tb.mu.Unlock()
			return
		}

		// Not enough tokens: calculate time to sleep.
		needed := req - tb.tokens
		tb.tokens = 0
		tb.last = now
		sleepDur := time.Duration(float64(needed) / float64(rate) * float64(time.Second))
		tb.mu.Unlock()

		if sleepDur > 0 {
			// Cap sleep at 500ms intervals to keep connections responsive.
			if sleepDur > 500*time.Millisecond {
				sleepDur = 500 * time.Millisecond
			}
			time.Sleep(sleepDur)
		}
	}
}
