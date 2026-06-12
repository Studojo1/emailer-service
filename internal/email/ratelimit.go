package email

import (
	"context"
	"sync"
	"time"
)

// rateLimiter is a simple thread-safe token bucket. It is the single global
// throttle in front of the ACS provider so that BOTH instant event sends and
// the scheduler draw from one shared budget — nothing can bypass the cap and
// blow the provider quota during a burst.
type rateLimiter struct {
	mu           sync.Mutex
	tokens       float64
	maxTokens    float64
	refillPerSec float64
	last         time.Time
}

// newRateLimiter builds a bucket sized to perHour sends, starting full so a
// fresh process can absorb an initial burst up to the hourly cap.
func newRateLimiter(perHour int) *rateLimiter {
	if perHour <= 0 {
		perHour = 180
	}
	return &rateLimiter{
		tokens:       float64(perHour),
		maxTokens:    float64(perHour),
		refillPerSec: float64(perHour) / 3600.0,
		last:         time.Now(),
	}
}

// refill adds tokens for time elapsed since the last touch. Caller holds mu.
func (rl *rateLimiter) refill() {
	now := time.Now()
	elapsed := now.Sub(rl.last).Seconds()
	if elapsed <= 0 {
		return
	}
	rl.tokens += elapsed * rl.refillPerSec
	if rl.tokens > rl.maxTokens {
		rl.tokens = rl.maxTokens
	}
	rl.last = now
}

// wait blocks until a token is available or ctx is cancelled, then consumes one.
// Used for marketing / onboarding / sequence sends, which can be paced freely.
func (rl *rateLimiter) wait(ctx context.Context) error {
	for {
		rl.mu.Lock()
		rl.refill()
		if rl.tokens >= 1 {
			rl.tokens--
			rl.mu.Unlock()
			return nil
		}
		deficit := 1 - rl.tokens
		sleep := time.Duration(deficit / rl.refillPerSec * float64(time.Second))
		rl.mu.Unlock()

		if sleep < 50*time.Millisecond {
			sleep = 50 * time.Millisecond
		}
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// tryTake consumes a token if one is available and reports whether it did, but
// never blocks. Used for time-sensitive transactional mail (password resets,
// payment receipts): it still draws down the shared budget so the bucket stays
// honest, but a low budget never delays a security email.
func (rl *rateLimiter) tryTake() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.refill()
	if rl.tokens >= 1 {
		rl.tokens--
		return true
	}
	return false
}
