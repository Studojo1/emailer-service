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

// newRateLimiter builds a bucket sized to perHour sends.
//
// The bucket starts with a SMALL initial allowance, not full. Starting full
// meant every pod restart (i.e. every deploy) handed the sender a fresh hour's
// worth of tokens at once; a deploy during an active bulk/flow send then fired
// that whole burst into ACS and tripped the provider's real
// PerSubscriptionPerHourLimitExceeded cap (HTTP 429), throttling ALL sends for
// ~an hour. ACS enforces a rolling hourly window on its side, so our bucket must
// not assume a clean slate on restart. A small warm-up allowance lets a fresh
// pod send a little immediately, then it refills at the steady perHour rate.
func newRateLimiter(perHour int) *rateLimiter {
	if perHour <= 0 {
		perHour = 180
	}
	// Warm-up allowance: at most ~1 minute's worth of tokens (perHour/60), capped
	// low. Enough to not stall a quiet pod, small enough that a restart mid-send
	// cannot burst past the ACS hourly window.
	initial := float64(perHour) / 60.0
	if initial > 5 {
		initial = 5
	}
	if initial < 1 {
		initial = 1
	}
	return &rateLimiter{
		tokens:       initial,
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
