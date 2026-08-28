package ratelimit

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// TokenBucketConfig configures a TokenBucket limiter.
type TokenBucketConfig struct {
	// Capacity is the maximum number of tokens a bucket can hold. It is the
	// largest burst a single key can ever make from a standing start.
	Capacity int64
	// RefillPerSec is how many tokens are added back per second in steady
	// state. Expressed as a float so rates like "100 per minute" (1.666.../s)
	// are exact rather than rounded.
	RefillPerSec float64
	// Clock is the time source. Leave nil to use RealClock; tests pass a
	// FakeClock so refill can be exercised by advancing time.
	Clock Clock
}

// TokenBucket is an in-memory token-bucket rate limiter.
//
// Model: every key owns a bucket that holds up to Capacity tokens. Each
// allowed request removes one token. Tokens are added back continuously at
// RefillPerSec. A key may burst until its bucket is empty, then is paced at
// the refill rate.
//
// There is no background goroutine topping buckets up. Instead each call does
// a "lazy refill": it looks at how long it has been since the bucket was last
// touched and credits the tokens that would have accrued in that interval,
// clamped to Capacity. This keeps the limiter allocation-free at rest and
// means an idle bucket costs nothing until it is used again.
type TokenBucket struct {
	capacity     float64
	refillPerSec float64
	clock        Clock

	// mu guards the entries map itself (lookup + lazy insert). Once an entry
	// pointer is obtained it is stable for the life of the bucket, so the
	// per-key read-modify-write serialises on the entry's own mutex and does
	// not hold mu. That keeps unrelated keys fully parallel.
	mu      sync.Mutex
	entries map[string]*tbEntry
}

type tbEntry struct {
	mu     sync.Mutex
	tokens float64   // current token count; fractional between requests
	last   time.Time // when tokens/last were last recomputed (also = last access)
}

// NewTokenBucket validates cfg and returns a ready limiter.
func NewTokenBucket(cfg TokenBucketConfig) (*TokenBucket, error) {
	if cfg.Capacity <= 0 {
		return nil, errors.New("ratelimit: TokenBucket capacity must be > 0")
	}
	if cfg.RefillPerSec <= 0 {
		return nil, errors.New("ratelimit: TokenBucket refill rate must be > 0")
	}
	clk := cfg.Clock
	if clk == nil {
		clk = RealClock{}
	}
	return &TokenBucket{
		capacity:     float64(cfg.Capacity),
		refillPerSec: cfg.RefillPerSec,
		clock:        clk,
		entries:      make(map[string]*tbEntry),
	}, nil
}

// entryFor returns the bucket for key, creating a full one on first use.
func (tb *TokenBucket) entryFor(key string) *tbEntry {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	e, ok := tb.entries[key]
	if !ok {
		e = &tbEntry{tokens: tb.capacity, last: tb.clock.Now()}
		tb.entries[key] = e
	}
	return e
}

// Allow implements RateLimiter. The in-memory token bucket never returns a
// non-nil error; the signature carries one only because backends that do I/O
// (Redis) need it.
func (tb *TokenBucket) Allow(_ context.Context, key string) (Result, error) {
	e := tb.entryFor(key)

	e.mu.Lock()
	defer e.mu.Unlock()

	now := tb.clock.Now()
	elapsed := now.Sub(e.last)
	if elapsed < 0 {
		// Clock went backwards (NTP step, or a FakeClock misuse). Don't hand
		// out negative time as free tokens; just don't refill this round.
		elapsed = 0
	}
	// Lazy refill: tokens that would have dripped in during `elapsed`, capped
	// so an idle bucket can never hold more than Capacity.
	e.tokens = math.Min(tb.capacity, e.tokens+elapsed.Seconds()*tb.refillPerSec)
	e.last = now

	res := Result{Limit: int64(tb.capacity)}
	if e.tokens >= 1 {
		e.tokens--
		res.Allowed = true
		res.Remaining = int64(math.Floor(e.tokens))
		return res, nil
	}

	// Under one token: tell the caller how long until one has accrued.
	deficit := 1 - e.tokens
	res.RetryAfter = time.Duration(deficit / tb.refillPerSec * float64(time.Second))
	return res, nil
}

// Len reports how many buckets currently exist. Used by tests and, later, by
// the TTL janitor.
func (tb *TokenBucket) Len() int {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return len(tb.entries)
}
