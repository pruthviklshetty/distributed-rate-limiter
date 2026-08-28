// Package ratelimit contains the rate-limiting algorithms and the single
// interface they all implement. Every algorithm (token bucket, sliding window)
// and every storage backend (in-memory, Redis) satisfies RateLimiter, so the
// HTTP middleware and the rest of the program can treat them interchangeably.
package ratelimit

import (
	"context"
	"time"
)

// Result is the outcome of one rate-limit decision for one key.
//
// The fields map directly onto the response headers the middleware sets:
//   - Limit      -> X-RateLimit-Limit
//   - Remaining  -> X-RateLimit-Remaining
//   - RetryAfter -> Retry-After   (only meaningful when Allowed is false)
type Result struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit is the configured ceiling for the key. For the token bucket this
	// is the bucket capacity (the largest burst the key can ever make).
	Limit int64
	// Remaining is how much budget is left for the key immediately after this
	// decision. For the token bucket it is the whole-token count still in the
	// bucket (floored).
	Remaining int64
	// RetryAfter is how long the caller should wait before the next request
	// would be allowed. It is zero when Allowed is true. Callers that need a
	// whole-second Retry-After header should round this up.
	RetryAfter time.Duration
}

// RateLimiter decides whether a request identified by key may proceed.
//
// Implementations must be safe for concurrent use by multiple goroutines.
// The error return is reserved for infrastructure failures (for example a
// Redis backend that cannot reach its server); an ordinary "over the limit"
// outcome is not an error, it is a Result with Allowed set to false. The
// middleware treats a non-nil error as a signal to fail open.
type RateLimiter interface {
	Allow(ctx context.Context, key string) (Result, error)
}
