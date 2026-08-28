package ratelimit

import "context"

// FallbackLimiter degrades from a primary limiter to a secondary one whenever
// the primary returns an error.
//
// The intended use is Primary = RedisTokenBucket, Fallback = in-memory
// TokenBucket. If Redis becomes unreachable, the alternative to this type is
// the middleware's fail-open behaviour, which removes rate limiting entirely
// for the duration of the outage. That is a bad failure mode: a Redis blip
// becomes an unprotected origin. Falling back to a per-instance in-memory
// limiter keeps limiting active — global counts become approximate (each
// instance enforces its own copy) but the service stays protected, and the
// system self-heals the moment Redis answers again.
type FallbackLimiter struct {
	Primary  RateLimiter
	Fallback RateLimiter
	// OnFallback, if set, is called each time a primary error forces the
	// fallback path. It must not block (it runs on the request path); use it
	// for a metric or a rate-limited log line.
	OnFallback func(err error)
}

// NewFallbackLimiter wires a primary and a fallback limiter together.
func NewFallbackLimiter(primary, fallback RateLimiter) *FallbackLimiter {
	return &FallbackLimiter{Primary: primary, Fallback: fallback}
}

// Allow tries the primary limiter. On error it reports the error through
// OnFallback (if set) and returns the fallback limiter's decision with a nil
// error, so callers treat the degraded result as authoritative rather than
// failing open on top of it.
func (f *FallbackLimiter) Allow(ctx context.Context, key string) (Result, error) {
	res, err := f.Primary.Allow(ctx, key)
	if err == nil {
		return res, nil
	}
	if f.OnFallback != nil {
		f.OnFallback(err)
	}
	return f.Fallback.Allow(ctx, key)
}
