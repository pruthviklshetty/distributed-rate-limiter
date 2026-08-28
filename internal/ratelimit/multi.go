package ratelimit

import (
	"context"
	"errors"
)

// MultiLimiter composes several limiters into one. A request is allowed only
// if every tier allows it. The classic use is a small fast burst limit
// (e.g. 10/sec) together with a larger sustained limit (e.g. 100/min): short
// spikes are absorbed while the long-run average is still capped.
//
// Because each tier is just a RateLimiter, the tiers can mix backends freely —
// an in-memory burst limiter in front of a Redis-backed sustained limiter, for
// instance.
//
// Consumption semantics (worth being explicit about in an interview): tiers
// are evaluated in order and evaluation stops at the first denial. A tier that
// allowed the request before a later tier denied it has still spent one unit —
// there is no rollback, because RateLimiter has no "release" operation. The
// effect is that a request rejected by tier N has already been counted by
// tiers 1..N-1. Put the limit most likely to reject first (usually the burst
// limit) to keep that wasted consumption small.
type MultiLimiter struct {
	tiers []RateLimiter
}

// NewMultiLimiter composes tiers, evaluated in the order given.
func NewMultiLimiter(tiers ...RateLimiter) (*MultiLimiter, error) {
	if len(tiers) == 0 {
		return nil, errors.New("ratelimit: MultiLimiter needs at least one tier")
	}
	return &MultiLimiter{tiers: tiers}, nil
}

// Allow returns Allowed only if all tiers allow.
//
//   - First tier error is returned immediately (the middleware fails open).
//   - First tier denial is returned immediately, with that tier's RetryAfter,
//     and no later tier is consulted.
//   - If all tiers allow, the returned Limit/Remaining come from the most
//     constrained tier (smallest Remaining) so the response headers reflect
//     whichever limit the client is closest to hitting.
func (m *MultiLimiter) Allow(ctx context.Context, key string) (Result, error) {
	binding := Result{Allowed: true, Remaining: -1}
	for _, tier := range m.tiers {
		res, err := tier.Allow(ctx, key)
		if err != nil {
			return Result{}, err
		}
		if !res.Allowed {
			return res, nil
		}
		if binding.Remaining < 0 || res.Remaining < binding.Remaining {
			binding.Remaining = res.Remaining
			binding.Limit = res.Limit
		}
	}
	if binding.Remaining < 0 {
		binding.Remaining = 0
	}
	return binding, nil
}
