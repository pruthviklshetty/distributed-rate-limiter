// Package limiter wires the ratelimit algorithms and backends into a single
// hot-swappable RateLimiter. The dashboard can change the active algorithm at
// runtime (POST /api/config/algorithm) without a restart: Switch builds the
// new limiter, swaps it in behind an RWMutex, and stops the old janitor.
//
// Per-key state does not carry across a swap — the new algorithm starts fresh.
// The choice is not persisted, so a redeploy reverts to the configured -algo.
package limiter

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/pruthviklshetty/distributed-rate-limiter/internal/api"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/ratelimit"
	"github.com/redis/go-redis/v9"
)

// Supported algorithm identifiers, as accepted by -algo and the switch API.
const (
	AlgoTokenBucket   = "token-bucket"
	AlgoSlidingWindow = "sliding-window"
)

// Params are the limits held constant across an algorithm swap.
type Params struct {
	Limit   int64         // token-bucket capacity, or sliding-window request limit
	Refill  float64       // token-bucket refill rate (tokens/sec)
	Window  time.Duration // sliding-window length
	IdleTTL time.Duration // evict a key's state after this much inactivity
}

// Switch is a RateLimiter whose underlying algorithm can be replaced at
// runtime. It is safe for concurrent use.
type Switch struct {
	ctx context.Context
	rdb *redis.Client // nil => in-memory only
	p   Params

	mu    sync.RWMutex
	algo  string
	cur   ratelimit.RateLimiter
	label string
	stopJ func() // stops the current limiter's janitor
}

// NewSwitch builds a Switch starting on algo. rdb may be nil (in-memory only);
// when set, the token-bucket algorithm uses the Redis backend with the
// in-memory limiter as its fallback.
func NewSwitch(ctx context.Context, rdb *redis.Client, algo string, p Params) (*Switch, error) {
	s := &Switch{ctx: ctx, rdb: rdb, p: p}
	cur, label, stopJ, err := s.build(algo)
	if err != nil {
		return nil, err
	}
	s.algo, s.cur, s.label, s.stopJ = algo, cur, label, stopJ
	return s, nil
}

// Allow implements ratelimit.RateLimiter by forwarding to the current algorithm.
func (s *Switch) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	s.mu.RLock()
	cur := s.cur
	s.mu.RUnlock()
	return cur.Allow(ctx, key)
}

// SetAlgorithm swaps the active algorithm. It is a no-op if algo is already
// active, and returns an error (leaving the current limiter untouched) if algo
// is unknown or the new limiter fails to build.
func (s *Switch) SetAlgorithm(algo string) error {
	if algo != AlgoTokenBucket && algo != AlgoSlidingWindow {
		return fmt.Errorf("limiter: unknown algorithm %q (want %q or %q)", algo, AlgoTokenBucket, AlgoSlidingWindow)
	}

	s.mu.Lock()
	if algo == s.algo {
		s.mu.Unlock()
		return nil
	}
	cur, label, stopJ, err := s.build(algo)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	oldStop := s.stopJ
	s.algo, s.cur, s.label, s.stopJ = algo, cur, label, stopJ
	s.mu.Unlock()

	// stopJ blocks until the old janitor goroutine returns; do it outside the
	// lock so in-flight Allow calls are not held up.
	oldStop()
	return nil
}

// ConfigInfo reports the active configuration for GET /api/config. KeyBy is
// left empty for the caller (which knows the middleware's key function) to set.
func (s *Switch) ConfigInfo() api.ConfigInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tier := api.TierInfo{Name: s.algo, Limit: s.p.Limit}
	switch s.algo {
	case AlgoTokenBucket:
		tier.RefillPerSec = s.p.Refill
	case AlgoSlidingWindow:
		tier.WindowSeconds = s.p.Window.Seconds()
	}

	backend := "in-memory"
	if s.algo == AlgoTokenBucket && s.rdb != nil {
		backend = "redis+fallback"
	}

	return api.ConfigInfo{
		Algorithm: s.label,
		Backend:   backend,
		Tiers:     []api.TierInfo{tier},
	}
}

// Algorithm returns the current display label (e.g. "token-bucket+redis"),
// used to tag decision events.
func (s *Switch) Algorithm() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.label
}

// Close stops the current janitor. The Redis client, if any, is owned by the
// caller and closed separately.
func (s *Switch) Close() {
	s.mu.Lock()
	stop := s.stopJ
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// build constructs a limiter for algo from the Switch's fixed params and
// dependencies, returning it with a display label and a janitor-stop func.
func (s *Switch) build(algo string) (ratelimit.RateLimiter, string, func(), error) {
	switch algo {
	case AlgoTokenBucket:
		local, err := ratelimit.NewTokenBucket(ratelimit.TokenBucketConfig{
			Capacity: s.p.Limit, RefillPerSec: s.p.Refill,
		})
		if err != nil {
			return nil, "", nil, err
		}
		stopJ := local.StartJanitor(s.ctx, s.p.IdleTTL, s.p.IdleTTL/4)

		if s.rdb == nil {
			return local, AlgoTokenBucket, stopJ, nil
		}
		remote, err := ratelimit.NewRedisTokenBucket(ratelimit.RedisTokenBucketConfig{
			Client: s.rdb, Capacity: s.p.Limit, RefillPerSec: s.p.Refill,
			KeyPrefix: "rl:tb:", IdleTTL: s.p.IdleTTL,
		})
		if err != nil {
			stopJ()
			return nil, "", nil, err
		}
		fb := ratelimit.NewFallbackLimiter(remote, local)
		fb.OnFallback = func(err error) { slog.Warn("redis fallback to local limiter", "err", err) }
		return fb, AlgoTokenBucket + "+redis", stopJ, nil

	case AlgoSlidingWindow:
		sw, err := ratelimit.NewSlidingWindow(ratelimit.SlidingWindowConfig{
			Limit: s.p.Limit, WindowLen: s.p.Window,
		})
		if err != nil {
			return nil, "", nil, err
		}
		stopJ := sw.StartJanitor(s.ctx, s.p.IdleTTL, s.p.IdleTTL/4)
		return sw, AlgoSlidingWindow, stopJ, nil

	default:
		return nil, "", nil, fmt.Errorf("limiter: unknown algorithm %q", algo)
	}
}
