package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMultiLimiter_RequiresATier(t *testing.T) {
	if _, err := NewMultiLimiter(); err == nil {
		t.Fatal("expected error with no tiers")
	}
}

// A burst tier (3 tokens, 3/s) in front of a sustained tier (5 tokens, slow
// refill). Each tier can be the one that rejects, depending on traffic shape.
func TestMultiLimiter_BothMustPass(t *testing.T) {
	clk := NewFakeClock(epoch)
	burst := mustTokenBucket(t, 3, 3, clk)            // 3/sec
	sustained := mustTokenBucket(t, 5, 5.0/60.0, clk) // 5/min
	m, err := NewMultiLimiter(burst, sustained)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// First 3 pass (both tiers have room).
	for i := 0; i < 3; i++ {
		if r, _ := m.Allow(ctx, "k"); !r.Allowed {
			t.Fatalf("request %d should pass", i+1)
		}
	}
	// 4th: burst is empty -> rejected by the burst tier.
	if r, _ := m.Allow(ctx, "k"); r.Allowed {
		t.Fatal("4th request should be rejected by the burst tier")
	}

	// Refill the burst tier only (1s -> +3 burst tokens; sustained gains 0.08).
	clk.Advance(time.Second)

	// Sustained had 5, spent 3, so 2 left: exactly two more pass.
	for i := 0; i < 2; i++ {
		if r, _ := m.Allow(ctx, "k"); !r.Allowed {
			t.Fatalf("post-refill request %d should pass", i+1)
		}
	}
	// Now burst still has a token but sustained is exhausted -> sustained rejects.
	r, _ := m.Allow(ctx, "k")
	if r.Allowed {
		t.Fatal("request should be rejected by the sustained tier")
	}
	if r.RetryAfter <= 0 {
		t.Fatalf("expected a RetryAfter from the sustained tier, got %v", r.RetryAfter)
	}
}

// When all tiers allow, headers reflect the tier with the least budget left.
func TestMultiLimiter_ReportsMostConstrainedTier(t *testing.T) {
	clk := NewFakeClock(epoch)
	wide := mustTokenBucket(t, 100, 1, clk)
	narrow := mustTokenBucket(t, 10, 1, clk)
	m, _ := NewMultiLimiter(wide, narrow)

	r, err := m.Allow(context.Background(), "k")
	if err != nil || !r.Allowed {
		t.Fatalf("got %+v, %v", r, err)
	}
	if r.Limit != 10 || r.Remaining != 9 {
		t.Fatalf("Limit/Remaining = %d/%d, want 10/9 (the narrow tier)", r.Limit, r.Remaining)
	}
}

func TestMultiLimiter_PropagatesTierError(t *testing.T) {
	clk := NewFakeClock(epoch)
	good := mustTokenBucket(t, 5, 1, clk)
	bad := &stubLimiter{err: errors.New("backend down")}
	m, _ := NewMultiLimiter(good, bad)

	if _, err := m.Allow(context.Background(), "k"); err == nil {
		t.Fatal("expected the tier error to propagate")
	}
}

// Tiers can mix backends: in-memory burst + Redis sustained, both enforced.
func TestMultiLimiter_MixedBackends(t *testing.T) {
	_, rdb := newTestRedis(t)
	clk := NewFakeClock(epoch)
	burst := mustTokenBucket(t, 10, 10, clk)
	sustained := mustRedisTB(t, rdb, 4, 1, clk)
	m, _ := NewMultiLimiter(burst, sustained)
	ctx := context.Background()

	allowed := 0
	for i := 0; i < 20; i++ {
		if r, err := m.Allow(ctx, "k"); err == nil && r.Allowed {
			allowed++
		}
	}
	// Burst allows 10, but the Redis sustained tier caps it at 4.
	if allowed != 4 {
		t.Fatalf("allowed %d, want 4 (Redis sustained tier is binding)", allowed)
	}
}

// -race: concurrent load never exceeds the smallest tier capacity.
func TestMultiLimiter_ConcurrentNeverOverGrants(t *testing.T) {
	clk := NewFakeClock(epoch)
	a := mustTokenBucket(t, 200, 1, clk)
	b := mustTokenBucket(t, 75, 1, clk) // binding
	m, _ := NewMultiLimiter(a, b)

	var allowed int64
	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if r, _ := m.Allow(context.Background(), "shared"); r.Allowed {
					atomic.AddInt64(&allowed, 1)
				}
			}
		}()
	}
	wg.Wait()

	if allowed != 75 {
		t.Fatalf("granted %d, want exactly 75 (smallest tier)", allowed)
	}
}
