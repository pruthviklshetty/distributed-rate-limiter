package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func mustSliding(t *testing.T, limit int64, window time.Duration, clk Clock) *SlidingWindow {
	t.Helper()
	sw, err := NewSlidingWindow(SlidingWindowConfig{Limit: limit, WindowLen: window, Clock: clk})
	if err != nil {
		t.Fatalf("NewSlidingWindow: %v", err)
	}
	return sw
}

func TestSlidingWindow_ConfigValidation(t *testing.T) {
	if _, err := NewSlidingWindow(SlidingWindowConfig{Limit: 0, WindowLen: time.Second}); err == nil {
		t.Error("expected error for zero limit")
	}
	if _, err := NewSlidingWindow(SlidingWindowConfig{Limit: 1, WindowLen: 0}); err == nil {
		t.Error("expected error for zero window")
	}
}

func TestSlidingWindow_AllowsUpToLimitInWindow(t *testing.T) {
	clk := NewFakeClock(epoch)
	sw := mustSliding(t, 10, time.Minute, clk)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		r, err := sw.Allow(ctx, "k")
		if err != nil {
			t.Fatalf("Allow %d: %v", i, err)
		}
		if !r.Allowed {
			t.Fatalf("request %d should be allowed", i+1)
		}
		if want := int64(9 - i); r.Remaining != want {
			t.Fatalf("request %d: Remaining = %d, want %d", i+1, r.Remaining, want)
		}
	}
	if r, _ := sw.Allow(ctx, "k"); r.Allowed {
		t.Fatal("11th request should be rejected")
	}
}

// The defining property: unlike a fixed-window counter, a full window followed
// immediately by the next window does NOT hand the client a second full quota.
func TestSlidingWindow_SmoothsTheBoundaryBurst(t *testing.T) {
	clk := NewFakeClock(epoch)
	sw := mustSliding(t, 10, time.Minute, clk)
	ctx := context.Background()

	// Fill window 1.
	for i := 0; i < 10; i++ {
		if r, _ := sw.Allow(ctx, "k"); !r.Allowed {
			t.Fatalf("fill request %d should pass", i+1)
		}
	}

	// Step exactly one window. The previous count (10) now overlaps the
	// rolling window 100%, so estimate == 10 and the very next request is
	// rejected. A fixed-window counter would allow 10 more here.
	clk.Advance(time.Minute)
	if r, _ := sw.Allow(ctx, "k"); r.Allowed {
		t.Fatal("request right after the boundary should be rejected (fixed window would allow it)")
	}

	// Halfway into window 2 the previous count is weighted 0.5 -> estimate 5,
	// so exactly 5 more requests fit before the limit.
	clk.Advance(30 * time.Second)
	allowed := 0
	for i := 0; i < 10; i++ {
		if r, _ := sw.Allow(ctx, "k"); r.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("mid-window allowed %d, want 5 (prev count weighted 0.5)", allowed)
	}
}

func TestSlidingWindow_WindowSlidesFullyAfterIdle(t *testing.T) {
	clk := NewFakeClock(epoch)
	sw := mustSliding(t, 10, time.Minute, clk)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		sw.Allow(ctx, "k")
	}
	// Skip several whole windows: both counters clear, full quota returns.
	clk.Advance(5 * time.Minute)
	allowed := 0
	for i := 0; i < 20; i++ {
		if r, _ := sw.Allow(ctx, "k"); r.Allowed {
			allowed++
		}
	}
	if allowed != 10 {
		t.Fatalf("after long idle allowed %d, want 10", allowed)
	}
}

func TestSlidingWindow_KeysAreIndependent(t *testing.T) {
	clk := NewFakeClock(epoch)
	sw := mustSliding(t, 3, time.Minute, clk)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if r, _ := sw.Allow(ctx, "a"); !r.Allowed {
			t.Fatalf("a #%d should pass", i+1)
		}
	}
	if r, _ := sw.Allow(ctx, "a"); r.Allowed {
		t.Fatal("a should be exhausted")
	}
	for i := 0; i < 3; i++ {
		if r, _ := sw.Allow(ctx, "b"); !r.Allowed {
			t.Fatalf("b #%d should pass", i+1)
		}
	}
}

func TestSlidingWindow_RetryAfterWithinWindow(t *testing.T) {
	clk := NewFakeClock(epoch)
	sw := mustSliding(t, 5, time.Minute, clk)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		sw.Allow(ctx, "k")
	}
	r, _ := sw.Allow(ctx, "k")
	if r.Allowed {
		t.Fatal("6th should be rejected")
	}
	if r.RetryAfter <= 0 || r.RetryAfter > time.Minute {
		t.Fatalf("RetryAfter = %v, want (0, 1m]", r.RetryAfter)
	}
}

// -race: frozen clock, exactly Limit grants out of a concurrent stampede.
func TestSlidingWindow_ConcurrentNeverOverGrants(t *testing.T) {
	clk := NewFakeClock(epoch)
	const limit = 100
	sw := mustSliding(t, limit, time.Minute, clk)

	var allowed int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 50; i++ {
				if r, _ := sw.Allow(context.Background(), "shared"); r.Allowed {
					atomic.AddInt64(&allowed, 1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if allowed != limit {
		t.Fatalf("granted %d, want exactly %d", allowed, limit)
	}
}

func TestSlidingWindow_EvictIdle(t *testing.T) {
	clk := NewFakeClock(epoch)
	sw := mustSliding(t, 5, time.Minute, clk)
	ctx := context.Background()

	sw.Allow(ctx, "k1")
	sw.Allow(ctx, "k2")
	clk.Advance(2 * time.Minute)
	sw.Allow(ctx, "k2") // refresh k2

	clk.Advance(2 * time.Minute)
	if removed := sw.evictIdle(3 * time.Minute); removed != 1 {
		t.Fatalf("evictIdle removed %d, want 1", removed)
	}
	if sw.Len() != 1 {
		t.Fatalf("Len = %d, want 1", sw.Len())
	}
}
