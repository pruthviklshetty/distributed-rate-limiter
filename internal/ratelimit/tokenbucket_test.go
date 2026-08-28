package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// epoch is an arbitrary fixed start time for the fake clock.
var epoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func mustTokenBucket(t *testing.T, cap int64, refill float64, clk Clock) *TokenBucket {
	t.Helper()
	tb, err := NewTokenBucket(TokenBucketConfig{Capacity: cap, RefillPerSec: refill, Clock: clk})
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}
	return tb
}

func TestTokenBucket_ConfigValidation(t *testing.T) {
	if _, err := NewTokenBucket(TokenBucketConfig{Capacity: 0, RefillPerSec: 1}); err == nil {
		t.Error("expected error for zero capacity")
	}
	if _, err := NewTokenBucket(TokenBucketConfig{Capacity: 1, RefillPerSec: 0}); err == nil {
		t.Error("expected error for zero refill rate")
	}
}

// Burst: a fresh key may spend its whole capacity immediately, then is denied.
func TestTokenBucket_BurstUpToCapacity(t *testing.T) {
	clk := NewFakeClock(epoch)
	tb := mustTokenBucket(t, 5, 1, clk) // clock frozen: no refill during the test

	for i := 0; i < 5; i++ {
		res, err := tb.Allow(context.Background(), "k")
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if !res.Allowed {
			t.Fatalf("request %d: expected Allowed", i+1)
		}
		if want := int64(5 - i - 1); res.Remaining != want {
			t.Fatalf("request %d: Remaining = %d, want %d", i+1, res.Remaining, want)
		}
	}

	res, _ := tb.Allow(context.Background(), "k")
	if res.Allowed {
		t.Fatal("6th request should be denied")
	}
	if res.Remaining != 0 {
		t.Fatalf("denied Remaining = %d, want 0", res.Remaining)
	}
	// One token accrues in 1s at 1 token/sec, so Retry-After should be ~1s.
	if res.RetryAfter <= 0 || res.RetryAfter > time.Second {
		t.Fatalf("denied RetryAfter = %v, want (0, 1s]", res.RetryAfter)
	}
	if res.Limit != 5 {
		t.Fatalf("Limit = %d, want 5", res.Limit)
	}
}

// Refill: after draining the bucket, advancing time restores tokens at the
// configured rate and no faster.
func TestTokenBucket_RefillOverTime(t *testing.T) {
	clk := NewFakeClock(epoch)
	tb := mustTokenBucket(t, 10, 2, clk) // 2 tokens/sec

	for i := 0; i < 10; i++ {
		if res, _ := tb.Allow(context.Background(), "k"); !res.Allowed {
			t.Fatalf("drain %d: unexpected denial", i)
		}
	}
	if res, _ := tb.Allow(context.Background(), "k"); res.Allowed {
		t.Fatal("bucket should be empty")
	}

	// After 1s, 2 tokens are back: exactly two requests succeed.
	clk.Advance(time.Second)
	for i := 0; i < 2; i++ {
		if res, _ := tb.Allow(context.Background(), "k"); !res.Allowed {
			t.Fatalf("post-refill request %d should be allowed", i+1)
		}
	}
	if res, _ := tb.Allow(context.Background(), "k"); res.Allowed {
		t.Fatal("only 2 tokens should have refilled in 1s")
	}

	// Partial accrual: 0.5s at 2/sec = 1 token.
	clk.Advance(500 * time.Millisecond)
	if res, _ := tb.Allow(context.Background(), "k"); !res.Allowed {
		t.Fatal("1 token should have accrued in 0.5s")
	}
	if res, _ := tb.Allow(context.Background(), "k"); res.Allowed {
		t.Fatal("only 1 token should have accrued in 0.5s")
	}
}

// Cap: an arbitrarily long idle period cannot push a bucket past its capacity.
func TestTokenBucket_NeverExceedsCapacity(t *testing.T) {
	clk := NewFakeClock(epoch)
	tb := mustTokenBucket(t, 3, 1000, clk)

	// Idle for an hour with a fast refill rate, then verify only `capacity`
	// requests go through before denial.
	clk.Advance(time.Hour)

	allowed := 0
	for i := 0; i < 100; i++ {
		if res, _ := tb.Allow(context.Background(), "k"); res.Allowed {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("allowed %d requests after long idle, want 3 (capacity)", allowed)
	}
}

// Independence: draining one key does not affect another.
func TestTokenBucket_KeysAreIndependent(t *testing.T) {
	clk := NewFakeClock(epoch)
	tb := mustTokenBucket(t, 2, 1, clk)

	for i := 0; i < 2; i++ {
		if res, _ := tb.Allow(context.Background(), "a"); !res.Allowed {
			t.Fatalf("key a request %d should be allowed", i+1)
		}
	}
	if res, _ := tb.Allow(context.Background(), "a"); res.Allowed {
		t.Fatal("key a should be drained")
	}

	// key b is untouched and must still have its full capacity.
	for i := 0; i < 2; i++ {
		if res, _ := tb.Allow(context.Background(), "b"); !res.Allowed {
			t.Fatalf("key b request %d should be allowed", i+1)
		}
	}
	if tb.Len() != 2 {
		t.Fatalf("Len = %d, want 2", tb.Len())
	}
}

// Concurrency: with the clock frozen so no tokens refill, exactly `capacity`
// requests out of many concurrent ones may be allowed — never more. Run under
// `go test -race` to exercise the locking.
func TestTokenBucket_ConcurrentNeverOverGrants(t *testing.T) {
	clk := NewFakeClock(epoch)
	const capacity = 100
	tb := mustTokenBucket(t, capacity, 1, clk)

	const goroutines = 64
	const perGoroutine = 50 // 3200 attempts against 100 tokens

	var allowed int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < perGoroutine; i++ {
				if res, _ := tb.Allow(context.Background(), "shared"); res.Allowed {
					atomic.AddInt64(&allowed, 1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if allowed != capacity {
		t.Fatalf("granted %d requests, want exactly %d", allowed, capacity)
	}
}

// Concurrency across distinct keys: no shared state should be corrupted and
// every key gets its own full capacity. Also a -race target.
func TestTokenBucket_ConcurrentDistinctKeys(t *testing.T) {
	clk := NewFakeClock(epoch)
	tb := mustTokenBucket(t, 4, 1, clk)

	const keys = 200
	var wg sync.WaitGroup
	results := make([]int64, keys)
	for k := 0; k < keys; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := string(rune('A'+k%26)) + string(rune('0'+k/26))
			var got int64
			for i := 0; i < 20; i++ {
				if res, _ := tb.Allow(context.Background(), key); res.Allowed {
					got++
				}
			}
			results[k] = got
		}(k)
	}
	wg.Wait()

	for k, got := range results {
		if got != 4 {
			t.Fatalf("key index %d: allowed %d, want 4", k, got)
		}
	}
	if tb.Len() != keys {
		t.Fatalf("Len = %d, want %d", tb.Len(), keys)
	}
}
