package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	// Tight timeouts and no retries so tests that deliberately close Redis
	// fail fast instead of spending seconds in go-redis backoff.
	rdb := redis.NewClient(&redis.Options{
		Addr:         mr.Addr(),
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
		MaxRetries:   -1,
	})
	t.Cleanup(func() {
		_ = rdb.Close()
		mr.Close()
	})
	return mr, rdb
}

func mustRedisTB(t *testing.T, rdb redis.Scripter, cap int64, refill float64, clk Clock) *RedisTokenBucket {
	t.Helper()
	tb, err := NewRedisTokenBucket(RedisTokenBucketConfig{
		Client: rdb, Capacity: cap, RefillPerSec: refill, Clock: clk,
		KeyPrefix: "rl:tb:", IdleTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewRedisTokenBucket: %v", err)
	}
	return tb
}

// Same observable behaviour as the in-memory bucket: burst to capacity, deny,
// refill by advancing the clock, and never exceed capacity after a long idle.
func TestRedisTokenBucket_BurstRefillAndCap(t *testing.T) {
	_, rdb := newTestRedis(t)
	clk := NewFakeClock(epoch)
	tb := mustRedisTB(t, rdb, 5, 1, clk)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		res, err := tb.Allow(ctx, "k")
		if err != nil {
			t.Fatalf("Allow %d: %v", i, err)
		}
		if !res.Allowed || res.Remaining != int64(4-i) {
			t.Fatalf("Allow %d: Allowed=%v Remaining=%d", i, res.Allowed, res.Remaining)
		}
	}
	res, _ := tb.Allow(ctx, "k")
	if res.Allowed {
		t.Fatal("6th request should be denied")
	}
	if res.RetryAfter <= 0 || res.RetryAfter > time.Second {
		t.Fatalf("RetryAfter = %v, want (0, 1s]", res.RetryAfter)
	}

	clk.Advance(3 * time.Second)
	allowed := 0
	for i := 0; i < 10; i++ {
		if r, _ := tb.Allow(ctx, "k"); r.Allowed {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("after 3s refill: allowed %d, want 3", allowed)
	}

	clk.Advance(time.Hour)
	allowed = 0
	for i := 0; i < 20; i++ {
		if r, _ := tb.Allow(ctx, "k"); r.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("after long idle: allowed %d, want 5 (capacity)", allowed)
	}
}

func TestRedisTokenBucket_KeysIndependent(t *testing.T) {
	_, rdb := newTestRedis(t)
	clk := NewFakeClock(epoch)
	tb := mustRedisTB(t, rdb, 2, 1, clk)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if r, _ := tb.Allow(ctx, "a"); !r.Allowed {
			t.Fatalf("a #%d should pass", i)
		}
	}
	if r, _ := tb.Allow(ctx, "a"); r.Allowed {
		t.Fatal("a should be drained")
	}
	for i := 0; i < 2; i++ {
		if r, _ := tb.Allow(ctx, "b"); !r.Allowed {
			t.Fatalf("b #%d should pass", i)
		}
	}
}

// The atomicity proof: many goroutines (simulating many API instances sharing
// one Redis) race on one key with the clock frozen so nothing refills. The Lua
// script guarantees exactly `capacity` grants, never more.
func TestRedisTokenBucket_AtomicUnderConcurrentLoad(t *testing.T) {
	_, rdb := newTestRedis(t)
	clk := NewFakeClock(epoch)
	const capacity = 50
	tb := mustRedisTB(t, rdb, capacity, 1, clk)

	const goroutines = 300
	var allowed int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if r, err := tb.Allow(context.Background(), "shared"); err == nil && r.Allowed {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if allowed != capacity {
		t.Fatalf("Lua script granted %d, want exactly %d", allowed, capacity)
	}
}

// Before/after demonstration. With a forced read-then-write interleave:
//   - the naive GET/SET limiter over-grants wildly (every caller saw a full
//     bucket during its read),
//   - the Lua-script limiter grants exactly capacity on the identical load.
func TestRedis_ScriptFixesTheNaiveRace(t *testing.T) {
	const capacity = 5
	const callers = 40

	t.Run("naive GET/SET over-grants", func(t *testing.T) {
		_, rdb := newTestRedis(t)
		clk := NewFakeClock(epoch)
		naive, err := NewRedisNaiveTokenBucket(RedisTokenBucketConfig{
			Client: rdb, Capacity: capacity, RefillPerSec: 1, Clock: clk, KeyPrefix: "n:",
		})
		if err != nil {
			t.Fatal(err)
		}

		// Barrier: no goroutine writes until all have read.
		var readWG sync.WaitGroup
		readWG.Add(callers)
		release := make(chan struct{})
		naive.beforeWrite = func() {
			readWG.Done()
			<-release
		}
		go func() { readWG.Wait(); close(release) }()

		var allowed int64
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if r, err := naive.Allow(context.Background(), "k"); err == nil && r.Allowed {
					atomic.AddInt64(&allowed, 1)
				}
			}()
		}
		wg.Wait()

		if allowed <= capacity {
			t.Fatalf("naive limiter granted %d; expected over-grant (> %d)", allowed, capacity)
		}
		t.Logf("naive limiter granted %d against capacity %d (race)", allowed, capacity)
	})

	t.Run("Lua script holds the line", func(t *testing.T) {
		_, rdb := newTestRedis(t)
		clk := NewFakeClock(epoch)
		tb := mustRedisTB(t, rdb, capacity, 1, clk)

		var allowed int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if r, err := tb.Allow(context.Background(), "k"); err == nil && r.Allowed {
					atomic.AddInt64(&allowed, 1)
				}
			}()
		}
		close(start)
		wg.Wait()

		if allowed != capacity {
			t.Fatalf("Lua limiter granted %d, want exactly %d", allowed, capacity)
		}
	})
}

// Every call sets a TTL, so Redis evicts idle keys without a janitor.
func TestRedisTokenBucket_SetsTTLForCleanup(t *testing.T) {
	mr, rdb := newTestRedis(t)
	tb := mustRedisTB(t, rdb, 5, 1, NewFakeClock(epoch))

	if _, err := tb.Allow(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	ttl := mr.TTL("rl:tb:k")
	if ttl <= 0 || ttl > time.Hour {
		t.Fatalf("TTL = %v, want (0, 1h]", ttl)
	}
}

// A Redis that cannot be reached surfaces as an error (which FallbackLimiter
// and the middleware then handle).
func TestRedisTokenBucket_ErrorWhenRedisDown(t *testing.T) {
	mr, rdb := newTestRedis(t)
	tb := mustRedisTB(t, rdb, 5, 1, NewFakeClock(epoch))
	mr.Close()

	if _, err := tb.Allow(context.Background(), "k"); err == nil {
		t.Fatal("expected an error when Redis is unreachable")
	}
}

// --- FallbackLimiter ---

type stubLimiter struct {
	res   Result
	err   error
	calls atomic.Int64
}

func (s *stubLimiter) Allow(context.Context, string) (Result, error) {
	s.calls.Add(1)
	return s.res, s.err
}

func TestFallbackLimiter_PrimaryHealthyPassesThrough(t *testing.T) {
	primary := &stubLimiter{res: Result{Allowed: true, Limit: 10, Remaining: 7}}
	fallback := &stubLimiter{}
	f := NewFallbackLimiter(primary, fallback)

	res, err := f.Allow(context.Background(), "k")
	if err != nil || !res.Allowed || res.Remaining != 7 {
		t.Fatalf("got %+v, %v", res, err)
	}
	if fallback.calls.Load() != 0 {
		t.Fatalf("fallback called %d times, want 0", fallback.calls.Load())
	}
}

func TestFallbackLimiter_PrimaryErrorUsesFallback(t *testing.T) {
	primary := &stubLimiter{err: errors.New("redis down")}
	fallback := mustTokenBucket(t, 2, 1, NewFakeClock(epoch))

	var fallbacks int64
	f := NewFallbackLimiter(primary, fallback)
	f.OnFallback = func(error) { atomic.AddInt64(&fallbacks, 1) }

	allowed := 0
	for i := 0; i < 5; i++ {
		res, err := f.Allow(context.Background(), "k")
		if err != nil {
			t.Fatalf("fallback path should not error: %v", err)
		}
		if res.Allowed {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("fallback limiter allowed %d, want 2 (its capacity)", allowed)
	}
	if fallbacks != 5 {
		t.Fatalf("OnFallback fired %d times, want 5", fallbacks)
	}
}

func TestFallbackLimiter_RedisOutageStillLimitsLocally(t *testing.T) {
	mr, rdb := newTestRedis(t)
	clk := NewFakeClock(epoch)
	primary := mustRedisTB(t, rdb, 100, 1, clk)
	local := mustTokenBucket(t, 3, 1, clk)
	f := NewFallbackLimiter(primary, local)
	ctx := context.Background()

	// Healthy: served by Redis (capacity 100), local bucket untouched.
	for i := 0; i < 10; i++ {
		if r, _ := f.Allow(ctx, "k"); !r.Allowed {
			t.Fatalf("request %d should pass via Redis", i)
		}
	}

	// Redis goes away: now the local capacity-3 bucket is the enforcer.
	mr.Close()
	allowed := 0
	for i := 0; i < 10; i++ {
		if r, err := f.Allow(ctx, "k"); err == nil && r.Allowed {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("during outage local limiter allowed %d, want 3", allowed)
	}
}
