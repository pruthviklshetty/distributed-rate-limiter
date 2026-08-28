package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A single sweep, driven entirely by the fake clock: only buckets idle past
// the TTL are removed, active ones survive, and an evicted key comes back full.
func TestTokenBucket_EvictIdle(t *testing.T) {
	clk := NewFakeClock(epoch)
	tb := mustTokenBucket(t, 5, 1, clk)

	// Three keys created at epoch.
	for _, k := range []string{"k1", "k2", "k3"} {
		if _, err := tb.Allow(context.Background(), k); err != nil {
			t.Fatalf("Allow(%s): %v", k, err)
		}
	}
	if tb.Len() != 3 {
		t.Fatalf("Len = %d, want 3", tb.Len())
	}

	// 40s later, only k2 sees traffic — its `last` moves to epoch+40s.
	clk.Advance(40 * time.Second)
	if _, err := tb.Allow(context.Background(), "k2"); err != nil {
		t.Fatalf("Allow(k2): %v", err)
	}

	// Another 30s: k1 and k3 have been idle 70s, k2 only 30s.
	clk.Advance(30 * time.Second)

	removed := tb.evictIdle(60 * time.Second)
	if removed != 2 {
		t.Fatalf("evictIdle removed %d, want 2", removed)
	}
	if tb.Len() != 1 {
		t.Fatalf("Len = %d after eviction, want 1", tb.Len())
	}

	// k2 still has its consumed state (capacity 5, one taken, none refilled
	// because... actually 70s at 1/s refills it fully; the point is it exists).
	tb.mu.Lock()
	_, k2Alive := tb.entries["k2"]
	_, k1Alive := tb.entries["k1"]
	tb.mu.Unlock()
	if !k2Alive {
		t.Error("k2 should have survived eviction")
	}
	if k1Alive {
		t.Error("k1 should have been evicted")
	}

	// A fresh request for the evicted k1 recreates it at full capacity.
	res, _ := tb.Allow(context.Background(), "k1")
	if !res.Allowed || res.Remaining != 4 {
		t.Fatalf("recreated k1: Allowed=%v Remaining=%d, want true/4", res.Allowed, res.Remaining)
	}
}

// Nothing is evicted while every key is still within the TTL.
func TestTokenBucket_EvictIdle_KeepsActive(t *testing.T) {
	clk := NewFakeClock(epoch)
	tb := mustTokenBucket(t, 5, 1, clk)

	for _, k := range []string{"a", "b", "c"} {
		tb.Allow(context.Background(), k)
	}
	clk.Advance(10 * time.Second)

	if removed := tb.evictIdle(60 * time.Second); removed != 0 {
		t.Fatalf("evictIdle removed %d, want 0", removed)
	}
	if tb.Len() != 3 {
		t.Fatalf("Len = %d, want 3", tb.Len())
	}
}

// The background loop actually evicts, and stop() halts it and waits for the
// goroutine to exit. The tick interval is real time (a short 5ms) but the TTL
// comparison is fake-clock driven.
func TestTokenBucket_StartJanitor_EvictsAndStops(t *testing.T) {
	clk := NewFakeClock(epoch)
	tb := mustTokenBucket(t, 5, 1, clk)
	tb.Allow(context.Background(), "old")
	if tb.Len() != 1 {
		t.Fatalf("Len = %d, want 1", tb.Len())
	}

	stop := tb.StartJanitor(context.Background(), 30*time.Second, 5*time.Millisecond)
	defer stop()

	// Make "old" exceed the TTL in fake time; the real ticker will fire soon.
	clk.Advance(31 * time.Second)

	deadline := time.After(2 * time.Second)
	for tb.Len() != 0 {
		select {
		case <-deadline:
			t.Fatalf("janitor did not evict within 2s; Len = %d", tb.Len())
		case <-time.After(2 * time.Millisecond):
		}
	}

	// stop() must return promptly and leave no sweeper running.
	returned := make(chan struct{})
	go func() { stop(); close(returned) }()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("stop() did not return within 1s")
	}
}

// Context cancellation stops the loop just like stop().
func TestTokenBucket_StartJanitor_StopsOnContextCancel(t *testing.T) {
	clk := NewFakeClock(epoch)
	tb := mustTokenBucket(t, 5, 1, clk)

	ctx, cancel := context.WithCancel(context.Background())
	stop := tb.StartJanitor(ctx, time.Second, time.Millisecond)

	cancel()

	returned := make(chan struct{})
	go func() { stop(); close(returned) }()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("janitor goroutine did not exit after context cancel")
	}
}

// evictIdle running continuously alongside heavy concurrent Allow traffic must
// not race or panic, and memory must stay bounded. A -race target.
func TestTokenBucket_EvictConcurrentWithAllow(t *testing.T) {
	clk := NewFakeClock(epoch)
	tb := mustTokenBucket(t, 8, 4, clk)

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Sweeper: evict anything idle > 5s, advancing fake time each pass so
	// keys continually age out.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			clk.Advance(time.Second)
			tb.evictIdle(5 * time.Second)
		}
	}()

	// Traffic across a rotating key space so buckets are constantly created
	// and become eligible for eviction.
	const workers = 32
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := "key-" + string(rune('a'+(i+w)%16))
				if _, err := tb.Allow(context.Background(), key); err != nil {
					t.Errorf("Allow: %v", err)
					return
				}
			}
		}(w)
	}

	// Let traffic finish, then stop the sweeper.
	time.Sleep(50 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	if got := tb.Len(); got > 16 {
		t.Fatalf("Len = %d, expected <= 16 (rotating key space)", got)
	}
}
