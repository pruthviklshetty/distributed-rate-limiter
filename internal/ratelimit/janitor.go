package ratelimit

import (
	"context"
	"time"
)

// This file adds idle-bucket eviction to TokenBucket.
//
// Why it is needed: entryFor creates a bucket the first time a key is seen and
// never removes it. When keys are effectively unbounded — per-IP limiting
// behind a large NAT, a scanner cycling source addresses, per-request API
// tokens — the entries map grows without limit and is a slow memory leak.
//
// The fix is a janitor that periodically drops buckets which have not been
// touched for longer than a configurable idle TTL. A dropped bucket is
// harmless to lose: the key's very next request lazily recreates it, full.
// Any key still receiving traffic has a recent `last` and is never evicted.

// evictIdle removes every bucket whose last activity is older than idleFor and
// returns how many were removed. It is the single-sweep primitive the
// background janitor calls; tests call it directly after advancing a FakeClock
// so eviction can be verified without real time passing.
//
// Locking: tb.mu is held for the whole sweep, which briefly blocks new-key
// creation. Each bucket's own mutex is taken only to read its `last` field
// (a multi-word value that a concurrent Allow may be writing). Token math
// never blocks, so these per-bucket locks are held for nanoseconds. The lock
// order (tb.mu then entry.mu) matches Allow, which releases tb.mu before
// taking entry.mu, so there is no deadlock cycle.
func (tb *TokenBucket) evictIdle(idleFor time.Duration) int {
	now := tb.clock.Now()

	tb.mu.Lock()
	defer tb.mu.Unlock()

	removed := 0
	for key, e := range tb.entries {
		e.mu.Lock()
		idle := now.Sub(e.last) > idleFor
		e.mu.Unlock()
		if idle {
			// Deleting while a concurrent Allow already holds this entry's
			// pointer is safe: that request completes against the now-orphaned
			// bucket and the key restarts full on its next request. For a rate
			// limiter that is an acceptable outcome for a key that had gone
			// idle anyway.
			delete(tb.entries, key)
			removed++
		}
	}
	return removed
}

// idleEvictor is anything with buckets to sweep. Both TokenBucket and
// SlidingWindow implement it, so they share one janitor loop.
type idleEvictor interface {
	evictIdle(idleFor time.Duration) int
}

// startJanitor launches a background goroutine that calls e.evictIdle every
// `interval` until either the returned stop function is called or ctx is
// cancelled (e.g. on server shutdown). stop cancels the loop and blocks until
// the goroutine has returned, so callers can rely on no sweep running after it
// returns.
//
// The ticker uses real time deliberately: it is a housekeeping cadence, not
// limiter logic. All time-sensitive eviction behaviour lives in evictIdle,
// which is driven by the injected Clock and is what the tests exercise.
func startJanitor(ctx context.Context, e idleEvictor, idleFor, interval time.Duration) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				e.evictIdle(idleFor)
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

// StartJanitor runs idle-bucket eviction in the background. See startJanitor.
func (tb *TokenBucket) StartJanitor(ctx context.Context, idleFor, interval time.Duration) (stop func()) {
	return startJanitor(ctx, tb, idleFor, interval)
}
