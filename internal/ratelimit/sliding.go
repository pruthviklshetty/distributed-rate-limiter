package ratelimit

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// SlidingWindow is the weighted sliding-window-counter algorithm behind the
// same RateLimiter interface, so it can be compared with TokenBucket directly.
//
// Model: time is divided into fixed windows of WindowLen. Each key keeps only
// two integers — the request count in the current window and the count in the
// previous one. On each request the limiter estimates the number of requests
// in the *rolling* WindowLen ending "now" as:
//
//	estimate = prevCount * (fraction of the previous window still in view)
//	         + curCount
//
// If estimate + 1 <= Limit the request is allowed and curCount is incremented.
//
// Why the weighting: a plain fixed-window counter resets to zero at each
// boundary, so a client can send Limit requests just before the boundary and
// Limit more just after — 2*Limit in a hair over one window. Bleeding the
// previous window's count in, scaled by how much of it still overlaps the
// rolling window, removes that boundary burst while keeping the O(1) memory of
// a counter (no per-request timestamp log).
//
// Approximation: the estimate assumes the previous window's requests were
// spread evenly across it. Real traffic that was bunched at one end makes the
// estimate slightly high or low near the boundary. This is the standard
// accuracy/memory trade versus a true sliding log, and is covered in the
// Stage 6 benchmarks.
type SlidingWindow struct {
	limit     float64
	windowLen time.Duration
	clock     Clock

	mu      sync.Mutex
	entries map[string]*swEntry
}

type swEntry struct {
	mu        sync.Mutex
	curStart  time.Time // start of the current fixed window
	curCount  float64
	prevCount float64
	last      time.Time // last access, for the janitor
}

// SlidingWindowConfig configures a SlidingWindow limiter.
type SlidingWindowConfig struct {
	// Limit is the maximum number of requests permitted in any rolling window
	// of length WindowLen.
	Limit int64
	// WindowLen is the window size, e.g. time.Minute for "N per minute".
	WindowLen time.Duration
	// Clock is the time source; defaults to RealClock. Tests pass a FakeClock.
	Clock Clock
}

// NewSlidingWindow validates cfg and returns a ready limiter.
func NewSlidingWindow(cfg SlidingWindowConfig) (*SlidingWindow, error) {
	if cfg.Limit <= 0 {
		return nil, errors.New("ratelimit: SlidingWindow limit must be > 0")
	}
	if cfg.WindowLen <= 0 {
		return nil, errors.New("ratelimit: SlidingWindow window length must be > 0")
	}
	clk := cfg.Clock
	if clk == nil {
		clk = RealClock{}
	}
	return &SlidingWindow{
		limit:     float64(cfg.Limit),
		windowLen: cfg.WindowLen,
		clock:     clk,
		entries:   make(map[string]*swEntry),
	}, nil
}

func (s *SlidingWindow) entryFor(key string, now time.Time) *swEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok {
		e = &swEntry{curStart: now, last: now}
		s.entries[key] = e
	}
	return e
}

// roll advances e's windows so that curStart is the window containing now.
// Called with e.mu held.
func (s *SlidingWindow) roll(e *swEntry, now time.Time) {
	elapsed := now.Sub(e.curStart)
	if elapsed < s.windowLen {
		return // still in the same window
	}
	steps := int64(elapsed / s.windowLen)
	if steps == 1 {
		// Moved into the immediately following window.
		e.prevCount = e.curCount
		e.curCount = 0
	} else {
		// Skipped at least one whole window: nothing from before is in view.
		e.prevCount = 0
		e.curCount = 0
	}
	e.curStart = e.curStart.Add(time.Duration(steps) * s.windowLen)
}

// Allow implements RateLimiter. Like the in-memory token bucket it never
// returns a non-nil error.
func (s *SlidingWindow) Allow(_ context.Context, key string) (Result, error) {
	now := s.clock.Now()
	e := s.entryFor(key, now)

	e.mu.Lock()
	defer e.mu.Unlock()
	if now.After(e.last) {
		e.last = now
	}

	s.roll(e, now)

	// How much of the previous fixed window still overlaps the rolling window
	// that ends at now. into = time elapsed into the current window.
	into := now.Sub(e.curStart)
	overlap := float64(s.windowLen-into) / float64(s.windowLen)
	if overlap < 0 {
		overlap = 0
	} else if overlap > 1 {
		overlap = 1
	}
	estimate := e.prevCount*overlap + e.curCount

	res := Result{Limit: int64(s.limit)}
	if estimate+1 <= s.limit {
		e.curCount++
		res.Allowed = true
		res.Remaining = int64(math.Max(0, math.Floor(s.limit-(estimate+1))))
		return res, nil
	}

	res.Remaining = 0
	// Approximate: the surest relief comes when this window rolls over and the
	// previous (currently heavily weighted) count leaves the picture.
	res.RetryAfter = s.windowLen - into
	if res.RetryAfter <= 0 {
		res.RetryAfter = time.Millisecond
	}
	return res, nil
}

// evictIdle removes entries not touched for longer than idleFor. Same
// contract, locking, and rationale as TokenBucket.evictIdle.
func (s *SlidingWindow) evictIdle(idleFor time.Duration) int {
	now := s.clock.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for key, e := range s.entries {
		e.mu.Lock()
		idle := now.Sub(e.last) > idleFor
		e.mu.Unlock()
		if idle {
			delete(s.entries, key)
			removed++
		}
	}
	return removed
}

// StartJanitor runs idle-entry eviction in the background. See startJanitor.
func (s *SlidingWindow) StartJanitor(ctx context.Context, idleFor, interval time.Duration) (stop func()) {
	return startJanitor(ctx, s, idleFor, interval)
}

// Len reports how many keys are currently tracked.
func (s *SlidingWindow) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}
