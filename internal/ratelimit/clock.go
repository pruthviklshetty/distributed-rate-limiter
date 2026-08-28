package ratelimit

import (
	"sync"
	"time"
)

// Clock is the only source of "now" the algorithms are allowed to use.
//
// Refill and window math is entirely driven by elapsed time, so injecting a
// clock lets tests advance time explicitly instead of calling time.Sleep.
// That makes the time-dependent tests fast and deterministic.
type Clock interface {
	Now() time.Time
}

// RealClock is the production Clock; it just reads the wall clock.
type RealClock struct{}

// Now returns the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// FakeClock is a manually advanced Clock for tests. Its zero value is not
// usable; construct it with NewFakeClock. It is safe for concurrent use so it
// can back the concurrency/-race tests.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock returns a FakeClock reading start until it is advanced.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the clock's current simulated time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the simulated time forward by d.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
