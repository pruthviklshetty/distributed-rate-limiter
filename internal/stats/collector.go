// Package stats provides an in-memory EventSink that aggregates rate-limit
// decisions for the dashboard API: a ring buffer of recent decisions, rolling
// per-second allow/reject counters, and per-key totals — all with bounded
// memory.
package stats

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pruthviklshetty/distributed-rate-limiter/internal/events"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/ratelimit"
)

// Config tunes the collector's fixed-size buffers. Zero values get defaults.
type Config struct {
	RingSize      int // recent decisions kept for /api/events replay and /api/stats
	WindowSeconds int // how many seconds of per-second counters to retain
	MaxKeys       int // cap on distinct keys tracked (LRU-evicted past this)
	InboxSize     int // buffered channel depth between Record and the worker
	Clock         ratelimit.Clock
}

func (c *Config) withDefaults() {
	if c.RingSize <= 0 {
		c.RingSize = 512
	}
	if c.WindowSeconds <= 0 {
		c.WindowSeconds = 120
	}
	if c.MaxKeys <= 0 {
		c.MaxKeys = 1024
	}
	if c.InboxSize <= 0 {
		c.InboxSize = 4096
	}
	if c.Clock == nil {
		c.Clock = ratelimit.RealClock{}
	}
}

type keyStat struct {
	allowed       int64
	rejected      int64
	lastRemaining int64
	lastSeen      time.Time
}

type secCell struct {
	sec    int64
	allow  int64
	reject int64
}

// Collector implements events.Sink. Record is non-blocking: it hands the event
// to a buffered channel and returns, dropping the event (and counting the
// drop) if the channel is full, so the request path never waits on stats. A
// single worker goroutine is the only writer of the aggregates, which keeps
// the locking simple and makes the -race test meaningful.
type Collector struct {
	cfg   Config
	in    chan events.Event
	done  chan struct{}
	close sync.Once

	dropped atomic.Int64

	mu            sync.Mutex
	ring          []events.Event
	ringNext      int
	ringLen       int
	cells         []secCell
	perKey        map[string]*keyStat
	totalAllowed  int64
	totalRejected int64
	subs          map[chan events.Event]struct{}
}

// New starts a collector and its worker goroutine. Call Close to stop it.
func New(cfg Config) *Collector {
	cfg.withDefaults()
	c := &Collector{
		cfg:    cfg,
		in:     make(chan events.Event, cfg.InboxSize),
		done:   make(chan struct{}),
		ring:   make([]events.Event, cfg.RingSize),
		cells:  make([]secCell, cfg.WindowSeconds),
		perKey: make(map[string]*keyStat),
		subs:   make(map[chan events.Event]struct{}),
	}
	go c.run()
	return c
}

// Record implements events.Sink. Safe for concurrent use; never blocks.
func (c *Collector) Record(e events.Event) {
	select {
	case c.in <- e:
	default:
		c.dropped.Add(1)
	}
}

// Close stops the worker goroutine. Safe to call more than once.
func (c *Collector) Close() {
	c.close.Do(func() { close(c.done) })
}

func (c *Collector) run() {
	for {
		select {
		case <-c.done:
			return
		case e := <-c.in:
			c.apply(e)
		}
	}
}

func (c *Collector) apply(e events.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Recent-decisions ring.
	c.ring[c.ringNext] = e
	c.ringNext = (c.ringNext + 1) % len(c.ring)
	if c.ringLen < len(c.ring) {
		c.ringLen++
	}

	// Rolling per-second counters (circular by unix second).
	sec := e.Timestamp.Unix()
	cell := &c.cells[cellIndex(sec, len(c.cells))]
	if cell.sec != sec {
		cell.sec, cell.allow, cell.reject = sec, 0, 0
	}

	// Totals + per-key.
	ks := c.perKey[e.Key]
	if ks == nil {
		ks = &keyStat{}
		c.perKey[e.Key] = ks
		c.evictKeysLocked()
	}
	ks.lastRemaining = e.Remaining
	ks.lastSeen = e.Timestamp
	if e.Allowed {
		c.totalAllowed++
		cell.allow++
		ks.allowed++
	} else {
		c.totalRejected++
		cell.reject++
		ks.rejected++
	}

	// Fan out to SSE subscribers, never blocking on a slow client.
	for ch := range c.subs {
		select {
		case ch <- e:
		default:
		}
	}
}

// evictKeysLocked keeps perKey within MaxKeys by dropping the least recently
// seen entry. O(n) but only runs when a genuinely new key pushes over the cap,
// and n is bounded by MaxKeys.
func (c *Collector) evictKeysLocked() {
	if len(c.perKey) <= c.cfg.MaxKeys {
		return
	}
	var oldestKey string
	var oldest time.Time
	first := true
	for k, v := range c.perKey {
		if first || v.lastSeen.Before(oldest) {
			oldestKey, oldest, first = k, v.lastSeen, false
		}
	}
	delete(c.perKey, oldestKey)
}

// Subscribe returns a channel of live events and an unsubscribe function. The
// channel is buffered; events are dropped for a subscriber that falls behind.
func (c *Collector) Subscribe(buffer int) (<-chan events.Event, func()) {
	if buffer <= 0 {
		buffer = 64
	}
	ch := make(chan events.Event, buffer)
	c.mu.Lock()
	c.subs[ch] = struct{}{}
	c.mu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			c.mu.Lock()
			delete(c.subs, ch)
			c.mu.Unlock()
		})
	}
}
