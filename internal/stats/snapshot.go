package stats

import (
	"sort"

	"github.com/pruthviklshetty/distributed-rate-limiter/internal/events"
)

// PerSecond is one second of allow/reject counts.
type PerSecond struct {
	Second int64 `json:"second"` // unix seconds
	Allow  int64 `json:"allow"`
	Reject int64 `json:"reject"`
}

// KeyStat is the running tally for one key.
type KeyStat struct {
	Key           string `json:"key"`
	Allowed       int64  `json:"allowed"`
	Rejected      int64  `json:"rejected"`
	LastRemaining int64  `json:"lastRemaining"`
	LastSeenUnix  int64  `json:"lastSeenUnix"`
}

// Snapshot is the aggregate view served by GET /api/stats.
type Snapshot struct {
	TotalAllowed  int64       `json:"totalAllowed"`
	TotalRejected int64       `json:"totalRejected"`
	Dropped       int64       `json:"dropped"`
	TrackedKeys   int         `json:"trackedKeys"`
	PerSecond     []PerSecond `json:"perSecond"`
	TopKeys       []KeyStat   `json:"topKeys"`
}

func cellIndex(sec int64, n int) int {
	m := sec % int64(n)
	if m < 0 {
		m += int64(n)
	}
	return int(m)
}

// Snapshot returns the aggregate counters, the last `seconds` of per-second
// series (oldest first, zero-filled so the chart axis is continuous), and the
// `top` busiest keys by total decisions.
func (c *Collector) Snapshot(seconds, top int) Snapshot {
	if seconds <= 0 || seconds > c.cfg.WindowSeconds {
		seconds = min(60, c.cfg.WindowSeconds)
	}
	if top <= 0 {
		top = 10
	}
	now := c.cfg.Clock.Now().Unix()

	c.mu.Lock()
	defer c.mu.Unlock()

	series := make([]PerSecond, 0, seconds)
	for s := now - int64(seconds) + 1; s <= now; s++ {
		cell := c.cells[cellIndex(s, len(c.cells))]
		if cell.sec == s {
			series = append(series, PerSecond{Second: s, Allow: cell.allow, Reject: cell.reject})
		} else {
			series = append(series, PerSecond{Second: s})
		}
	}

	keys := make([]KeyStat, 0, len(c.perKey))
	for k, v := range c.perKey {
		keys = append(keys, KeyStat{
			Key: k, Allowed: v.allowed, Rejected: v.rejected,
			LastRemaining: v.lastRemaining, LastSeenUnix: v.lastSeen.Unix(),
		})
	}
	sort.Slice(keys, func(i, j int) bool {
		ti := keys[i].Allowed + keys[i].Rejected
		tj := keys[j].Allowed + keys[j].Rejected
		if ti != tj {
			return ti > tj
		}
		return keys[i].Key < keys[j].Key // stable tie-break
	})
	if len(keys) > top {
		keys = keys[:top]
	}

	return Snapshot{
		TotalAllowed:  c.totalAllowed,
		TotalRejected: c.totalRejected,
		Dropped:       c.dropped.Load(),
		TrackedKeys:   len(c.perKey),
		PerSecond:     series,
		TopKeys:       keys,
	}
}

// KeySnapshot returns the tally for one key.
func (c *Collector) KeySnapshot(key string) (KeyStat, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.perKey[key]
	if !ok {
		return KeyStat{}, false
	}
	return KeyStat{
		Key: key, Allowed: v.allowed, Rejected: v.rejected,
		LastRemaining: v.lastRemaining, LastSeenUnix: v.lastSeen.Unix(),
	}, true
}

// Recent returns up to n most-recent decisions, newest first.
func (c *Collector) Recent(n int) []events.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n <= 0 || n > c.ringLen {
		n = c.ringLen
	}
	out := make([]events.Event, 0, n)
	// ringNext points one past the newest; walk backwards.
	for i := 0; i < n; i++ {
		idx := (c.ringNext - 1 - i + len(c.ring)) % len(c.ring)
		out = append(out, c.ring[idx])
	}
	return out
}

// Dropped reports how many events Record discarded because the inbox was full.
func (c *Collector) Dropped() int64 { return c.dropped.Load() }
