package stats

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pruthviklshetty/distributed-rate-limiter/internal/events"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/ratelimit"
)

var refTime = time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func ev(key string, allowed bool, remaining int64, ts time.Time) events.Event {
	return events.Event{Key: key, Algorithm: "token-bucket", Allowed: allowed, Remaining: remaining, Timestamp: ts}
}

func TestCollector_AggregatesAndSnapshot(t *testing.T) {
	clk := ratelimit.NewFakeClock(refTime)
	c := New(Config{Clock: clk})
	defer c.Close()

	for i := 0; i < 7; i++ {
		c.Record(ev("a", true, int64(10-i), refTime))
	}
	for i := 0; i < 3; i++ {
		c.Record(ev("a", false, 0, refTime))
	}
	c.Record(ev("b", true, 5, refTime))

	waitFor(t, time.Second, func() bool {
		s := c.Snapshot(0, 0)
		return s.TotalAllowed == 8 && s.TotalRejected == 3
	})

	s := c.Snapshot(120, 10)
	if s.TrackedKeys != 2 {
		t.Fatalf("TrackedKeys = %d, want 2", s.TrackedKeys)
	}
	// Per-second series: the refTime second should carry 8 allow / 3 reject.
	var found bool
	for _, ps := range s.PerSecond {
		if ps.Second == refTime.Unix() {
			found = true
			if ps.Allow != 8 || ps.Reject != 3 {
				t.Fatalf("per-second cell = %d/%d, want 8/3", ps.Allow, ps.Reject)
			}
		}
	}
	if !found {
		t.Fatal("refTime second missing from per-second series")
	}
	// Top keys: "a" (10 decisions) before "b" (1).
	if len(s.TopKeys) != 2 || s.TopKeys[0].Key != "a" || s.TopKeys[1].Key != "b" {
		t.Fatalf("TopKeys = %+v", s.TopKeys)
	}
	if s.TopKeys[0].Allowed != 7 || s.TopKeys[0].Rejected != 3 {
		t.Fatalf("key a tally = %d/%d, want 7/3", s.TopKeys[0].Allowed, s.TopKeys[0].Rejected)
	}

	ks, ok := c.KeySnapshot("b")
	if !ok || ks.Allowed != 1 || ks.LastRemaining != 5 {
		t.Fatalf("KeySnapshot(b) = %+v ok=%v", ks, ok)
	}
	if _, ok := c.KeySnapshot("nope"); ok {
		t.Fatal("KeySnapshot for unknown key should report ok=false")
	}
}

// Record must return immediately even when the worker cannot keep up. Closing
// the collector stops the drain, so after InboxSize sends everything else is
// dropped — and no call blocks.
func TestCollector_RecordNeverBlocks(t *testing.T) {
	c := New(Config{InboxSize: 8, Clock: ratelimit.NewFakeClock(refTime)})
	c.Close()
	time.Sleep(5 * time.Millisecond) // let run() observe done

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100_000; i++ {
			c.Record(ev("k", true, 1, refTime))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Record blocked")
	}
	if c.Dropped() == 0 {
		t.Fatal("expected drops once the inbox filled")
	}
}

func TestCollector_BoundedMemory(t *testing.T) {
	c := New(Config{RingSize: 16, MaxKeys: 32, Clock: ratelimit.NewFakeClock(refTime)})
	defer c.Close()

	const distinct = 500
	for i := 0; i < distinct; i++ {
		c.Record(ev(fmt.Sprintf("key-%d", i), true, 1, refTime))
	}

	waitFor(t, time.Second, func() bool {
		return c.Snapshot(0, 0).TotalAllowed == distinct
	})

	s := c.Snapshot(0, 1000)
	if s.TrackedKeys > 32 {
		t.Fatalf("TrackedKeys = %d, want <= 32 (MaxKeys)", s.TrackedKeys)
	}
	if got := len(c.Recent(1000)); got > 16 {
		t.Fatalf("Recent returned %d, want <= 16 (RingSize)", got)
	}
}

// -race target: many concurrent Record callers plus concurrent snapshot
// readers. Every event is either applied or counted as dropped.
func TestCollector_ConcurrentRecordAndRead(t *testing.T) {
	c := New(Config{InboxSize: 2048, Clock: ratelimit.NewFakeClock(refTime)})
	defer c.Close()

	const writers = 32
	const perWriter = 2000
	var wg sync.WaitGroup

	stopReaders := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
					_ = c.Snapshot(30, 5)
					_ = c.Recent(20)
					_, _ = c.KeySnapshot("w0")
				}
			}
		}()
	}

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := fmt.Sprintf("w%d", w%8)
			for i := 0; i < perWriter; i++ {
				c.Record(ev(key, i%3 != 0, int64(i%10), refTime))
			}
		}(w)
	}

	// Wait a bounded time for every event to be applied or dropped.
	total := int64(writers * perWriter)
	waitFor(t, 5*time.Second, func() bool {
		s := c.Snapshot(0, 0)
		return s.TotalAllowed+s.TotalRejected+s.Dropped == total
	})
	close(stopReaders)
	wg.Wait()

	s := c.Snapshot(0, 0)
	if s.TotalAllowed+s.TotalRejected+s.Dropped != total {
		t.Fatalf("accounted %d, want %d", s.TotalAllowed+s.TotalRejected+s.Dropped, total)
	}
}

func TestCollector_SubscribeReceivesLiveEvents(t *testing.T) {
	c := New(Config{Clock: ratelimit.NewFakeClock(refTime)})
	defer c.Close()

	ch, cancel := c.Subscribe(16)

	for i := 0; i < 3; i++ {
		c.Record(ev("live", true, int64(i), refTime))
	}

	got := 0
	timeout := time.After(time.Second)
	for got < 3 {
		select {
		case <-ch:
			got++
		case <-timeout:
			t.Fatalf("received %d/3 live events", got)
		}
	}

	cancel()
	cancel() // idempotent
	// Further records must not panic or deliver to the cancelled subscriber.
	c.Record(ev("live", false, 0, refTime))
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("received event after unsubscribe")
		}
	case <-time.After(50 * time.Millisecond):
	}
}
