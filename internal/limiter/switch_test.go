package limiter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newSwitch(t *testing.T, algo string) *Switch {
	t.Helper()
	s, err := NewSwitch(context.Background(), nil, algo, Params{
		Limit: 50, Refill: 5, Window: time.Minute, IdleTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewSwitch: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestSwitch_StartsOnGivenAlgorithm(t *testing.T) {
	s := newSwitch(t, AlgoSlidingWindow)
	ci := s.ConfigInfo()
	if ci.Algorithm != AlgoSlidingWindow {
		t.Fatalf("Algorithm = %q, want %q", ci.Algorithm, AlgoSlidingWindow)
	}
	if ci.Tiers[0].WindowSeconds != 60 {
		t.Fatalf("WindowSeconds = %v, want 60", ci.Tiers[0].WindowSeconds)
	}
	if _, err := s.Allow(context.Background(), "k"); err != nil {
		t.Fatalf("Allow: %v", err)
	}
}

func TestSwitch_SetAlgorithm(t *testing.T) {
	s := newSwitch(t, AlgoTokenBucket)

	if err := s.SetAlgorithm(AlgoSlidingWindow); err != nil {
		t.Fatalf("SetAlgorithm: %v", err)
	}
	if got := s.Algorithm(); got != AlgoSlidingWindow {
		t.Fatalf("Algorithm = %q, want %q", got, AlgoSlidingWindow)
	}
	ci := s.ConfigInfo()
	if ci.Tiers[0].Name != AlgoSlidingWindow || ci.Tiers[0].RefillPerSec != 0 || ci.Tiers[0].WindowSeconds != 60 {
		t.Fatalf("tier after switch = %+v", ci.Tiers[0])
	}
	if _, err := s.Allow(context.Background(), "k"); err != nil {
		t.Fatalf("Allow after switch: %v", err)
	}

	// Switching back works too.
	if err := s.SetAlgorithm(AlgoTokenBucket); err != nil {
		t.Fatalf("SetAlgorithm back: %v", err)
	}
	if s.Algorithm() != AlgoTokenBucket {
		t.Fatalf("Algorithm = %q, want %q", s.Algorithm(), AlgoTokenBucket)
	}
}

func TestSwitch_SetAlgorithm_Invalid(t *testing.T) {
	s := newSwitch(t, AlgoTokenBucket)
	if err := s.SetAlgorithm("leaky-bucket"); err == nil {
		t.Fatal("expected error for unknown algorithm")
	}
	if s.Algorithm() != AlgoTokenBucket {
		t.Fatalf("algorithm changed after failed switch: %q", s.Algorithm())
	}
}

func TestSwitch_SetAlgorithm_NoOp(t *testing.T) {
	s := newSwitch(t, AlgoTokenBucket)
	if err := s.SetAlgorithm(AlgoTokenBucket); err != nil {
		t.Fatalf("no-op SetAlgorithm returned error: %v", err)
	}
}

// -race: continuous Allow traffic while the algorithm is flipped repeatedly.
func TestSwitch_ConcurrentAllowAndSwitch(t *testing.T) {
	s := newSwitch(t, AlgoTokenBucket)

	var stop atomic.Bool
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		algos := []string{AlgoTokenBucket, AlgoSlidingWindow}
		for i := 0; !stop.Load(); i++ {
			if err := s.SetAlgorithm(algos[i%2]); err != nil {
				t.Errorf("SetAlgorithm: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				if _, err := s.Allow(context.Background(), "shared"); err != nil {
					t.Errorf("Allow: %v", err)
					return
				}
			}
		}()
	}

	// Let the Allow goroutines finish, then stop the switcher.
	time.Sleep(30 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}
