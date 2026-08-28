package httpmw

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pruthviklshetty/distributed-rate-limiter/internal/events"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/ratelimit"
)

type recordingSink struct {
	mu     sync.Mutex
	events []events.Event
}

func (s *recordingSink) Record(e events.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *recordingSink) snapshot() []events.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.Event(nil), s.events...)
}

type errLimiter struct{}

func (errLimiter) Allow(context.Context, string) (ratelimit.Result, error) {
	return ratelimit.Result{}, errors.New("backend unreachable")
}

func okHandler() (http.Handler, *int) {
	calls := new(int)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*calls++
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok")
	}), calls
}

func newTB(t *testing.T, capacity int64, clk ratelimit.Clock) *ratelimit.TokenBucket {
	t.Helper()
	tb, err := ratelimit.NewTokenBucket(ratelimit.TokenBucketConfig{
		Capacity: capacity, RefillPerSec: 1, Clock: clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tb
}

func TestMiddleware_AllowsUntilLimitThen429(t *testing.T) {
	clk := ratelimit.NewFakeClock(time.Unix(0, 0))
	sink := &recordingSink{}
	m := New(Config{
		Limiter: newTB(t, 2, clk), KeyFunc: func(*http.Request) string { return "fixed" },
		Sink: sink, Algorithm: "token-bucket",
	})
	next, calls := okHandler()
	h := m.Handler(next)

	// First two pass.
	for i := 0; i < 2; i++ {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x", nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("request %d: code %d, want 200", i+1, rr.Code)
		}
		if got := rr.Header().Get("X-RateLimit-Limit"); got != "2" {
			t.Fatalf("X-RateLimit-Limit = %q, want \"2\"", got)
		}
		wantRem := strconv.Itoa(1 - i) // capacity 2: remaining goes 1, 0
		if got := rr.Header().Get("X-RateLimit-Remaining"); got != wantRem {
			t.Fatalf("request %d: X-RateLimit-Remaining = %q, want %q", i+1, got, wantRem)
		}
	}

	// Third is rejected.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("code %d, want 429", rr.Code)
	}
	if got := rr.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want \"1\"", got)
	}
	if got := rr.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Fatalf("X-RateLimit-Remaining = %q, want \"0\"", got)
	}
	if *calls != 2 {
		t.Fatalf("next handler called %d times, want 2", *calls)
	}

	evs := sink.snapshot()
	if len(evs) != 3 {
		t.Fatalf("sink saw %d events, want 3", len(evs))
	}
	if !evs[0].Allowed || !evs[1].Allowed || evs[2].Allowed {
		t.Fatalf("event Allowed flags = %v/%v/%v, want true/true/false",
			evs[0].Allowed, evs[1].Allowed, evs[2].Allowed)
	}
	if evs[0].Algorithm != "token-bucket" || evs[0].Key != "fixed" {
		t.Fatalf("event metadata wrong: %+v", evs[0])
	}
}

func TestMiddleware_FailsOpenOnLimiterError(t *testing.T) {
	sink := &recordingSink{}
	m := New(Config{Limiter: errLimiter{}, Sink: sink, Algorithm: "token-bucket"})
	next, calls := okHandler()
	h := m.Handler(next)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("code %d, want 200 (fail open)", rr.Code)
	}
	if *calls != 1 {
		t.Fatalf("next called %d times, want 1", *calls)
	}
	evs := sink.snapshot()
	if len(evs) != 1 || !evs[0].Allowed {
		t.Fatalf("expected one allowed (fail-open) event, got %+v", evs)
	}
}

func TestMiddleware_KeyByHeaderFallsBackToIP(t *testing.T) {
	clk := ratelimit.NewFakeClock(time.Unix(0, 0))
	// Capacity 1 so the second call from the same key is rejected.
	m := New(Config{Limiter: newTB(t, 1, clk), KeyFunc: KeyByHeader("X-API-Key")})
	next, _ := okHandler()
	h := m.Handler(next)

	do := func(apiKey, remoteAddr string) int {
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.RemoteAddr = remoteAddr
		if apiKey != "" {
			req.Header.Set("X-API-Key", apiKey)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr.Code
	}

	if code := do("alice", "1.1.1.1:1000"); code != 200 {
		t.Fatalf("alice first call: %d, want 200", code)
	}
	if code := do("alice", "2.2.2.2:2000"); code != 429 {
		t.Fatalf("alice second call (diff IP, same key): %d, want 429", code)
	}
	if code := do("bob", "1.1.1.1:1000"); code != 200 {
		t.Fatalf("bob first call: %d, want 200", code)
	}
	// No header -> keyed by IP. 3.3.3.3 is fresh.
	if code := do("", "3.3.3.3:3000"); code != 200 {
		t.Fatalf("no-key call from fresh IP: %d, want 200", code)
	}
	if code := do("", "3.3.3.3:3000"); code != 429 {
		t.Fatalf("no-key second call from same IP: %d, want 429", code)
	}
}

func TestKeyByIP(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.5:54321"
	if got := KeyByIP(req); got != "10.0.0.5" {
		t.Fatalf("KeyByIP = %q, want \"10.0.0.5\"", got)
	}
}
