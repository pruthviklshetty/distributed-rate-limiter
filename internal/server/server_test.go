package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pruthviklshetty/distributed-rate-limiter/internal/httpmw"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/limiter"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/stats"
)

func newSwitch(t *testing.T, limit int64) *limiter.Switch {
	t.Helper()
	s, err := limiter.NewSwitch(context.Background(), nil, limiter.AlgoTokenBucket, limiter.Params{
		Limit: limit, Refill: 0.001, Window: time.Minute, IdleTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestServer_Routing(t *testing.T) {
	c := stats.New(stats.Config{})
	defer c.Close()

	h := New(Options{
		Control:   newSwitch(t, 2),
		KeyFunc:   func(*http.Request) string { return "k" },
		KeyBy:     "ip",
		Collector: c,
	})

	get := func(path string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		return rr
	}

	// "/" is a small service banner.
	if rr := get("/"); rr.Code != 200 || !strings.Contains(rr.Body.String(), "distributed-rate-limiter") {
		t.Fatalf("/ = %d %q", rr.Code, rr.Body.String())
	}
	// Unknown paths 404.
	if rr := get("/no/such/path"); rr.Code != http.StatusNotFound {
		t.Fatalf("/no/such/path = %d, want 404", rr.Code)
	}
	// API config is served and not rate limited.
	if rr := get("/api/config"); rr.Code != 200 {
		t.Fatalf("/api/config = %d", rr.Code)
	}
	// Unrouted /api/* is a 404.
	if rr := get("/api/nope"); rr.Code != http.StatusNotFound {
		t.Fatalf("/api/nope = %d, want 404", rr.Code)
	}
	// /api/ping is rate limited: capacity 2 then 429, and sets headers.
	if rr := get("/api/ping"); rr.Code != 200 || rr.Header().Get("X-RateLimit-Limit") != "2" {
		t.Fatalf("ping #1 = %d, limit hdr %q", rr.Code, rr.Header().Get("X-RateLimit-Limit"))
	}
	get("/api/ping")
	if rr := get("/api/ping"); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("ping #3 = %d, want 429", rr.Code)
	}
}

// Without a collector the API is not mounted, but /api/ping is still limited.
func TestServer_NoCollector(t *testing.T) {
	h := New(Options{Control: newSwitch(t, 1), KeyFunc: httpmw.KeyByIP})

	get := func(path string) int {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		return rr.Code
	}
	if code := get("/api/config"); code != http.StatusNotFound {
		t.Fatalf("/api/config = %d, want 404 with no collector", code)
	}
	if code := get("/api/ping"); code != 200 {
		t.Fatalf("ping #1 = %d, want 200", code)
	}
	if code := get("/api/ping"); code != http.StatusTooManyRequests {
		t.Fatalf("ping #2 = %d, want 429", code)
	}
}

func TestServer_AlgorithmSwitchEndpoint(t *testing.T) {
	c := stats.New(stats.Config{})
	defer c.Close()

	h := New(Options{
		Control:   newSwitch(t, 5),
		KeyFunc:   func(*http.Request) string { return "k" },
		KeyBy:     "ip",
		Collector: c,
	})

	post := httptest.NewRecorder()
	h.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/api/config/algorithm",
		strings.NewReader(`{"algorithm":"sliding-window"}`)))
	if post.Code != 200 {
		t.Fatalf("switch = %d: %s", post.Code, post.Body.String())
	}

	get := httptest.NewRecorder()
	h.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/config", nil))
	if !strings.Contains(get.Body.String(), `"algorithm":"sliding-window"`) {
		t.Fatalf("config after switch: %s", get.Body.String())
	}
}
