package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pruthviklshetty/distributed-rate-limiter/internal/api"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/httpmw"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/ratelimit"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/stats"
)

func TestServer_Routing(t *testing.T) {
	lim, err := ratelimit.NewTokenBucket(ratelimit.TokenBucketConfig{Capacity: 2, RefillPerSec: 0.001})
	if err != nil {
		t.Fatal(err)
	}
	c := stats.New(stats.Config{})
	defer c.Close()

	ui := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "DASHBOARD")
	})

	h := New(Options{
		Limiter:   lim,
		KeyFunc:   func(*http.Request) string { return "k" },
		Algorithm: "token-bucket",
		Collector: c,
		APIConfig: api.ConfigInfo{Algorithm: "token-bucket", Backend: "in-memory"},
		UI:        ui,
	})

	get := func(path string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		return rr
	}

	// UI at "/".
	if rr := get("/"); rr.Code != 200 || !strings.Contains(rr.Body.String(), "DASHBOARD") {
		t.Fatalf("/ = %d %q", rr.Code, rr.Body.String())
	}
	// Unknown SPA path still hits the UI handler.
	if rr := get("/dashboard/whatever"); !strings.Contains(rr.Body.String(), "DASHBOARD") {
		t.Fatalf("SPA fallback failed: %q", rr.Body.String())
	}
	// API config is served and not rate limited.
	if rr := get("/api/config"); rr.Code != 200 {
		t.Fatalf("/api/config = %d", rr.Code)
	}
	// Unrouted /api/* is a 404, not the SPA.
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

func TestServer_NoUIPlaceholder(t *testing.T) {
	lim, _ := ratelimit.NewTokenBucket(ratelimit.TokenBucketConfig{Capacity: 1, RefillPerSec: 0.001})
	h := New(Options{Limiter: lim, KeyFunc: httpmw.KeyByIP})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != 200 || !strings.Contains(rr.Body.String(), "not embedded") {
		t.Fatalf("placeholder = %d %q", rr.Code, rr.Body.String())
	}
}
