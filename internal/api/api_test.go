package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pruthviklshetty/distributed-rate-limiter/internal/ratelimit"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/stats"
)

// fakeControl is a minimal api.Control for tests. It wraps a real limiter and
// tracks the "active" algorithm string. (The api package cannot import the
// real limiter.Switch — that package imports api.)
type fakeControl struct {
	lim   ratelimit.RateLimiter
	algo  string
	limit int64
}

func (f *fakeControl) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	return f.lim.Allow(ctx, key)
}
func (f *fakeControl) Algorithm() string { return f.algo }
func (f *fakeControl) SetAlgorithm(a string) error {
	if a != "token-bucket" && a != "sliding-window" {
		return errors.New("unknown algorithm " + a)
	}
	f.algo = a
	return nil
}
func (f *fakeControl) ConfigInfo() ConfigInfo {
	t := TierInfo{Name: f.algo, Limit: f.limit}
	if f.algo == "token-bucket" {
		t.RefillPerSec = 0.001
	} else {
		t.WindowSeconds = 60
	}
	return ConfigInfo{Algorithm: f.algo, Backend: "in-memory", Tiers: []TierInfo{t}}
}

func newTestHandlers(t *testing.T, capacity int64) (*Handlers, http.Handler) {
	t.Helper()
	c := stats.New(stats.Config{Clock: ratelimit.RealClock{}})
	t.Cleanup(c.Close)

	lim, err := ratelimit.NewTokenBucket(ratelimit.TokenBucketConfig{Capacity: capacity, RefillPerSec: 0.001})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handlers{
		Collector: c,
		Control:   &fakeControl{lim: lim, algo: "token-bucket", limit: capacity},
		KeyBy:     "ip",
	}
	mux := http.NewServeMux()
	h.Mount(mux)
	return h, mux
}

func TestAPI_Config(t *testing.T) {
	_, mux := newTestHandlers(t, 5)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/config", nil))

	if rr.Code != 200 {
		t.Fatalf("code %d", rr.Code)
	}
	var cfg ConfigInfo
	if err := json.Unmarshal(rr.Body.Bytes(), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Algorithm != "token-bucket" || cfg.Backend != "in-memory" || len(cfg.Tiers) != 1 || cfg.Tiers[0].Limit != 5 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestAPI_SetAlgorithm(t *testing.T) {
	_, mux := newTestHandlers(t, 5)

	post := func(body string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/config/algorithm",
			bytes.NewBufferString(body)))
		return rr
	}

	// Valid switch: 200, response reflects the new algorithm.
	rr := post(`{"algorithm":"sliding-window"}`)
	if rr.Code != 200 {
		t.Fatalf("switch code %d: %s", rr.Code, rr.Body.String())
	}
	var cfg ConfigInfo
	_ = json.Unmarshal(rr.Body.Bytes(), &cfg)
	if cfg.Algorithm != "sliding-window" || cfg.KeyBy != "ip" || cfg.Tiers[0].WindowSeconds != 60 {
		t.Fatalf("post-switch config = %+v", cfg)
	}

	// GET /api/config now shows the switched algorithm too.
	gr := httptest.NewRecorder()
	mux.ServeHTTP(gr, httptest.NewRequest(http.MethodGet, "/api/config", nil))
	_ = json.Unmarshal(gr.Body.Bytes(), &cfg)
	if cfg.Algorithm != "sliding-window" {
		t.Fatalf("GET /api/config after switch = %+v", cfg)
	}

	// Unknown algorithm: 400, unchanged.
	if rr := post(`{"algorithm":"leaky-bucket"}`); rr.Code != http.StatusBadRequest {
		t.Fatalf("bad algorithm code %d, want 400", rr.Code)
	}
	// Malformed body: 400.
	if rr := post(`not json`); rr.Code != http.StatusBadRequest {
		t.Fatalf("bad body code %d, want 400", rr.Code)
	}
}

func TestAPI_DemoBurstAndStats(t *testing.T) {
	_, mux := newTestHandlers(t, 3) // capacity 3

	body := bytes.NewBufferString(`{"key":"burstkey","count":10}`)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/demo/burst", body))
	if rr.Code != 200 {
		t.Fatalf("burst code %d", rr.Code)
	}
	var res burstResult
	if err := json.Unmarshal(rr.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.Requested != 10 || res.Allowed != 3 || res.Rejected != 7 {
		t.Fatalf("burst result = %+v, want requested 10 / allowed 3 / rejected 7", res)
	}

	// Stats should reflect the burst once the collector drains.
	deadline := time.Now().Add(time.Second)
	for {
		sr := httptest.NewRecorder()
		mux.ServeHTTP(sr, httptest.NewRequest(http.MethodGet, "/api/stats", nil))
		var snap stats.Snapshot
		_ = json.Unmarshal(sr.Body.Bytes(), &snap)
		if snap.TotalAllowed == 3 && snap.TotalRejected == 7 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stats never reflected burst: %s", sr.Body.String())
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Per-key endpoint.
	kr := httptest.NewRecorder()
	mux.ServeHTTP(kr, httptest.NewRequest(http.MethodGet, "/api/keys/burstkey", nil))
	if kr.Code != 200 {
		t.Fatalf("key code %d", kr.Code)
	}
	var ks stats.KeyStat
	_ = json.Unmarshal(kr.Body.Bytes(), &ks)
	if ks.Allowed != 3 || ks.Rejected != 7 {
		t.Fatalf("key stat = %+v", ks)
	}

	// Unknown key -> 404.
	nr := httptest.NewRecorder()
	mux.ServeHTTP(nr, httptest.NewRequest(http.MethodGet, "/api/keys/ghost", nil))
	if nr.Code != http.StatusNotFound {
		t.Fatalf("unknown key code %d, want 404", nr.Code)
	}
}

func TestAPI_EventsSSE(t *testing.T) {
	_, mux := newTestHandlers(t, 2)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}

	// Trigger a burst against the live server so events flow.
	go func() {
		time.Sleep(50 * time.Millisecond)
		http.Post(srv.URL+"/api/demo/burst", "application/json",
			bytes.NewBufferString(`{"key":"ssekey","count":5}`))
	}()

	sc := bufio.NewScanner(resp.Body)
	sawAllowed, sawRejected := false, false
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var dto eventDTO
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &dto); err != nil {
			continue
		}
		if dto.Key != "ssekey" {
			continue
		}
		if dto.Allowed {
			sawAllowed = true
		} else {
			sawRejected = true
		}
		if sawAllowed && sawRejected {
			return // success: streamed both an allow and a reject
		}
	}
	t.Fatalf("did not observe both allow and reject over SSE (allow=%v reject=%v)", sawAllowed, sawRejected)
}
