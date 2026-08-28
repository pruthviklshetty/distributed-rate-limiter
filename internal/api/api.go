// Package api serves the read-only dashboard endpoints (stats, per-key
// consumption, active config, and a live Server-Sent Events stream) plus a
// server-side burst trigger for the demo.
//
// Why SSE and not WebSocket: the dashboard only ever consumes a one-way
// server->client stream of decisions. SSE is a plain HTTP response with a
// text/event-stream body — it needs no handshake, no framing library, no
// separate protocol, and it reconnects on its own in the browser. A WebSocket
// would add a bidirectional transport and its dependencies to carry traffic
// that is entirely unidirectional. If the dashboard ever needed to push data
// back (it does not), WebSocket would start to earn its complexity.
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/pruthviklshetty/distributed-rate-limiter/internal/events"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/ratelimit"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/stats"
)

// TierInfo describes one limiter tier for GET /api/config.
type TierInfo struct {
	Name          string  `json:"name"`
	Limit         int64   `json:"limit"`
	RefillPerSec  float64 `json:"refillPerSec,omitempty"`
	WindowSeconds float64 `json:"windowSeconds,omitempty"`
}

// ConfigInfo is the payload of GET /api/config.
type ConfigInfo struct {
	Algorithm string     `json:"algorithm"`
	KeyBy     string     `json:"keyBy"`
	Backend   string     `json:"backend"` // "in-memory" | "redis+fallback"
	Tiers     []TierInfo `json:"tiers"`
}

// Handlers holds everything the API endpoints need.
type Handlers struct {
	Collector *stats.Collector
	Config    ConfigInfo
	// DemoLimiter is the SAME limiter the middleware enforces, so POST
	// /api/demo/burst consumes real budget and makes real requests get 429s.
	DemoLimiter ratelimit.RateLimiter
	Algorithm   string
}

// Mount registers the routes on mux. These are intentionally NOT wrapped by
// the rate-limiting middleware: a throttled client must still be able to load
// the dashboard that shows it being throttled.
func (h *Handlers) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/stats", h.handleStats)
	mux.HandleFunc("GET /api/keys/{key}", h.handleKey)
	mux.HandleFunc("GET /api/config", h.handleConfig)
	mux.HandleFunc("GET /api/events", h.handleEvents)
	mux.HandleFunc("POST /api/demo/burst", h.handleBurst)
}

func (h *Handlers) handleStats(w http.ResponseWriter, r *http.Request) {
	seconds := atoiDefault(r.URL.Query().Get("seconds"), 0)
	top := atoiDefault(r.URL.Query().Get("top"), 0)
	writeJSON(w, http.StatusOK, h.Collector.Snapshot(seconds, top))
}

func (h *Handlers) handleKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	ks, ok := h.Collector.KeySnapshot(key)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no traffic recorded for key", "key": key})
		return
	}
	writeJSON(w, http.StatusOK, ks)
}

func (h *Handlers) handleConfig(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.Config)
}

type eventDTO struct {
	Key       string `json:"key"`
	Algorithm string `json:"algorithm"`
	Allowed   bool   `json:"allowed"`
	Remaining int64  `json:"remaining"`
	TsUnixMs  int64  `json:"tsUnixMs"`
	LatencyUs int64  `json:"latencyUs"`
}

func toDTO(e events.Event) eventDTO {
	return eventDTO{
		Key: e.Key, Algorithm: e.Algorithm, Allowed: e.Allowed, Remaining: e.Remaining,
		TsUnixMs: e.Timestamp.UnixMilli(), LatencyUs: e.Latency.Microseconds(),
	}
}

func (h *Handlers) handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	rc := http.NewResponseController(w)

	ch, cancel := h.Collector.Subscribe(256)
	defer cancel()

	// Send a short backlog so a freshly opened dashboard is not blank.
	recent := h.Collector.Recent(25)
	for i := len(recent) - 1; i >= 0; i-- { // Recent is newest-first; emit oldest-first
		writeSSE(w, toDTO(recent[i]))
	}
	_ = rc.Flush()

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			// A comment line keeps proxies from closing an idle connection.
			if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
				return
			}
			_ = rc.Flush()
		case e := <-ch:
			writeSSE(w, toDTO(e))
			_ = rc.Flush()
		}
	}
}

type burstRequest struct {
	Key   string `json:"key"`
	Count int    `json:"count"`
}

type burstResult struct {
	Key       string `json:"key"`
	Requested int    `json:"requested"`
	Allowed   int    `json:"allowed"`
	Rejected  int    `json:"rejected"`
}

func (h *Handlers) handleBurst(w http.ResponseWriter, r *http.Request) {
	var req burstRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Key == "" {
		req.Key = "demo"
	}
	switch {
	case req.Count <= 0:
		req.Count = 20
	case req.Count > 1000:
		req.Count = 1000 // bound the work a single call can trigger
	}

	var out burstResult
	out.Key, out.Requested = req.Key, req.Count
	for i := 0; i < req.Count; i++ {
		start := time.Now()
		res, err := h.DemoLimiter.Allow(r.Context(), req.Key)
		latency := time.Since(start)
		allowed := err != nil || res.Allowed // fail-open, mirror the middleware
		if allowed {
			out.Allowed++
		} else {
			out.Rejected++
		}
		h.Collector.Record(events.Event{
			Key: req.Key, Algorithm: h.Algorithm, Allowed: allowed,
			Remaining: res.Remaining, Timestamp: time.Now(), Latency: latency,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func writeSSE(w http.ResponseWriter, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	_, _ = w.Write([]byte("data: "))
	_, _ = w.Write(b)
	_, _ = w.Write([]byte("\n\n"))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func atoiDefault(s string, def int) int {
	n := 0
	if s == "" {
		return def
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return def
		}
		n = n*10 + int(r-'0')
	}
	return n
}
