// Package server assembles the HTTP handler: the rate-limited endpoints plus
// the stats/config/events API. The API routes are mounted OUTSIDE the
// rate-limiting middleware so a client that is being throttled can still
// query the stats that show it being throttled.
package server

import (
	"encoding/json"
	"net/http"

	"github.com/pruthviklshetty/distributed-rate-limiter/internal/api"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/events"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/httpmw"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/stats"
)

// Options configures the HTTP handler.
type Options struct {
	// Control is the live limiter: it enforces requests, reports config, and
	// can switch algorithm at runtime. Required.
	Control api.Control
	KeyFunc httpmw.KeyFunc
	// KeyBy labels the key granularity in GET /api/config ("ip", "header:...").
	KeyBy string

	// Collector receives an event per decision and backs the API. If nil, a
	// no-op sink is used and the API is not mounted.
	Collector *stats.Collector
}

// New returns the top-level http.Handler.
func New(opts Options) http.Handler {
	var sink events.Sink = events.NopSink{}
	if opts.Collector != nil {
		sink = opts.Collector
	}

	mw := httpmw.New(httpmw.Config{
		Limiter:     opts.Control,
		KeyFunc:     opts.KeyFunc,
		Sink:        sink,
		AlgorithmFn: opts.Control.Algorithm,
	})

	// Endpoints subject to rate limiting.
	protected := http.NewServeMux()
	protected.HandleFunc("GET /api/ping", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"pong": true})
	})

	root := http.NewServeMux()
	root.Handle("/api/ping", mw.Handler(protected))

	if opts.Collector != nil {
		h := &api.Handlers{
			Collector: opts.Collector,
			Control:   opts.Control,
			KeyBy:     opts.KeyBy,
		}
		h.Mount(root)
	}

	root.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"service": "distributed-rate-limiter"})
	})

	return root
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
