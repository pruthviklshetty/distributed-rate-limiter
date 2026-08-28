// Package httpmw provides HTTP middleware that enforces a ratelimit.RateLimiter
// on incoming requests.
package httpmw

import (
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/pruthviklshetty/distributed-rate-limiter/internal/events"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/ratelimit"
)

// KeyFunc derives the rate-limit key from a request. Swapping it changes the
// granularity of limiting (per IP, per API key, per user) without touching the
// limiter or the middleware.
type KeyFunc func(*http.Request) string

// KeyByIP keys on the client IP taken from RemoteAddr. Behind a proxy or load
// balancer you would use KeyByHeader("X-Forwarded-For") or a trusted-proxy
// aware variant instead.
func KeyByIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// KeyByHeader keys on a request header, falling back to the client IP when the
// header is absent so an unauthenticated caller is still limited.
func KeyByHeader(name string) KeyFunc {
	return func(r *http.Request) string {
		if v := r.Header.Get(name); v != "" {
			return v
		}
		return KeyByIP(r)
	}
}

// Config configures a Middleware.
type Config struct {
	Limiter ratelimit.RateLimiter // required
	KeyFunc KeyFunc               // defaults to KeyByIP
	Sink    events.Sink           // defaults to events.NopSink
	// Algorithm labels events and is otherwise informational.
	Algorithm string
	// Logger is used only for the fail-open warning. Defaults to slog.Default().
	Logger *slog.Logger
}

// Middleware enforces a RateLimiter around an http.Handler.
type Middleware struct {
	limiter   ratelimit.RateLimiter
	keyFunc   KeyFunc
	sink      events.Sink
	algorithm string
	log       *slog.Logger
}

// New builds a Middleware from cfg, filling in defaults.
func New(cfg Config) *Middleware {
	m := &Middleware{
		limiter:   cfg.Limiter,
		keyFunc:   cfg.KeyFunc,
		sink:      cfg.Sink,
		algorithm: cfg.Algorithm,
		log:       cfg.Logger,
	}
	if m.keyFunc == nil {
		m.keyFunc = KeyByIP
	}
	if m.sink == nil {
		m.sink = events.NopSink{}
	}
	if m.log == nil {
		m.log = slog.Default()
	}
	return m
}

// Handler wraps next with rate limiting.
//
// On every response it sets X-RateLimit-Limit and X-RateLimit-Remaining. A
// rejected request gets a 429 with Retry-After (whole seconds, rounded up) and
// next is not called.
//
// Fail-open: if the limiter itself returns an error (e.g. Redis unreachable
// and no fallback configured) the request is allowed through rather than
// rejected. A rate limiter is a availability *protection*; turning a limiter
// outage into a site outage would defeat its purpose. The event is still
// recorded, flagged as allowed, so the dashboard shows the degradation.
func (m *Middleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := m.keyFunc(r)

		start := time.Now()
		res, err := m.limiter.Allow(r.Context(), key)
		latency := time.Since(start)

		if err != nil {
			m.log.WarnContext(r.Context(), "rate limiter error; failing open",
				"key", key, "algorithm", m.algorithm, "err", err)
			m.sink.Record(events.Event{
				Key: key, Algorithm: m.algorithm, Allowed: true,
				Remaining: -1, Timestamp: time.Now(), Latency: latency,
			})
			next.ServeHTTP(w, r)
			return
		}

		h := w.Header()
		h.Set("X-RateLimit-Limit", strconv.FormatInt(res.Limit, 10))
		h.Set("X-RateLimit-Remaining", strconv.FormatInt(max(res.Remaining, 0), 10))

		m.sink.Record(events.Event{
			Key: key, Algorithm: m.algorithm, Allowed: res.Allowed,
			Remaining: res.Remaining, Timestamp: time.Now(), Latency: latency,
		})

		if !res.Allowed {
			secs := int64(res.RetryAfter / time.Second)
			if res.RetryAfter%time.Second != 0 {
				secs++ // round up; never advertise a retry time that's too soon
			}
			if secs < 1 {
				secs = 1
			}
			h.Set("Retry-After", strconv.FormatInt(secs, 10))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}
