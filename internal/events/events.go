// Package events defines the reporting hook the HTTP middleware calls after
// every rate-limit decision. It is deliberately separate from the
// ratelimit.RateLimiter interface: limiters decide, the middleware reports.
// Keeping the two apart means an algorithm never has a dependency on metrics
// or a dashboard, and the reporting side can be swapped (no-op in tests, a
// stats collector in the server) without touching limiter code.
package events

import "time"

// Event is one rate-limit decision, as seen by the middleware.
type Event struct {
	Key       string        // the limiter key (IP, API key, user id, …)
	Algorithm string        // limiter label, e.g. "token-bucket"
	Allowed   bool          // whether the request was let through
	Remaining int64         // budget left for Key after this decision
	Timestamp time.Time     // when the decision was made
	Latency   time.Duration // how long the limiter's Allow call took
}

// Sink receives an Event after each decision.
//
// Implementations MUST NOT block: Record is called on the request-handling
// path. A sink that needs to do real work (aggregate, fan out to SSE clients,
// write to storage) should hand the event to a buffered channel and return
// immediately, dropping events if that channel is full rather than stalling
// the request.
type Sink interface {
	Record(Event)
}

// NopSink is the default Sink; it discards every event.
type NopSink struct{}

// Record does nothing.
func (NopSink) Record(Event) {}
