# Distributed API Rate Limiter

A rate limiter built in Go as a portfolio project. The focus is algorithm
design and concurrency correctness; a dashboard frontend (later stages) exists
to make that work visible.

The project is built in numbered stages. This README documents **only what is
actually built and committed**. Stages not yet done are listed as such.

## Status

| Stage | Description | State |
|-------|-------------|-------|
| 1 | In-memory token bucket | **Done** |
| 2 | TTL cleanup of idle buckets | **Done** |
| 3 | Redis backend with atomic Lua check-and-decrement + local fallback | **Done** |
| 4 | Multi-tier limiting (compose burst + sustained limits) | **Done** |
| 5 | Sliding window counter algorithm | **Done** |
| 6 | Benchmarks and HTTP load tests | **Done** |
| 7 | Stats collector (EventSink) + JSON/SSE API | **Done** |
| 8 | React + TypeScript dashboard | **Done** |
| 9 | Single binary via `go:embed` + Docker Compose | **Done** |

## Quick start

```
# one binary, dashboard embedded — needs Go 1.27+ and a built web/dist (committed)
go run .                       # http://localhost:8080

# rebuild the dashboard from source first (needs Node), then the binary
make build && ./distributed-rate-limiter        # or:  ./build.ps1  on Windows

# app + Redis (shared global counters)
docker compose up --build
```

Open `http://localhost:8080`, click **Send burst**, watch 429s appear live.

## Architecture

Every algorithm and every storage backend implements one interface, so they
are swappable without touching call sites:

```go
type RateLimiter interface {
    Allow(ctx context.Context, key string) (Result, error)
}

type Result struct {
    Allowed    bool
    Limit      int64         // configured ceiling for the key
    Remaining  int64         // budget left immediately after this decision
    RetryAfter time.Duration // wait before retrying; zero when Allowed
}
```

- The `error` return is for infrastructure failures only (e.g. Redis
  unreachable). Being over the limit is `Result{Allowed: false}`, not an error.
- A `Clock` interface (`RealClock` + `FakeClock`) is the only source of "now".
  All refill/window math is driven by elapsed time, so time-dependent tests
  advance a fake clock instead of sleeping.

### Package layout

```
internal/ratelimit/   RateLimiter + Result, Clock, and every algorithm/backend
internal/events/      EventSink hook the middleware calls after each decision
internal/httpmw/       HTTP middleware: KeyFunc, 429 + Retry-After, headers, fail-open
internal/server/      assembles the HTTP handler
cmd/loadtest/         standalone HTTP load generator (Stage 6)
main.go               runs the server (go run .)
```

The stats collector and JSON/SSE API packages are added in Stage 7; the
dashboard in `web/` in Stage 8.

## Stage 1 — In-memory Token Bucket

**Files:** [internal/ratelimit/ratelimit.go](internal/ratelimit/ratelimit.go),
[clock.go](internal/ratelimit/clock.go),
[tokenbucket.go](internal/ratelimit/tokenbucket.go),
[tokenbucket_test.go](internal/ratelimit/tokenbucket_test.go)

### Model

Each key owns a bucket holding up to `Capacity` tokens. Each allowed request
removes one token. Tokens are added back continuously at `RefillPerSec`. A key
can burst until its bucket empties, then is paced at the refill rate.

- **Capacity** is the largest burst a key can make from a standing start.
- **RefillPerSec** is a `float64` so rates like "100 per minute" (1.666.../s)
  are exact rather than rounded.

### Lazy refill (the non-obvious part)

There is **no background ticker**. A per-key goroutine topping every bucket up
on a schedule would cost CPU proportional to the number of keys, most of them
idle, and would need its own synchronisation with the request path.

Instead each `Allow` call does the refill itself:

```
elapsed  = now - bucket.last
tokens   = min(capacity, tokens + elapsed.Seconds() * refillPerSec)
last     = now
```

`elapsed` may be large (bucket idle for minutes) or zero (two requests in the
same instant). `min(capacity, …)` clamps the result, so a long idle period can
never overfill the bucket. An idle bucket costs nothing until it is next used.
If the clock appears to move backwards (NTP step), `elapsed` is treated as zero
rather than debiting tokens.

When a request is denied, `RetryAfter` is `(1 - tokens) / refillPerSec` — the
time until one whole token has accrued.

### Concurrency design

- One mutex (`tb.mu`) guards only the **map** of buckets: lookup and lazy
  insert. It is held briefly and never during token math.
- Each bucket has its **own mutex** for the read-modify-write of its token
  count. Bucket pointers are stable once created, so a goroutine takes the map
  lock, gets its bucket pointer, releases the map lock, then locks the bucket.
- Result: requests for different keys never contend; requests for the same key
  serialise only against each other.

### Tests

`go test -race ./...` and `go vet ./...` both pass.

| Test | What it proves |
|------|----------------|
| `BurstUpToCapacity` | Fresh key spends its whole capacity, then is denied; `Remaining`/`Limit`/`RetryAfter` are correct. |
| `RefillOverTime` | Advancing the fake clock restores tokens at exactly `RefillPerSec`, including fractional accrual. |
| `NeverExceedsCapacity` | An hour of idle time with a fast refill still allows only `capacity` requests. |
| `KeysAreIndependent` | Draining one key leaves another at full capacity. |
| `ConcurrentNeverOverGrants` | 3200 concurrent attempts against 100 tokens (clock frozen) allow **exactly** 100. |
| `ConcurrentDistinctKeys` | 200 keys hammered concurrently; each gets exactly its own capacity, map stays consistent. |

## Stage 2 — TTL cleanup of idle buckets

**Files:** [internal/ratelimit/janitor.go](internal/ratelimit/janitor.go),
[janitor_test.go](internal/ratelimit/janitor_test.go)

### The problem

`entryFor` creates a bucket the first time a key is seen and never removes it.
When the key space is effectively unbounded — per-IP limiting behind a large
NAT, a scanner cycling source addresses, per-request API tokens — the `entries`
map grows without limit. It is a slow memory leak.

### The fix

`evictIdle(idleFor)` does one sweep: it deletes every bucket whose last
activity is older than `idleFor` and returns the count removed. Losing a bucket
is harmless — the key's next request lazily recreates it, full. Any key still
receiving traffic has a recent `last` timestamp and is never evicted.

`StartJanitor(ctx, idleFor, interval)` runs `evictIdle` on a background ticker
and returns a `stop()` function that cancels the loop **and blocks until the
goroutine has exited**, so shutdown is clean. The loop also stops if `ctx` is
cancelled (server shutdown).

### Testability

The tick cadence uses real time on purpose — it is housekeeping, not limiter
logic. Everything time-sensitive lives in `evictIdle`, which reads the injected
`Clock`. Tests advance a `FakeClock` and call `evictIdle` directly, so eviction
behaviour is verified with zero real delay.

### Locking

`evictIdle` holds `tb.mu` for the whole sweep (briefly blocking new-key
creation) and takes each bucket's own mutex only to read its `last` field —
held for nanoseconds since token math never blocks. Lock order (`tb.mu` then
`entry.mu`) matches `Allow`, which releases `tb.mu` before taking `entry.mu`,
so there is no deadlock cycle. Deleting a bucket while a concurrent `Allow`
holds its pointer is safe: that request finishes against the orphan and the
key restarts full next time.

### Tests

| Test | What it proves |
|------|----------------|
| `EvictIdle` | Only buckets idle past the TTL are removed; an active key survives; an evicted key returns at full capacity. |
| `EvictIdle_KeepsActive` | Nothing is removed while all keys are within the TTL. |
| `StartJanitor_EvictsAndStops` | The background loop actually evicts; `stop()` returns promptly and leaves no sweeper running. |
| `StartJanitor_StopsOnContextCancel` | Cancelling the context exits the goroutine. |
| `EvictConcurrentWithAllow` | Continuous sweeping alongside 16k concurrent `Allow` calls: no race, no panic, memory stays bounded. |

## Stage 3 — Redis backend

**Files:** [internal/ratelimit/redis.go](internal/ratelimit/redis.go),
[redis_naive.go](internal/ratelimit/redis_naive.go),
[fallback.go](internal/ratelimit/fallback.go),
[redis_test.go](internal/ratelimit/redis_test.go)

`RedisTokenBucket` is the same token-bucket algorithm with its state in Redis
instead of a local map, so a fleet of API instances shares one global budget
per key. Tests run against [miniredis](https://github.com/alicebob/miniredis)
(an in-process Redis, including a Lua interpreter) so no server is needed.

### Why a Lua script (the interview point)

The whole check-and-decrement — read `tokens`+`ts`, lazily refill, decide,
write back, set TTL — runs as **one `EVAL`**. Redis executes a script
atomically: no command from any other connection interleaves between the
script's first read and its last write. So N instances hammering one key can
never collectively grant more than the capacity.

`RedisNaiveTokenBucket` does the identical work as separate `HMGET` → compute →
`HSET` round trips. Between the read and the write, every other client can run
the same read and see the same token count, so they all think they may
proceed. `TestRedis_ScriptFixesTheNaiveRace` demonstrates it: with a barrier
forcing all 40 callers to finish their read before any write, the naive
limiter grants **40** against a capacity of **5**; the Lua limiter grants
**exactly 5** on the identical load.

### Other details

- **Time source:** "now" is passed to the script as a millisecond argument
  from the injected `Clock`, not read from Redis. That keeps one notion of
  time across all instances and the tests; in production the app servers must
  have NTP-synced clocks. (Using Redis `TIME` instead would remove that
  requirement at the cost of test determinism.)
- **Fractional tokens:** Redis converts a Lua number passed to `redis.call`
  to an integer, so token counts are stored via `tostring()` to keep the
  fraction that accrues between requests.
- **Idle cleanup:** every call issues `PEXPIRE`, so Redis drops untouched keys
  on its own — the distributed equivalent of Stage 2's janitor, no goroutine
  needed.

### Local fallback

`FallbackLimiter{Primary, Fallback}` returns the primary's result unless the
primary errors, in which case it returns the fallback limiter's decision with
a **nil** error (so the caller treats it as authoritative rather than also
failing open). Intended wiring: `Primary` = Redis, `Fallback` = an in-memory
`TokenBucket`. If Redis blips, limiting stays active per-instance (global
counts become approximate) instead of vanishing entirely, and recovers
automatically when Redis answers again.

### Tests

| Test | What it proves |
|------|----------------|
| `RedisTokenBucket_BurstRefillAndCap` | Same burst/refill/cap semantics as the in-memory bucket, driven by the fake clock. |
| `RedisTokenBucket_AtomicUnderConcurrentLoad` | 300 goroutines racing one key with the clock frozen → **exactly** capacity grants. |
| `Redis_ScriptFixesTheNaiveRace` | Naive GET/SET over-grants 40-for-5; the Lua script holds the line at 5. |
| `RedisTokenBucket_SetsTTLForCleanup` | Every call sets a bounded key TTL. |
| `RedisTokenBucket_ErrorWhenRedisDown` | An unreachable Redis surfaces as an error. |
| `FallbackLimiter_*` | Pass-through when healthy; fallback (nil error) on primary error; a mid-test Redis outage keeps the local capacity-3 limiter enforcing. |

## Stage 4 — Multi-tier limiting

**Files:** [internal/ratelimit/multi.go](internal/ratelimit/multi.go),
[multi_test.go](internal/ratelimit/multi_test.go)

`MultiLimiter` composes any number of `RateLimiter`s; a request passes only if
**every** tier allows it. The classic use is a fast burst limit (e.g. 10/sec)
in front of a sustained limit (e.g. 100/min). Because each tier is just a
`RateLimiter`, tiers can mix backends — `TestMultiLimiter_MixedBackends` runs
an in-memory burst tier in front of a Redis sustained tier.

**Consumption semantics (be explicit in interviews):** tiers are checked in
order and evaluation stops at the first denial, but a tier that already
allowed the request has spent a unit — `RateLimiter` has no "release". So a
request rejected by tier N has still been counted by tiers 1..N-1. Put the
tier most likely to reject first (usually the burst limit) to keep that waste
small. On an allowed request the returned `Limit`/`Remaining` come from the
most-constrained tier, so headers show whichever limit the client is closest
to hitting.

| Test | What it proves |
|------|----------------|
| `BothMustPass` | Burst tier rejects a spike; after refill the sustained tier is the one that rejects, with its own `RetryAfter`. |
| `ReportsMostConstrainedTier` | Headers reflect the tier with the least budget left. |
| `PropagatesTierError` | A tier error propagates (middleware then fails open). |
| `MixedBackends` | In-memory + Redis tiers, Redis tier binding. |
| `ConcurrentNeverOverGrants` | 64 goroutines → exactly the smallest tier's capacity. |

## Stage 5 — Sliding Window Counter

**Files:** [internal/ratelimit/sliding.go](internal/ratelimit/sliding.go),
[sliding_test.go](internal/ratelimit/sliding_test.go)

Second algorithm behind the same interface. Time is divided into fixed windows
of `WindowLen`; each key keeps just two integers — the count in the current
window and in the previous one. Each request estimates the load over the
*rolling* window ending now as:

```
estimate = prevCount * (fraction of the previous window still in the rolling window)
         + curCount
```

and allows if `estimate + 1 <= Limit`.

**Why weight the previous window:** a plain fixed-window counter resets at each
boundary, so a client can send `Limit` just before it and `Limit` again just
after — `2*Limit` in barely over one window. Bleeding in the previous count,
scaled by overlap, removes that boundary burst while keeping O(1) memory (no
per-request timestamp log). `TestSlidingWindow_SmoothsTheBoundaryBurst` fills
a window, steps exactly one window forward, and asserts the very next request
is rejected — where a fixed-window counter would allow a second full quota.

**Approximation:** the estimate assumes the previous window's traffic was
evenly spread. Bunched traffic makes it slightly off near the boundary — the
standard accuracy/memory trade against a true sliding log.

| Test | What it proves |
|------|----------------|
| `AllowsUpToLimitInWindow` | Exactly `Limit` allowed; `Remaining` counts down correctly. |
| `SmoothsTheBoundaryBurst` | No second full quota right after a window boundary; ~half a quota mid-window. |
| `WindowSlidesFullyAfterIdle` | After skipping whole windows, full quota returns. |
| `KeysAreIndependent` / `RetryAfterWithinWindow` | Per-key isolation; sane `RetryAfter`. |
| `ConcurrentNeverOverGrants` | Frozen window, 64 goroutines → exactly `Limit`. |
| `EvictIdle` | Shares the Stage 2 janitor. |

## Stage 6 — Benchmarks and load tests

**Files:** [internal/ratelimit/bench_test.go](internal/ratelimit/bench_test.go),
[cmd/loadtest/main.go](cmd/loadtest/main.go),
[internal/httpmw/middleware.go](internal/httpmw/middleware.go),
[internal/server/server.go](internal/server/server.go), [main.go](main.go)

### HTTP middleware

`httpmw.New(...).Handler(next)` wraps any `RateLimiter`:

- pluggable `KeyFunc` — `KeyByIP`, `KeyByHeader(name)` (falls back to IP);
- sets `X-RateLimit-Limit` / `X-RateLimit-Remaining` on **every** response;
- rejected requests get `429` + `Retry-After` (whole seconds, rounded up) and
  `next` is not called;
- **fails open**: if the limiter itself errors, the request is let through
  (a limiter outage must not become a site outage) and the event is still
  recorded so the degradation is visible;
- calls the `events.Sink` after each decision. The sink contract is
  *must not block* — the request path never waits on reporting.

### Go benchmarks (`-benchmem`)

`go test -run=^$ -bench=. -benchmem ./internal/ratelimit`, AMD Ryzen 5 7235HS,
Go 1.27, `windows/amd64`:

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| `TokenBucket_Allow_SingleKey` | 64 | 0 | 0 |
| `SlidingWindow_Allow_SingleKey` | 97 | 0 | 0 |
| `TokenBucket_Allow_ParallelManyKeys` | 222 | 0 | 0 |
| `SlidingWindow_Allow_ParallelManyKeys` | 235 | 0 | 0 |
| `TokenBucket_Allow_ParallelHotKey` | 186 | 0 | 0 |
| `SlidingWindow_Allow_ParallelHotKey` | 205 | 0 | 0 |

Both algorithms are **allocation-free on the hot path**. Token bucket is a bit
cheaper single-threaded (less arithmetic); under contention they converge,
since the cost is dominated by mutex hand-off, not the token/window math.

### HTTP load test

`cmd/loadtest` drives the server and reports status-code mix + latency
percentiles. Against `token-bucket -limit 200 -refill 100`, keyed by a single
shared `X-API-Key`, 50 workers for 10s on loopback:

```
completed  186980  (18694 req/s)
  2xx      1199
  429      185781
  transport 0 errors
latency    mean 2.53ms  p50 2.03ms  p90 4.87ms  p99 10.81ms  max 66.5ms
```

**Accuracy under concurrency:** the theoretical allowance is
`200 (initial capacity) + 100/s × 10s = 1200`. The limiter granted **1199**
across ~187k concurrent attempts — off by one. The `-race`-clean locking holds
under real HTTP load.

**Latency overhead:** end-to-end latency (loopback HTTP + JSON) is
~0.2 ms serial and ~2.5 ms at 50-way concurrency. The limiter's own
contribution is the benchmark figure above (~60–220 ns), i.e. a negligible
fraction of request latency.

## Stage 7 — Stats collection and API

**Files:** [internal/stats/collector.go](internal/stats/collector.go),
[snapshot.go](internal/stats/snapshot.go),
[collector_test.go](internal/stats/collector_test.go),
[internal/api/api.go](internal/api/api.go),
[api_test.go](internal/api/api_test.go)

### Collector (`events.Sink`)

- **`Record` never blocks.** It does a non-blocking send to a buffered channel
  and returns; if the channel is full the event is dropped and a `dropped`
  counter is bumped. The request path never waits on stats.
- A **single worker goroutine** is the only writer of the aggregates, so the
  locking is a single mutex taken briefly by the worker and by snapshot
  readers. This is what makes the `-race` test meaningful.
- **Bounded memory**, all fixed-size:
  - ring buffer of the last `RingSize` (512) decisions — backs `/api/events`
    replay;
  - circular array of `WindowSeconds` (120) per-second `{allow, reject}` cells,
    indexed by `unixSecond % N`;
  - `perKey` map capped at `MaxKeys` (1024); a genuinely new key over the cap
    evicts the least-recently-seen entry.
- SSE fan-out: `Subscribe` returns a buffered channel; the worker does a
  non-blocking send to each subscriber, dropping for any client that falls
  behind.

Tests (all `-race`): aggregate correctness, `Record` never blocks (worker
stopped, 100k sends still return, drops recorded), bounded memory under 500
distinct keys, 32 concurrent writers + 4 concurrent snapshot readers with
every event accounted for (applied or dropped), and live subscribe/unsubscribe.

### API (mounted outside the rate-limit middleware)

| Endpoint | Returns |
|----------|---------|
| `GET /api/stats?seconds=&top=` | totals, `dropped`, zero-filled per-second series, top-N keys by decision count |
| `GET /api/keys/{key}` | one key's tally (allowed, rejected, last remaining, last seen); 404 if unseen |
| `GET /api/config` | active algorithm, key-by mode, backend, tier limits |
| `GET /api/events` | **SSE** stream of live decisions, with a ~25-event backlog on connect and a 15s keepalive comment |
| `POST /api/demo/burst` `{"key","count"}` | fires `count` requests server-side at the **real** limiter (bounded to 1000), returns `{allowed, rejected}` |

The demo endpoint shares the limiter instance the middleware enforces, so a
burst consumes real budget: right after `POST /api/demo/burst {"key":"demo","count":8}`
against `-limit 5`, a real `GET /api/ping` for `demo` returns `429`. Each demo
request is also fed to the collector, so the burst shows up on the SSE stream
and in `/api/stats`.

### Why SSE, not WebSocket

The dashboard only consumes a one-way server→client stream. SSE is a plain
`text/event-stream` HTTP response — no handshake, no framing library, no second
protocol — and the browser's `EventSource` reconnects on its own. A WebSocket
would add a bidirectional transport and its machinery to carry strictly
unidirectional traffic. The moment the dashboard needed to send data back
(it does not), WebSocket would start to pay for itself.

## Stage 8 — Dashboard frontend

**Files:** [web/](web/) — React 18 + TypeScript + Vite, Recharts for the chart.

`npm run build` (`tsc` typecheck + `vite build`) passes and emits `web/dist/`.
The page is deliberately one screen with four things:

1. **Live allowed-vs-rejected area chart**, fed by the SSE stream
   ([useLiveSeries.ts](web/src/useLiveSeries.ts)). Events are rolled up into
   per-second buckets client-side and the chart re-renders at 1 Hz, so a burst
   of thousands of events stays cheap and the time axis keeps moving while idle.
2. **Active config** — algorithm, backend, key-by mode, tier limits — from
   `GET /api/config`.
3. **Top keys table** — key, remaining, allowed, rejected — polled from
   `GET /api/stats` every 2 s.
4. **Send burst control** — key + count, `POST /api/demo/burst`. Because the
   demo hits the real limiter, the chart's reject area and the table's reject
   column jump within a second of clicking.

No router (one page), no auth, no config editing. Recharts pulls in d3, so the
JS bundle is ~530 kB (154 kB gzipped) — acceptable for a single-purpose
dashboard; code-splitting would be the fix if it mattered.

Dev: `cd web && npm run dev` proxies `/api` to `:8080`.

## Stage 9 — Single binary

**Files:** [embed.go](embed.go), [Makefile](Makefile), [build.ps1](build.ps1),
[Dockerfile](Dockerfile), [docker-compose.yml](docker-compose.yml)

- `embed.go` (`//go:embed all:web/dist`) bakes the built dashboard into the
  binary and serves it at `/`, with unknown non-`/api` paths falling back to
  `index.html`. `web/dist` is **committed** so `go run .` always works from a
  fresh clone; `make build` regenerates it. The `/api/*` namespace 404s
  instead of returning the SPA shell for unrouted paths.
- **`make build`** / **`build.ps1`**: build the frontend, then `go build` the
  binary. `make test` runs `go test -race ./...` + `go vet` (the per-stage
  gate). `make bench` runs the algorithm benchmarks.
- **Dockerfile**: 3 stages — `node` builds `web/dist`, `golang` builds a
  static `CGO_ENABLED=0` binary with those assets, `distroless/static` runs it.
- **docker-compose.yml**: `app` + `redis`, app configured (`RL_REDIS=redis:6379`)
  to use the Redis token bucket with in-memory fallback, waiting on a Redis
  healthcheck.

`go run .` serving the embedded UI + API, a server-side burst producing real
429s, and `/api/*` 404 behaviour are covered by
[embed_test.go](embed_test.go) and
[internal/server/server_test.go](internal/server/server_test.go). The Docker
image build was not run in the environment that produced this README (no Docker
daemon available); the Dockerfile follows standard multi-stage patterns.

## Algorithm tradeoffs

| | Token bucket | Sliding window counter |
|---|---|---|
| **Burst behaviour** | Allows a burst up to capacity, then paces at the refill rate. | Allows up to `Limit` per rolling window; no separate burst allowance. |
| **Boundary accuracy** | N/A (no fixed window). | Weighted estimate removes the fixed-window 2× boundary spike; slightly approximate for bunched traffic. |
| **State per key** | 2 values (`tokens`, `last`). | 2 counters + window start. |
| **Hot-path cost** | ~64 ns, 0 alloc | ~97 ns, 0 alloc |
| **Best for** | APIs that want to tolerate short spikes but cap sustained rate. | "Exactly N requests per minute" semantics with smooth enforcement. |
| **Retry-After** | Exact: time for one token to accrue. | Approximate: time until the current window rolls over. |

Compose both with `MultiLimiter` to get "burst up to X, but no more than Y per
minute".

## Running the server

```
go run . -algo token-bucket -limit 200 -refill 100 -key-by header:X-API-Key
go run . -algo sliding-window -limit 100 -window 1m
go run . -redis localhost:6379           # Redis primary + in-memory fallback
```

Flags: `-addr`, `-algo`, `-limit`, `-refill`, `-window`, `-key-by`
(`ip` | `header:<Name>`), `-redis`, `-idle-ttl`.

## Development

```
go test -race ./...                                  # or: make race
go vet ./...                                         # or: make vet
go test -run=^$ -bench=. -benchmem ./internal/ratelimit   # or: make bench
cd web && npm run build                              # dashboard; or: make web
```

Go 1.27+, Node 18+ (frontend only). Go dependencies:
`github.com/redis/go-redis/v9`; `github.com/alicebob/miniredis/v2` (tests only).
Every stage was gated on `go test -race ./...` and `go vet ./...` passing.
