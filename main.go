// Command distributed-rate-limiter runs the HTTP API guarded by a configurable
// rate limiter. Stages 7-9 add the stats API and the embedded dashboard; for
// now it serves GET /api/ping behind the limiter so the Stage 6 load test has
// a target.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pruthviklshetty/distributed-rate-limiter/internal/api"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/httpmw"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/ratelimit"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/server"
	"github.com/pruthviklshetty/distributed-rate-limiter/internal/stats"
	"github.com/redis/go-redis/v9"
)

type flags struct {
	addr          string
	algo          string
	limit         int64
	refill        float64
	window        time.Duration
	keyBy         string
	redis         string
	redisPassword string
	idleTTL       time.Duration
}

func parseFlags() flags {
	var f flags
	flag.StringVar(&f.addr, "addr", envOr("RL_ADDR", ":8080"), "listen address")
	flag.StringVar(&f.algo, "algo", envOr("RL_ALGO", "token-bucket"), "token-bucket | sliding-window")
	flag.Int64Var(&f.limit, "limit", 100, "token-bucket capacity, or sliding-window request limit")
	flag.Float64Var(&f.refill, "refill", 20, "token-bucket refill rate (tokens/sec)")
	flag.DurationVar(&f.window, "window", time.Minute, "sliding-window length")
	flag.StringVar(&f.keyBy, "key-by", "ip", "ip | header:<Name>")
	flag.StringVar(&f.redis, "redis", firstEnv("RL_REDIS", "REDIS_URL"),
		"Redis for a shared backend: a host:port address or a redis:// / rediss:// URL (empty = in-memory only)")
	flag.StringVar(&f.redisPassword, "redis-password", firstEnv("RL_REDIS_PASSWORD", "REDIS_PASSWORD", "REDISPASSWORD"),
		"Redis password for the host:port form (empty = no auth; ignored when -redis is a URL)")
	flag.DurationVar(&f.idleTTL, "idle-ttl", 10*time.Minute, "evict a key's state after this much inactivity")
	flag.Parse()
	return f
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// firstEnv returns the first non-empty environment variable among keys, or "".
// Managed Redis providers vary in what they inject (Railway sets REDIS_URL and
// REDISPASSWORD, others use REDIS_PASSWORD), so accept the common names.
func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// discardRedisLog silences go-redis's internal connection-pool logging. When
// Redis is unreachable it prints a line per retry straight to stderr; the
// FallbackLimiter's OnFallback hook already emits one clean WARN per fallback,
// so the internal chatter is pure noise.
type discardRedisLog struct{}

func (discardRedisLog) Printf(context.Context, string, ...any) {}

// newRedisClient builds a go-redis client from the -redis value. A value
// beginning with redis:// or rediss:// is parsed as a full connection URL
// (managed providers hand one out, with the password — and TLS, for rediss —
// baked in). Anything else is treated as a host:port address, with the
// password coming from -redis-password.
func newRedisClient(f flags) (*redis.Client, error) {
	redis.SetLogger(discardRedisLog{})

	var opt *redis.Options
	if strings.HasPrefix(f.redis, "redis://") || strings.HasPrefix(f.redis, "rediss://") {
		parsed, err := redis.ParseURL(f.redis)
		if err != nil {
			return nil, fmt.Errorf("parse -redis URL: %w", err)
		}
		opt = parsed
	} else {
		opt = &redis.Options{Addr: f.redis, Password: f.redisPassword}
	}

	// Fail fast to the in-memory fallback rather than stalling every request on
	// dial/retry backoff when Redis is unreachable: no retries (the
	// FallbackLimiter is the safety net, and the next request retries Redis
	// fresh) and short timeouts. URL query params (?dial_timeout=…) still win
	// where the caller set them.
	if opt.MaxRetries == 0 {
		opt.MaxRetries = -1
	}
	if opt.DialTimeout == 0 {
		opt.DialTimeout = 500 * time.Millisecond
	}
	if opt.ReadTimeout == 0 {
		opt.ReadTimeout = 500 * time.Millisecond
	}
	if opt.WriteTimeout == 0 {
		opt.WriteTimeout = 500 * time.Millisecond
	}
	return redis.NewClient(opt), nil
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(log)

	f := parseFlags()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	limiter, algoLabel, cleanup, err := buildLimiter(ctx, f)
	if err != nil {
		log.Error("build limiter", "err", err)
		os.Exit(1)
	}
	defer cleanup()

	keyFunc, err := buildKeyFunc(f.keyBy)
	if err != nil {
		log.Error("bad -key-by", "err", err)
		os.Exit(1)
	}

	collector := stats.New(stats.Config{})
	defer collector.Close()

	ui, err := dashboardHandler()
	if err != nil {
		log.Error("load embedded dashboard", "err", err)
		os.Exit(1)
	}

	handler := server.New(server.Options{
		Limiter:   limiter,
		KeyFunc:   keyFunc,
		Algorithm: algoLabel,
		Collector: collector,
		APIConfig: apiConfig(f, algoLabel),
		UI:        ui,
	})

	srv := &http.Server{
		Addr:              f.addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("listening", "addr", f.addr, "algo", algoLabel, "redis", f.redis != "")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("serve", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Error("shutdown", "err", err)
	}
}

// buildLimiter constructs the limiter from flags and returns it, a label for
// events, and a cleanup func that stops any background janitor.
func buildLimiter(ctx context.Context, f flags) (ratelimit.RateLimiter, string, func(), error) {
	noop := func() {}

	switch f.algo {
	case "token-bucket":
		local, err := ratelimit.NewTokenBucket(ratelimit.TokenBucketConfig{
			Capacity: f.limit, RefillPerSec: f.refill,
		})
		if err != nil {
			return nil, "", noop, err
		}
		stopJanitor := local.StartJanitor(ctx, f.idleTTL, f.idleTTL/4)

		if f.redis == "" {
			return local, "token-bucket", stopJanitor, nil
		}
		rdb, err := newRedisClient(f)
		if err != nil {
			return nil, "", noop, err
		}
		remote, err := ratelimit.NewRedisTokenBucket(ratelimit.RedisTokenBucketConfig{
			Client: rdb, Capacity: f.limit, RefillPerSec: f.refill,
			KeyPrefix: "rl:tb:", IdleTTL: f.idleTTL,
		})
		if err != nil {
			return nil, "", noop, err
		}
		fb := ratelimit.NewFallbackLimiter(remote, local)
		fb.OnFallback = func(err error) { slog.Warn("redis fallback to local limiter", "err", err) }
		cleanup := func() { stopJanitor(); _ = rdb.Close() }
		return fb, "token-bucket+redis", cleanup, nil

	case "sliding-window":
		sw, err := ratelimit.NewSlidingWindow(ratelimit.SlidingWindowConfig{
			Limit: f.limit, WindowLen: f.window,
		})
		if err != nil {
			return nil, "", noop, err
		}
		stopJanitor := sw.StartJanitor(ctx, f.idleTTL, f.idleTTL/4)
		return sw, "sliding-window", stopJanitor, nil

	default:
		return nil, "", noop, errors.New("unknown -algo: " + f.algo)
	}
}

// apiConfig builds the payload served by GET /api/config from the flags.
func apiConfig(f flags, algoLabel string) api.ConfigInfo {
	backend := "in-memory"
	if f.redis != "" {
		backend = "redis+fallback"
	}
	tier := api.TierInfo{Name: f.algo, Limit: f.limit}
	switch f.algo {
	case "token-bucket":
		tier.RefillPerSec = f.refill
	case "sliding-window":
		tier.WindowSeconds = f.window.Seconds()
	}
	return api.ConfigInfo{
		Algorithm: algoLabel,
		KeyBy:     f.keyBy,
		Backend:   backend,
		Tiers:     []api.TierInfo{tier},
	}
}

func buildKeyFunc(spec string) (httpmw.KeyFunc, error) {
	switch {
	case spec == "ip":
		return httpmw.KeyByIP, nil
	case strings.HasPrefix(spec, "header:"):
		name := strings.TrimPrefix(spec, "header:")
		if name == "" {
			return nil, errors.New("header name is empty")
		}
		return httpmw.KeyByHeader(name), nil
	default:
		return nil, errors.New("expected 'ip' or 'header:<Name>'")
	}
}
