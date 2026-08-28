package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisNaiveTokenBucket is a DELIBERATELY BROKEN Redis token bucket. It exists
// only so the race that RedisTokenBucket's Lua script prevents can be
// demonstrated and tested.
//
// It does the identical work — read tokens + timestamp, lazily refill, decide,
// write back — but as three separate Redis round trips instead of one atomic
// script. Between the read and the write, any number of other clients can run
// the same read and see the same token count. Each then believes it may
// proceed, and the bucket is over-drawn: with N concurrent callers and a
// capacity of C, up to N requests are granted instead of C.
//
// Do not use this type for anything real.
type RedisNaiveTokenBucket struct {
	rdb          redis.Cmdable
	capacity     int64
	capacityF    float64
	refillPerSec float64
	clock        Clock
	keyPrefix    string
	idleTTL      time.Duration

	// beforeWrite, if set, is called after the read and refill computation but
	// before the write-back. Tests use it as a barrier so every goroutine
	// finishes its read before any write happens, turning the race from
	// "usually observable" into "always observable".
	beforeWrite func()
}

// NewRedisNaiveTokenBucket builds the broken limiter used for the race demo.
func NewRedisNaiveTokenBucket(cfg RedisTokenBucketConfig) (*RedisNaiveTokenBucket, error) {
	if cfg.Client == nil {
		return nil, errors.New("ratelimit: RedisNaiveTokenBucket requires a Client")
	}
	cmdable, ok := cfg.Client.(redis.Cmdable)
	if !ok {
		return nil, errors.New("ratelimit: RedisNaiveTokenBucket needs a redis.Cmdable client")
	}
	if cfg.Capacity <= 0 || cfg.RefillPerSec <= 0 {
		return nil, errors.New("ratelimit: RedisNaiveTokenBucket capacity and refill must be > 0")
	}
	clk := cfg.Clock
	if clk == nil {
		clk = RealClock{}
	}
	ttl := cfg.IdleTTL
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &RedisNaiveTokenBucket{
		rdb:          cmdable,
		capacity:     cfg.Capacity,
		capacityF:    float64(cfg.Capacity),
		refillPerSec: cfg.RefillPerSec,
		clock:        clk,
		keyPrefix:    cfg.KeyPrefix,
		idleTTL:      ttl,
	}, nil
}

func (r *RedisNaiveTokenBucket) Allow(ctx context.Context, key string) (Result, error) {
	k := r.keyPrefix + key
	now := r.clock.Now()

	// --- READ (round trip 1) ---
	state, err := r.rdb.HMGet(ctx, k, "tokens", "ts").Result()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimit/redis-naive: hmget: %w", err)
	}
	tokens := r.capacityF
	ts := now
	if len(state) == 2 && state[0] != nil {
		if s, ok := state[0].(string); ok {
			if v, perr := strconv.ParseFloat(s, 64); perr == nil {
				tokens = v
			}
		}
	}
	if len(state) == 2 && state[1] != nil {
		if s, ok := state[1].(string); ok {
			if ms, perr := strconv.ParseInt(s, 10, 64); perr == nil {
				ts = time.UnixMilli(ms)
			}
		}
	}

	elapsed := now.Sub(ts)
	if elapsed < 0 {
		elapsed = 0
	}
	tokens = math.Min(r.capacityF, tokens+elapsed.Seconds()*r.refillPerSec)

	if r.beforeWrite != nil {
		r.beforeWrite()
	}

	res := Result{Limit: r.capacity}
	if tokens >= 1 {
		tokens--
		res.Allowed = true
		res.Remaining = int64(math.Floor(tokens))
	} else {
		res.RetryAfter = time.Duration((1-tokens)/r.refillPerSec*1000) * time.Millisecond
	}

	// --- WRITE (round trips 2 and 3) ---
	if err := r.rdb.HSet(ctx, k, "tokens", strconv.FormatFloat(tokens, 'f', -1, 64), "ts", now.UnixMilli()).Err(); err != nil {
		return Result{}, fmt.Errorf("ratelimit/redis-naive: hset: %w", err)
	}
	r.rdb.PExpire(ctx, k, r.idleTTL)

	return res, nil
}
