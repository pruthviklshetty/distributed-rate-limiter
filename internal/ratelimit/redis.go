package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisTokenBucket is the token-bucket algorithm backed by Redis instead of a
// local map, so a fleet of API instances shares one global budget per key.
//
// The entire check-and-decrement runs inside a single Lua script (see
// tokenBucketLua). That is the whole point of this type: EVAL executes
// atomically on the Redis server — no other command from any client can
// interleave between the script's reads and its writes — so N instances
// hammering the same key can never collectively grant more than the capacity.
// RedisNaiveTokenBucket in this file does the same work with separate
// GET/SET round trips and is used only to demonstrate the race the script
// removes.
//
// Idle keys need no janitor here: every call sets PEXPIRE, so Redis drops a
// key on its own once it has been untouched for IdleTTL. That is the
// distributed equivalent of Stage 2's in-memory eviction.
type RedisTokenBucket struct {
	rdb          redis.Scripter
	script       *redis.Script
	capacity     int64
	refillPerSec float64
	clock        Clock
	keyPrefix    string
	idleTTL      time.Duration
}

// RedisTokenBucketConfig configures a RedisTokenBucket.
type RedisTokenBucketConfig struct {
	// Client is any go-redis client (single node, cluster, ring). Required.
	Client redis.Scripter
	// Capacity is the bucket size: the largest burst a key can make.
	Capacity int64
	// RefillPerSec is tokens added back per second in steady state.
	RefillPerSec float64
	// KeyPrefix namespaces this limiter's keys in Redis (e.g. "rl:tb:").
	KeyPrefix string
	// IdleTTL is how long a key survives in Redis after its last request.
	// It only bounds memory; it does not affect the rate. Defaults to 1h.
	IdleTTL time.Duration
	// Clock is the time source. The current time is passed to the Lua script
	// as an argument (not read from Redis) so every instance and the tests
	// share one notion of "now" — in production that means the app servers
	// must have reasonably synced clocks (NTP). Defaults to RealClock.
	Clock Clock
}

// tokenBucketLua performs a full lazy-refill token-bucket step atomically.
//
//	KEYS[1] = bucket key
//	ARGV[1] = capacity (tokens)
//	ARGV[2] = refill per second
//	ARGV[3] = now in unix milliseconds
//	ARGV[4] = tokens requested (always 1 here)
//	ARGV[5] = idle TTL in milliseconds
//
// Returns { allowed(0|1), remaining(int), retry_after_ms(int) }.
//
// Token counts are stored with tostring() because a Lua number handed to
// redis.call is truncated to an integer by Redis; the string keeps the
// fractional part that accrues between requests.
const tokenBucketLua = `
local capacity = tonumber(ARGV[1])
local refill   = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local want     = tonumber(ARGV[4])
local ttl      = tonumber(ARGV[5])

local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil then
  tokens = capacity
  ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + (elapsed / 1000.0) * refill)

local allowed = 0
if tokens >= want then
  tokens = tokens - want
  allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], ttl)

local retry_after_ms = 0
if allowed == 0 then
  retry_after_ms = math.ceil(((want - tokens) / refill) * 1000)
end

return { allowed, math.floor(tokens), retry_after_ms }
`

// NewRedisTokenBucket validates cfg and returns a ready limiter.
func NewRedisTokenBucket(cfg RedisTokenBucketConfig) (*RedisTokenBucket, error) {
	if cfg.Client == nil {
		return nil, errors.New("ratelimit: RedisTokenBucket requires a Client")
	}
	if cfg.Capacity <= 0 {
		return nil, errors.New("ratelimit: RedisTokenBucket capacity must be > 0")
	}
	if cfg.RefillPerSec <= 0 {
		return nil, errors.New("ratelimit: RedisTokenBucket refill rate must be > 0")
	}
	clk := cfg.Clock
	if clk == nil {
		clk = RealClock{}
	}
	ttl := cfg.IdleTTL
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &RedisTokenBucket{
		rdb:          cfg.Client,
		script:       redis.NewScript(tokenBucketLua),
		capacity:     cfg.Capacity,
		refillPerSec: cfg.RefillPerSec,
		clock:        clk,
		keyPrefix:    cfg.KeyPrefix,
		idleTTL:      ttl,
	}, nil
}

// Allow implements RateLimiter. A non-nil error means Redis could not be
// reached or the script failed; the caller (middleware or FallbackLimiter)
// decides what to do with that.
func (r *RedisTokenBucket) Allow(ctx context.Context, key string) (Result, error) {
	now := r.clock.Now().UnixMilli()
	raw, err := r.script.Run(ctx, r.rdb,
		[]string{r.keyPrefix + key},
		r.capacity, r.refillPerSec, now, 1, r.idleTTL.Milliseconds(),
	).Result()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimit/redis: eval: %w", err)
	}

	vals, ok := raw.([]interface{})
	if !ok || len(vals) != 3 {
		return Result{}, fmt.Errorf("ratelimit/redis: unexpected script reply %T %v", raw, raw)
	}
	allowed, _ := vals[0].(int64)
	remaining, _ := vals[1].(int64)
	retryMs, _ := vals[2].(int64)

	return Result{
		Allowed:    allowed == 1,
		Limit:      r.capacity,
		Remaining:  remaining,
		RetryAfter: time.Duration(retryMs) * time.Millisecond,
	}, nil
}
