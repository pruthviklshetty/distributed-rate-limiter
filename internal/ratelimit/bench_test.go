package ratelimit

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// These benchmarks compare the two in-memory algorithms on the hot path
// (Allow). Run with:
//
//	go test -run=^$ -bench=. -benchmem ./internal/ratelimit
//
// -benchmem reports B/op and allocs/op, which is the number that matters here:
// the request path should not allocate.

func benchTB(b *testing.B) *TokenBucket {
	b.Helper()
	tb, err := NewTokenBucket(TokenBucketConfig{Capacity: 1 << 30, RefillPerSec: 1e9, Clock: RealClock{}})
	if err != nil {
		b.Fatal(err)
	}
	return tb
}

func benchSW(b *testing.B) *SlidingWindow {
	b.Helper()
	sw, err := NewSlidingWindow(SlidingWindowConfig{Limit: 1 << 30, WindowLen: time.Hour, Clock: RealClock{}})
	if err != nil {
		b.Fatal(err)
	}
	return sw
}

func BenchmarkTokenBucket_Allow_SingleKey(b *testing.B) {
	tb := benchTB(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tb.Allow(ctx, "k")
	}
}

func BenchmarkSlidingWindow_Allow_SingleKey(b *testing.B) {
	sw := benchSW(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sw.Allow(ctx, "k")
	}
}

// Parallel, many keys: models real traffic where goroutines mostly touch
// different buckets and contention is on the map lock, not one entry lock.
func BenchmarkTokenBucket_Allow_ParallelManyKeys(b *testing.B) {
	tb := benchTB(b)
	ctx := context.Background()
	keys := makeKeys(1024)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = tb.Allow(ctx, keys[i&1023])
			i++
		}
	})
}

func BenchmarkSlidingWindow_Allow_ParallelManyKeys(b *testing.B) {
	sw := benchSW(b)
	ctx := context.Background()
	keys := makeKeys(1024)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = sw.Allow(ctx, keys[i&1023])
			i++
		}
	})
}

// Parallel, single hot key: worst case, every goroutine serialises on one
// entry mutex.
func BenchmarkTokenBucket_Allow_ParallelHotKey(b *testing.B) {
	tb := benchTB(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = tb.Allow(ctx, "hot")
		}
	})
}

func BenchmarkSlidingWindow_Allow_ParallelHotKey(b *testing.B) {
	sw := benchSW(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = sw.Allow(ctx, "hot")
		}
	})
}

func makeKeys(n int) []string {
	ks := make([]string, n)
	for i := range ks {
		ks[i] = "key-" + strconv.Itoa(i)
	}
	return ks
}
