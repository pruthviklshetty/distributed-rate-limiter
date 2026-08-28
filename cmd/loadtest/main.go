// Command loadtest drives the rate-limiter HTTP server to produce real numbers
// on per-request latency overhead and on accuracy under concurrency.
//
// Example:
//
//	go run ./cmd/loadtest -url http://localhost:8080/api/ping -c 50 -d 10s -key demo
//
// With -key set, every request carries the same X-API-Key header, so the
// server limits them all as one key: the count of 200s over the run should
// track the configured limit, and everything else should be 429. Without
// -key, requests are limited per source IP (all the same here) unless the
// server is keyed differently.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "http://localhost:8080/api/ping", "target URL")
	conc := flag.Int("c", 50, "concurrent workers")
	dur := flag.Duration("d", 10*time.Second, "test duration (ignored if -n > 0)")
	total := flag.Int64("n", 0, "total requests to send (0 = use -d)")
	key := flag.String("key", "", "if set, send this as X-API-Key on every request")
	keyHeader := flag.String("key-header", "X-API-Key", "header name for -key")
	flag.Parse()

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *conc * 2,
			MaxIdleConnsPerHost: *conc * 2,
			MaxConnsPerHost:     *conc * 2,
		},
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	if *total <= 0 {
		ctx, cancel = context.WithTimeout(ctx, *dur)
		defer cancel()
	}

	var (
		sent     atomic.Int64
		ok2xx    atomic.Int64
		too429   atomic.Int64
		otherRC  atomic.Int64
		errCount atomic.Int64
	)
	latCh := make(chan time.Duration, 1<<16)
	var lats []time.Duration
	var latWG sync.WaitGroup
	latWG.Add(1)
	go func() {
		defer latWG.Done()
		for d := range latCh {
			lats = append(lats, d)
		}
	}()

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if *total > 0 {
					if sent.Add(1) > *total {
						return
					}
				} else if ctx.Err() != nil {
					return
				}

				req, _ := http.NewRequest(http.MethodGet, *url, nil)
				if *key != "" {
					req.Header.Set(*keyHeader, *key)
				}
				t0 := time.Now()
				resp, err := client.Do(req)
				elapsed := time.Since(t0)
				if err != nil {
					errCount.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				latCh <- elapsed
				switch {
				case resp.StatusCode >= 200 && resp.StatusCode < 300:
					ok2xx.Add(1)
				case resp.StatusCode == http.StatusTooManyRequests:
					too429.Add(1)
				default:
					otherRC.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	wall := time.Since(start)
	close(latCh)
	latWG.Wait()

	completed := ok2xx.Load() + too429.Load() + otherRC.Load()
	fmt.Printf("target        %s\n", *url)
	fmt.Printf("workers       %d\n", *conc)
	fmt.Printf("wall time     %s\n", wall.Round(time.Millisecond))
	fmt.Printf("completed     %d  (%.0f req/s)\n", completed, float64(completed)/wall.Seconds())
	fmt.Printf("  2xx         %d\n", ok2xx.Load())
	fmt.Printf("  429         %d\n", too429.Load())
	fmt.Printf("  other       %d\n", otherRC.Load())
	fmt.Printf("  transport   %d errors\n", errCount.Load())
	printLatency(lats)
}

func printLatency(lats []time.Duration) {
	if len(lats) == 0 {
		fmt.Println("latency       (no samples)")
		return
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration {
		idx := int(p / 100 * float64(len(lats)))
		if idx >= len(lats) {
			idx = len(lats) - 1
		}
		return lats[idx]
	}
	var sum time.Duration
	for _, d := range lats {
		sum += d
	}
	fmt.Printf("latency       mean %s  p50 %s  p90 %s  p99 %s  max %s\n",
		(sum / time.Duration(len(lats))).Round(time.Microsecond),
		pct(50).Round(time.Microsecond),
		pct(90).Round(time.Microsecond),
		pct(99).Round(time.Microsecond),
		lats[len(lats)-1].Round(time.Microsecond),
	)
}
