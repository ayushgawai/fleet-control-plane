// fleetload simulates a device fleet in-process against the real Store/Queue:
// one goroutine per device sends heartbeats, a new desired config is pushed to
// every device, and a pool of delivery goroutines drains the queue while
// injecting failures (first delivery dropped for ~10%) and duplicate
// deliveries/acks. It then verifies no device is left on a stale version.
//
//	go run ./cmd/fleetload -devices 100000
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayushgawai/fleet-control-plane/internal/fleet"
)

func pct(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	return d[int(p*float64(len(d)-1))]
}

func main() {
	n := flag.Int("devices", 100000, "simulated devices")
	beats := flag.Int("beats", 3, "heartbeats per device")
	failPct := flag.Int("fail", 10, "percent of devices whose first config delivery is dropped")
	workers := flag.Int("workers", runtime.NumCPU()*4, "concurrent delivery goroutines")
	flag.Parse()
	s, q, bus := fleet.NewStore(), fleet.NewQueue(), fleet.NewBus()
	ids := make([]string, *n)
	for i := range ids {
		ids[i] = fmt.Sprintf("cam-%06d", i)
		s.Register(fleet.Device{ID: ids[i], Org: fmt.Sprintf("org-%d", i%50), Model: "sim"})
	}

	// Heartbeats: every device is its own goroutine.
	var wg sync.WaitGroup
	start := time.Now()
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for b := 0; b < *beats; b++ {
				s.Heartbeat(id, 90*time.Second)
				bus.Publish("heartbeats", []byte(id))
			}
		}(id)
	}
	wg.Wait()
	hbDur := time.Since(start)
	online := 0
	for _, id := range ids {
		if s.Online(id) {
			online++
		}
	}

	// Two config rounds: v1 then v2, so a late duplicate of v1 must not win.
	for round := 0; round < 2; round++ {
		for _, id := range ids {
			sh, _ := s.Shadow(id)
			sh, err := s.SetDesired(id, map[string]any{"fps": float64(15 * (round + 1)), "ir": round == 1}, sh.Version)
			if err != nil {
				panic(err)
			}
			c := fleet.Command{ID: fmt.Sprintf("%s:v%d", id, sh.Version), DeviceID: id, Version: sh.Version, Config: sh.Desired, SentAt: time.Now()}
			q.Send(c)
			q.Send(c) // duplicate delivery injected
		}
	}

	var (
		mu       sync.Mutex
		lat      []time.Duration
		failures atomic.Int64
		dropped  sync.Map
		failCap  = int64(*n * *failPct / 100)
	)
	pushStart := time.Now()
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				c, ok := q.Receive()
				if !ok {
					return
				}
				// Drop the first delivery of one command per affected device.
				if c.Attempts == 1 && failures.Load() < failCap {
					if _, seen := dropped.LoadOrStore(c.DeviceID, true); !seen {
						failures.Add(1)
						q.Fail(c)
						continue
					}
				}
				if _, err := s.Report(c.DeviceID, c.Config, c.Version); err != nil {
					panic(err)
				}
				first := q.Ack(c.ID)
				q.Ack(c.ID) // duplicate ack injected
				if first {
					d := time.Since(c.SentAt)
					mu.Lock()
					lat = append(lat, d)
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	pushDur := time.Since(pushStart)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	stale := 0
	for _, id := range ids {
		sh, _ := s.Shadow(id)
		if sh.ReportedVersion != sh.Version {
			stale++
		}
	}
	total := bus.Count("heartbeats")
	fmt.Printf("devices=%d online=%d heartbeats=%d ingest=%.0f hb/s (%s)\n", *n, online, total, float64(total)/hbDur.Seconds(), hbDur.Round(time.Millisecond))
	fmt.Printf("config pushes=%d acks=%d drained in %s, injected failures=%d, duplicate deliveries=%d, dlq=%d\n", 2*(*n), len(lat), pushDur.Round(time.Millisecond), failures.Load(), 2*(*n), q.DLQLen())
	fmt.Printf("config-push ack latency p50=%s p99=%s max=%s\n", pct(lat, .5).Round(time.Microsecond), pct(lat, .99).Round(time.Microsecond), pct(lat, 1).Round(time.Microsecond))
	fmt.Printf("stale devices after failures+duplicates: %d\n", stale)
	if stale > 0 {
		os.Exit(1)
	}
	fmt.Println("OK: every device converged to the latest desired version")
}
