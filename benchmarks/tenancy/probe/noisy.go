// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

// noisy is the noisy-neighbour test: quiet tenants send an occasional
// point read while heavy tenants run one statement flat out, and the
// quiet tenants' latency is what is reported. Run it with no heavy
// tenants first; that is the figure the others are compared with.
//
// Quiet queries are open loop at -rate across all quiet tenants, each
// to a random one on a new connection, so a quiet tenant is asked now
// and then with gaps between: the case where it has to get its share
// back from a neighbour that was using the whole machine. With
// -quiet-hold each quiet tenant has that many connections, opened and
// used once before anything is measured, and a query takes a free one:
// what a client with a connection pool sees.
//
// Each heavy tenant has -conns connections' worth of work: that many
// loops, each sending -sql again as soon as the last one ends, on a
// new connection each time or, with -hold, on one it keeps. In the
// statement {r} becomes a random id, {e} the id 999 above it (the two
// bound a 1,000-row range that fits below -rows), and {s} a random
// 32-character string.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func readTargets(path string, limit int) []string {
	raw, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	t := strings.Fields(string(raw))
	if limit >= 0 && limit < len(t) {
		t = t[:limit]
	}
	return t
}

func fill(sql string, rows int) string {
	if strings.Contains(sql, "{r}") {
		r := 1 + rand.Intn(max(1, rows-1000))
		sql = strings.ReplaceAll(sql, "{r}", fmt.Sprint(r))
		sql = strings.ReplaceAll(sql, "{e}", fmt.Sprint(r+999))
	}
	if strings.Contains(sql, "{s}") {
		s := fmt.Sprintf("%016x%016x", rand.Uint64(), rand.Uint64())
		sql = strings.ReplaceAll(sql, "{s}", s)
	}
	return sql
}

func noisy(args []string) {
	fs := flag.NewFlagSet("noisy", flag.ExitOnError)
	quietFile := fs.String("quiet", "", "file of quiet tenants' targets")
	heavyFile := fs.String("heavy", "", "file of heavy tenants' targets")
	heavyN := fs.Int("heavy-n", 0, "how many of the heavy tenants to run")
	conns := fs.Int("conns", 4, "loops per heavy tenant")
	sql := fs.String("sql", "", "the heavy statement")
	rate := fs.Int("rate", 50, "quiet queries per second, across them all")
	warm := fs.Duration("warm", 10*time.Second, "heavy load alone, before measuring")
	dur := fs.Duration("dur", 60*time.Second, "measured time")
	rows := fs.Int("rows", 100000, "rows per tenant; ids are 1..rows")
	hold := fs.Bool("hold", false,
		"heavy loops keep one connection each, as a pooled client would")
	quietHold := fs.Int("quiet-hold", 0,
		"connections each quiet tenant keeps; 0 = a new one per query")
	showPhases := fs.Bool("phases", false,
		"also print where the quiet queries' time went to stderr")
	traceFile := fs.String("trace", "",
		"file for one line per quiet query: due_ms,ms,late,connect,startup,query")
	fs.Parse(args)

	// A heavy statement may run for a long time behind its neighbours,
	// and a starved quiet query has to be seen, not cut off.
	convTimeout = 120 * time.Second
	quiet := readTargets(*quietFile, -1)
	var heavy []string
	if *heavyN > 0 {
		heavy = readTargets(*heavyFile, *heavyN)
	}

	// The quiet tenants' pools. Each connection has answered a query
	// before it is used, as one in a pool has.
	pools := map[string]chan *session{}
	for _, target := range quiet {
		if *quietHold == 0 {
			break
		}
		pool := make(chan *session, *quietHold)
		for c := 0; c < *quietHold; c++ {
			s, err := open(target, 2*time.Second)
			if err == nil {
				err = s.exec("SELECT v FROM t WHERE id = 1")
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, "quiet pool:", target, err)
				os.Exit(1)
			}
			pool <- s
		}
		pools[target] = pool
	}

	var stop atomic.Bool
	var done, failed, busyUs atomic.Int64
	var heavyErr atomic.Value
	var hw sync.WaitGroup
	for _, target := range heavy {
		for c := 0; c < *conns; c++ {
			hw.Add(1)
			go func() {
				defer hw.Done()
				var s *session
				for !stop.Load() {
					t0 := time.Now()
					var err error
					if !*hold {
						err = run(target, fill(*sql, *rows), 2*time.Second, false)
					} else {
						if s == nil {
							s, err = open(target, 2*time.Second)
						}
						if err == nil {
							if err = s.exec(fill(*sql, *rows)); err != nil {
								s.c.Close()
								s = nil
							}
						}
					}
					if stop.Load() {
						return
					}
					if err != nil {
						failed.Add(1)
						heavyErr.Store(err.Error())
						time.Sleep(10 * time.Millisecond)
						continue
					}
					done.Add(1)
					busyUs.Add(time.Since(t0).Microseconds())
				}
			}()
		}
	}
	time.Sleep(*warm)
	done.Store(0)
	failed.Store(0)
	busyUs.Store(0)

	var mu sync.Mutex
	var lat, late, conn, start, qry []float64
	var trace strings.Builder
	errs := 0
	var lastErr error
	var wg sync.WaitGroup
	gap := time.Second / time.Duration(*rate)
	begin := time.Now()
	for q := 0; time.Duration(q)*gap < *dur; q++ {
		due := begin.Add(time.Duration(q) * gap)
		waitUntil(due)
		target := quiet[rand.Intn(len(quiet))]
		wg.Add(1)
		go func() {
			defer wg.Done()
			q := fmt.Sprintf("SELECT v FROM t WHERE id = %d", 1+rand.Intn(*rows))
			began := time.Since(due)
			var ph phases
			var err error
			if pool := pools[target]; pool != nil {
				// The wait for a free connection is the query's: a
				// pooled client waits the same way.
				s := <-pool
				t0 := time.Now()
				var got bool
				got, err = s.query(q)
				ph.query = time.Since(t0)
				if err == nil && !got {
					err = fmt.Errorf("no row")
				}
				if err != nil {
					s.c.Close()
					s, _ = open(target, 2*time.Second)
				}
				if s != nil {
					pool <- s
				}
			} else {
				ph, err = runTimed(target, q, 2*time.Second, true)
			}
			ms := float64(time.Since(due).Microseconds()) / 1000
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs++
				lastErr = err
				return
			}
			lat = append(lat, ms)
			f := func(d time.Duration) float64 {
				return float64(d.Microseconds()) / 1000
			}
			late = append(late, f(began))
			conn = append(conn, f(ph.connect))
			start = append(start, f(ph.startup))
			qry = append(qry, f(ph.query))
			if *traceFile != "" {
				fmt.Fprintf(&trace, "%.1f,%.3f,%.3f,%.3f,%.3f,%.3f\n",
					f(due.Sub(begin)), ms, f(began), f(ph.connect),
					f(ph.startup), f(ph.query))
			}
		}()
	}
	wg.Wait()
	heavyDone, heavyFailed, heavyUs := done.Load(), failed.Load(), busyUs.Load()
	stop.Store(true)
	if lastErr != nil {
		fmt.Fprintln(os.Stderr, "last quiet error:", lastErr)
	}
	if e := heavyErr.Load(); e != nil {
		fmt.Fprintln(os.Stderr, "last heavy error:", e)
	}

	if *traceFile != "" {
		if err := os.WriteFile(*traceFile, []byte(trace.String()), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
	if *showPhases {
		// "late" is the sender's own delay: how long after it was due
		// a query's goroutine got to run.
		for _, x := range []struct {
			name string
			a    []float64
		}{{"late", late}, {"connect", conn}, {"startup", start}, {"query", qry}} {
			sort.Float64s(x.a)
			if len(x.a) == 0 {
				continue
			}
			fmt.Fprintf(os.Stderr, "%-8s p50 %.2f  p90 %.2f  p99 %.2f ms\n",
				x.name, x.a[len(x.a)/2], x.a[len(x.a)*9/10], x.a[len(x.a)*99/100])
		}
	}

	sort.Float64s(lat)
	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(p*float64(len(lat)-1))]
	}
	heavyMs := 0.0
	if heavyDone > 0 {
		heavyMs = float64(heavyUs) / float64(heavyDone) / 1000
	}
	// p50_ms,p90_ms,p99_ms,p999_ms,max_ms,ok,errors,
	// heavy_done,heavy_failed,heavy_mean_ms
	fmt.Printf("%.2f,%.2f,%.2f,%.2f,%.2f,%d,%d,%d,%d,%.1f\n",
		pct(0.5), pct(0.9), pct(0.99), pct(0.999), pct(1), len(lat), errs,
		heavyDone, heavyFailed, heavyMs)
	// The heavy loops are left to die with the process: waiting for a
	// statement that takes a minute would only delay the next step.
	os.Exit(0)
}
