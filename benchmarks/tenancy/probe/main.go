// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

// probe times queries against Postgres over the wire protocol, with no
// driver in the way. Each query opens its own connection, as a client
// reaching a tenant that has been idle would.
//
//	probe first -addr ip:port[/db] -t0 <unix nanos>   ms from t0 to first row
//	probe load  -addrs file -rate N -dur 15s          open-loop latency
//	probe sql   -addr ip:port[/db] -q "..."           run one statement
//	probe noisy ...                                   see noisy.go
//	probe agents ...                                  see agents.go
//
// A target is ip:port, or ip:port/database when the tenant is a
// database on a shared server; the database is "postgres" otherwise.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

func sqlOnce(args []string) {
	fs := flag.NewFlagSet("sql", flag.ExitOnError)
	addr := fs.String("addr", "", "ip:port or ip:port/database")
	q := fs.String("q", "", "the statement")
	fs.Parse(args)
	if err := run(*addr, *q, 2*time.Second, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func first(args []string) {
	fs := flag.NewFlagSet("first", flag.ExitOnError)
	addr := fs.String("addr", "", "ip:port or ip:port/database")
	t0 := fs.Int64("t0", 0,
		"start of the clock, unix nanoseconds; 0 = when probe starts")
	timeout := fs.Duration("timeout", 30*time.Second, "give up after")
	q := fs.String("q", query,
		"the statement; given, it need not return a row (a blank Postgres"+
			" can be waited for with \"SELECT 1\")")
	fs.Parse(args)
	// The default statement is the point read of row 1, held to its
	// value; a statement given is only waited for.
	check := pointCheck(1)
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "q" {
			check = nil
		}
	})

	// A clock taken by the caller also counts the time to start this
	// process, a few ms: right when the caller then launches a server,
	// too coarse for timing a query against one already up.
	start := time.Now()
	if *t0 != 0 {
		start = time.Unix(0, *t0)
	}
	var last error
	for time.Since(start) < *timeout {
		// A short dial and a fresh attempt: a SYN the guest missed
		// would otherwise be resent only after a full second.
		if last = run(*addr, *q, 5*time.Millisecond, check); last == nil {
			fmt.Printf("%.1f\n", msSince(start))
			return
		}
		time.Sleep(500 * time.Microsecond)
	}
	fmt.Fprintln(os.Stderr, "timed out:", last)
	os.Exit(1)
}

func load(args []string) {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	file := fs.String("addrs", "", "file of targets, one per line")
	rate := fs.Int("rate", 200, "queries per second, across all tenants")
	dur := fs.Duration("dur", 15*time.Second, "how long to send")
	persec := fs.Bool("persec", false,
		"also print to stderr the worst latency of each second, and its"+
			" connect, startup and query times")
	fs.Parse(args)
	addrs := readTargets(*file, -1)

	var mu sync.Mutex
	var lat []float64
	worst := make([]float64, int(dur.Seconds())+1)
	worstPh := make([]phases, len(worst))
	errs := 0
	var wg sync.WaitGroup
	gap := time.Second / time.Duration(*rate)
	begin := time.Now()
	// Open loop: each query is due at a fixed time and its latency is
	// measured from then, so a stalled server cannot slow the sender
	// and hide its own tail.
	for n := 0; time.Duration(n)*gap < *dur; n++ {
		due := begin.Add(time.Duration(n) * gap)
		waitUntil(due)
		addr := addrs[rand.Intn(len(addrs))]
		wg.Add(1)
		go func() {
			defer wg.Done()
			ph, err := runTimed(addr, query, 2*time.Second, pointCheck(1))
			lms := msSince(due)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs++
				return
			}
			lat = append(lat, lms)
			if sec := int(due.Sub(begin).Seconds()); lms > worst[sec] {
				worst[sec] = lms
				worstPh[sec] = ph
			}
		}()
	}
	wg.Wait()
	if *persec {
		for sec, w := range worst {
			ph := worstPh[sec]
			fmt.Fprintf(os.Stderr, "%d,%.1f,%.1f,%.1f,%.1f\n", sec, w,
				ms(ph.connect), ms(ph.startup), ms(ph.query))
		}
	}

	lat = sorted(lat)
	// p50_ms,p90_ms,p99_ms,ok,errors. No p99.9 or max: a run is a few
	// thousand queries, and its top few are noise.
	fmt.Printf("%.2f,%.2f,%.2f,%d,%d\n",
		pct(lat, 0.5), pct(lat, 0.9), pct(lat, 0.99), len(lat), errs)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr,
			"usage: probe first|load|sql|noisy|agents [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "first":
		first(os.Args[2:])
	case "load":
		load(os.Args[2:])
	case "sql":
		sqlOnce(os.Args[2:])
	case "noisy":
		noisy(os.Args[2:])
	case "agents":
		agents(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "unknown mode:", os.Args[1])
		os.Exit(2)
	}
}
