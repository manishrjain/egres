// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

// probe times queries against Postgres over the wire protocol, with no
// driver in the way. Each query opens its own connection, as a client
// reaching a tenant that has been idle would.
//
//	probe first -addr ip:port[/db] -t0 <unix nanos>   ms from t0 to first row
//	probe load  -addrs file -rate N -dur 15s          open-loop latency
//	probe sql   -addr ip:port[/db] -q "..."           run one statement
//	probe fleet ...                                   see fleet.go
//	probe noisy ...                                   see noisy.go
//
// A target is ip:port, or ip:port/database when the tenant is a
// database on a shared server; the database is "postgres" otherwise.
package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const query = "SELECT v FROM t WHERE id = 1"

// How long one connection may take from connect to its last reply.
var convTimeout = 10 * time.Second

func startup(user, db string) []byte {
	body := []byte{0, 3, 0, 0}
	for _, s := range []string{"user", user, "database", db} {
		body = append(body, s...)
		body = append(body, 0)
	}
	body = append(body, 0)
	out := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(out, uint32(4+len(body)))
	return append(out, body...)
}

func simpleQuery(sql string) []byte {
	out := []byte{'Q', 0, 0, 0, 0}
	out = append(out, sql...)
	out = append(out, 0)
	binary.BigEndian.PutUint32(out[1:], uint32(len(out)-1))
	return out
}

// untilReady reads messages up to ReadyForQuery and reports whether a
// DataRow was among them.
func untilReady(r *bufio.Reader) (bool, error) {
	var hdr [5]byte
	row := false
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return false, err
		}
		n := int(binary.BigEndian.Uint32(hdr[1:])) - 4
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			return false, err
		}
		switch hdr[0] {
		case 'E':
			return false, errors.New("server: " + string(body))
		case 'D':
			row = true
		case 'Z':
			return row, nil
		}
	}
}

// once connects, runs the point read and waits for its row.
func once(target string, dial time.Duration) error {
	return run(target, query, dial, true)
}

// run connects, sends one statement and reads its reply; with wantRow
// a reply without a row is an error.
func run(target, sql string, dial time.Duration, wantRow bool) error {
	_, err := runTimed(target, sql, dial, wantRow)
	return err
}

// phases is where one connection's time went: the TCP connect, the
// wait for the server to answer the startup message, and the query.
type phases struct{ connect, startup, query time.Duration }

func runTimed(target, sql string, dial time.Duration, wantRow bool) (phases, error) {
	var ph phases
	addr, db, found := strings.Cut(target, "/")
	if !found {
		db = "postgres"
	}
	t0 := time.Now()
	c, err := net.DialTimeout("tcp", addr, dial)
	if err != nil {
		return ph, err
	}
	defer c.Close()
	t1 := time.Now()
	ph.connect = t1.Sub(t0)
	c.SetDeadline(t1.Add(convTimeout))
	r := bufio.NewReader(c)
	if _, err := c.Write(startup("postgres", db)); err != nil {
		return ph, err
	}
	if _, err := untilReady(r); err != nil {
		return ph, err
	}
	t2 := time.Now()
	ph.startup = t2.Sub(t1)
	if _, err := c.Write(simpleQuery(sql)); err != nil {
		return ph, err
	}
	row, err := untilReady(r)
	if err != nil {
		return ph, err
	}
	ph.query = time.Since(t2)
	if wantRow && !row {
		return ph, errors.New("no row")
	}
	return ph, nil
}

// session is a connection kept open for statement after statement.
type session struct {
	c net.Conn
	r *bufio.Reader
}

func open(target string, dial time.Duration) (*session, error) {
	addr, db, found := strings.Cut(target, "/")
	if !found {
		db = "postgres"
	}
	c, err := net.DialTimeout("tcp", addr, dial)
	if err != nil {
		return nil, err
	}
	s := &session{c: c, r: bufio.NewReader(c)}
	c.SetDeadline(time.Now().Add(convTimeout))
	if _, err := c.Write(startup("postgres", db)); err != nil {
		c.Close()
		return nil, err
	}
	if _, err := untilReady(s.r); err != nil {
		c.Close()
		return nil, err
	}
	return s, nil
}

func (s *session) exec(sql string) error {
	_, err := s.query(sql)
	return err
}

// query is exec that also says whether a row came back.
func (s *session) query(sql string) (bool, error) {
	s.c.SetDeadline(time.Now().Add(convTimeout))
	if _, err := s.c.Write(simpleQuery(sql)); err != nil {
		return false, err
	}
	return untilReady(s.r)
}

func sqlOnce(args []string) {
	fs := flag.NewFlagSet("sql", flag.ExitOnError)
	addr := fs.String("addr", "", "ip:port or ip:port/database")
	q := fs.String("q", "", "the statement")
	fs.Parse(args)
	if err := run(*addr, *q, 2*time.Second, false); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// waitUntil returns at t. A bare sleep returns up to a millisecond
// late, which would be added to every latency measured from t; this
// sleeps most of the way and spins the rest.
func waitUntil(t time.Time) {
	if d := time.Until(t) - 2*time.Millisecond; d > 0 {
		time.Sleep(d)
	}
	for time.Now().Before(t) {
	}
}

func first(args []string) {
	fs := flag.NewFlagSet("first", flag.ExitOnError)
	addr := fs.String("addr", "", "ip:port or ip:port/database")
	t0 := fs.Int64("t0", 0,
		"start of the clock, unix nanoseconds; 0 = when probe starts")
	timeout := fs.Duration("timeout", 30*time.Second, "give up after")
	fs.Parse(args)

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
		if last = once(*addr, 5*time.Millisecond); last == nil {
			fmt.Printf("%.1f\n", float64(time.Since(start).Microseconds())/1000)
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

	raw, err := os.ReadFile(*file)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	addrs := strings.Fields(string(raw))

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
			ph, err := runTimed(addr, query, 2*time.Second, true)
			ms := float64(time.Since(due).Microseconds()) / 1000
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs++
				return
			}
			lat = append(lat, ms)
			if sec := int(due.Sub(begin).Seconds()); ms > worst[sec] {
				worst[sec] = ms
				worstPh[sec] = ph
			}
		}()
	}
	wg.Wait()
	if *persec {
		msOf := func(d time.Duration) float64 {
			return float64(d.Microseconds()) / 1000
		}
		for sec, ms := range worst {
			ph := worstPh[sec]
			fmt.Fprintf(os.Stderr, "%d,%.1f,%.1f,%.1f,%.1f\n", sec, ms,
				msOf(ph.connect), msOf(ph.startup), msOf(ph.query))
		}
	}

	sort.Float64s(lat)
	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(p*float64(len(lat)-1))]
	}
	// p50_ms,p90_ms,p99_ms,max_ms,ok,errors
	fmt.Printf("%.2f,%.2f,%.2f,%.2f,%d,%d\n",
		pct(0.5), pct(0.9), pct(0.99), pct(1), len(lat), errs)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr,
			"usage: probe first|load|sql|fleet|noisy [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "first":
		first(os.Args[2:])
	case "load":
		load(os.Args[2:])
	case "sql":
		sqlOnce(os.Args[2:])
	case "fleet":
		fleet(os.Args[2:])
	case "noisy":
		noisy(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "unknown mode:", os.Args[1])
		os.Exit(2)
	}
}
