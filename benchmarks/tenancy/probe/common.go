// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

// Pieces the subcommands share: percentiles, open-loop timing, the
// rows a tenant's table is loaded with.
package main

import (
	"container/heap"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The point read every subcommand uses to see that a tenant answers.
const query = "SELECT v FROM t WHERE id = 1"

// pct is the p-th quantile (0..1) of a sorted slice, 0 when it is empty.
func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(p*float64(len(sorted)-1))]
}

// pctCell is pct as a CSV cell: empty when there are no samples, so a
// column with nothing in it cannot be read as a latency of zero.
func pctCell(sorted []float64, p float64) string {
	if len(sorted) == 0 {
		return ""
	}
	return fmt.Sprintf("%.2f", pct(sorted, p))
}

func sorted(a []float64) []float64 {
	b := append([]float64(nil), a...)
	sort.Float64s(b)
	return b
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func msSince(t time.Time) float64 { return ms(time.Since(t)) }

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

// clock is waitUntil for many goroutines at once. One goroutine does
// the last-moment spin for all of them: thousands of sessions each
// spinning before each statement would take a core per five hundred
// statements a second, on the machine being measured.
type clock struct {
	mu   sync.Mutex
	h    dueHeap
	kick chan struct{}
}

type dueAt struct {
	t  time.Time
	ch chan struct{}
}

type dueHeap []dueAt

func (h dueHeap) Len() int           { return len(h) }
func (h dueHeap) Less(i, j int) bool { return h[i].t.Before(h[j].t) }
func (h dueHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *dueHeap) Push(x any)        { *h = append(*h, x.(dueAt)) }
func (h *dueHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func newClock() *clock {
	c := &clock{kick: make(chan struct{}, 1)}
	go c.run()
	return c
}

// wait returns at t, or at once if t has passed.
func (c *clock) wait(t time.Time) {
	if !time.Now().Before(t) {
		return
	}
	ch := make(chan struct{})
	c.mu.Lock()
	heap.Push(&c.h, dueAt{t, ch})
	first := c.h[0].ch == ch
	c.mu.Unlock()
	if first {
		select {
		case c.kick <- struct{}{}:
		default:
		}
	}
	<-ch
}

func (c *clock) run() {
	for {
		c.mu.Lock()
		var next time.Time
		if len(c.h) > 0 {
			next = c.h[0].t
		}
		c.mu.Unlock()
		if next.IsZero() {
			<-c.kick
			continue
		}
		if !c.sleepUntil(next) {
			continue // an earlier time came in
		}
		now := time.Now()
		c.mu.Lock()
		for len(c.h) > 0 && !c.h[0].t.After(now) {
			close(heap.Pop(&c.h).(dueAt).ch)
		}
		c.mu.Unlock()
	}
}

// sleepUntil is waitUntil that gives up, returning false, on a kick.
func (c *clock) sleepUntil(t time.Time) bool {
	if d := time.Until(t) - 2*time.Millisecond; d > 0 {
		tm := time.NewTimer(d)
		select {
		case <-tm.C:
		case <-c.kick:
			tm.Stop()
			return false
		}
	}
	for time.Now().Before(t) {
		select {
		case <-c.kick:
			return false
		default:
		}
	}
	return true
}

// md5Text is what Postgres's md5(g) gives for an integer g: the hex
// digest of its decimal text. The tables the images were built with
// hold (g, md5(g)); a table loaded here holds the same.
func md5Text(g int) string {
	sum := md5.Sum([]byte(strconv.Itoa(g)))
	return hex.EncodeToString(sum[:])
}

// The checks a reply is held to. Row id holds value(id, k): md5(id)
// as the seed or an insert wrote it, md5(id):k after its k-th update.
// So a read that returns the wrong row, junk, a value from another
// table, or (where the reader knows the row's version, agents.go) a
// version older than the last write it confirmed, fails here, and the
// failure counts as an error in the test's row like a refusal would.

func value(id, k int) string {
	if k == 0 {
		return md5Text(id)
	}
	return md5Text(id) + ":" + strconv.Itoa(k)
}

// parseValue is the version v carries as a value of row id, or -1
// when v is not one.
func parseValue(id int, v string) int {
	m := md5Text(id)
	if v == m {
		return 0
	}
	if !strings.HasPrefix(v, m+":") {
		return -1
	}
	k, err := strconv.Atoi(v[len(m)+1:])
	if err != nil || k < 1 {
		return -1
	}
	return k
}

func short(v string) string {
	if len(v) > 48 {
		return v[:48] + "..."
	}
	return v
}

// oneValue: the reply is one row of one column, and returns it.
func oneValue(r *result, what string) (string, error) {
	if len(r.rows) != 1 || len(r.rows[0]) < 1 {
		return "", fmt.Errorf("%s: %d rows, want 1", what, len(r.rows))
	}
	return r.rows[0][0], nil
}

// pointCheck: one row, the value of row id as it was loaded. For a
// row that may have been updated, tenant.pointCheck.
func pointCheck(id int) checker {
	return func(r *result) error {
		v, err := oneValue(r, fmt.Sprintf("point id %d", id))
		if err != nil {
			return err
		}
		if v != value(id, 0) {
			return fmt.Errorf("point id %d: got %q, want %s", id, short(v),
				value(id, 0))
		}
		return nil
	}
}

// countCheck: one row, one number between lo and hi.
func countCheck(lo, hi int) checker {
	return func(r *result) error {
		v, err := oneValue(r, "count")
		if err != nil {
			return err
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < lo || n > hi {
			return fmt.Errorf("count: got %q, want %d..%d", short(v), lo, hi)
		}
		return nil
	}
}

// tagCheck: the command tag, "UPDATE 1" or "INSERT 0 1": the write
// touched the one row it named.
func tagCheck(want string) checker {
	return func(r *result) error {
		if r.tag != want {
			return fmt.Errorf("tag %q, want %q", r.tag, want)
		}
		return nil
	}
}

// diskIO is the KiB read and written on the drive since boot, from
// /proc/diskstats; 0, 0 without one or when it cannot be read.
func diskIO(disk string) (read, written int64) {
	if disk == "" {
		return 0, 0
	}
	raw, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return 0, 0
	}
	return parseDiskstats(string(raw), disk)
}

// parseDiskstats: a line per device, the name in field 3, sectors
// (512 B) read in field 6 and written in field 10.
func parseDiskstats(text, disk string) (read, written int64) {
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(line)
		if len(f) < 10 || f[2] != disk {
			continue
		}
		r, _ := strconv.ParseInt(f[5], 10, 64)
		w, _ := strconv.ParseInt(f[9], 10, 64)
		return r / 2, w / 2
	}
	return 0, 0
}

// insertRows is one INSERT of the rows from..to, both included. The
// rows are spelled out because Egres has no INSERT ... SELECT.
func insertRows(from, to int) string {
	var b strings.Builder
	b.WriteString("INSERT INTO t VALUES ")
	for g := from; g <= to; g++ {
		if g > from {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "(%d, '%s')", g, md5Text(g))
	}
	return b.String()
}

// rowBatches splits the rows 1..rows into INSERTs of a thousand.
func rowBatches(rows int) [][2]int {
	var b [][2]int
	for g := 1; g <= rows; g += 1000 {
		b = append(b, [2]int{g, min(g+999, rows)})
	}
	return b
}

// loadRows creates t and fills it with the rows 1..rows.
func loadRows(s *session, rows int) error {
	if err := s.exec("CREATE TABLE t (id bigint PRIMARY KEY, v text)"); err != nil {
		return err
	}
	for _, b := range rowBatches(rows) {
		if err := s.exec(insertRows(b[0], b[1])); err != nil {
			return err
		}
	}
	return nil
}

// randText is 32 random hex characters, from r or, when r is nil, from
// the shared source.
func randText(r *rand.Rand) string {
	if r == nil {
		return fmt.Sprintf("%016x%016x", rand.Uint64(), rand.Uint64())
	}
	return fmt.Sprintf("%016x%016x", r.Uint64(), r.Uint64())
}

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
