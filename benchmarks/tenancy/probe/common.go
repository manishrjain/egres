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
