// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

// A checkpoint of what the agents probe knows of its tenants, so that
// a run can go on after the probe or the server stopped: the count,
// and for each tenant the rows it has and the rows it has updated,
// with their versions. Written at every window and every few minutes
// of growth, atomically beside the results; read by -resume.
package main

import (
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// ckptTenant is one tenant's state, sparse: only rows with something
// to say are listed.
type ckptTenant struct {
	ID, Rows, NextID, Holes int32
	// Rows at a version above 0, and the version.
	VerRow []int32
	Ver    []uint16
	// Rows whose update's reply was lost: the version known to be
	// there (the row is at that or above).
	DoubtRow []int32
	DoubtLo  []uint16
	Absent   []int32
	Unsure   []int32
	Open     bool
}

// Count is how many tenants there are, Next the id after the last one
// sent for; they differ only when an onboarding failed at the end.
type ckpt struct {
	Taken       time.Time
	Count, Next int
	Tenants     []ckptTenant
}

// snapshot copies the tenant under its lock.
func (t *tenant) snapshot() ckptTenant {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := ckptTenant{ID: int32(t.id), Rows: int32(t.rows), NextID: int32(t.nextID),
		Holes: int32(t.holes), Open: t.open}
	for id, v := range t.ver {
		if v > 0 {
			c.VerRow = append(c.VerRow, int32(id))
			c.Ver = append(c.Ver, v)
		}
	}
	for id, lo := range t.doubt {
		c.DoubtRow = append(c.DoubtRow, int32(id))
		c.DoubtLo = append(c.DoubtLo, uint16(lo))
	}
	for id := range t.absent {
		c.Absent = append(c.Absent, int32(id))
	}
	for id := range t.unsure {
		c.Unsure = append(c.Unsure, int32(id))
	}
	return c
}

// restore is a tenant as the checkpoint had it, open: whatever was
// written after the checkpoint is above what it says, so a row is
// accepted at its saved version or newer until it is read once.
func (c ckptTenant) restore() *tenant {
	t := &tenant{id: int(c.ID), rows: int(c.Rows), nextID: int(c.NextID),
		holes: int(c.Holes), open: true, refresh: true}
	for i, id := range c.VerRow {
		t.setVersion(int(id), int(c.Ver[i]))
	}
	if len(c.DoubtRow) > 0 {
		t.doubt = map[int]int{}
		for i, id := range c.DoubtRow {
			t.doubt[int(id)] = int(c.DoubtLo[i])
		}
	}
	if len(c.Absent) > 0 {
		t.absent = map[int]bool{}
		for _, id := range c.Absent {
			t.absent[int(id)] = true
		}
	}
	if len(c.Unsure) > 0 {
		t.unsure = map[int]bool{}
		for _, id := range c.Unsure {
			t.unsure[int(id)] = true
		}
	}
	return t
}

// checkpoint writes the run's state to path, whole or not at all. One
// at a time: the window's and the ticker's may coincide.
func (a *agentRun) checkpoint(path string) error {
	a.ckMu.Lock()
	defer a.ckMu.Unlock()
	a.mu.Lock()
	tenants := append([]*tenant(nil), a.tenants...)
	c := ckpt{Taken: time.Now(), Count: a.count, Next: len(a.tenants)}
	a.mu.Unlock()
	c.Tenants = make([]ckptTenant, 0, len(tenants))
	for _, t := range tenants {
		if t != nil {
			c.Tenants = append(c.Tenants, t.snapshot())
		}
	}
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	if err := gob.NewEncoder(f).Encode(&c); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

// checkpoints writes one every `every` while the run goes on, and
// says so once if it cannot.
func (a *agentRun) checkpoints(path string, every time.Duration) {
	var said bool
	for {
		time.Sleep(every)
		if err := a.checkpoint(path); err != nil && !said {
			fmt.Fprintln(os.Stderr, "agents: checkpoint:", err)
			said = true
		}
	}
}

// resume loads the checkpoint: the tenants it lists are the run's,
// idle, open to newer versions. Tenants onboarded after it was taken
// are found on the server and adopted (adopt).
func (a *agentRun) resume(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var c ckpt
	if err := gob.NewDecoder(f).Decode(&c); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tenants = make([]*tenant, c.Next)
	for _, ct := range c.Tenants {
		if int(ct.ID) < c.Next {
			a.tenants[ct.ID] = ct.restore()
		}
	}
	for id, t := range a.tenants {
		if t != nil {
			a.idle = append(a.idle, id)
		}
	}
	a.count = c.Count
	a.next = c.Next
	fmt.Fprintf(os.Stderr, "agents: resumed %d tenants from a checkpoint of %s\n",
		c.Count, c.Taken.UTC().Format(time.RFC3339))
	return nil
}

// adopt takes in the tenants the server has beyond the checkpoint's
// count: those onboarded after it was taken. Each is looked up for
// what it holds; one whose database is not there ends the search.
// A tenant with every seeded row is as good as a fresh one; a partial
// load (the crash caught it) is kept with its rows unknown, so that
// its reads are held only to the count.
func (a *agentRun) adopt() {
	const par = 32
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		found   = map[int]*tenant{}
		stopped = false
		stopAt  = -1
	)
	start := a.next
	next := start
	sem := make(chan struct{}, par)
	for {
		mu.Lock()
		done := stopped && next > stopAt+par
		id := next
		next++
		mu.Unlock()
		if done {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			t, there, err := a.lookup(id)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				fmt.Fprintf(os.Stderr, "agents: adopting tenant %d: %v\n", id, err)
			}
			if !there || err != nil {
				if !stopped || id < stopAt {
					stopped, stopAt = true, id
				}
				return
			}
			found[id] = t
		}()
	}
	wg.Wait()
	// Tenants are onboarded in order, so the first one missing ends
	// the run's tenants; a later one found is a stray and not adopted.
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for id := start; id < stopAt; id++ {
		t, ok := found[id]
		if !ok {
			break
		}
		for len(a.tenants) <= id {
			a.tenants = append(a.tenants, nil)
		}
		a.tenants[id] = t
		a.idle = append(a.idle, id)
		a.count++
		a.next = id + 1
		n++
	}
	if n > 0 {
		fmt.Fprintf(os.Stderr, "agents: adopted %d tenants onboarded after the "+
			"checkpoint; %d tenants in all\n", n, a.count)
	}
}

// warm reads one row from every tenant before a resumed run starts
// its sessions: a server just restarted has every tenant's tables to
// open and its levels to settle (at 295K it ran 500 compactions a
// second for four minutes), and sessions let loose on it then saw
// only timeouts. How long this takes is the restart as a tenant sees
// it, printed with the latencies; none of it is in a window.
func (a *agentRun) warm() {
	const par = 64
	a.mu.Lock()
	ids := append([]int(nil), a.idle...)
	a.mu.Unlock()
	start := time.Now()
	var (
		mu     sync.Mutex
		lat    []float64
		failed int
		wg     sync.WaitGroup
		sem    = make(chan struct{}, par)
	)
	for _, id := range ids {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			t0 := time.Now()
			err := func() error {
				s, err := openRetry(a.target(id), time.Now().Add(onboardWait))
				if err != nil {
					return err
				}
				defer s.close()
				_, err = s.query("SELECT v FROM t WHERE id = 1")
				return err
			}()
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed++
				if failed <= 3 {
					fmt.Fprintf(os.Stderr, "agents: warming tenant %d: %v\n", id, err)
				}
				return
			}
			lat = append(lat, ms(time.Since(t0)))
		}()
	}
	wg.Wait()
	sort.Float64s(lat)
	fmt.Fprintf(os.Stderr, "agents: warmed %d tenants in %s: a point read each, "+
		"p50 %.2f p95 %.2f max %.2f ms; %d failed\n", len(lat),
		time.Since(start).Round(time.Second), pct(lat, 0.5), pct(lat, 0.95),
		pct(lat, 1), failed)
}

// lookup asks the server what tenant id holds: its row count and its
// highest id. there is false when the database is not there.
func (a *agentRun) lookup(id int) (t *tenant, there bool, err error) {
	s, err := open(a.target(id), 2*time.Second)
	if err != nil {
		var pe *pgError
		if errors.As(err, &pe) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer s.close()
	count, top, err := measureRows(s)
	if err != nil {
		var pe *pgError
		if errors.As(err, &pe) {
			// The database is there and the table is not: the crash
			// caught the onboarding before its CREATE TABLE.
			return nil, false, nil
		}
		return nil, false, err
	}
	t = &tenant{id: id, open: true}
	t.adoptRows(count, top, a.rows)
	return t, true, nil
}

// measureRows is the table's row count and its highest id.
func measureRows(s *session) (count, top int, err error) {
	r, err := s.query("SELECT count(*) FROM t")
	if err != nil {
		return 0, 0, err
	}
	v, err := oneValue(r, "count")
	if err != nil {
		return 0, 0, err
	}
	if count, err = strconv.Atoi(v); err != nil {
		return 0, 0, fmt.Errorf("count: %q", v)
	}
	r, err = s.query("SELECT id FROM t ORDER BY id DESC LIMIT 1")
	if err != nil {
		return 0, 0, err
	}
	if len(r.rows) == 0 {
		return count, 0, nil
	}
	v, err = oneValue(r, "top id")
	if err != nil {
		return 0, 0, err
	}
	if top, err = strconv.Atoi(v); err != nil {
		return 0, 0, fmt.Errorf("top id: %q", v)
	}
	return count, top, nil
}

// adoptRows sets what the table holds from a measurement: the seeded
// rows when they are all there, else nothing certain but the count.
func (t *tenant) adoptRows(count, top, seeded int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextID = top + 1
	t.holes = top - count
	t.absent, t.unsure = nil, nil
	t.unknown = false
	if count >= seeded && top >= seeded {
		t.rows = seeded
	} else {
		t.rows = 0
		t.unknown = true
	}
}
