// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"testing"
)

// A checkpoint comes back as it went: counts, versions, doubts and
// the rows an insert may or may not have made; and a resumed tenant
// takes a newer version than it remembers, but not an older one.
func TestCheckpointRoundTrip(t *testing.T) {
	a := &agentRun{running: map[*sessionRun]bool{}}
	t1 := &tenant{id: 0, rows: 10, nextID: 11}
	t1.setVersion(3, 2)
	t1.doubt = map[int]int{3: 1}
	t2 := &tenant{id: 1, rows: 10, nextID: 13}
	t2.absent = map[int]bool{11: true}
	t2.unsure = map[int]bool{12: true}
	a.tenants = []*tenant{t1, t2, nil} // the third onboarding failed
	a.count = 2
	path := filepath.Join(t.TempDir(), "checkpoint.bin")
	if err := a.checkpoint(path); err != nil {
		t.Fatal(err)
	}
	b := &agentRun{running: map[*sessionRun]bool{}}
	if err := b.resume(path); err != nil {
		t.Fatal(err)
	}
	if b.count != 2 || b.next != 3 || len(b.tenants) != 3 || b.tenants[2] != nil {
		t.Fatalf("count %d next %d tenants %d", b.count, b.next, len(b.tenants))
	}
	r1 := b.tenants[0]
	if r1.rows != 10 || r1.nextID != 11 || r1.version(3) != 2 ||
		r1.doubt[3] != 1 || !r1.open || !r1.refresh {
		t.Fatalf("tenant 0 came back as %+v", r1)
	}
	r2 := b.tenants[1]
	if !r2.absent[11] || !r2.unsure[12] || r2.nextID != 13 {
		t.Fatalf("tenant 1 came back as %+v", r2)
	}
	// Row 3 was at 1 or 2; after the resume 2, 3 and beyond are taken
	// and settle it, 0 is not.
	if err := r1.accept(3, value(3, 5)); err != nil {
		t.Fatal(err)
	}
	if r1.version(3) != 5 || len(r1.doubt) != 0 {
		t.Fatalf("version %d doubt %v after a newer read", r1.version(3), r1.doubt)
	}
	if err := r1.accept(3, value(3, 0)); err == nil {
		t.Fatal("an older version than the checkpoint's was accepted")
	}
	// A measured table: 13 rows up to id 15 leaves 2 holes, and the
	// count check wants 13.
	r2.adoptRows(13, 15, 10)
	if r2.nextID != 16 || r2.holes != 2 || r2.rows != 10 || r2.unknown {
		t.Fatalf("after measuring: %+v", r2)
	}
	if err := r2.countCheck()(&result{rows: [][]string{{"13"}}}); err != nil {
		t.Fatal(err)
	}
	// A load the crash caught half way: nothing certain but the count.
	r2.adoptRows(4, 4, 10)
	if !r2.unknown || r2.exists(1) {
		t.Fatalf("a partial load is not unknown: %+v", r2)
	}
}
