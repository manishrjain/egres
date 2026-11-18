// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseMix(t *testing.T) {
	m, err := parseMix("point=60,range=15,update=15,insert=8,count=2")
	if err != nil {
		t.Fatal(err)
	}
	if m != (mix{60, 15, 15, 8, 2}) {
		t.Fatalf("got %v", m)
	}
	// Kinds left out have no share, and the order does not matter.
	m, err = parseMix("count=50, point=50")
	if err != nil {
		t.Fatal(err)
	}
	if m != (mix{50, 0, 0, 0, 50}) {
		t.Fatalf("got %v", m)
	}
	for _, bad := range []string{
		"", "point=60", "point=60,range=50", "point=100,point=0",
		"select=100", "point", "point=x", "point=-10,range=110",
	} {
		if _, err := parseMix(bad); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
}

func TestMixPick(t *testing.T) {
	m := mix{60, 15, 0, 23, 2}
	var got mix
	for r := 0; r < 100; r++ {
		got[m.pick(r)]++
	}
	if got != m {
		t.Fatalf("over 0..99 picked %v, want %v", got, m)
	}
}

func TestPct(t *testing.T) {
	var a []float64
	for i := 1; i <= 100; i++ {
		a = append(a, float64(i))
	}
	rand.New(rand.NewSource(1)).Shuffle(len(a), func(i, j int) {
		a[i], a[j] = a[j], a[i]
	})
	s := sorted(a)
	if a[0] == 1 && a[99] == 100 {
		t.Fatal("shuffle did nothing")
	}
	for _, c := range []struct{ p, want float64 }{
		{0, 1}, {0.5, 50}, {0.9, 90}, {0.99, 99}, {1, 100},
	} {
		if got := pct(s, c.p); got != c.want {
			t.Errorf("pct(%v) = %v, want %v", c.p, got, c.want)
		}
	}
	if pct(nil, 0.5) != 0 || pctCell(nil, 0.5) != "" {
		t.Error("an empty set should give 0 and an empty cell")
	}
	if got := pctCell(s, 0.99); got != "99.00" {
		t.Errorf("pctCell = %q", got)
	}
}

func TestMd5Rows(t *testing.T) {
	// From Postgres: SELECT md5(1::text), md5(10::text), md5(1000::text).
	for g, want := range map[int]string{
		1:    "c4ca4238a0b923820dcc509a6f75849b",
		10:   "d3d9446802a44259755d38e6d163e820",
		1000: "a9b7ba70783b617e9998dc4dd82eb3c5",
	} {
		if got := md5Text(g); got != want {
			t.Errorf("md5Text(%d) = %s, want %s", g, got, want)
		}
	}
	want := "INSERT INTO t VALUES (1, 'c4ca4238a0b923820dcc509a6f75849b'), " +
		"(2, 'c81e728d9d4c2f636f067f89cc14862c')"
	if got := insertRows(1, 2); got != want {
		t.Errorf("insertRows(1, 2) =\n%s\nwant\n%s", got, want)
	}
	if n := strings.Count(insertRows(1, 1000), "("); n != 1000 {
		t.Errorf("insertRows(1, 1000) has %d rows", n)
	}
	for rows, want := range map[int][][2]int{
		1:    {{1, 1}},
		1000: {{1, 1000}},
		2500: {{1, 1000}, {1001, 2000}, {2001, 2500}},
	} {
		got := rowBatches(rows)
		if len(got) != len(want) {
			t.Errorf("rowBatches(%d) = %v", rows, got)
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("rowBatches(%d) = %v", rows, got)
			}
		}
	}
}

func TestParseSteps(t *testing.T) {
	s, err := parseSteps("1000, 2000,5000")
	if err != nil {
		t.Fatal(err)
	}
	if len(s) != 3 || s[0] != 1000 || s[1] != 2000 || s[2] != 5000 {
		t.Fatalf("got %v", s)
	}
	for _, bad := range []string{"", "0", "1000,1000", "2000,1000", "1k"} {
		if _, err := parseSteps(bad); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
}

func TestArrivalDue(t *testing.T) {
	begin := time.Unix(1000, 0)
	for _, c := range []struct {
		k    int
		rate float64
		want time.Duration
	}{
		{0, 30, 0}, {30, 30, time.Second}, {45, 30, 1500 * time.Millisecond},
		{3, 0.5, 6 * time.Second},
	} {
		if got := arrivalDue(begin, c.k, c.rate).Sub(begin); got != c.want {
			t.Errorf("arrival %d at %v/s: due at %v, want %v", c.k, c.rate,
				got, c.want)
		}
	}
}

func TestStatement(t *testing.T) {
	text := strings.Repeat("a", 32)
	m7, m1001 := md5Text(7), md5Text(1001)
	rows := func(cells ...[]string) *result { return &result{rows: cells} }
	tag := func(s string) *result { return &result{tag: s} }
	for _, c := range []struct {
		kind      int
		want      string
		good, bad *result
	}{
		{kPoint, "SELECT v FROM t WHERE id = 7",
			rows([]string{m7}), rows([]string{md5Text(8)})},
		{kRange, "SELECT id, v FROM t WHERE id BETWEEN 7 AND 16",
			rows([]string{"7", m7}, []string{"8", md5Text(8)},
				[]string{"9", md5Text(9)}, []string{"10", md5Text(10)}),
			rows([]string{"7", m7}, []string{"8", md5Text(8)},
				[]string{"9", md5Text(9)})},
		{kUpdate, "UPDATE t SET v = '" + m7 + ":" + text + "' WHERE id = 7",
			tag("UPDATE 1"), tag("UPDATE 0")},
		{kInsert, "INSERT INTO t VALUES (1001, '" + m1001 + "')",
			tag("INSERT 0 1"), tag("INSERT 0 0")},
		{kCount, "SELECT count(*) FROM t",
			rows([]string{"1000"}), rows([]string{"1001"})},
	} {
		// 10 rows loaded, inserts up to 1000 so far: a range past 10
		// may be short, a count is 10..1000.
		sql, check := statement(c.kind, 7, 1001, 10, text)
		if sql != c.want {
			t.Errorf("%s: %q, want %q", kindNames[c.kind], sql, c.want)
		}
		if err := check(c.good); err != nil {
			t.Errorf("%s: good reply rejected: %v", kindNames[c.kind], err)
		}
		if err := check(c.bad); err == nil {
			t.Errorf("%s: bad reply accepted", kindNames[c.kind])
		}
	}
	// An updated row still passes a point read: its md5 leads.
	if err := pointCheck(7)(rows([]string{m7 + ":" + text})); err != nil {
		t.Errorf("updated row: %v", err)
	}
	// A range over seeded rows must have every one of them.
	if err := rangeCheck(7, 16, 1000)(rows([]string{"7", m7})); err == nil {
		t.Error("short range over seeded rows accepted")
	}
	if s := randText(rand.New(rand.NewSource(1))); len(s) != 32 {
		t.Errorf("randText gave %q", s)
	}
}

// A DataRow's columns come out as the server sent them, NULL as "".
func TestDataRow(t *testing.T) {
	body := []byte{0, 3}
	for _, col := range []string{"7", "", "abc"} {
		if col == "" {
			body = append(body, 0xff, 0xff, 0xff, 0xff)
			continue
		}
		body = append(body, 0, 0, 0, byte(len(col)))
		body = append(body, col...)
	}
	got := dataRow(body)
	if len(got) != 3 || got[0] != "7" || got[1] != "" || got[2] != "abc" {
		t.Fatalf("dataRow: %q", got)
	}
}

// A row has as many cells as the header, on both backends, with no
// samples in it at all.
func TestRowShape(t *testing.T) {
	want := strings.Count(header, ",")
	shared := &agentRun{running: map[*sessionRun]bool{}}
	vmRun := &agentRun{running: map[*sessionRun]bool{},
		p: &platform{run: t.TempDir()}}
	for name, a := range map[string]*agentRun{"shared": shared, "vm": vmRun} {
		row := a.window(0).csv
		if got := strings.Count(row, ","); got != want {
			t.Errorf("%s: %d commas in %q, header has %d", name, got, row, want)
		}
	}
}

// The stop rule reads a window's figures, and a bound of 0 is off.
func TestStopRule(t *testing.T) {
	r := stopRule{readP99: 50, arriveP99: 5000, errPct: 1}
	ok := windowRow{readP99: 49, arriveP99: 4999, errPct: 0.9}
	if why := r.breached(ok); why != "" {
		t.Errorf("fit: %s", why)
	}
	for _, w := range []windowRow{{readP99: 51}, {arriveP99: 5001}, {errPct: 1.1}} {
		if r.breached(w) == "" {
			t.Errorf("%+v passed", w)
		}
	}
	if why := (stopRule{}).breached(windowRow{readP99: 1e9, errPct: 100}); why != "" {
		t.Errorf("rule off: %s", why)
	}
}

func TestRecordWindow(t *testing.T) {
	a := &agentRun{running: map[*sessionRun]bool{}}
	now := time.Now()
	a.winStart = now
	a.record(0, kPoint, now.Add(-time.Second), 1, nil, true, false, 0) // before
	a.record(0, kPoint, now, 2, nil, true, true, 50*time.Millisecond)
	a.record(0, kCount, now, 3, errors.New("x"), false, false, 0)
	a.winEnd = now.Add(time.Second)
	a.record(0, kPoint, now.Add(time.Second), 4, nil, false, false, 0) // after
	if a.ok != 1 || a.errs != 1 || a.woke != 1 || len(a.lat[kPoint]) != 1 ||
		a.lat[kPoint][0] != 2 || a.wakeLat[0] != 50 {
		t.Fatalf("ok %d errs %d woke %d lat %v wake %v", a.ok, a.errs, a.woke,
			a.lat, a.wakeLat)
	}
}

func TestClock(t *testing.T) {
	c := newClock()
	start := time.Now()
	var wg sync.WaitGroup
	// Out of order, so a later wait has to give way to an earlier one.
	for _, d := range []time.Duration{30, 10, 20, 5, 0, -5} {
		due := start.Add(d * time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.wait(due)
			if late := time.Since(due); late < 0 || late > 20*time.Millisecond {
				t.Errorf("wait for +%v returned %v late", d, late)
			}
		}()
	}
	wg.Wait()
}

func msg(typ byte, body string) []byte {
	out := []byte{typ, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(out[1:], uint32(4+len(body)))
	return append(out, body...)
}

// A statement's error leaves the connection where the next statement's
// reply starts.
func TestUntilReadyAfterError(t *testing.T) {
	var b bytes.Buffer
	b.Write(msg('E', "SERROR\x00C42P04\x00Mdatabase exists\x00\x00"))
	b.Write(msg('Z', "I"))
	b.Write(msg('T', "whatever"))
	b.Write(msg('D', "\x00\x01\x00\x00\x00\x03row"))
	b.Write(msg('C', "SELECT 1\x00"))
	b.Write(msg('Z', "I"))
	r := bufio.NewReader(&b)
	res, err := untilReady(r)
	var pe *pgError
	if res != nil || !errors.As(err, &pe) || pe.code != "42P04" {
		t.Fatalf("first reply: %v, err %v", res, err)
	}
	res, err = untilReady(r)
	if err != nil || len(res.rows) != 1 || res.rows[0][0] != "row" ||
		res.tag != "SELECT 1" {
		t.Fatalf("second reply: %+v, err %v", res, err)
	}
	// At startup the server closes after its error: the error, not EOF.
	b.Reset()
	b.Write(msg('E', "SFATAL\x00C57P03\x00Mstarting up\x00\x00"))
	if _, err := untilReady(bufio.NewReader(&b)); !errors.As(err, &pe) ||
		pe.code != "57P03" {
		t.Fatalf("startup: %v", err)
	}
}

func TestVMConfig(t *testing.T) {
	p := &platform{kernel: "/a/vmlinux", rootfs: "/a/rootfs.ext4", memMiB: 128}
	raw, err := p.config(300, "/r/vm300")
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	// i+256 = 556 = 2<<8 | 44.
	for _, want := range []string{
		`"guest_mac": "06:00:0a:c8:02:2c"`, `"host_dev_name": "fctap300"`,
		"ip=10.200.2.44::10.200.0.1:255.255.0.0::eth0:off", "init=/fc-init",
		`"cache_type": "Writeback"`, `"path_on_host": "/r/vm300/data.ext4"`,
		`"mem_size_mib": 128`, `"boot-source"`, `"machine-config"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("config lacks %s:\n%s", want, s)
		}
	}
	if vmAddr(0) != "10.200.1.0:5432" {
		t.Errorf("vmAddr(0) = %s", vmAddr(0))
	}
}
