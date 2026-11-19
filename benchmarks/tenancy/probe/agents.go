// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

// agents is the agentic workload, on both platforms. Tenants onboard
// over time, each creating its database and loading its table through
// SQL, as an agent setting up its own store would. A share of the
// tenants (-active) are in a session at any moment, each sending a mix
// of reads and writes on one connection it keeps; the rest are idle.
//
// -backend shared: every tenant is a database, tenant_<i>, on one
// server (Egres), created on its "egres" database. The server must
// start with none of them.
//
// -backend vm: every tenant is a Firecracker VM, made at its onboarding
// from a blank data image (Postgres initialised, no table) under
// -run/vm<i>. The taps fctap<i> must exist (netns_setup.sh) and this
// must run where they are (in_netns.sh). Here the probe is also the
// platform's control plane (vm.go): it puts idle VMs to sleep when the
// test's cgroup holds more than -budget-mib (all but the clean,
// unmapped file cache), least recently used first, holds onboardings
// until it is under, and a session for a sleeping tenant wakes it.
//
// The run grows by onboardings, open loop at -onboard a second, to each
// count in -steps; there it pauses onboardings for -window, sessions still
// running, and prints one CSV row (see header). Every latency is from
// when its statement was due, so a stalled server cannot hide its tail
// by slowing the sender. A session's first statement is also reported
// on its own (first_*): the time to a result from a tenant that was
// idle, which on the micro-VM side includes waking it.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	kPoint = iota
	kRange
	kUpdate
	kInsert
	kCount
	numKinds
)

var kindNames = [numKinds]string{"point", "range", "update", "insert", "count"}

const header = "tenants,awake,held_mib,disk_mib,read_mib_s,write_mib_s," +
	"onboard_p50_ms,onboard_p95_ms,onboard_p99_ms," +
	"first_p50_ms,first_p95_ms,first_p99_ms," +
	"wake_p50_ms,wake_p95_ms,wake_p99_ms,woke_pct,sleeps_per_s,wakes_per_s," +
	"point_p50_ms,point_p95_ms,point_p99_ms," +
	"range_p50_ms,range_p95_ms,range_p99_ms," +
	"update_p50_ms,update_p95_ms,update_p99_ms," +
	"insert_p50_ms,insert_p95_ms,insert_p99_ms," +
	"count_p50_ms,count_p95_ms,count_p99_ms,ok,errors"

// Onboardings under way at once. Past this an onboarding waits its turn, and
// the wait is in its latency.
const maxOnboarding = 32

// mix is each kind's share of the statements, in percent.
type mix [numKinds]int

func parseMix(s string) (mix, error) {
	var m mix
	seen := [numKinds]bool{}
	sum := 0
	for _, part := range strings.Split(s, ",") {
		name, val, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return m, fmt.Errorf("mix: %q is not kind=percent", part)
		}
		k := -1
		for i, n := range kindNames {
			if n == name {
				k = i
			}
		}
		if k < 0 {
			return m, fmt.Errorf("mix: no statement kind %q", name)
		}
		if seen[k] {
			return m, fmt.Errorf("mix: %s given twice", name)
		}
		seen[k] = true
		n, err := strconv.Atoi(val)
		if err != nil || n < 0 || n > 100 {
			return m, fmt.Errorf("mix: %s=%s is not a percentage", name, val)
		}
		m[k] = n
		sum += n
	}
	if sum != 100 {
		return m, fmt.Errorf("mix: adds up to %d, not 100", sum)
	}
	return m, nil
}

// pick is the kind for r in 0..99.
func (m mix) pick(r int) int {
	for k, n := range m {
		if r < n {
			return k
		}
		r -= n
	}
	return numKinds - 1 // not reached for a mix that adds up to 100
}

// parseSteps reads the tenant counts to measure at, which must rise.
func parseSteps(s string) ([]int, error) {
	var steps []int
	for _, part := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("steps: %q is not a tenant count", part)
		}
		if len(steps) > 0 && n <= steps[len(steps)-1] {
			return nil, fmt.Errorf("steps: %d does not follow %d", n,
				steps[len(steps)-1])
		}
		steps = append(steps, n)
	}
	return steps, nil
}

// onboardingDue is when the k-th onboarding of a growth begun at begin is due.
func onboardingDue(begin time.Time, k int, perSec float64) time.Time {
	return begin.Add(time.Duration(float64(k) * float64(time.Second) / perSec))
}

// tenant is what the probe knows of a tenant's table, so that a read
// can be held to it: which rows exist and the version each updated
// row is at. Only the tenant's one session at a time touches it.
type tenant struct {
	id int
	// Rows 1..rows were loaded; the id the next insert uses.
	rows, nextID int
	// ver[id] is the version of row id's last attempted update (0:
	// never updated, the row holds md5(id)); nil until the first.
	ver []uint16
	// Rows whose last update(s) may not have committed, the reply
	// lost with the connection: the version known to be there. The
	// row is then at any version from that to ver[id], and a read
	// settles it.
	doubt map[int]int
	// Inserts a server error refused (the row is not there) and
	// inserts whose reply was lost (it may be).
	absent, unsure map[int]bool
}

func (t *tenant) version(id int) int {
	if id < len(t.ver) {
		return int(t.ver[id])
	}
	return 0
}

func (t *tenant) setVersion(id, k int) {
	if t.ver == nil {
		t.ver = make([]uint16, t.rows+1)
	}
	for id >= len(t.ver) {
		t.ver = append(t.ver, 0)
	}
	t.ver[id] = uint16(k)
}

// accept holds v to what row id may be at: a version from the one
// known to be there to the last one written. Seeing the last one
// settles the row; seeing an older one does not, since a write whose
// reply was lost may still be running on the abandoned connection.
func (t *tenant) accept(id int, v string) error {
	k := parseValue(id, v)
	hi := t.version(id)
	lo, doubted := t.doubt[id]
	if !doubted {
		lo = hi
	}
	if k < lo || k > hi {
		want := value(id, hi)
		if doubted {
			want = value(id, lo) + ".." + strconv.Itoa(hi)
		}
		return fmt.Errorf("id %d: got %q, want %s", id, short(v), want)
	}
	if doubted && k == hi {
		delete(t.doubt, id)
	}
	return nil
}

// exists says whether row id is certainly there.
func (t *tenant) exists(id int) bool {
	return id <= t.rows || (id < t.nextID && !t.absent[id] && !t.unsure[id])
}

func (t *tenant) pointCheck(id int) checker {
	return func(r *result) error {
		v, err := oneValue(r, fmt.Sprintf("point id %d", id))
		if err != nil {
			return err
		}
		return t.accept(id, v)
	}
}

// rangeCheck: rows (id, v) for from..to, every id once, each v its
// row's, and every row that exists among them.
func (t *tenant) rangeCheck(from, to int) checker {
	return func(r *result) error {
		seen := map[int]bool{}
		for _, row := range r.rows {
			if len(row) < 2 {
				return fmt.Errorf("range %d..%d: a row of %d columns", from, to,
					len(row))
			}
			id, err := strconv.Atoi(row[0])
			if err != nil || id < from || id > to {
				return fmt.Errorf("range %d..%d: id %q outside it", from, to, row[0])
			}
			if seen[id] {
				return fmt.Errorf("range %d..%d: id %d twice", from, to, id)
			}
			seen[id] = true
			if err := t.accept(id, row[1]); err != nil {
				return fmt.Errorf("range %d..%d: %w", from, to, err)
			}
			delete(t.unsure, id)
		}
		for id := from; id <= to; id++ {
			if t.exists(id) && !seen[id] {
				return fmt.Errorf("range %d..%d: id %d missing (%d rows)", from, to,
					id, len(r.rows))
			}
		}
		return nil
	}
}

// countCheck: the rows that certainly exist, up to those that may.
func (t *tenant) countCheck() checker {
	hi := t.nextID - 1 - len(t.absent)
	return countCheck(hi-len(t.unsure), hi)
}

// statement is the SQL for one statement of kind, the check its reply
// is held to, and done, to be told how the statement ended: ok, a
// server error (nothing committed), or lost (the reply did not come
// back, so a write may or may not have committed). r is a random id
// in 1..rows. An update writes the row's next version, an insert the
// next id with the value the seed would have given it.
func (t *tenant) statement(kind, r int) (sql string, check checker,
	done func(err error, lost bool)) {
	switch kind {
	case kPoint:
		return fmt.Sprintf("SELECT v FROM t WHERE id = %d", r), t.pointCheck(r),
			nil
	case kRange:
		return fmt.Sprintf("SELECT id, v FROM t WHERE id BETWEEN %d AND %d",
			r, r+9), t.rangeCheck(r, r+9), nil
	case kUpdate:
		k := t.version(r) + 1
		return fmt.Sprintf("UPDATE t SET v = '%s' WHERE id = %d", value(r, k), r),
			tagCheck("UPDATE 1"), func(err error, lost bool) {
				switch {
				case err == nil:
					t.setVersion(r, k)
				case lost:
					if _, in := t.doubt[r]; !in {
						if t.doubt == nil {
							t.doubt = map[int]int{}
						}
						t.doubt[r] = t.version(r)
					}
					t.setVersion(r, k)
				}
			}
	case kInsert:
		n := t.nextID
		t.nextID++
		return fmt.Sprintf("INSERT INTO t VALUES (%d, '%s')", n, value(n, 0)),
			tagCheck("INSERT 0 1"), func(err error, lost bool) {
				switch {
				case err == nil:
				case lost:
					if t.unsure == nil {
						t.unsure = map[int]bool{}
					}
					t.unsure[n] = true
				default:
					if t.absent == nil {
						t.absent = map[int]bool{}
					}
					t.absent[n] = true
				}
			}
	default:
		return "SELECT count(*) FROM t", t.countCheck(), nil
	}
}

type sessionRun struct {
	t *tenant
	// Every statement of this session due before this time (unix ns)
	// has finished: what a window's end waits for.
	mark atomic.Int64
}

type agentRun struct {
	addr    string    // shared
	p       *platform // vm; nil for shared
	pid     int       // shared: the server's, for its PSS
	disk    string    // the drive WORK is on, for its traffic; "" = none
	clk     *clock
	rows    int
	mix     mix
	rate    float64
	sessLen time.Duration
	active  float64
	begin   time.Time
	next    int // the next tenant to onboard; the main goroutine's

	mu      sync.Mutex // guards what follows
	rng     *rand.Rand
	tenants []*tenant // by id; nil until it has onboarded
	count   int
	idle    []int // tenants with no session
	running map[*sessionRun]bool
	trace   *bufio.Writer
	// The window under way: statements due in [winStart, winEnd) are
	// counted, none when winStart is zero, and winEnd is zero until
	// the window ends.
	winStart, winEnd time.Time
	lat              [numKinds][]float64
	firstLat         []float64 // each session's first statement
	wakeLat          []float64
	woke, ok, errs   int
	lastErr          error
	// Since the last row.
	onboardLat []float64
}

func (a *agentRun) target(id int) string {
	if a.p != nil {
		return vmAddr(id)
	}
	return fmt.Sprintf("%s/tenant_%d", a.addr, id)
}

func (a *agentRun) traceLine(due time.Time, id int, kind string, lms float64,
	woke bool, err error) {
	if a.trace != nil {
		fmt.Fprintf(a.trace, "%.1f,%d,%s,%.3f,%t,%t\n",
			ms(due.Sub(a.begin)), id, kind, lms, woke, err == nil)
	}
}

func (a *agentRun) record(id, kind int, due time.Time, lms float64, err error,
	first, woke bool, wake time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.traceLine(due, id, kindNames[kind], lms, woke, err)
	if a.winStart.IsZero() || due.Before(a.winStart) ||
		(!a.winEnd.IsZero() && !due.Before(a.winEnd)) {
		return
	}
	if err != nil {
		a.errs++
		a.lastErr = err
	} else {
		a.ok++
		a.lat[kind] = append(a.lat[kind], lms)
		if first {
			a.firstLat = append(a.firstLat, lms)
		}
	}
	if woke {
		a.woke++
		a.wakeLat = append(a.wakeLat, ms(wake))
	}
}

// schedule keeps about active × tenants in a session: whenever there
// are fewer, it starts one for a tenant picked at random from the
// idle ones.
func (a *agentRun) schedule() {
	for {
		time.Sleep(10 * time.Millisecond)
		a.mu.Lock()
		want := int(math.Round(a.active * float64(a.count)))
		for len(a.running) < want && len(a.idle) > 0 {
			k := a.rng.Intn(len(a.idle))
			id := a.idle[k]
			a.idle[k] = a.idle[len(a.idle)-1]
			a.idle = a.idle[:len(a.idle)-1]
			s := &sessionRun{t: a.tenants[id]}
			start := time.Now()
			s.mark.Store(start.UnixNano())
			a.running[s] = true
			go a.session(s, start, a.rng.Int63())
		}
		a.mu.Unlock()
	}
}

func (a *agentRun) endSession(s *sessionRun) {
	s.mark.Store(math.MaxInt64)
	a.mu.Lock()
	delete(a.running, s)
	a.idle = append(a.idle, s.t.id)
	a.mu.Unlock()
}

// session is one tenant's session: statements open loop at -rate for
// -session on one connection. A VM is held awake for all of it; a
// sleeping one is woken first, inside the first statement's latency.
func (a *agentRun) session(s *sessionRun, start time.Time, seed int64) {
	defer a.endSession(s)
	rng := rand.New(rand.NewSource(seed))
	t := s.t
	gap := time.Duration(float64(time.Second) / a.rate)
	var held error // why the tenant could not be had, if it could not
	woke := false
	var took time.Duration
	if a.p != nil {
		v := a.p.get(t.id)
		woke, took, held = a.p.acquire(v)
		if held == nil {
			defer a.p.release(v)
		}
	}
	var conn *session
	defer func() {
		if conn != nil {
			conn.close()
		}
	}()
	for k := 0; time.Duration(k)*gap < a.sessLen; k++ {
		due := start.Add(time.Duration(k) * gap)
		a.clk.wait(due)
		kind := a.mix.pick(rng.Intn(100))
		sql, check, done := t.statement(kind, 1+rng.Intn(a.rows))
		err := held
		if err == nil && conn == nil {
			conn, err = openRetry(a.target(t.id), time.Now().Add(convTimeout))
		}
		if err == nil {
			var res *result
			res, err = conn.query(sql)
			var pe *pgError
			lost := err != nil && !errors.As(err, &pe)
			if lost {
				conn.close()
				conn = nil
			}
			if err == nil {
				if err = check(res); err != nil {
					err = fmt.Errorf("%s: %w", kindNames[kind], err)
				}
			}
			if done != nil {
				done(err, lost)
			}
		} else if done != nil {
			done(err, false)
		}
		a.record(t.id, kind, due, msSince(due), err, k == 0, woke && k == 0,
			took)
		s.mark.Store(due.Add(gap).UnixNano())
	}
}

// onboard brings tenant i in: its database (or VM), its table, its
// rows, and then the first row of a point read, which is where its
// latency ends.
func (a *agentRun) onboard(i int, due time.Time) error {
	if a.p != nil {
		v, err := a.p.create(i)
		if err != nil {
			return err
		}
		defer a.p.release(v)
	} else {
		err := run(a.addr+"/egres", fmt.Sprintf("CREATE DATABASE tenant_%d", i),
			2*time.Second, nil)
		if err != nil {
			return fmt.Errorf("tenant_%d: %w", i, err)
		}
	}
	err := func() error {
		s, err := openRetry(a.target(i), time.Now().Add(convTimeout))
		if err != nil {
			return err
		}
		defer s.close()
		if err := loadRows(s, a.rows); err != nil {
			return err
		}
		if err := s.check(query, pointCheck(1)); err != nil {
			return fmt.Errorf("after the load: %w", err)
		}
		return nil
	}()
	if err != nil {
		return fmt.Errorf("tenant %d: %w", i, err)
	}
	lms := msSince(due)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.traceLine(due, i, "onboard", lms, false, nil)
	a.onboardLat = append(a.onboardLat, lms)
	for len(a.tenants) <= i {
		a.tenants = append(a.tenants, nil)
	}
	a.tenants[i] = &tenant{id: i, rows: a.rows, nextID: a.rows + 1}
	a.count++
	a.idle = append(a.idle, i)
	return nil
}

// grow adds tenants, open loop at perSec, until `to` have been sent
// for, and waits for them. It stops short at the host's floors or
// after an onboarding fails, and says why. A failed onboarding is a stop,
// not a fault: the platform did not take a tenant in the time
// allowed, and the count it had is measured.
func (a *agentRun) grow(to int, perSec, floorGiB, diskFloorGiB float64) (
	why string) {
	sem := make(chan struct{}, maxOnboarding)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	failure := func() error {
		mu.Lock()
		defer mu.Unlock()
		return firstErr
	}
	begin := time.Now()
	for k := 0; a.next < to; k++ {
		due := onboardingDue(begin, k, perSec)
		waitUntil(due)
		if failure() != nil {
			break
		}
		if avail := memAvailableGiB(); avail < floorGiB {
			why = fmt.Sprintf("host memory available %.1f GiB, under the "+
				"floor of %.0f", avail, floorGiB)
			break
		}
		if a.p != nil {
			if free := diskFreeGiB(a.p.run); free < diskFloorGiB {
				why = fmt.Sprintf("disk free under -run %.1f GiB, under the "+
					"floor of %.0f", free, diskFloorGiB)
				break
			}
		}
		sem <- struct{}{}
		i := a.next
		a.next++
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := a.onboard(i, due); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if err := failure(); err != nil {
		return "the platform did not take an onboarding: " + err.Error()
	}
	return why
}

// A window's row, with the figures the stop rule reads.
type windowRow struct {
	csv string
	// p95 of the reads (point and range), the onboardings' p95, and the
	// share of statements that failed, all in the window; 0 where there
	// were no samples. p95 is the figure reported and stopped on; p99
	// is in the row for a sanity check, since at a few thousand samples
	// it is the worst few dozen queries.
	readP95, onboardP95, errPct float64
}

// window measures for d with onboardings paused and returns the CSV row.
func (a *agentRun) window(d time.Duration) windowRow {
	a.mu.Lock()
	a.winStart, a.winEnd = time.Now(), time.Time{}
	a.lat = [numKinds][]float64{}
	a.firstLat, a.wakeLat = nil, nil
	a.woke, a.ok, a.errs, a.lastErr = 0, 0, 0, nil
	start := a.winStart
	a.mu.Unlock()
	if a.p != nil {
		a.p.measure(true)
	}
	read0, written0 := diskIO(a.disk)
	end := start.Add(d)
	// Shared: the cgroup's figure, as the VM side's; the server's PSS
	// when there is no cgroup limit.
	var heldSum, heldN int64
	for time.Now().Before(end) {
		time.Sleep(min(250*time.Millisecond, time.Until(end)))
		if a.p == nil {
			kb := heldKB()
			if kb < 0 && a.pid > 0 {
				kb = pssKB(a.pid)
			}
			if kb >= 0 {
				heldSum += kb
				heldN++
			}
		}
	}

	a.mu.Lock()
	a.winEnd = time.Now()
	stop := a.winEnd
	read1, written1 := diskIO(a.disk)
	var open []*sessionRun
	for s := range a.running {
		open = append(open, s)
	}
	a.mu.Unlock()
	var awakeAvg, heldMiB float64
	var wakes, sleeps int
	if a.p != nil {
		awakeAvg, heldMiB, wakes, sleeps = a.p.measure(false)
	}
	// A statement due in the window may not have finished yet; the row
	// waits for it, up to what the statement's own timeouts allow.
	limit := stop.Add(wakeTimeout + 2*convTimeout)
	for _, s := range open {
		for s.mark.Load() < stop.UnixNano() && time.Now().Before(limit) {
			time.Sleep(10 * time.Millisecond)
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	secs := max(stop.Sub(start).Seconds(), 1e-3) // -window 0: no NaN
	f0 := func(x float64) string { return fmt.Sprintf("%.0f", x) }
	f2 := func(x float64) string { return fmt.Sprintf("%.2f", x) }
	onboard := sorted(a.onboardLat)
	first := sorted(a.firstLat)
	wake := sorted(a.wakeLat)
	// The drive's traffic over the window, MiB a second; "" without
	// a drive to read.
	ioRate := func(before, after int64) string {
		if a.disk == "" {
			return ""
		}
		return f2(float64(after-before) / 1024 / secs)
	}
	cells := []string{strconv.Itoa(a.count)}
	if a.p != nil {
		woke := 0.0
		if n := a.ok + a.errs; n > 0 {
			woke = 100 * float64(a.woke) / float64(n)
		}
		cells = append(cells, f0(awakeAvg), f0(heldMiB),
			f0(diskUsedMiB(a.p.run)),
			ioRate(read0, read1), ioRate(written0, written1),
			pctCell(onboard, 0.5), pctCell(onboard, 0.95), pctCell(onboard, 0.99),
			pctCell(first, 0.5), pctCell(first, 0.95), pctCell(first, 0.99),
			pctCell(wake, 0.5), pctCell(wake, 0.95), pctCell(wake, 0.99),
			f2(woke),
			f2(float64(sleeps)/secs), f2(float64(wakes)/secs))
	} else {
		held := ""
		if heldN > 0 {
			held = f0(float64(heldSum) / float64(heldN) / 1024)
		}
		cells = append(cells, "", held, "",
			ioRate(read0, read1), ioRate(written0, written1),
			pctCell(onboard, 0.5), pctCell(onboard, 0.95), pctCell(onboard, 0.99),
			pctCell(first, 0.5), pctCell(first, 0.95), pctCell(first, 0.99),
			"", "", "", "", "", "")
	}
	for k := range numKinds {
		l := sorted(a.lat[k])
		cells = append(cells, pctCell(l, 0.5), pctCell(l, 0.95), pctCell(l, 0.99))
	}
	cells = append(cells, strconv.Itoa(a.ok), strconv.Itoa(a.errs))
	row := windowRow{csv: strings.Join(cells, ","),
		readP95: max(pct(sorted(a.lat[kPoint]), 0.95),
			pct(sorted(a.lat[kRange]), 0.95)),
		onboardP95: pct(onboard, 0.95)}
	if n := a.ok + a.errs; n > 0 {
		row.errPct = 100 * float64(a.errs) / float64(n)
	}

	if a.lastErr != nil {
		fmt.Fprintln(os.Stderr, "agents: last error:", a.lastErr)
	}
	a.winStart = time.Time{}
	a.onboardLat = nil
	if a.trace != nil {
		a.trace.Flush()
	}
	return row
}

// stopRule is when a count no longer fits: the reads' p95 over a
// bound, onboardings too slow, or statements failing. A bound of 0 is
// off. Returns why, or "".
type stopRule struct{ readP95, onboardP95, errPct float64 }

func (r stopRule) breached(w windowRow) string {
	switch {
	case r.readP95 > 0 && w.readP95 > r.readP95:
		return fmt.Sprintf("reads' p95 %.1f ms over %.1f", w.readP95, r.readP95)
	case r.onboardP95 > 0 && w.onboardP95 > r.onboardP95:
		return fmt.Sprintf("onboardings' p95 %.0f ms over %.0f", w.onboardP95,
			r.onboardP95)
	case r.errPct > 0 && w.errPct > r.errPct:
		return fmt.Sprintf("%.2f%% of statements failed, over %.1f%%",
			w.errPct, r.errPct)
	}
	return ""
}

func agents(args []string) {
	fs := flag.NewFlagSet("agents", flag.ExitOnError)
	backend := fs.String("backend", "", "shared or vm")
	addr := fs.String("addr", "", "shared: the server, ip:port")
	pid := fs.Int("pid", 0,
		"shared: the server's pid, for held_mib when there is no cgroup limit")
	runDir := fs.String("run", "", "vm: directory for the VMs' vm<i>/")
	fc := fs.String("fc", "", "vm: the firecracker binary")
	kernel := fs.String("kernel", "", "vm: the guest kernel")
	rootfs := fs.String("rootfs", "", "vm: the guest's root image")
	blank := fs.String("blank", "",
		"vm: the blank data image, initdb done and no table")
	memMiB := fs.Int("mem-mib", 128, "vm: guest memory, MiB")
	cpus := fs.String("cpus", "",
		"vm: the CPU list the VMs run on (taskset -c); empty = any")
	wakeBy := fs.String("wake", "restore", "vm: wake by restore or boot")
	budgetMiB := fs.Int64("budget-mib", 0,
		"vm: what the cgroup may hold, MiB, all but the clean unmapped file "+
			"cache (the awake VMs' PSS without a cgroup limit); 0 = no budget")
	disk := fs.String("disk", "",
		"the drive WORK is on (nvme0n1), for read_mib_s and write_mib_s")
	floorGiB := fs.Float64("floor-gib", 16,
		"stop onboardings when the host's MemAvailable is under this")
	diskFloorGiB := fs.Float64("disk-floor-gib", 5,
		"vm: stop onboardings when the disk under -run has less free")
	stepsFlag := fs.String("steps", "1000,2000,5000,10000,20000,50000,100000",
		"tenant counts to measure at; onboardings stop at the last")
	onboard := fs.Float64("onboard", 30, "onboardings per second while growing")
	window := fs.Duration("window", 2*time.Minute,
		"measured time at each step, onboardings paused")
	active := fs.Float64("active", 0.10, "share of tenants in a session at a time")
	sessLen := fs.Duration("session", 2*time.Minute, "session length")
	rate := fs.Float64("rate", 1, "statements per second per session")
	rows := fs.Int("rows", 1000, "rows loaded at onboarding; ids are 1..rows")
	mixFlag := fs.String("mix", "point=60,range=15,update=15,insert=8,count=2",
		"statement mix, percent")
	traceFile := fs.String("trace", "",
		"file for one line per statement: due_ms,tenant,kind,ms,woke,ok"+
			" (onboardings too, kind onboard)")
	seed := fs.Int64("seed", 1, "seed for the random choices")
	stopRead := fs.Float64("stop-p95-ms", 50,
		"stop growing when the reads' p95 in a window is over this; 0 = never")
	stopOnboard := fs.Float64("stop-onboard-ms", 5000,
		"stop growing when the onboardings' p95 is over this; 0 = never")
	stopErr := fs.Float64("stop-errors-pct", 1,
		"stop growing when more than this share of statements fail; 0 = never")
	fs.Parse(args)

	fail := func(msg string) {
		fmt.Fprintln(os.Stderr, "agents:", msg)
		os.Exit(2)
	}
	m, err := parseMix(*mixFlag)
	if err != nil {
		fail(err.Error())
	}
	steps, err := parseSteps(*stepsFlag)
	if err != nil {
		fail(err.Error())
	}
	switch {
	case *backend == "shared" && *addr == "":
		fail("-backend shared needs -addr")
	case *backend == "vm" && (*runDir == "" || *fc == "" || *kernel == "" ||
		*rootfs == "" || *blank == ""):
		fail("-backend vm needs -run, -fc, -kernel, -rootfs and -blank")
	case *backend != "shared" && *backend != "vm":
		fail("need -backend shared|vm")
	case *wakeBy != "restore" && *wakeBy != "boot":
		fail("-wake is restore or boot")
	case *rows < 1 || *rate <= 0 || *onboard <= 0 || *active < 0 || *active > 1:
		fail("-rows, -rate and -onboard must be positive, -active in 0..1")
	}

	a := &agentRun{addr: *addr, pid: *pid, disk: *disk, clk: newClock(),
		rows: *rows,
		mix:  m, rate: *rate, sessLen: *sessLen, active: *active,
		begin: time.Now(), rng: rand.New(rand.NewSource(*seed)),
		running: map[*sessionRun]bool{}}
	if *backend == "vm" {
		if err := os.MkdirAll(*runDir, 0o755); err != nil {
			fail(err.Error())
		}
		a.p = &platform{fc: *fc, kernel: *kernel, rootfs: *rootfs,
			blank: *blank, run: *runDir, memMiB: *memMiB, cpus: *cpus,
			boot: *wakeBy == "boot", budget: *budgetMiB * 1024}
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sig
			a.p.killAll()
			os.Exit(1)
		}()
		go a.p.control()
	}
	var tf *os.File
	if *traceFile != "" {
		if tf, err = os.Create(*traceFile); err != nil {
			fail(err.Error())
		}
		a.trace = bufio.NewWriterSize(tf, 1<<20)
	}
	exit := func(code int) {
		a.mu.Lock()
		if a.trace != nil {
			a.trace.Flush()
			tf.Close()
		}
		a.mu.Unlock()
		if a.p != nil {
			a.p.killAll()
		}
		os.Exit(code)
	}
	go a.schedule()

	rule := stopRule{*stopRead, *stopOnboard, *stopErr}
	fmt.Println(header)
	for _, step := range steps {
		t0 := time.Now()
		why := a.grow(step, *onboard, *floorGiB, *diskFloorGiB)
		a.mu.Lock()
		n := a.count
		a.mu.Unlock()
		fmt.Fprintf(os.Stderr, "agents: %d tenants, grown in %s\n", n,
			time.Since(t0).Round(time.Second))
		if why != "" {
			fmt.Fprintf(os.Stderr, "agents: stopping at %d tenants: %s\n", n, why)
		}
		if n > 0 {
			w := a.window(*window)
			fmt.Println(w.csv)
			// A breach is measured once more at the same count before it
			// ends the run: a drive's slow phase can lift one window's
			// p95 on its own, and a count that fails twice has not fit.
			// Both rows are printed. The onboardings' tail is the growth's
			// own, and a window pauses onboardings, so a second window
			// cannot re-measure it: that breach stands as it is.
			if over := rule.breached(w); over != "" && why == "" {
				if rule.onboardP95 > 0 && w.onboardP95 > rule.onboardP95 {
					why = over
					fmt.Fprintf(os.Stderr, "agents: stopping at %d tenants: "+
						"%s\n", n, why)
				} else {
					fmt.Fprintf(os.Stderr, "agents: %d tenants: %s; measuring "+
						"again\n", n, over)
					w = a.window(*window)
					fmt.Println(w.csv)
					if over = rule.breached(w); over != "" {
						why = over + " in two windows"
						fmt.Fprintf(os.Stderr, "agents: stopping at %d tenants: "+
							"%s\n", n, why)
					}
				}
			}
		}
		if a.p != nil {
			a.p.mu.Lock()
			if a.p.snapFails > 0 {
				fmt.Fprintf(os.Stderr, "agents: %d snapshots failed, last: %v\n",
					a.p.snapFails, a.p.lastSnapErr)
			}
			a.p.mu.Unlock()
		}
		if why != "" {
			break
		}
	}
	exit(0)
}
