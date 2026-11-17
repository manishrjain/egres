// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

// fleet is the sleeping-tenant test: a fixed rate of point reads over
// n tenants, most of them idle at any moment, on a fixed memory
// budget.
//
// A tenth of the tenants (-hot) are busy at a time and take nine
// queries in ten. The busy set is a window that slides through the
// tenant list, so each tenant is busy for -session and then goes
// quiet, and every tenant has had its turn after session/hot. The
// tenth query goes to a random quiet tenant.
//
// -mode shared: every tenant is a database on one server (Egres). The
// server is given the budget; this side only sends.
//
// -mode vm: every tenant is a Firecracker VM that prep left asleep,
// with a memory snapshot beside its disk. This side is the platform's
// control plane as well as the client. A query for a sleeping tenant
// wakes it (restore from the snapshot, or boot); once the awake VMs
// hold more than the budget, the least recently used idle ones are
// killed. Killing is free because the queries only read: the disk
// stays as the snapshot left it, which is checked (see evict), so the
// same snapshot serves the next wake. That is the cheapest a platform
// could make sleep; a real one would have to write a snapshot.
//
// A query's clock starts when it was due, so waiting for a wake is in
// its latency.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	asleep = iota
	waking
	awake
	evicting
)

type vm struct {
	id    int
	dir   string
	state int
	// Closed when the wake or the eviction under way is over.
	done     chan struct{}
	inflight int
	last     time.Time
	cmd      *exec.Cmd
	// The disk's mtime when the snapshot was taken. A later one means
	// the guest wrote, and the snapshot no longer matches the disk.
	stamp  int64
	snapOK bool
}

type fleetState struct {
	mu     sync.Mutex
	vms    []*vm
	fc     string
	boot   bool
	budget int64 // KiB

	wakes, evictions, dirty int
	awakeSum, memSum        int64
	samples                 int64
	awakePeak               int
	memPeak                 int64
}

func vmAddr(i int) string {
	n := i + 256
	return fmt.Sprintf("10.200.%d.%d:5432", n>>8, n&255)
}

// dropCache asks the kernel to forget a file's cached pages, so the
// next read of it comes from disk. Only clean pages go.
func dropCache(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	const dontNeed = 4
	syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, dontNeed, 0, 0)
}

func mtimeNs(path string) int64 {
	st, err := os.Stat(path)
	if err != nil {
		return -1
	}
	return st.ModTime().UnixNano()
}

func api(sock, method, path, body string) error {
	c := http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}, Timeout: 30 * time.Second}
	req, err := http.NewRequest(method, "http://localhost"+path,
		bytes.NewBufferString(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var b bytes.Buffer
		b.ReadFrom(resp.Body)
		return fmt.Errorf("%s %s: %s %s", method, path, resp.Status, b.String())
	}
	return nil
}

// wake starts the VM's process and returns once the guest is running
// (restore) or starting (boot); the caller's query retries until
// Postgres answers.
func (f *fleetState) wake(v *vm) error {
	sock := filepath.Join(v.dir, "api.sock")
	os.Remove(sock)
	args := []string{"--api-sock", sock}
	restore := !f.boot && v.snapOK
	if !restore {
		args = append(args, "--config-file", filepath.Join(v.dir, "config.json"))
	}
	cmd := exec.Command(f.fc, args...)
	if err := cmd.Start(); err != nil {
		return err
	}
	v.cmd = cmd
	if !restore {
		return nil
	}
	for i := 0; ; i++ {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if i > 25000 {
			cmd.Process.Kill()
			cmd.Wait()
			return fmt.Errorf("vm%d: no api socket", v.id)
		}
		time.Sleep(200 * time.Microsecond)
	}
	body := fmt.Sprintf(`{"snapshot_path": %q, "mem_backend": {`+
		`"backend_type": "File", "backend_path": %q}, "resume_vm": true}`,
		filepath.Join(v.dir, "snap.state"), filepath.Join(v.dir, "snap.mem"))
	if err := api(sock, "PUT", "/snapshot/load", body); err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		return err
	}
	return nil
}

// acquire makes the tenant awake and holds it against eviction until
// release. woke says this caller had to wake it; waited says it waited
// on a wake or an eviction someone else began.
func (f *fleetState) acquire(v *vm) (woke, waited bool, err error) {
	f.mu.Lock()
	for {
		switch v.state {
		case awake:
			v.inflight++
			v.last = time.Now()
			f.mu.Unlock()
			return woke, waited, nil
		case asleep:
			v.state = waking
			v.done = make(chan struct{})
			f.mu.Unlock()
			err := f.wake(v)
			f.mu.Lock()
			close(v.done)
			if err != nil {
				v.state = asleep
				f.mu.Unlock()
				return false, waited, err
			}
			v.state = awake
			f.wakes++
			woke = true
		default:
			done := v.done
			f.mu.Unlock()
			<-done
			waited = true
			f.mu.Lock()
		}
	}
}

func (f *fleetState) release(v *vm) {
	f.mu.Lock()
	v.inflight--
	v.last = time.Now()
	f.mu.Unlock()
}

// evict kills the VM. Called with the state already set to evicting.
func (f *fleetState) evict(v *vm) {
	v.cmd.Process.Kill()
	v.cmd.Wait()
	disk := filepath.Join(v.dir, "data.ext4")
	// The snapshot's memory is only right for the disk it was taken
	// with. If the guest wrote since, this tenant boots from now on.
	wrote := !f.boot && v.snapOK && mtimeNs(disk) != v.stamp
	dropCache(disk)
	dropCache(filepath.Join(v.dir, "snap.mem"))
	f.mu.Lock()
	if wrote {
		v.snapOK = false
		f.dirty++
	}
	f.evictions++
	v.state = asleep
	close(v.done)
	f.mu.Unlock()
}

func pssKB(pid int) int64 {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/smaps_rollup", pid))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "Pss:") {
			kb, _ := strconv.ParseInt(strings.Fields(line)[1], 10, 64)
			return kb
		}
	}
	return 0
}

// control keeps the awake VMs within the budget: four times a second
// it adds up what they hold and, when that is over, kills idle ones,
// least recently used first, until it is a little under.
func (f *fleetState) control(stop <-chan struct{}, measuring *bool) {
	type held struct {
		v   *vm
		pid int
		kb  int64
	}
	for {
		select {
		case <-stop:
			return
		case <-time.After(250 * time.Millisecond):
		}
		f.mu.Lock()
		var up []held
		for _, v := range f.vms {
			if v.state == awake {
				up = append(up, held{v: v, pid: v.cmd.Process.Pid})
			}
		}
		f.mu.Unlock()
		var sum int64
		for i := range up {
			up[i].kb = pssKB(up[i].pid)
			sum += up[i].kb
		}
		f.mu.Lock()
		if *measuring {
			f.samples++
			f.awakeSum += int64(len(up))
			f.memSum += sum
			f.awakePeak = max(f.awakePeak, len(up))
			f.memPeak = max(f.memPeak, sum)
		}
		if sum > f.budget {
			sort.Slice(up, func(a, b int) bool {
				return up[a].v.last.Before(up[b].v.last)
			})
			for _, h := range up {
				if sum <= f.budget*97/100 {
					break
				}
				if h.v.state != awake || h.v.inflight > 0 ||
					h.v.cmd.Process.Pid != h.pid {
					continue
				}
				h.v.state = evicting
				h.v.done = make(chan struct{})
				sum -= h.kb
				go f.evict(h.v)
			}
		}
		f.mu.Unlock()
	}
}

func (f *fleetState) killAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, v := range f.vms {
		if v.cmd != nil && (v.state == awake || v.state == waking) {
			v.cmd.Process.Kill()
		}
	}
}

// sleepAll ends the run: every VM still awake is put to sleep the way
// the controller would have, so its disk is checked too.
func (f *fleetState) sleepAll() {
	var wg sync.WaitGroup
	f.mu.Lock()
	for _, v := range f.vms {
		if v.state != awake {
			continue
		}
		v.state = evicting
		v.done = make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.evict(v)
		}()
	}
	f.mu.Unlock()
	wg.Wait()
}

// pointRead runs one point read, retrying a connection that fails until
// the deadline: a VM just woken refuses or drops the first attempts.
func pointRead(target string, rows int, deadline time.Time, retry bool) error {
	sql := fmt.Sprintf("SELECT v FROM t WHERE id = %d", 1+rand.Intn(rows))
	dial := 5 * time.Millisecond
	if !retry {
		return run(target, sql, 2*time.Second, true)
	}
	for {
		err := run(target, sql, dial, true)
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(500 * time.Microsecond)
		dial = min(dial*2, time.Second)
	}
}

func fleet(args []string) {
	fs := flag.NewFlagSet("fleet", flag.ExitOnError)
	mode := fs.String("mode", "", "vm or shared")
	n := fs.Int("n", 0, "tenants")
	addr := fs.String("addr", "", "shared: the server, ip:port")
	runDir := fs.String("run", "", "vm: directory holding vm<i>/")
	fc := fs.String("fc", "", "vm: the firecracker binary")
	wakeBy := fs.String("wake", "restore", "vm: restore or boot")
	budgetMiB := fs.Int64("budget-mib", 16384, "vm: memory the awake VMs may hold")
	rate := fs.Int("rate", 200, "queries per second, across all tenants")
	hot := fs.Float64("hot", 0.10, "share of tenants busy at a time")
	session := fs.Duration("session", 30*time.Second, "how long a tenant stays busy")
	warm := fs.Duration("warm", 60*time.Second, "sent before measuring starts")
	dur := fs.Duration("dur", 0, "measured time; 0 = one full rotation")
	rows := fs.Int("rows", 100000, "rows per tenant; ids are 1..rows")
	timeout := fs.Duration("timeout", 10*time.Second, "give up on a query after")
	trace := fs.String("trace", "", "write due_ms,tenant,ms,woke per query here")
	fs.Parse(args)
	if *n < 1 || (*mode != "vm" && *mode != "shared") {
		fmt.Fprintln(os.Stderr, "fleet: need -mode vm|shared and -n")
		os.Exit(2)
	}
	if *dur == 0 {
		*dur = time.Duration(float64(*session) / *hot)
	}
	h := max(1, int(math.Round(*hot*float64(*n))))

	f := &fleetState{fc: *fc, boot: *wakeBy == "boot",
		budget: *budgetMiB * 1024}
	measuring := false
	stop := make(chan struct{})
	if *mode == "vm" {
		for i := 0; i < *n; i++ {
			d := filepath.Join(*runDir, fmt.Sprintf("vm%d", i))
			v := &vm{id: i, dir: d}
			raw, err := os.ReadFile(filepath.Join(d, "snap.stamp"))
			if err != nil {
				fmt.Fprintf(os.Stderr, "vm%d is not prepared: %v\n", i, err)
				os.Exit(1)
			}
			v.stamp, _ = strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
			disk := filepath.Join(d, "data.ext4")
			v.snapOK = mtimeNs(disk) == v.stamp
			if !v.snapOK && !f.boot {
				f.dirty++
			}
			dropCache(disk)
			dropCache(filepath.Join(d, "snap.mem"))
			f.vms = append(f.vms, v)
		}
		if f.dirty > 0 {
			fmt.Fprintf(os.Stderr, "%d tenants start with a stale snapshot "+
				"and will boot\n", f.dirty)
		}
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sig
			f.killAll()
			os.Exit(1)
		}()
		go f.control(stop, &measuring)
	}

	var tr *os.File
	if *trace != "" {
		var err error
		if tr, err = os.Create(*trace); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer tr.Close()
	}

	var mu sync.Mutex
	var lat, wakeLat []float64
	errs, waits := 0, 0
	var lastErr error
	var wg sync.WaitGroup
	gap := time.Second / time.Duration(*rate)
	begin := time.Now()
	total := *warm + *dur
	slide := *session / time.Duration(h)
	for q := 0; time.Duration(q)*gap < total; q++ {
		at := time.Duration(q) * gap
		due := begin.Add(at)
		waitUntil(due)
		if at >= *warm && !measuring {
			f.mu.Lock()
			measuring = true
			f.wakes, f.evictions = 0, 0
			f.mu.Unlock()
		}
		off := int(at/slide) % *n
		idx := (off + rand.Intn(h)) % *n
		if *n > h && rand.Intn(10) == 0 {
			idx = (off + h + rand.Intn(*n-h)) % *n
		}
		counted := at >= *warm
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			woke, waited := false, false
			deadline := due.Add(*timeout)
			if *mode == "vm" {
				v := f.vms[idx]
				if woke, waited, err = f.acquire(v); err == nil {
					err = pointRead(vmAddr(idx), *rows, deadline, true)
					f.release(v)
				}
			} else {
				target := fmt.Sprintf("%s/tenant_%d", *addr, idx)
				err = pointRead(target, *rows, deadline, false)
			}
			ms := float64(time.Since(due).Microseconds()) / 1000
			mu.Lock()
			defer mu.Unlock()
			if tr != nil {
				fmt.Fprintf(tr, "%d,%d,%.2f,%t,%t\n",
					at.Milliseconds(), idx, ms, woke, err == nil)
			}
			if !counted {
				return
			}
			if err != nil {
				errs++
				lastErr = err
				return
			}
			lat = append(lat, ms)
			if woke {
				wakeLat = append(wakeLat, ms)
			}
			if waited {
				waits++
			}
		}()
	}
	wg.Wait()
	close(stop)
	var evictions int
	if *mode == "vm" {
		f.mu.Lock()
		evictions = f.evictions
		f.mu.Unlock()
		f.sleepAll()
	}
	if lastErr != nil {
		fmt.Fprintln(os.Stderr, "last error:", lastErr)
	}

	pct := func(a []float64, p float64) float64 {
		if len(a) == 0 {
			return 0
		}
		return a[int(p*float64(len(a)-1))]
	}
	sort.Float64s(lat)
	sort.Float64s(wakeLat)
	f.mu.Lock()
	defer f.mu.Unlock()
	samples := max(f.samples, 1)
	share := 0.0
	if len(lat) > 0 {
		share = 100 * float64(len(wakeLat)) / float64(len(lat))
	}
	// p50_ms,p90_ms,p99_ms,p999_ms,max_ms,ok,errors,woke_pct,
	// wake_p50_ms,wake_p99_ms,waited,awake_avg,awake_peak,
	// mem_avg_mib,mem_peak_mib,evictions,stale
	fmt.Printf("%.2f,%.2f,%.2f,%.2f,%.2f,%d,%d,%.2f,%.2f,%.2f,%d,"+
		"%d,%d,%d,%d,%d,%d\n",
		pct(lat, 0.5), pct(lat, 0.9), pct(lat, 0.99), pct(lat, 0.999),
		pct(lat, 1), len(lat), errs, share,
		pct(wakeLat, 0.5), pct(wakeLat, 0.99), waits,
		f.awakeSum/samples, f.awakePeak,
		f.memSum/samples/1024, f.memPeak/1024, evictions, f.dirty)
}
