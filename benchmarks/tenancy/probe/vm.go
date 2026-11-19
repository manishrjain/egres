// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

// The micro-VM platform's control plane, for agents -backend vm: one
// Firecracker VM per tenant, made from a blank data image when the
// tenant onboards, put to sleep when memory is over budget and woken
// when a session starts for it.
//
// Sleep pauses the guest, writes a full snapshot (memory and device
// state) and kills the process; wake restores the snapshot, or boots
// the VM afresh and lets Postgres recover from its disk. The snapshot
// is written at every sleep because sessions write: the disk a
// sleeping VM wakes to is the one it went to sleep with, so its memory
// has to be from the same moment.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
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
	sleeping
	broken // its onboarding failed; nothing uses it
)

// How long a launched VM has to answer SQL.
const wakeTimeout = 60 * time.Second

type vm struct {
	id    int
	dir   string
	state int
	// Closed when the wake or the sleep under way is over.
	done chan struct{}
	// Sessions (and the onboarding) using it; never put to sleep while > 0.
	holds int
	last  time.Time
	cmd   *exec.Cmd
	// Closed when cmd has exited and been reaped.
	exited chan struct{}
	// A snapshot that matches the disk is on hand. Without one, or with
	// -wake boot, the VM boots.
	snapOK bool
}

type platform struct {
	mu  sync.Mutex
	vms []*vm // by tenant; nil before its onboarding

	fc, kernel, rootfs, blank, run string
	cpus                           string // the VMs' CPU list; "" = any
	memMiB                         int
	boot                           bool
	budget                         int64 // KiB; 0 = no budget, nothing sleeps
	// Over budget at the last reading: an onboarding waits for room.
	over bool

	// What the measurement window reads; counted while measuring.
	measuring         bool
	wakes, sleeps     int
	awakeSum, heldSum int64
	samples           int64
	snapFails         int
	lastSnapErr       error
}

func vmIP(i int) string {
	n := i + 256
	return fmt.Sprintf("10.200.%d.%d", n>>8, n&255)
}

func vmAddr(i int) string { return vmIP(i) + ":5432" }

func vmMAC(i int) string {
	n := i + 256
	return fmt.Sprintf("06:00:0a:c8:%02x:%02x", n>>8, n&255)
}

// bootArgs is lib.sh's boot_args with the console off.
func bootArgs(i int) string {
	return "8250.nr_uarts=0 quiet loglevel=0 reboot=k panic=1 pci=off" +
		" init=/fc-init ip=" + vmIP(i) + "::10.200.0.1:255.255.0.0::eth0:off"
}

type fcDrive struct {
	ID       string `json:"drive_id"`
	Path     string `json:"path_on_host"`
	Root     bool   `json:"is_root_device"`
	ReadOnly bool   `json:"is_read_only"`
	Cache    string `json:"cache_type,omitempty"`
}

type fcConfig struct {
	Boot struct {
		Kernel string `json:"kernel_image_path"`
		Args   string `json:"boot_args"`
	} `json:"boot-source"`
	Drives  []fcDrive `json:"drives"`
	Machine struct {
		VCPUs  int `json:"vcpu_count"`
		MemMiB int `json:"mem_size_mib"`
	} `json:"machine-config"`
	Net []fcNet `json:"network-interfaces"`
}

type fcNet struct {
	ID  string `json:"iface_id"`
	MAC string `json:"guest_mac"`
	Tap string `json:"host_dev_name"`
}

// config is backends/vm.sh's vm_config. Writeback passes the guest's
// flushes to the host's drive, so a commit Postgres fsyncs is on the
// device, as Egres's are: both sides pay for durability. (Firecracker's
// default, Unsafe, stops a flush at the host's page cache.)
func (p *platform) config(i int, dir string) ([]byte, error) {
	var c fcConfig
	c.Boot.Kernel = p.kernel
	c.Boot.Args = bootArgs(i)
	c.Drives = []fcDrive{
		{ID: "rootfs", Path: p.rootfs, Root: true, ReadOnly: true},
		{ID: "data", Path: filepath.Join(dir, "data.ext4"), Cache: "Writeback"},
	}
	c.Machine.VCPUs = 1
	c.Machine.MemMiB = p.memMiB
	c.Net = []fcNet{{"eth0", vmMAC(i), fmt.Sprintf("fctap%d", i)}}
	return json.MarshalIndent(c, "", "  ")
}

// dropCache asks the kernel to forget a file's cached pages, so the
// next read of it comes from disk. Only clean pages go; dirty ones are
// queued for writing.
func dropCache(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	const dontNeed = 4
	syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, dontNeed, 0, 0)
}

func api(sock, method, path, body string) error {
	// No keep-alive: the process on the other end is killed soon, and
	// thousands of sleeps would otherwise leave a socket each.
	c := http.Client{Transport: &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}, Timeout: 60 * time.Second}
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

// heldKB is what the probe's cgroup holds that the kernel cannot
// simply drop: all but the clean, unmapped file cache. That is the
// tenants' processes and the probe with the kernel's own, the snapshot
// pages a restored VM has mapped, and what is dirty or being written
// (a snapshot just taken). The figure the harness budgets on
// (common.sh mem_held_kb). -1 when the cgroup has no memory limit: the
// test is not under in_cgroup, and a caller falls back to PSS.
func heldKB() int64 {
	raw, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return -1
	}
	dir := ""
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "0::") {
			dir = "/sys/fs/cgroup" + strings.TrimPrefix(line, "0::")
		}
	}
	limit, err := os.ReadFile(dir + "/memory.max")
	if err != nil || strings.TrimSpace(string(limit)) == "max" {
		return -1
	}
	cur, err := os.ReadFile(dir + "/memory.current")
	if err != nil {
		return -1
	}
	current, _ := strconv.ParseInt(strings.TrimSpace(string(cur)), 10, 64)
	stat, err := os.ReadFile(dir + "/memory.stat")
	if err != nil {
		return -1
	}
	var file, pinned int64
	for _, line := range strings.Split(string(stat), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		n, _ := strconv.ParseInt(f[1], 10, 64)
		switch f[0] {
		case "file":
			file = n
		case "file_mapped", "file_dirty", "file_writeback":
			pinned += n
		}
	}
	return (current - max(file-pinned, 0)) / 1024
}

// memAvailableGiB is the host's MemAvailable.
func memAvailableGiB() float64 {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if f := strings.Fields(line); len(f) >= 2 && f[0] == "MemAvailable:" {
			kb, _ := strconv.ParseFloat(f[1], 64)
			return kb / (1 << 20)
		}
	}
	return 0
}

// diskFreeGiB is the space left to an ordinary user under dir.
func diskFreeGiB(dir string) float64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0
	}
	return float64(st.Bavail) * float64(st.Bsize) / (1 << 30)
}

// diskUsedMiB is du of dir: the blocks its files hold, which for the
// sparse data images is far less than their size.
func diskUsedMiB(dir string) float64 {
	var blocks int64
	filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if info, err := d.Info(); err == nil {
			if st, ok := info.Sys().(*syscall.Stat_t); ok {
				blocks += st.Blocks
			}
		}
		return nil
	})
	return float64(blocks) * 512 / (1 << 20)
}

func (p *platform) get(i int) *vm {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.vms[i]
}

// launch starts the VM's process: with its config, to boot, or bare,
// to have a snapshot loaded into it, which this does before returning.
func (p *platform) launch(v *vm, restore bool) error {
	sock := filepath.Join(v.dir, "api.sock")
	os.Remove(sock)
	args := []string{"--api-sock", sock}
	if !restore {
		args = append(args, "--config-file", filepath.Join(v.dir, "config.json"))
	}
	logf, err := os.OpenFile(filepath.Join(v.dir, "console.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(p.fc, args...)
	if p.cpus != "" {
		// The VM is the platform's: on its CPUs, not the probe's,
		// which this process is pinned to. taskset execs into it, so
		// the pid is the VM's.
		cmd = exec.Command("taskset", append([]string{"-c", p.cpus, p.fc},
			args...)...)
	}
	cmd.Stdout, cmd.Stderr = logf, logf
	err = cmd.Start()
	logf.Close()
	if err != nil {
		return err
	}
	exited := make(chan struct{})
	go func() {
		cmd.Wait()
		close(exited)
	}()
	p.mu.Lock()
	v.cmd, v.exited = cmd, exited
	p.mu.Unlock()
	os.WriteFile(filepath.Join(v.dir, "pid"),
		[]byte(strconv.Itoa(cmd.Process.Pid)+"\n"), 0o644)
	if !restore {
		return nil
	}
	for i := 0; ; i++ {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if i > 25000 {
			p.kill(v)
			return fmt.Errorf("vm%d: no api socket", v.id)
		}
		time.Sleep(200 * time.Microsecond)
	}
	body := fmt.Sprintf(`{"snapshot_path": %q, "mem_backend": {`+
		`"backend_type": "File", "backend_path": %q}, "resume_vm": true}`,
		filepath.Join(v.dir, "snap.state"), filepath.Join(v.dir, "snap.mem"))
	if err := api(sock, "PUT", "/snapshot/load", body); err != nil {
		p.kill(v)
		return fmt.Errorf("vm%d: %w", v.id, err)
	}
	return nil
}

// kill ends the VM's process and waits for it. Not with p.mu held:
// the wait is on the process.
func (p *platform) kill(v *vm) {
	p.mu.Lock()
	cmd, exited := v.cmd, v.exited
	v.cmd = nil
	p.mu.Unlock()
	if cmd == nil {
		return
	}
	cmd.Process.Kill()
	<-exited
	os.Remove(filepath.Join(v.dir, "pid"))
	os.Remove(filepath.Join(v.dir, "api.sock"))
}

// create makes tenant i's VM from the blank image, boots it and waits
// for Postgres to answer. On success the VM is held, as acquire would
// hold it, until release: the onboarding still has to load its table.
func (p *platform) create(i int) (*vm, error) {
	v := &vm{id: i, dir: filepath.Join(p.run, fmt.Sprintf("vm%d", i)),
		state: waking, done: make(chan struct{})}
	p.mu.Lock()
	for len(p.vms) <= i {
		p.vms = append(p.vms, nil)
	}
	p.vms[i] = v
	p.mu.Unlock()

	err := func() error {
		// Admission: a new VM waits for the budget to have room, as a
		// platform would hold an onboarding rather than run out of memory.
		// The wait is in the onboarding's latency; a platform that cannot
		// make room in the time a wake is allowed has refused it.
		deadline := time.Now().Add(wakeTimeout)
		for {
			p.mu.Lock()
			over := p.over
			p.mu.Unlock()
			if !over {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("no room for an onboarding within %s",
					wakeTimeout)
			}
			time.Sleep(50 * time.Millisecond)
		}
		// Left from an earlier run: this tenant starts blank.
		if err := os.RemoveAll(v.dir); err != nil {
			return err
		}
		if err := os.MkdirAll(v.dir, 0o755); err != nil {
			return err
		}
		cp := exec.Command("cp", "--sparse=always", p.blank,
			filepath.Join(v.dir, "data.ext4"))
		if out, err := cp.CombinedOutput(); err != nil {
			return fmt.Errorf("cp: %v %s", err, out)
		}
		conf, err := p.config(i, v.dir)
		if err != nil {
			return err
		}
		err = os.WriteFile(filepath.Join(v.dir, "config.json"), conf, 0o644)
		if err != nil {
			return err
		}
		if err := p.launch(v, false); err != nil {
			return err
		}
		return waitSQL(vmAddr(i), "SELECT 1", time.Now().Add(wakeTimeout))
	}()
	if err != nil {
		p.kill(v)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	close(v.done)
	if err != nil {
		v.state = broken
		return nil, fmt.Errorf("vm%d: %w", i, err)
	}
	v.state = awake
	v.holds = 1
	v.last = time.Now()
	return v, nil
}

// wake starts a sleeping VM and returns once Postgres answers.
func (p *platform) wake(v *vm) error {
	if err := p.launch(v, !p.boot && v.snapOK); err != nil {
		return err
	}
	err := waitSQL(vmAddr(v.id), "SELECT 1", time.Now().Add(wakeTimeout))
	if err != nil {
		p.kill(v)
		return fmt.Errorf("vm%d: %w", v.id, err)
	}
	return nil
}

// acquire makes the tenant awake and holds it against sleep until
// release. woke says this caller had to wake it, and took the time
// given, from launch to Postgres answering.
func (p *platform) acquire(v *vm) (woke bool, took time.Duration, err error) {
	p.mu.Lock()
	for {
		switch v.state {
		case awake:
			v.holds++
			v.last = time.Now()
			p.mu.Unlock()
			return woke, took, nil
		case asleep:
			v.state = waking
			v.done = make(chan struct{})
			p.mu.Unlock()
			t0 := time.Now()
			err := p.wake(v)
			took = time.Since(t0)
			p.mu.Lock()
			close(v.done)
			if err != nil {
				v.state = asleep
				p.mu.Unlock()
				return false, 0, err
			}
			v.state = awake
			if p.measuring {
				p.wakes++
			}
			woke = true
		case broken:
			p.mu.Unlock()
			return false, 0, fmt.Errorf("vm%d is broken", v.id)
		default:
			done := v.done
			p.mu.Unlock()
			<-done
			p.mu.Lock()
		}
	}
}

func (p *platform) release(v *vm) {
	p.mu.Lock()
	v.holds--
	v.last = time.Now()
	p.mu.Unlock()
}

// sleep puts the VM to sleep. Called with the state already set to
// sleeping. With -wake boot there is no snapshot to write: the kill
// leaves the disk as a power cut would, and Postgres recovers from it.
func (p *platform) sleep(v *vm) {
	var err error
	if !p.boot {
		sock := filepath.Join(v.dir, "api.sock")
		err = api(sock, "PATCH", "/vm", `{"state": "Paused"}`)
		if err == nil {
			err = api(sock, "PUT", "/snapshot/create", fmt.Sprintf(
				`{"snapshot_type": "Full", "snapshot_path": %q, `+
					`"mem_file_path": %q}`,
				filepath.Join(v.dir, "snap.state"),
				filepath.Join(v.dir, "snap.mem")))
		}
	}
	p.kill(v)
	// A wake should read its disk and snapshot from disk, as it would
	// once the host had cached other tenants instead. What the snapshot
	// left dirty is only queued for writing, so a wake soon after the
	// sleep may still find some of it in memory.
	dropCache(filepath.Join(v.dir, "data.ext4"))
	dropCache(filepath.Join(v.dir, "snap.mem"))
	p.mu.Lock()
	// A failed snapshot leaves an older one that no longer matches the
	// disk: this tenant boots at its next wake.
	v.snapOK = !p.boot && err == nil
	if err != nil {
		p.snapFails++
		p.lastSnapErr = fmt.Errorf("vm%d: %w", v.id, err)
	}
	if p.measuring {
		p.sleeps++
	}
	v.state = asleep
	close(v.done)
	p.mu.Unlock()
}

// Sleeps under way at once. A sleep writes a 128 MiB snapshot; more
// at once than the drive takes only queue, as dirty pages that still
// count against the budget.
const maxSleeping = 16

// control keeps the VMs within the budget: four times a second it reads
// what the cgroup holds (heldKB; without a cgroup limit, the awake
// VMs' PSS) and, when that is over, puts idle VMs to sleep, least
// recently used first, until it is a little under, and holds onboardings
// (create) until it is. A VM on its way to sleep still holds its
// memory until its snapshot is written and it is killed; that is
// taken off the reading, or every tick until then would put more to
// sleep.
func (p *platform) control() {
	type held struct {
		v   *vm
		pid int
		kb  int64
	}
	for {
		time.Sleep(250 * time.Millisecond)
		p.mu.Lock()
		var up, going []held
		for _, v := range p.vms {
			if v == nil || v.cmd == nil || v.cmd.Process == nil {
				continue
			}
			h := held{v: v, pid: v.cmd.Process.Pid}
			switch v.state {
			case awake:
				up = append(up, h)
			case sleeping:
				going = append(going, h)
			}
		}
		p.mu.Unlock()
		var pss int64
		for i := range up {
			up[i].kb = pssKB(up[i].pid)
			pss += up[i].kb
		}
		sum := heldKB()
		if sum < 0 {
			sum = pss
		} else {
			for _, h := range going {
				sum -= pssKB(h.pid)
			}
		}
		p.mu.Lock()
		if p.measuring {
			p.samples++
			p.awakeSum += int64(len(up))
			p.heldSum += sum
		}
		p.over = p.budget > 0 && sum > p.budget
		if p.over {
			sort.Slice(up, func(a, b int) bool {
				return up[a].v.last.Before(up[b].v.last)
			})
			slots := maxSleeping - len(going)
			for _, h := range up {
				if sum <= p.budget*97/100 || slots <= 0 {
					break
				}
				slots--
				// Changed since it was read: woken again, or held.
				if h.v.state != awake || h.v.holds > 0 || h.v.cmd == nil ||
					h.v.cmd.Process.Pid != h.pid {
					continue
				}
				h.v.state = sleeping
				h.v.done = make(chan struct{})
				sum -= h.kb
				go p.sleep(h.v)
			}
		}
		p.mu.Unlock()
	}
}

// measure starts (on) or ends a measurement window, and at its end
// returns what the window saw.
func (p *platform) measure(on bool) (awake, held float64, wakes, sleeps int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if on {
		p.measuring = true
		p.wakes, p.sleeps = 0, 0
		p.awakeSum, p.heldSum, p.samples = 0, 0, 0
		return
	}
	p.measuring = false
	n := float64(max(p.samples, 1))
	return float64(p.awakeSum) / n, float64(p.heldSum) / n / 1024,
		p.wakes, p.sleeps
}

func (p *platform) killAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, v := range p.vms {
		if v != nil && v.cmd != nil {
			v.cmd.Process.Kill()
		}
	}
}
