# Tenancy benchmark

Many small tenants on one machine, two ways. On one side each tenant
is a Firecracker micro-VM running stock Postgres 18 (1 vCPU,
128 MiB). On the other, each tenant is a database on one Egres
server. One control runs beside them: a Postgres per tenant in a
container with no VM around it, which splits the micro-VM's cost into
Postgres's part and the VM's. The same scripts rerun the whole
comparison on another machine and record what they ran on.

Every tenant on every side gets its data the same way, through SQL:
its database (a VM, on the micro-VM side), table `t`, and rows
`(id, md5(id))`.

## The tests

Each test is one script that runs against a platform, `BACKEND=vm`,
`egres` or `pgsolo`:

1. **`noisy`: isolation.** 50 quiet tenants send an occasional point
   read while 0 to 64 heavy tenants each run one statement flat out,
   a count over 100,000 rows or an update of 1,000 of them. Reported:
   the quiet tenants' latency at each heavy count.
2. **`footprint`: what a tenant costs, and how many fit.** Tenants are
   brought up to each count in turn; after each, point reads at
   random tenants, and the memory and disk held. After the last count
   the memory is sampled for ten minutes as the tenants go quiet.
   `footprint_fit.txt` estimates from the slope how many tenants fit
   in the memory each side gets.
3. **`agents`: the workload.** Tenants arrive over time and load their
   data; a tenth of them are in a session at any moment, sending a
   mix of reads and writes; the rest are idle. On the micro-VM side
   the probe is also the control plane: when the memory runs out, idle
   VMs are put to sleep (snapshotted to disk) and woken, by restoring
   the snapshot or booting afresh, when a session starts for them. At
   each tenant count: the latency of every statement kind, the time
   an arrival took to its first row, the time to a result from a
   tenant that was idle (a session's first statement, a wake included
   on the micro-VM side), wakes and sleeps, memory and disk.

`drive_idle` runs first: whether the drive slows down when idle,
which would show in every latency.

Each script's header says what it does and lists its settings.

## A machine

- **Bare metal**, x86_64. Firecracker needs KVM, and nested
  virtualisation would tax only the micro-VM side. `machine.txt`
  records `virtualisation: none`.
- **About 128 GB of memory.** The tanland rehearsal had 121 GB. Each
  side gets `MEM_GIB` (64) of it, enforced: every test runs in a
  transient cgroup allowed that much and no swap (`systemd-run
  --user --scope`; the user's slice must have the memory controller,
  as Ubuntu's does), so the host page cache the tenants' files take
  comes out of the same memory as their processes. The tenants'
  processes are kept to `BUDGET_PCT` (90) of it; the rest is the
  kernel's and the cache's. The host must also keep `FLOOR_GIB` (16)
  free. `pgsolo`'s containers are docker's, outside the cgroup; its
  counts are small.
- **A datacenter NVMe drive, 1 TB free under WORK.** A sleeping
  micro-VM holds a 128 MiB snapshot; a consumer drive's write stalls
  set the micro-VM wake times and the Egres write results, as
  tanland's Kingston NV1 did.
- **Open files: a hard limit of 4 per Egres tenant.** 524,288 allows
  about 131,000 tenants; `./setup.sh check` prints the figure.
- **Passwordless sudo**, for the taps' namespace, loop mounts, the
  containers' memory and dropping the page cache.
- Ubuntu 26.04 is what the rehearsal ran.

## Setup

```
cd benchmarks/tenancy
cp local.example.sh local.sh      # set WORK, on the drive to measure
./setup.sh packages               # Ubuntu packages; log in again after
sudo ./setup.sh host              # kvm group, 4,000 taps in a namespace
./setup.sh all                    # everything else, then the check
```

Zig is not packaged: install the version the top-level README names.
`./setup.sh check` lists what is in place and what is missing, each
with the step that fixes it. The namespace does not survive a reboot:
run `sudo ./setup.sh host` again.

`./setup.sh all` runs, in order:

- `assets`: Firecracker and the guest kernel, each checked against a
  pinned sha256, then the guest images.
  - The kernel is the one Firecracker's getting-started guide
    fetches, `firecracker-ci/v1.15/x86_64/vmlinux-6.1.155`.
  - The images are built from `postgres:18` pinned by digest
    (`PG_IMAGE` in `env.sh`): a root filesystem, and a blank data
    directory every tenant starts from.
- `probe`: the load and timing client (Go).
- `egres`: this checkout's server, a ReleaseFast build into
  `$WORK/egres-bin`, its commit beside it. Set `EGRES_BIN` in
  `local.sh` to measure another build.
- `calibrate`: the drive, once, so every Egres start is given the
  same profile and skips its own 2.5 s probe.

## A run

```
./run.sh list                              # the tests and the suite
./run.sh suite                             # every step of suite.txt
./run.sh suite footprint-egres             # the suite from that step on
BACKEND=egres ./run.sh footprint 1000 10000 100000   # one test
```

Each run gets a directory under `$WORK/results`, and
`<test>-<backend>.latest` points at the newest run of each. A suite's
steps are numbered directories under `suite-<time>`. A suite's
`summary.txt` lists each step's exit status and duration, and it has
a copy of the `suite.txt` it ran. A failed step does not stop the
suite. Each run's directory holds:

- `command.txt`: the test, its arguments and every setting it was
  given.
- `machine.txt`: the machine, the drive and every version
  (`machine.sh`). It covers Egres's commit and sha256, the harness's
  commit, the Firecracker version, the kernel's hash and the images'
  digest.
- `output.log` and `status.txt`: what the test printed, and its start,
  end and exit status.
- The test's CSVs and traces. `logs/` has one server log per Egres
  start; Egres's meter and memory reports are saved after each step
  of `noisy` and at the end of `agents`.

`suite.txt` holds the settings the tanland rehearsal published.
Edit it, or point `SUITE` at another file, to change a run.

Every test re-runs itself in its memory cgroup first (`in_cgroup` in
`common.sh`; `CGROUP=0` skips it, and the memory columns fall back to
PSS). The micro-VM tests run inside the taps' namespace
(`in_netns.sh`, done by `run.sh`). Egres listens 50 above its own
ports (SQL 3260, admin 5260), so a development server on the machine
is left alone.
One test at a time, on an otherwise idle machine: every test measures
the whole host.

## Layout

- `env.sh`: where everything lives (`WORK`), read by every script.
- `local.sh`: this machine's settings (ignored by git).
- `common.sh`: what the tests share, and the backend interface its
  header lists; it sources `backends/$BACKEND.sh`.
- `backends/`: `vm.sh`, `egres.sh`, `pgsolo.sh`, one platform each.
- `footprint.sh`, `noisy.sh`, `agents.sh`, `drive_idle.sh`: the tests.
- `setup.sh`, `run.sh`, `machine.sh`, `suite.txt`: making a machine
  ready, and running on it.
- `netns_setup.sh`, `in_netns.sh`: the taps' namespace, and running a
  command in it.
- `build_images.sh`, `fc-init`: the guest images and the guest's init.
- `probe/`: the client. `first` times one first row; `sql` runs one
  statement; `load` is open-loop point reads; `noisy` is test 1's
  traffic; `agents` is test 3's traffic and the micro-VMs' control
  plane.
