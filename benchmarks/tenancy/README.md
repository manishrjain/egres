# Tenancy benchmark

Micro-VMs against Egres for many small tenants. On one side, each
tenant is a Firecracker micro-VM running stock Postgres 18 (1 vCPU,
128 MiB). On the other, each tenant is a database on one Egres
server. Two controls run beside them: one Postgres per tenant with no
VM around it, and one Postgres holding every tenant as a database.
The same scripts rerun the whole comparison on another machine and
record what they ran on.

## The tests

| Test | Micro-VMs | Egres | Measures |
|---|---|---|---|
| 1 | `test1_footprint` | `egres_test1_footprint` | Idle cost, memory and disk |
| 2 | `test2_density` | `egres_test2_density` | Tenants until p99 > 50 ms |
| 3 | `test3_coldstart` | `egres_test3_coldstart` | Cold tenant to first row |
| 4 | `test4_sleeping` | `egres_test4_sleeping` | 16 GiB, more than fit awake |
| 5 | `test5_noisy` | `egres_test5_noisy` | Quiet tenants beside heavy |

- `pg_solo`: one Postgres per tenant in a container, no VM: the
  control for what the VM itself costs (test 1).
- `pg_shared`: one Postgres, every tenant a database, idle, under
  test 2's load and with a connection held per tenant.
- `egres_restart`: the server restarted with every tenant a density
  run left, its first touches, and a clean stop.
- `drive_idle`: whether the drive slows down when idle, which would
  show in every latency.
- `egres_calibrate`: measures the drive once, so later Egres starts
  skip their own 2.5 s probe.

Each script's header says what it does and lists its settings.

## A machine

- **Bare metal**, x86_64. Firecracker needs KVM, and nested
  virtualisation would tax only the micro-VM side. `machine.txt`
  records `virtualisation: none`.
- **About 128 GB of memory.** The tanland rehearsal had 121 GB. The
  micro-VM side stops at the memory floor (`FLOOR_GIB`, 16).
- **A datacenter NVMe drive, 1 TB free under WORK.** Putting 4,000
  micro-VMs to sleep in test 4 writes about 663 GiB, 512 GiB of it
  snapshots. A consumer drive sets test 4's restore results, as
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
    (`PG_IMAGE` in `env.sh`).
- `probe`: the load and timing client (Go).
- `egres`: this checkout's server, a ReleaseFast build into
  `$WORK/egres-bin`, its commit beside it. Set `EGRES_BIN` in
  `local.sh` to measure another build.
- `calibrate`: the drive, once.

## A run

```
./run.sh list                         # the tests and the suite
./run.sh suite                        # every step of suite.txt
./run.sh suite density-egres          # the suite from that step on
STEP=2000 MAX=120000 ./run.sh egres_test2_density   # one test
```

Each run gets a directory under `$WORK/results`, and
`<test>.latest` points at the newest run of each test. A suite's
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
  start.

`suite.txt` holds the settings the tanland rehearsal published.
Edit it, or point `SUITE` at another file, to change a run. Rough
durations on tanland:
- Egres density to 84,000 tenants: 1 h 45 min.
- Putting 4,000 micro-VMs to sleep: 1 h 30 min.
- Each test 4 row: 7 min.
- The noisy-neighbour sweeps: about 20 min per side.

The micro-VM tests run inside the taps' namespace (`in_netns.sh`,
done by `run.sh`). Egres listens 50 above its own ports (SQL 3260,
admin 5260), so a development server on the machine is left alone.
One test at a time, on an otherwise idle machine: every test measures
the whole host.

## Layout

- `env.sh`: where everything lives (`WORK`), read by every script.
- `local.sh`: this machine's settings (ignored by git).
- `lib.sh`, `egres_lib.sh`: the micro-VM and Egres sides' shared
  pieces.
- `netns_setup.sh`, `in_netns.sh`: the taps' namespace, and running a
  command in it.
- `build_images.sh`, `build_big_image.sh`, `fc-init`: the guest images
  and the guest's init.
- `probe/`: the client. `first` times one first row; `load` is
  open-loop point reads; `fleet` is test 4's traffic and the
  micro-VMs' control plane; `noisy` is test 5's traffic.
