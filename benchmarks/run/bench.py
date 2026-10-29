#!/usr/bin/env python3
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""Benchmark runs: one server (Egres or PostgreSQL) through one workload,
a load with readers alongside, then pgbench runs back to back, with
Prometheus and Grafana drawing the whole run. The same commands rerun
the same benchmark on any machine.

    bench.py <env> begin       new output dir, empty data dir, monitor up
    bench.py <env> start       the server (initdb for Postgres)
    bench.py <env> load        the load driver, detached, with readers
    bench.py <env> watch       the watchdog, detached: workload runs after
                               the load, incidents, restarts, summary
    bench.py <env> all         begin, start, load, watch
    bench.py <env> status      one screen of state
    bench.py <env> workload N  one workload run by hand, run number N
    bench.py <env> stop        stop the watchdog, load, pgbench, server
    bench.py envs              list the envs

An env is a row of ENVS below: a workload, a server, a memory cap. The
machine's paths come from local.toml next to this file (copy
local.example.toml). `--set key.path=value` overrides any resolved
setting for one invocation (`--set load.last=120000 --set runs=1`),
`--local key=value` one of local.toml's (`--local wal_root=/data`),
and `--name` suffixes the env name, so a small trial keeps its own
dirs.

A run's output dir is <out_root>/<name>-<start time>; `begin` creates
it and points <out_root>/<name>.latest at it, and every later command
follows the pointer, so a run can span days without a date being
typed anywhere. The resolved env is written there as env.json.
"""

import argparse
import copy
import json
import os
import shutil
import subprocess
import sys
import time
import tomllib
from pathlib import Path

import workloads

HERE = Path(__file__).resolve().parent
BENCH = HERE.parent
MONITOR = BENCH / "monitor"
GIB = 2**30

# ---------------------------------------------------------------------------
# The benchmarks
# ---------------------------------------------------------------------------

# Appended to postgresql.conf at initdb. Autovacuum keeps up with the
# load instead of resting at the default cost limit: hint bits and the
# visibility map get set while the rows arrive, and the load and the
# readers pay for it, as Egres pays for compaction while loading. No
# VACUUM is run by hand at any point.
PG_CONF = """
shared_buffers = 16GB
effective_cache_size = 48GB
max_wal_size = 64GB
checkpoint_timeout = 30min
wal_buffers = 256MB
random_page_cost = 1.1
effective_io_concurrency = 200
autovacuum_vacuum_cost_delay = 0
autovacuum_vacuum_insert_scale_factor = 0.01
autovacuum_naptime = 10s
autovacuum_max_workers = 4
"""

# A server: ports, the database the workload lives in, its config.
SERVERS = {
    # `memory` is the server's --memory flag body ("total_bytes=.."); empty
    # leaves the server's default (90% of RAM or of the cgroup cap).
    # `store` is the --store body; empty means the run's data dir
    # ("dir_path=<data>"), an s3 body ("s3_endpoint=..;s3_bucket=..")
    # puts the SSTs on an object store, with AWS_ACCESS_KEY_ID and
    # AWS_SECRET_ACCESS_KEY taken from this process's environment.
    "egres": {"sql_port": 3210, "admin_port": 5210, "dbname": "egres",
              "zdb_extra": "num_compactors=8", "memory": "", "store": ""},
    "postgres": {"port": 5434, "dbname": "bench", "conf": PG_CONF},
}

# An env: one benchmark. memory_max is a systemd scope's MemoryMax
# (page cache included, swap off) or None. During the load the
# workload's reader groups run beside it (`readers.<group>` is a
# group's client count, 0 for none); then `runs` workload runs of
# `secs` seconds with `clients` clients, `latency_sample` of their
# transactions logged for percentiles. `runs` 0 ends with the load.
ENVS = {
    "egres-orders": {"server": "egres", "workload": "orders",
                     "memory_max": None},
    "postgres-orders": {"server": "postgres", "workload": "orders",
                        "memory_max": None},
    # benchmarks/longrun drives this one: days of the bank workload
    # with faults, checked between them.
    "egres-bank": {"server": "egres", "workload": "bank",
                   "memory_max": None},
}
ENV_DEFAULTS = {"runs": 2, "secs": 7200, "clients": 64,
                "latency_sample": 0.1}


def resolve_env(name: str, sets: list[str]) -> dict:
    """The env as one nested dict: its row, the defaults, its server
    and workload tables, then `--set` overrides typed by the value
    they replace."""
    if name not in ENVS:
        sys.exit(f"unknown env {name!r}; one of {', '.join(ENVS)}")
    env = copy.deepcopy(ENV_DEFAULTS)
    env.update(copy.deepcopy(ENVS[name]))
    env["server"] = {"kind": env["server"],
                     **copy.deepcopy(SERVERS[env["server"]])}
    # The workload's settings; its scripts and generator stay in
    # workloads.py, reached by name. The load block sits at the top
    # for `--set load.last=...`.
    w = workloads.WORKLOADS[env["workload"]]
    env["workload"] = {"name": env["workload"], "vars": dict(w["vars"]),
                       "reads": copy.deepcopy(w["reads"]),
                       "mix": list(w["mix"])}
    env["readers"] = {g["name"]: g["clients"] for g in w["reads"]}
    env["load"] = dict(w["load"])
    for item in sets:
        key, _, raw = item.partition("=")
        node = env
        parts = key.split(".")
        for p in parts[:-1]:
            node = node[p]
        old = node.get(parts[-1])
        if raw.lower() in ("none", ""):
            val = None
        elif isinstance(old, bool):
            val = raw.lower() in ("1", "true", "yes")
        elif isinstance(old, int):
            val = int(raw)
        elif isinstance(old, float):
            val = float(raw)
        elif old is None and raw.isdigit():
            # A setting that is off by default: load.stop_bytes.
            val = int(raw)
        else:
            val = raw
        node[parts[-1]] = val
    return env


# ---------------------------------------------------------------------------
# A run: paths and helpers every command shares
# ---------------------------------------------------------------------------

class Run:
    def __init__(self, env_name: str, suffix: str, env: dict, local: dict):
        self.env_name = env_name
        self.suffix = suffix
        self.name = env_name + (f"-{suffix}" if suffix else "")
        self.env = env
        self.local = local
        self.out_root = Path(local["out_root"])
        self.data = Path(local["data_root"]) / self.name
        self.wal = Path(local["wal_root"]) / f"{self.name}-wal"
        self.pgbin = Path(local["pgbin"])
        self.psql = self.pgbin / "psql"
        self.pgbench = self.pgbin / "pgbench"
        self.egres_bin = Path(local.get("egres_bin")
                              or BENCH.parent / "zig-out" / "bin" / "egres")
        latest = self.out_root / f"{self.name}.latest"
        if latest.exists():
            self.out = Path(latest.read_text().strip())
        else:
            self.out = self.out_root / f"{self.name}-unstarted"
        self.textfile = self.out / "textfile"
        self.results = self.out / "results"
        srv = env["server"]
        self.kind = srv["kind"]
        if self.kind == "egres":
            self.conn = (f"host=127.0.0.1 port={srv['sql_port']} "
                         f"dbname={srv['dbname']} sslmode=disable")
            self.pg_url = ""
            self.server_log = self.out / "server.log"
        else:
            self.conn = f"host=127.0.0.1 port={srv['port']} " \
                        f"dbname={srv['dbname']}"
            self.pg_url = (f"postgresql://{os.environ.get('USER', '')}"
                           f"@127.0.0.1:{srv['port']}/{srv['dbname']}"
                           f"?sslmode=disable")
            self.server_log = self.out / "postgres.log"
        self.no_monitor = False

    def event(self, msg: str):
        with open(self.out / "events.log", "a") as f:
            f.write(f"{utc()} {msg}\n")

    def pid(self, name: str):
        try:
            return int((self.out / name).read_text().strip())
        except (OSError, ValueError):
            return None

    def write_monitor_env(self):
        """Tell the monitoring stack where this run's files are.
        `docker compose up -d` recreates the containers whose mounts or
        environment changed, so a new run's dirs are picked up without
        a restart by hand. --no-monitor leaves the stack alone (a trial
        beside a real run must not repoint its exporters)."""
        cg = ""
        cg_path = self.out / "cgroup.path"
        if cg_path.exists():
            cg = cg_path.read_text().strip().removeprefix("/sys/fs/cgroup")
        (MONITOR / ".env").write_text(
            f"EGRES_DATA_DIR={self.data}\nTEXTFILE_DIR={self.textfile}\n"
            f"WAL_ROOT={self.wal.parent}\nWAL_NAME={self.wal.name}\n"
            f"CGROUP_PATH={cg}\nPG_CONN={self.pg_url}\n")
        if self.no_monitor:
            return
        up = subprocess.run(["docker", "compose", "ps", "-q"], cwd=MONITOR,
                            capture_output=True, text=True)
        if up.stdout.strip():
            subprocess.run(["docker", "compose", "up", "-d",
                            "--quiet-pull"], cwd=MONITOR,
                           capture_output=True)


def utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def alive(pid) -> bool:
    """Whether `pid` is a running process. A zombie — exited, not yet
    reaped — is dead: a server the watchdog itself started becomes one
    when it exits, and `kill(pid, 0)` alone would keep reporting it
    alive forever. Reaping is attempted in case it is our child."""
    if not pid:
        return False
    pid = int(pid)
    try:
        os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        pass
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    # A zombie still answers kill(pid, 0); read its state. Linux has
    # /proc; macOS does not, and answering False there made stop_server
    # skip its SIGTERM, so the harness never stopped a server on a Mac.
    if not os.path.isdir("/proc"):
        return "Z" not in ps_field(pid, "stat")
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("State:"):
                    return "Z" not in line.split()[1]
    except OSError:
        return False
    return True


def load_local() -> dict:
    path = HERE / "local.toml"
    if not path.exists():
        sys.exit(f"no {path}: copy local.example.toml and edit it")
    with open(path, "rb") as f:
        return tomllib.load(f)


def ps_field(pid: int, field: str) -> str:
    """One `ps -o <field>=` column for `pid`, stripped; empty when the
    process is gone. The portable stand-in for /proc on macOS."""
    out = subprocess.run(["ps", "-o", f"{field}=", "-p", str(pid)],
                         capture_output=True, text=True)
    return out.stdout.strip()


def cgroup_of(pid: int) -> Path:
    line = Path(f"/proc/{pid}/cgroup").read_text().strip()
    return Path("/sys/fs/cgroup" + line.split(":", 2)[2])


def scope_prefix(run: Run) -> list[str]:
    """A systemd user scope that caps the server's memory, page cache
    included, with swap off. Both servers size themselves from it."""
    cap = run.env["memory_max"]
    if not cap:
        return []
    return ["systemd-run", "--user", "--scope", "--quiet",
            "-p", f"MemoryMax={cap}", "-p", "MemorySwapMax=0"]


def record_pid(run: Run, pid: int, what: str):
    (run.out / "server.pid").write_text(f"{pid}\n")
    cg_path = run.out / "cgroup.path"
    if run.env["memory_max"]:
        cg = cgroup_of(pid)
        cg_path.write_text(f"{cg}\n")
        cap = (cg / "memory.max").read_text().strip()
        run.event(f"started {what} pid {pid} cgroup={cg} memory.max={cap}")
    else:
        cg_path.unlink(missing_ok=True)
        run.event(f"started {what} pid {pid}")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_begin(run: Run):
    stamp = time.strftime("%Y%m%dT%H%MZ", time.gmtime())
    out = run.out_root / f"{run.name}-{stamp}"
    out.mkdir(parents=True)
    (run.out_root / f"{run.name}.latest").write_text(f"{out}\n")
    # `current` follows the newest run of any env. Swapped in by rename,
    # so it never goes missing.
    tmp = run.out_root / ".current.tmp"
    tmp.unlink(missing_ok=True)
    tmp.symlink_to(out.name)
    os.replace(tmp, run.out_root / "current")
    run.out, run.textfile, run.results = out, out / "textfile", \
        out / "results"
    run.server_log = out / run.server_log.name
    if run.data.exists() and any(run.data.iterdir()):
        sys.exit(f"data dir {run.data} is not empty; move it away first")
    for d in (run.data, run.wal, run.results, run.textfile):
        d.mkdir(parents=True, exist_ok=True)
    (out / "env.json").write_text(json.dumps(run.env, indent=2) + "\n")
    (out / "local.json").write_text(json.dumps(run.local, indent=2) + "\n")
    run.event(f"begin env={run.name} data={run.data} wal={run.wal}")
    run.write_monitor_env()
    if not run.no_monitor:
        subprocess.run(["docker", "compose", "up", "-d", "--quiet-pull"],
                       cwd=MONITOR, check=True)
    print(f"run dir: {out}")


def server_running(run: Run) -> bool:
    return alive(run.pid("server.pid"))


def cmd_start(run: Run):
    if server_running(run):
        sys.exit(f"server already running: pid {run.pid('server.pid')}")
    if run.kind == "egres":
        start_egres(run)
    else:
        start_postgres(run)


def start_egres(run: Run):
    srv = run.env["server"]
    if not os.access(run.egres_bin, os.X_OK):
        sys.exit(f"no server binary at {run.egres_bin}; build it or set "
                 "egres_bin in local.toml")
    run.data.mkdir(parents=True, exist_ok=True)
    run.wal.mkdir(parents=True, exist_ok=True)
    # Rotate the previous log rather than append: an incident is easier
    # to read when a log starts at a process start.
    if run.server_log.exists() and run.server_log.stat().st_size:
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        run.server_log.rename(run.out / f"server.{stamp}.log")
    # Raft's port is unused on one node but must not clash with another
    # server on the box; sql_port + 1500 stays clear of the admin port.
    listen = (f"sql-port={srv['sql_port']};client-port={srv['admin_port']};"
              f"raft-port={srv['sql_port'] + 1500}")
    store = srv["store"] or f"dir_path={run.data}"
    cmd = scope_prefix(run) + [
        str(run.egres_bin), "--raft=id=1", f"--listen={listen}",
        f"--store={store}", f"--wal=dir={run.wal}"]
    if srv["zdb_extra"]:
        cmd.append(f"--zdb={srv['zdb_extra']}")
    if srv["memory"]:
        cmd.append(f"--memory={srv['memory']}")
    with open(run.server_log, "w") as log:
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT,
                                stdin=subprocess.DEVNULL,
                                start_new_session=True)
    # Under a scope the child is systemd-run until it execs the server;
    # the server's own pid is the newest egres once it is up.
    time.sleep(1)
    pid = proc.pid
    if run.env["memory_max"]:
        found = subprocess.run(["pgrep", "-n", "-x", "egres"],
                               capture_output=True, text=True).stdout
        pid = int(found.strip() or pid)
    record_pid(run, pid, "egres")
    run.write_monitor_env()
    # Ready = SQL listener up and the single-node election won, the
    # same two lines the e2e scripts wait for.
    for _ in range(120):
        text = run.server_log.read_text(errors="replace")
        if "sql listener started" in text and "become-leader" in text:
            print(f"ready: pid {pid}; log {run.server_log}")
            return
        time.sleep(1)
    sys.exit(f"server not ready after 120s; see {run.server_log}")


def psql_ok(run: Run, conn: str, sql: str = "SELECT 1") -> bool:
    rc = subprocess.run([str(run.psql), conn, "-X", "-q", "-c", sql],
                        capture_output=True).returncode
    return rc == 0


def start_postgres(run: Run):
    srv = run.env["server"]
    if not (run.data / "PG_VERSION").exists():
        # A fresh cluster: checksums on, pg_wal on the WAL root, the
        # env's config appended, no system socket dir needed.
        run.data.parent.mkdir(parents=True, exist_ok=True)
        if run.wal.exists() and not any(run.wal.iterdir()):
            run.wal.rmdir()
        with open(run.out / "initdb.log", "w") as log:
            subprocess.run([str(run.pgbin / "initdb"), "-D", str(run.data),
                            "--waldir", str(run.wal), "--data-checksums"],
                           stdout=log, stderr=subprocess.STDOUT, check=True)
        with open(run.data / "postgresql.conf", "a") as conf:
            conf.write(f"\n# benchmarks/run: {run.name}\n"
                       f"port = {srv['port']}\n"
                       "listen_addresses = '127.0.0.1'\n"
                       "unix_socket_directories = '/tmp'\n"
                       + srv["conf"])
        run.event(f"initdb {run.data} wal={run.wal} checksums=on")
    cmd = scope_prefix(run) + [str(run.pgbin / "pg_ctl"), "-D",
                               str(run.data), "-l", str(run.server_log),
                               "start"]
    subprocess.run(cmd, check=True, capture_output=True)
    pid = int((run.data / "postmaster.pid").read_text().split()[0])
    record_pid(run, pid, "postmaster")
    run.write_monitor_env()
    admin = f"host=127.0.0.1 port={srv['port']} dbname=postgres"
    exists = (f"SELECT 1/count(*) FROM pg_database "
              f"WHERE datname = '{srv['dbname']}'")
    for _ in range(120):
        if psql_ok(run, admin):
            # Asked through the catalog: a connect attempt to a missing
            # database would leave a FATAL line in the server log.
            if not psql_ok(run, admin, exists):
                subprocess.run([str(run.psql), admin, "-X", "-q", "-c",
                                f"CREATE DATABASE {srv['dbname']}"],
                               check=True)
                run.event(f"created database {srv['dbname']}")
            print(f"ready: pid {pid}; log {run.server_log}")
            return
        time.sleep(1)
    sys.exit(f"postgres not ready after 120s; see {run.server_log}")


def detach(run: Run, sub: str, extra: list[str], out_name: str):
    """Re-run this script in the foreground form of `sub`, in its own
    session, so closing the terminal does not end hours of work."""
    cmd = [sys.executable, str(HERE / "bench.py"), run.env_name, sub,
           "--foreground"] + extra
    if run.suffix:
        cmd += ["--name", run.suffix]
    if run.no_monitor:
        cmd.append("--no-monitor")
    for s in run.sets:
        cmd += ["--set", s]
    for s in run.locals:
        cmd += ["--local", s]
    with open(run.out / out_name, "w") as out:
        subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT,
                         stdin=subprocess.DEVNULL, start_new_session=True)


def cmd_load(run: Run, foreground: bool, resume: bool):
    if not foreground:
        # `stop` leaves a STOP file, which would end this load at once.
        (run.out / "STOP").unlink(missing_ok=True)
        detach(run, "load", ["--resume"] if resume else [],
               "load_driver.out")
        time.sleep(2)
        print(f"load driver pid {run.pid('load.pid')}; log "
              f"{run.out / 'load.log'}")
        return
    import loader
    loader.run_load(run, resume)


def cmd_watch(run: Run, foreground: bool, runs_done: int):
    if not foreground:
        detach(run, "watch", ["--runs-done", str(runs_done)], "watch.out")
        time.sleep(2)
        print(f"watchdog pid {run.pid('watch.pid')}; log "
              f"{run.out / 'watch.log'}")
        return
    import watchdog
    watchdog.Watch(run, runs_done).loop()


def cmd_workload(run: Run, run_no: int, secs):
    import pgbench
    res = pgbench.workload(run, run_no, secs or run.env["secs"])
    print(json.dumps(res, indent=2))


def cmd_stop(run: Run):
    for name in ("watch.pid", "load.pid"):
        pid = run.pid(name)
        if alive(pid):
            os.kill(pid, 15)
            print(f"stopped {name} {pid}")
    (run.out / "STOP").touch()
    # pgbench leads its own session; its pid is the group's.
    pid = run.pid("workload.pid")
    if alive(pid):
        try:
            os.killpg(pid, 15)
            print("stopped workload")
        except OSError:
            pass
    stop_server(run)
    run.event("stop by hand")


def stop_server(run: Run):
    pid = run.pid("server.pid")
    if not alive(pid):
        return
    if run.kind == "postgres":
        subprocess.run([str(run.pgbin / "pg_ctl"), "-D", str(run.data),
                        "stop", "-m", "fast"], capture_output=True)
    else:
        os.kill(pid, 15)
    for _ in range(60):
        if not alive(pid):
            break
        time.sleep(1)
    print(f"stopped server {pid}")


def cmd_status(run: Run):
    print(f"run: {run.out}\nnow: {utc()}")
    pid = run.pid("server.pid")
    if alive(pid):
        rss = ""
        if os.path.isdir("/proc"):
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                if line.startswith("VmRSS"):
                    rss = line.split()[1]
        else:
            rss = ps_field(pid, "rss")
        print(f"server: {run.kind} pid {pid} rss_kb {rss}")
    else:
        print("server: NOT RUNNING")
    cg_path = run.out / "cgroup.path"
    if cg_path.exists():
        cg = Path(cg_path.read_text().strip())
        cur = int((cg / "memory.current").read_text()) / GIB
        cap = (cg / "memory.max").read_text().strip()
        cap = f"{int(cap) / GIB:.0f} GiB" if cap != "max" else "no cap"
        oom = [l for l in (cg / "memory.events").read_text().splitlines()
               if l.startswith("oom_kill ")]
        print(f"cgroup: {cur:.1f} GiB of {cap}; {oom[0] if oom else ''}")
    for label, path, n in (("events", run.out / "events.log", 5),
                           ("load", run.out / "load.log", 3),
                           ("watch", run.out / "watch.log", 2),
                           (run.server_log.name, run.server_log, 3)):
        print(f"--- {label}")
        if path.exists():
            print("\n".join(path.read_text(errors="replace")
                            .splitlines()[-n:]))
    print("--- pgbench progress (last 3)")
    lines = []
    for log in sorted(run.results.glob("pgbench-*.log")):
        lines += [l for l in log.read_text(errors="replace").splitlines()
                  if l.startswith("progress")]
    print("\n".join(lines[-3:]))
    print("--- textfile metrics")
    for prom in sorted(run.textfile.glob("*.prom")):
        print("\n".join(l for l in prom.read_text().splitlines()
                        if not l.startswith("#")))
    print("--- disk")
    for d in (run.data, run.wal):
        if d.exists():
            size = subprocess.run(["du", "-sh", str(d)], capture_output=True,
                                  text=True).stdout.strip()
            print(size)
    for d in (run.data, run.wal, run.out):
        if d.exists():
            total, used, free = shutil.disk_usage(d)
            print(f"{d}: {free / GIB:.0f} GiB free of {total / GIB:.0f}")


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "envs":
        for name, e in ENVS.items():
            print(f"{name:18} server={e['server']:9} "
                  f"workload={e['workload']:7} cap={e['memory_max']}")
        return
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("env")
    ap.add_argument("command", choices=["begin", "start", "load", "watch",
                                        "all", "status", "workload",
                                        "stop"])
    ap.add_argument("arg", nargs="*")
    ap.add_argument("--set", action="append", default=[],
                    help="override a resolved setting: load.last=1000")
    ap.add_argument("--local", action="append", default=[],
                    help="override a local.toml setting: wal_root=/data")
    ap.add_argument("--name", help="suffix for the run name (a trial)")
    ap.add_argument("--no-monitor", action="store_true")
    ap.add_argument("--foreground", action="store_true")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--runs-done", type=int, default=0)
    a = ap.parse_args()
    env = resolve_env(a.env, a.set)
    local = load_local()
    for item in a.local:
        key, _, val = item.partition("=")
        if key not in local:
            sys.exit(f"--local: no {key!r} in local.toml")
        local[key] = val
    run = Run(a.env, a.name or "", env, local)
    run.no_monitor = a.no_monitor
    run.sets = a.set
    run.locals = a.local
    match a.command:
        case "begin":
            cmd_begin(run)
        case "start":
            cmd_start(run)
        case "load":
            cmd_load(run, a.foreground, a.resume)
        case "watch":
            cmd_watch(run, a.foreground, a.runs_done)
        case "all":
            cmd_begin(run)
            cmd_start(run)
            cmd_load(run, False, False)
            cmd_watch(run, False, 0)
        case "status":
            cmd_status(run)
        case "workload":
            if not a.arg:
                sys.exit("workload needs a run number")
            cmd_workload(run, int(a.arg[0]),
                         int(a.arg[1]) if len(a.arg) > 1 else None)
        case "stop":
            cmd_stop(run)


if __name__ == "__main__":
    main()
