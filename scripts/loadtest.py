#!/usr/bin/env python3
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""Load-test driver for a local zdb Raft cluster.

Spawns N `egres` processes, bootstraps namespaces, drives the `zdb-bench`
load generator, and samples each node's `/stats` into a metrics TSV.

Subcommands (default `run`):

    loadtest.py run --nodes 1 --duration 30m   # single-node 30-min run
    loadtest.py stats --node 2                  # one-shot stats dump
    loadtest.py clean                           # wipe data/logs/metrics

Server and bench binaries are taken from `<repo>/zig-out/bin`; only the
writable workdir (data/, logs/, metrics.tsv) is configurable via --workdir.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("loadtest")

REPO_ROOT = Path(__file__).resolve().parent.parent
SERVER_BIN = REPO_ROOT / "zig-out" / "bin" / "egres"
BENCH_BIN = REPO_ROOT / "zig-out" / "bin" / "zdb-bench"

METRICS_HEADER = (
    "ts_unix\tnode\tflushes\tcompactions\t"
    "user_bytes\tflush_bytes\tcompact_bytes\tdisk_bytes\n"
)


@dataclass(frozen=True)
class Ports:
    """Port bases shared by `run` (to spawn) and `stats` (to connect).

    Node i listens on raft `raft_base + i` and client `client_base + i`
    (the server applies `offset=i` to both bases).
    """

    raft_base: int = 4210
    client_base: int = 5210

    def raft(self, node: int) -> int:
        return self.raft_base + node

    def client(self, node: int) -> int:
        return self.client_base + node


# --- TCP text protocol ------------------------------------------------------


def _connect(ports: Ports, node: int, timeout: float = 5.0) -> socket.socket:
    return socket.create_connection(("127.0.0.1", ports.client(node)), timeout)


def send_recv_line(ports: Ports, node: int, cmd: str) -> str:
    """Send one command line, return one response line (newline-stripped)."""
    with _connect(ports, node) as sock:
        sock.sendall(cmd.encode())
        line = sock.makefile("r", errors="replace").readline()
    if not line:
        raise ConnectionError("peer closed before responding")
    return line.rstrip("\r\n")


def admin_dump(ports: Ports, node: int, command: str) -> str:
    """Send a one-line admin command, read its multi-line response up to the
    blank-line terminator (e.g. `zdb` stats, `workers`)."""
    with _connect(ports, node) as sock:
        sock.sendall(f"{command}\n".encode())
        lines = []
        for raw in sock.makefile("r", errors="replace"):
            line = raw.rstrip("\r\n")
            if line == "":  # blank line terminates the dump
                break
            lines.append(line)
    return "\n".join(lines)


def fetch_stats(ports: Ports, node: int) -> str:
    return admin_dump(ports, node, "zdb metrics")


def sum_metric(blob: str, prefix: str, label: str | None = None) -> int:
    """Sum the values of Prometheus lines named `prefix` (optionally filtered
    to those containing `label`). Flat sum — no real label parser needed."""
    total = 0
    for line in blob.splitlines():
        # The char after the name must be `{` or ` ` so `foo_total` does not
        # also match `foo_total_bytes`.
        if not line.startswith(prefix) or line[len(prefix) : len(prefix) + 1] not in ("{", " "):
            continue
        if label is not None and label not in line:
            continue
        try:
            total += int(line.rsplit(" ", 1)[1])
        except (ValueError, IndexError):
            continue
    return total


def run_until_ok(ports: Ports, cmd: str, what: str, timeout: float) -> None:
    """Retry `cmd` against the current leader until it answers OK, following
    NOT_LEADER redirects. Raises TimeoutError past `timeout` seconds."""
    leader = 1
    deadline = time.monotonic() + timeout
    while time.monotonic() <= deadline:
        try:
            resp = send_recv_line(ports, leader, cmd)
        except OSError:
            time.sleep(0.5)
            continue
        if resp.startswith("OK"):
            log.info("%s: ok (node %d)", what, leader)
            return
        if resp.startswith("NOT_LEADER "):
            new_leader = _parse_int(resp[len("NOT_LEADER ") :])
            if new_leader and new_leader != leader:
                leader = new_leader
                log.info("%s: redirected to leader %d", what, leader)
            time.sleep(0.2)
        else:
            time.sleep(0.5)
    raise TimeoutError(f"{what} timed out after {timeout:g}s")


def _parse_int(text: str) -> int:
    try:
        return int(text.strip())
    except ValueError:
        return 0


# --- process management -----------------------------------------------------


@dataclass
class Server:
    node: int
    proc: subprocess.Popen
    log_file: object  # open file handle backing the child's stdout/stderr


def spawn_server(
    ports: Ports, data_dir: Path, log_path: Path, node: int, seed: str | None
) -> Server:
    """Launch one egres, redirecting stdout+stderr into `log_path`.
    `seed` is empty for node 1 (self-bootstrap) and the seed raft addr
    otherwise (the joiner learns the cluster from it)."""
    data_dir.mkdir(parents=True, exist_ok=True)
    raft = f"id={node}; cluster-id=1; tick-ms=20"
    if seed:
        raft += f"; seeds={seed}"
    argv = [
        str(SERVER_BIN),
        f"--listen=raft-port={ports.raft_base}; client-port={ports.client_base}; offset={node}",
        f"--raft={raft}",
        # One store shared by every node (followers read the leader's
        # SSTs from it); the WAL stays per node.
        f"--store=dir_path={data_dir.parent / 'sst'}",
        f"--wal=dir={data_dir / 'wal'}",
    ]
    log_file = open(log_path, "w")
    # start_new_session detaches the server into its own session so it
    # survives the driver exiting (needed for --keep-alive) and isn't killed
    # by a Ctrl-C / terminal-close aimed at the driver. Orderly teardown still
    # works: stop_server sends SIGTERM straight to the pid.
    proc = subprocess.Popen(
        argv, stdin=subprocess.DEVNULL, stdout=log_file, stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    log.info("node %d started pid=%d client=127.0.0.1:%d", node, proc.pid, ports.client(node))
    return Server(node, proc, log_file)


def stop_server(srv: Server, grace: float) -> None:
    """SIGTERM, then SIGKILL if it overstays the grace window."""
    srv.proc.terminate()
    try:
        srv.proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        srv.proc.kill()
        srv.proc.wait()
    srv.log_file.close()


def kill_stale() -> None:
    """Best-effort SIGKILL of leftover servers/benches from a crashed run.
    Matches the exact process name (`-x`, not `-f`) so this script — whose
    own argv mentions the binaries — never targets itself."""
    for name in ("egres", "zdb-bench"):
        subprocess.run(["pkill", "-9", "-x", name], check=False)
        log.info("killed any stale %s", name)
    time.sleep(0.5)


# --- filesystem helpers -----------------------------------------------------


def dir_size_bytes(path: Path) -> int:
    if not path.is_dir():
        return 0
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def clean_workdir(work_root: Path, keep_logs: bool = False) -> None:
    targets = [work_root / "data", work_root / "metrics.tsv"]
    if not keep_logs:
        targets.append(work_root / "logs")
    for t in targets:
        if t.is_dir():
            shutil.rmtree(t, ignore_errors=True)
        else:
            t.unlink(missing_ok=True)
    log.info("cleaned %s (keep_logs=%s)", work_root, keep_logs)


# --- monitor ----------------------------------------------------------------


def monitor_loop(
    ports: Ports,
    nodes: int,
    data_root: Path,
    metrics_path: Path,
    interval: float,
    stop: threading.Event,
) -> None:
    """Sample every node's stats into `metrics_path` until `stop` is set.
    `stop.wait` makes the sleep interruptible, so teardown is prompt."""
    with open(metrics_path, "w") as f:
        f.write(METRICS_HEADER)
        while not stop.is_set():
            now = int(time.time())
            for node in range(1, nodes + 1):
                try:
                    blob = fetch_stats(ports, node)
                except OSError:
                    continue
                row = (
                    now,
                    node,
                    sum_metric(blob, "zdb_flushes_total"),
                    sum_metric(blob, "zdb_compactions_total"),
                    sum_metric(blob, "zdb_bytes_written_total", 'source="user"'),
                    sum_metric(blob, "zdb_bytes_written_total", 'source="flush"'),
                    sum_metric(blob, "zdb_bytes_written_total", 'source="compact"'),
                    dir_size_bytes(data_root / f"node{node}"),
                )
                f.write("\t".join(map(str, row)) + "\n")
                f.flush()
            stop.wait(interval)


# --- subcommands ------------------------------------------------------------


def cmd_run(args: argparse.Namespace) -> None:
    ports = Ports(args.raft_port_base, args.client_port_base)
    work = args.workdir or REPO_ROOT
    data_root, log_root = work / "data", work / "logs"

    if args.kill_existing:
        kill_stale()
    clean_workdir(work)
    data_root.mkdir(parents=True, exist_ok=True)
    log_root.mkdir(parents=True, exist_ok=True)

    servers: list[Server] = []
    stop = threading.Event()
    monitor: threading.Thread | None = None
    keep = False  # set true only on a clean finish when --keep-alive is given
    try:
        seed = f"127.0.0.1:{ports.raft(1)}"
        for node in range(1, args.nodes + 1):
            servers.append(
                spawn_server(
                    ports,
                    data_root / f"node{node}",
                    log_root / f"node{node}.log",
                    node,
                    None if node == 1 else seed,
                )
            )
            if node == 1:
                time.sleep(1)  # let the seed elect itself before joiners attach

        time.sleep(1)  # let Raft settle
        promote_learners(ports, args.nodes, args.bootstrap_timeout)
        bootstrap_namespaces(ports, args.namespaces, args.bootstrap_timeout)

        monitor = threading.Thread(
            target=monitor_loop,
            args=(ports, args.nodes, data_root, work / "metrics.tsv",
                  args.monitor_interval, stop),
            daemon=True,
        )
        monitor.start()

        bench = start_bench(ports, args)
        wait_for_bench(bench, args.duration_ms / 1000)
        keep = args.keep_alive  # reached only after a successful load
    finally:
        stop.set()
        if monitor is not None:
            monitor.join(timeout=5)
        print_worker_cpu(ports, args.nodes)
        dump_final_stats(ports, args.nodes, log_root)
        if keep:
            # Leave the servers running so they can be inspected. They were
            # spawned with start_new_session, so they outlive this process.
            log.info("--keep-alive: leaving %d server(s) running:", len(servers))
            for srv in servers:
                log.info("  node %d → client 127.0.0.1:%d (pid %d)",
                         srv.node, ports.client(srv.node), srv.proc.pid)
            log.info("inspect e.g. `workers` / `zdb` on those ports; "
                     "stop with: pkill -TERM -x egres")
        else:
            for srv in servers:
                stop_server(srv, args.stop_grace)

    log.info("run complete; metrics at %s", work / "metrics.tsv")


def promote_learners(ports: Ports, nodes: int, timeout: float) -> None:
    """Promote joiners (nodes 2..N) from learner to voter so they count
    toward quorum. No-op for a single-node cluster."""
    for node in range(2, nodes + 1):
        run_until_ok(ports, f"conf to_voter {node}\n", f"promote node {node}", timeout)


# Namespaces below 16 are reserved and 16 is the `egres` database genesis
# creates; the benchmark's namespaces start after it (benchmark.zig).
FIRST_BENCH_NAMESPACE = 17


def bootstrap_namespaces(ports: Ports, count: int, timeout: float) -> None:
    for ns in range(FIRST_BENCH_NAMESPACE, FIRST_BENCH_NAMESPACE + count):
        run_until_ok(ports, f"ns create {ns}\n", f"create ns {ns}", timeout)


def start_bench(ports: Ports, args: argparse.Namespace) -> subprocess.Popen:
    payload = "; ".join(
        (
            f"addr=127.0.0.1:{ports.client(1)}",
            f"clients={args.clients}",
            f"verify-clients={args.verify_clients}",
            f"records={args.records}",
            f"value-size={args.value_size}",
            f"namespaces={args.namespaces}",
            f"batch-size={args.batch_size}",
            "phase=load",
        )
    )
    log.info("starting bench: %s", payload)
    return subprocess.Popen([str(BENCH_BIN), f"--bench={payload}"])


def wait_for_bench(bench: subprocess.Popen, duration: float) -> None:
    """Block until the bench finishes on its own or the duration elapses,
    whichever comes first; SIGTERM it on timeout."""
    deadline = time.monotonic() + duration
    while True:
        try:
            bench.wait(timeout=1)
            return  # bench finished its workload
        except subprocess.TimeoutExpired:
            pass
        if time.monotonic() >= deadline:
            log.info("duration reached, stopping bench")
            bench.terminate()
            bench.wait()
            return


def print_worker_cpu(ports: Ports, nodes: int) -> None:
    """Print each node's worker-CPU histogram — how evenly the fiber runtime
    spread the load's work across its workers (sampled before teardown)."""
    for node in range(1, nodes + 1):
        try:
            blob = admin_dump(ports, node, "workers")
        except OSError:
            continue
        print(f"\n=== worker CPU — node {node} ===\n{blob}")


def dump_final_stats(ports: Ports, nodes: int, log_root: Path) -> None:
    """Write each node's final stats blob next to its log — the histograms
    the TSV monitor doesn't track stay visible post-run."""
    for node in range(1, nodes + 1):
        try:
            blob = fetch_stats(ports, node)
        except OSError:
            continue
        path = log_root / f"stats-node{node}.txt"
        path.write_text(blob)
        log.info("final stats dumped: %s", path)


def cmd_stats(args: argparse.Namespace) -> None:
    ports = Ports(args.raft_port_base, args.client_port_base)
    print(fetch_stats(ports, args.node))


def cmd_clean(args: argparse.Namespace) -> None:
    clean_workdir(args.workdir or REPO_ROOT)


# --- CLI --------------------------------------------------------------------

_UNIT_MS = {"ms": 1, "s": 1_000, "m": 60_000, "h": 3_600_000}


def duration_ms(text: str) -> int:
    """Parse '30s' / '5m' / '2h' / '500ms' / bare-milliseconds into ms."""
    text = text.strip()
    for suffix, mult in _UNIT_MS.items():  # 'ms' before 's' so it wins
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * mult)
    return int(text)


def workdir(text: str) -> Path:
    return Path(text).resolve()


def build_parser() -> argparse.ArgumentParser:
    # Options shared by every subcommand (port bases + writable workdir).
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--raft-port-base", type=int, default=9000)
    common.add_argument("--client-port-base", type=int, default=8000)
    common.add_argument("--workdir", type=workdir, default=None,
                        help="holds data/, logs/, metrics.tsv (default: repo root)")

    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd")

    run = sub.add_parser("run", parents=[common], help="full end-to-end run")
    run.add_argument("--nodes", type=int, default=3)
    run.add_argument("--namespaces", type=int, default=1)
    run.add_argument("--clients", type=int, default=16)
    run.add_argument("--verify-clients", type=int, default=0)
    run.add_argument("--records", type=int, default=100_000_000)
    run.add_argument("--value-size", type=int, default=1024)
    run.add_argument("--batch-size", type=int, default=256)
    run.add_argument("--duration", dest="duration_ms", type=duration_ms, default="6m",
                     help="wall-clock budget (30s / 5m / 2h / bare ms)")
    run.add_argument("--monitor-interval", type=float, default=10.0, help="seconds")
    run.add_argument("--bootstrap-timeout", type=float, default=30.0, help="seconds")
    run.add_argument("--stop-grace", type=float, default=10.0, help="seconds")
    run.add_argument("--kill-existing", action="store_true",
                     help="SIGKILL stale egres/zdb-bench first")
    run.add_argument("--keep-alive", action="store_true",
                     help="leave the server(s) running after the load (to inspect)")
    run.set_defaults(func=cmd_run)

    stats = sub.add_parser("stats", parents=[common], help="dump one node's stats")
    stats.add_argument("--node", type=int, default=1)
    stats.set_defaults(func=cmd_stats)

    clean = sub.add_parser("clean", parents=[common], help="wipe data/logs/metrics")
    clean.set_defaults(func=cmd_clean)

    return parser


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    # Subcommand is optional and defaults to `run`; anything that is not a
    # known verb (e.g. a bare `--nodes 1`) is treated as `run`'s arguments.
    argv = sys.argv[1:]
    if not argv or (argv[0] not in {"run", "stats", "clean"} and not argv[0].startswith("-h")):
        argv = ["run", *argv]

    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except (TimeoutError, FileNotFoundError) as err:
        log.error("%s", err)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
