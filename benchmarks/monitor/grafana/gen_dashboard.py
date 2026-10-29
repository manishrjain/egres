#!/usr/bin/env python3
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""Writes grafana/dashboards/egres.json: one dashboard for a long run.

Hand-editing Grafana JSON is error-prone, so the panels are declared
here as (title, unit, [(expr, legend), ...]) and laid out two across.
Rerun after changing it; Grafana reloads provisioned files on its own.
"""

import json
import os

W, H = 12, 8  # panel width (of 24) and height in grid units


def ts(title, unit, targets, stack=False, fill=10, desc=""):
    """A time-series panel."""
    return {
        "type": "timeseries",
        "title": title,
        "description": desc,
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {
                    "lineWidth": 1,
                    "fillOpacity": fill,
                    "stacking": {"mode": "normal" if stack else "none"},
                    "showPoints": "never",
                },
            },
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "table", "placement": "bottom",
                       "calcs": ["lastNotNull", "max", "mean"]},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
        "targets": [
            {"refId": chr(65 + i), "expr": expr, "legendFormat": legend}
            for i, (expr, legend) in enumerate(targets)
        ],
    }


def stat(title, unit, targets, desc=""):
    """A big-number panel of the latest values."""
    p = ts(title, unit, targets, desc=desc)
    p["type"] = "stat"
    p["options"] = {
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": ""},
        "textMode": "value_and_name",
        "colorMode": "none",
        "graphMode": "area",
    }
    return p


def row(title):
    return {"type": "row", "title": title, "collapsed": False}


EGRES = 'groupname="egres"'
DISK = 'device=~"nvme0n1|sd[a-z]"'

PANELS = [
    row("Throughput"),
    ts("Rows accepted per second", "rowsps", [
        ('sum(rate(egres_load_rows_total[1m]))', "load rows/s"),
        ('sum(pgbench_tps)', "pgbench tps"),
    ], desc="Load phase: rows the load streams committed. Workload "
            "phase: pgbench transactions per second."),
    ts("Admin round trip", "ms", [
        ('egres_admin_seconds * 1000', "admin round trip"),
    ]),
    ts("Insert latency, one statement", "s", [
        ('histogram_quantile(0.5, sum(rate('
         'egres_load_insert_seconds_bucket[1m])) by (le))', "p50"),
        ('histogram_quantile(0.95, sum(rate('
         'egres_load_insert_seconds_bucket[1m])) by (le))', "p95"),
        ('histogram_quantile(0.99, sum(rate('
         'egres_load_insert_seconds_bucket[1m])) by (le))', "p99"),
    ], fill=0, desc="A multi-row INSERT's round trip as psql timed it "
                    "during the load. Quantiles are read off histogram "
                    "buckets; load-summary.md has the exact ones."),
    ts("Query latency by script", "ms", [
        ('pgbench_latency_p50_ms', "{{script}} p50"),
        ('pgbench_latency_p95_ms', "{{script}} p95"),
        ('pgbench_latency_p99_ms', "{{script}} p99"),
        ('pgbench_latency_max_ms', "{{script}} max"),
    ], fill=0, desc="From pgbench's per-transaction log, over the "
                    "transactions of the last few seconds."),
    ts("Queries per second, by pgbench", "none", [
        ('pgbench_tps', "{{run}}"),
    ], fill=0),
    ts("Bytes written per second, by source", "Bps", [
        ('sum(rate(zdb_bytes_written_total[1m])) by (source)', "{{source}}"),
    ], stack=True, desc="user = accepted writes; flush and compact = "
                        "the LSM rewriting them."),
    ts("Write amplification", "none", [
        ('sum(rate(zdb_bytes_written_total[10m])) / '
         'sum(rate(zdb_bytes_written_total{source="user"}[10m]))',
         "total / user (10m)"),
    ], fill=0),
    stat("Totals", "short", [
        ('sum(egres_load_rows_total)', "rows loaded"),
        ('sum(zdb_bytes_written_total{source="user"})', "user bytes"),
        ('egres_data_dir_bytes', "data dir bytes"),
        ('egres_sst_files', "sst files"),
        ('sum(pgbench_failed_total)', "pgbench failed"),
    ]),
    ts("Load streams and pgbench clients", "none", [
        ('egres_load_streams_running', "load streams"),
        ('egres_load_chunks_failed_total', "failed chunks"),
        ('pgbench_clients', "pgbench clients"),
    ], fill=0),

    row("Storage"),
    ts("Data on disk", "bytes", [
        ('egres_data_dir_bytes', "data dir"),
        ('egres_sst_bytes', "sst"),
        ('egres_wal_bytes', "wal"),
    ]),
    ts("SST files", "none", [
        ('egres_sst_files', "files"),
    ], fill=0),
    ts("Disk throughput", "Bps", [
        (f'sum(rate(node_disk_read_bytes_total{{{DISK}}}[1m])) by (device)',
         "read {{device}}"),
        (f'sum(rate(node_disk_written_bytes_total{{{DISK}}}[1m])) by (device)',
         "write {{device}}"),
    ]),
    ts("Disk operations per second", "iops", [
        (f'sum(rate(node_disk_reads_completed_total{{{DISK}}}[1m]))'
         ' by (device)', "read {{device}}"),
        (f'sum(rate(node_disk_writes_completed_total{{{DISK}}}[1m]))'
         ' by (device)', "write {{device}}"),
    ], fill=0, desc="Requests the device completed, after the kernel "
                    "merged adjacent ones."),
    stat("Disk bytes over the shown range", "bytes", [
        (f'sum(increase(node_disk_read_bytes_total{{{DISK}}}[$__range]))'
         ' by (device)', "read {{device}}"),
        (f'sum(increase(node_disk_written_bytes_total{{{DISK}}}[$__range]))'
         ' by (device)', "written {{device}}"),
        (f'increase(namedprocess_namegroup_read_bytes_total{{{EGRES}}}'
         '[$__range])', "read by egres"),
        (f'increase(namedprocess_namegroup_write_bytes_total{{{EGRES}}}'
         '[$__range])', "written by egres"),
    ], desc="Set the time range to the load to read its totals; "
            "load-summary.md has them exactly."),
    ts("Egres process I/O", "Bps", [
        (f'rate(namedprocess_namegroup_read_bytes_total{{{EGRES}}}[1m])',
         "read"),
        (f'rate(namedprocess_namegroup_write_bytes_total{{{EGRES}}}[1m])',
         "write"),
    ]),
    ts("Disk busy", "percentunit", [
        (f'rate(node_disk_io_time_seconds_total{{{DISK}}}[1m])',
         "{{device}}"),
    ], fill=0),
    ts("Filesystem free", "bytes", [
        ('node_filesystem_avail_bytes{fstype=~"ext4|xfs|zfs|btrfs"}',
         "{{mountpoint}}"),
    ], fill=0),
    ts("Flushes and compactions per minute", "none", [
        ('sum(rate(zdb_flushes_total[5m])) * 60', "flushes"),
        ('sum(rate(zdb_compactions_total[5m])) * 60', "compactions"),
    ], fill=0),

    row("Egres internals"),
    ts("L0 tables", "none", [
        ('egres_l0_tables', "all trees"),
        ('egres_l0_tables_max_tree', "busiest tree"),
        ('egres_inflight_jobs', "in-flight jobs"),
    ], fill=0, desc="Backpressure starts at 32 tables in one tree."),
    ts("Write backpressure", "none", [
        ('rate(zdb_write_backpressure_ms_sum[1m]) / 1000',
         "seconds slept per second, all writers"),
        ('rate(zdb_write_backpressure_ms_count[1m])',
         "braked batches per second"),
        ('rate(zdb_write_backpressure_ms_sum[1m]) / '
         'rate(zdb_write_backpressure_ms_count[1m])',
         "mean sleep per braked batch, ms"),
    ], fill=0, desc="A write batch sleeps before it is proposed when a "
                    "tree it writes to has over 32 L0 tables, or over 8 "
                    "memtables wait for a flush: 20 ms per table or "
                    "memtable over, 500 ms at most. Batches that did "
                    "not sleep are not counted."),
    ts("I/O limiter waits", "none", [
        ('sum by (priority) (rate(vfs_bandwidth_wait_us_total[1m])) / 1000',
         "{{priority}} ms slept per second"),
        ('sum by (priority) (rate(vfs_bandwidth_timeouts_total[1m]))',
         "{{priority}} timeouts per second"),
    ], fill=0, desc="Time I/O spent waiting for the bandwidth limiter's "
                    "pages, by priority: p0 reads never wait, p1 is the "
                    "WAL and L0 work, p2 the L1+ compactions. A timeout "
                    "is a grant that waited 100 ms and took the minimum."),
    ts("Memtables", "bytes", [
        ('egres_memtable_active_bytes', "active"),
        ('egres_memtable_immutable_bytes', "immutable, pending flush"),
        ('egres_l0_bytes', "L0 bytes"),
    ], fill=0),
    ts("Block cache", "bytes", [
        ('egres_block_cache_bytes', "bytes"),
        ('egres_largest_tree_bytes', "largest tree"),
    ], fill=0),
    ts("Block cache hit ratio and trees", "none", [
        ('rate(egres_block_cache_hits_total[5m]) / '
         '(rate(egres_block_cache_hits_total[5m]) + '
         'rate(egres_block_cache_misses_total[5m]))', "hit ratio (5m)"),
        ('egres_trees', "trees"),
        ('egres_memtable_immutable_count', "immutable memtables"),
    ], fill=0),

    row("Postgres (P1 run)"),
    ts("Commits and rollbacks per second", "none", [
        ('sum(rate(pg_stat_database_xact_commit{datname="big"}[1m]))',
         "commits/s"),
        ('sum(rate(pg_stat_database_xact_rollback{datname="big"}[1m]))',
         "rollbacks/s"),
    ], fill=0),
    ts("Database and WAL size", "bytes", [
        ('pg_database_size_bytes{datname="big"}', "big"),
        ('egres_wal_bytes', "wal dir (bridge)"),
    ]),
    ts("Buffer cache hit ratio and checkpoints", "none", [
        ('rate(pg_stat_database_blks_hit{datname="big"}[5m]) / '
         '(rate(pg_stat_database_blks_hit{datname="big"}[5m]) + '
         'rate(pg_stat_database_blks_read{datname="big"}[5m]))',
         "hit ratio (5m)"),
        ('rate(pg_stat_checkpointer_num_timed[5m]) * 60', "timed ckpt/min"),
        ('rate(pg_stat_checkpointer_num_requested[5m]) * 60',
         "requested ckpt/min"),
    ], fill=0),
    ts("Postgres process", "none", [
        ('rate(namedprocess_namegroup_cpu_seconds_total'
         '{groupname="postgres"}[1m])', "cores"),
        ('namedprocess_namegroup_memory_bytes'
         '{groupname="postgres",memtype="resident"} / 2^30', "RSS GiB"),
        ('namedprocess_namegroup_num_procs{groupname="postgres"}',
         "processes"),
    ], fill=0),

    row("Host"),
    ts("CPU", "percent", [
        ('(1 - sum(rate(node_cpu_seconds_total{mode=~"idle|iowait"}[1m]))'
         ' / count(node_cpu_seconds_total{mode="idle"})) * 100',
         "host cpu %"),
        (f'sum(rate(namedprocess_namegroup_cpu_seconds_total{{{EGRES}}}'
         '[1m])) * 100 / 32', "egres % of 32 threads"),
        ('avg(rate(node_cpu_seconds_total{mode="iowait"}[1m])) * 100',
         "iowait %"),
    ], fill=0, desc="host cpu is user and system time. Waiting on I/O "
                    "is its own line: a CPU with nothing to run while "
                    "a disk request is out, which is not work."),
    ts("Memory", "bytes", [
        ('egres_cgroup_memory_bytes', "server cgroup used"),
        ('egres_cgroup_memory_max_bytes', "server cgroup cap"),
        (f'namedprocess_namegroup_memory_bytes{{{EGRES},memtype="resident"}}',
         "egres RSS"),
        ('node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes',
         "host used"),
        ('node_memory_Cached_bytes', "page cache"),
        ('node_memory_MemTotal_bytes', "total"),
    ], fill=0),
    ts("Egres memory: live vs allocator", "bytes", [
        ('egres_mem_live_bytes', "live (held by egres)"),
        ('egres_mem_block_cache_bytes', "block cache"),
        ('egres_governor_cache_cap_bytes', "cache cap"),
        ('egres_governor_margin_bytes', "margin"),
        ('egres_governor_total_bytes', "total (live)"),
        ('egres_jemalloc_resident_bytes', "jemalloc resident"),
        ('egres_cgroup_memory_max_bytes', "cgroup cap"),
    ], fill=0, desc="The cache cap is the total (--memory total_bytes, live "
                    "bytes) less the margin, the recent peak of what the "
                    "rest of egres holds. jemalloc resident is what the "
                    "cgroup charges for the heap; the gap above live is "
                    "the allocator's overhead, which the governor measures "
                    "and keeps total (resident) under the cap."),
    ts("Allocator overhead", "none", [
        ('egres_jemalloc_resident_bytes / egres_mem_live_bytes',
         "jemalloc resident / live"),
        ('egres_governor_ratio', "governor's ratio (recent peak)"),
    ], fill=0, desc="1.0 is no overhead. The governor allows total / "
                    "ratio live bytes, the ratio being the recent peak "
                    "of resident / live, so resident stays near total."),
    ts("Egres threads and open files", "none", [
        (f'namedprocess_namegroup_num_threads{{{EGRES}}}', "threads"),
        (f'namedprocess_namegroup_open_filedesc{{{EGRES}}}', "open fds"),
    ], fill=0),
    ts("Load 1m and up", "none", [
        ('node_load1', "load1"),
        ('egres_up', "egres up"),
    ], fill=0),
]


def layout(panels):
    out, x, y, pid = [], 0, 0, 1
    for p in panels:
        p = dict(p)
        p["id"] = pid
        pid += 1
        if p["type"] == "row":
            if x:
                y += H
                x = 0
            p["gridPos"] = {"x": 0, "y": y, "w": 24, "h": 1}
            y += 1
        else:
            p["gridPos"] = {"x": x, "y": y, "w": W, "h": H}
            x += W
            if x >= 24:
                x = 0
                y += H
        out.append(p)
    return out


def main():
    dash = {
        "uid": "egres-run",
        "title": "Egres run",
        "timezone": "browser",
        "refresh": "30s",
        "time": {"from": "now-6h", "to": "now"},
        "schemaVersion": 39,
        "version": 1,
        "editable": True,
        "panels": layout(PANELS),
    }
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(here, "dashboards", "egres.json")
    with open(path, "w") as f:
        json.dump(dash, f, indent=2)
        f.write("\n")
    print(f"wrote {path} ({len(dash['panels'])} panels)")


if __name__ == "__main__":
    main()
