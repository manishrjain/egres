#!/usr/bin/env python3
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""The charts: one per test, every side a line, so the behaviour along
the axis leads to the final number. Reads the result directories run.sh
leaves (command.txt for the test and its settings, the test's CSV) and
writes noisy.svg, footprint.svg and agents.svg. Plain SVG, no library,
so it runs where the tests run and a page embeds the files as they
are.
    ./chart.py OUT_DIR RESULT_DIR...    e.g. $WORK/results/suite-*/*
Of several runs of one test on one side, the latest is drawn (its
status.txt start). A micro-VM agents run with WAKE=boot is skipped
when a restore run exists; pgsolo is not drawn.
"""
import csv
import math
import os
import sys

# The sides, in the order they are drawn and coloured. The colours are
# the first three slots of a palette validated for colour-blind
# readers, with the text in text colours, never a series colour.
SIDES = [
    ("egres", "Egres", "#2a78d6"),
    ("vm", "Micro-VMs", "#eb6834"),
    ("pgshared", "Shared Postgres", "#1baf7a"),
]
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
GRID = "#e6e5e1"
FONT = "font-family=\"system-ui, -apple-system, 'Segoe UI', sans-serif\""


def read_run(d):
    """A run: its test, settings and rows. None when d is not one."""
    cmd = os.path.join(d, "command.txt")
    if not os.path.isfile(cmd):
        return None
    run = {"dir": d, "settings": {}}
    for line in open(cmd):
        line = line.rstrip("\n")
        if line.startswith("test: "):
            run["test"] = line[6:]
        elif line.startswith("args: "):
            run["args"] = line[6:]
        elif "=" in line:
            k, v = line.split("=", 1)
            run["settings"][k] = v
    if "test" not in run:
        return None
    run["backend"] = run["settings"].get("BACKEND", "")
    run["start"] = ""
    st = os.path.join(d, "status.txt")
    if os.path.isfile(st):
        for line in open(st):
            if line.startswith("start: "):
                run["start"] = line[7:].strip()
    f = os.path.join(d, run["test"] + ".csv")
    if not os.path.isfile(f):
        return None
    with open(f) as fh:
        run["rows"] = list(csv.DictReader(fh))
    return run


def pick_runs(dirs):
    """The run to draw per (test, side): the latest, restore over boot."""
    best = {}
    for d in dirs:
        run = read_run(d)
        if not run or run["backend"] not in [s[0] for s in SIDES]:
            continue
        if run["test"] == "agents" and run["backend"] == "vm" and \
                run["settings"].get("WAKE", "restore") != "restore":
            continue
        key = (run["test"], run["backend"])
        if key not in best or run["start"] > best[key]["start"]:
            best[key] = run
    return best


def num(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


class Panel:
    """One plot: an x and a y scale into a box, axes, grid, lines."""

    def __init__(self, x0, y0, w, h, xs, ys, xlog, ylog, title, ylabel,
                 xlabel):
        self.x0, self.y0, self.w, self.h = x0, y0, w, h
        self.xlog, self.ylog = xlog, ylog
        self.xmin, self.xmax = min(xs), max(xs)
        self.ymin, self.ymax = min(ys), max(ys)
        if ylog:
            self.ymin = 10 ** math.floor(math.log10(max(self.ymin, 1e-9)))
            self.ymax = 10 ** math.ceil(math.log10(self.ymax * 1.0001))
        else:
            self.ymin = 0
            self.ymax *= 1.08
        self.title, self.ylabel, self.xlabel = title, ylabel, xlabel
        self.out = []
        self.labels = []

    def sx(self, x):
        if self.xlog:
            a, b = math.log10(self.xmin), math.log10(self.xmax)
            t = (math.log10(x) - a) / (b - a) if b > a else 0.5
        else:
            t = (x - self.xmin) / (self.xmax - self.xmin) if \
                self.xmax > self.xmin else 0.5
        return self.x0 + t * self.w

    def sy(self, y):
        if self.ylog:
            a, b = math.log10(self.ymin), math.log10(self.ymax)
            t = (math.log10(max(y, self.ymin)) - a) / (b - a)
        else:
            t = (y - self.ymin) / (self.ymax - self.ymin)
        return self.y0 + self.h - t * self.h

    def yticks(self):
        if self.ylog:
            e = int(math.log10(self.ymin))
            ticks = []
            while 10 ** e <= self.ymax:
                ticks.append(10 ** e)
                e += 1
            return ticks
        step = nice(self.ymax / 5)
        return [i * step for i in range(int(self.ymax / step) + 1)]

    def frame(self, xticks, xfmt):
        o = self.out
        o.append(f'<text x="{self.x0}" y="{self.y0 - 14}" fill="{INK}" '
                 f'font-size="14" font-weight="600" {FONT}>{self.title}</text>')
        for y in self.yticks():
            py = self.sy(y)
            o.append(f'<line x1="{self.x0}" x2="{self.x0 + self.w}" '
                     f'y1="{py:.1f}" y2="{py:.1f}" stroke="{GRID}" '
                     f'stroke-width="1"/>')
            o.append(f'<text x="{self.x0 - 8}" y="{py + 4:.1f}" fill="{INK2}" '
                     f'font-size="11" text-anchor="end" {FONT}>{fmt(y)}</text>')
        for x in xticks:
            px = self.sx(x)
            o.append(f'<text x="{px:.1f}" y="{self.y0 + self.h + 16}" '
                     f'fill="{INK2}" font-size="11" text-anchor="middle" '
                     f'{FONT}>{xfmt(x)}</text>')
        o.append(f'<line x1="{self.x0}" x2="{self.x0 + self.w}" '
                 f'y1="{self.y0 + self.h}" y2="{self.y0 + self.h}" '
                 f'stroke="{INK2}" stroke-width="1"/>')
        o.append(f'<text x="{self.x0 - 8}" y="{self.y0 - 14}" fill="{INK2}" '
                 f'font-size="11" text-anchor="end" {FONT}>{self.ylabel}'
                 f'</text>')
        o.append(f'<text x="{self.x0 + self.w}" y="{self.y0 + self.h + 32}" '
                 f'fill="{INK2}" font-size="11" text-anchor="end" {FONT}>'
                 f'{self.xlabel}</text>')

    def rule(self, y, label):
        """A horizontal reference, such as a stop threshold or a budget."""
        py = self.sy(y)
        self.out.append(
            f'<line x1="{self.x0}" x2="{self.x0 + self.w}" y1="{py:.1f}" '
            f'y2="{py:.1f}" stroke="{INK2}" stroke-width="1" '
            f'stroke-dasharray="4 4"/>')
        self.out.append(
            f'<text x="{self.x0 + 4}" y="{py - 5:.1f}" fill="{INK2}" '
            f'font-size="11" {FONT}>{label}</text>')

    def line(self, pts, colour, label, end_note=None):
        """One side: a 2px line, 8px markers, its name and last value
        at the end. pts are (x, y, tooltip)."""
        if not pts:
            return
        path = " ".join(
            f'{"M" if i == 0 else "L"}{self.sx(x):.1f},{self.sy(y):.1f}'
            for i, (x, y, _) in enumerate(pts))
        o = self.out
        o.append(f'<path d="{path}" fill="none" stroke="{colour}" '
                 f'stroke-width="2" stroke-linejoin="round"/>')
        for x, y, tip in pts:
            o.append(f'<circle cx="{self.sx(x):.1f}" cy="{self.sy(y):.1f}" '
                     f'r="4" fill="{colour}" stroke="{SURFACE}" '
                     f'stroke-width="2"><title>{tip}</title></circle>')
        x, y, _ = pts[-1]
        note = f'{label} {fmt(y)}' if end_note is None else end_note
        self.labels.append([self.sx(x) + 8, self.sy(y) + 4, note])

    def end_labels(self):
        """The lines' names at their ends, 14px apart at least: a
        label that would sit on another is moved down."""
        self.labels.sort(key=lambda l: l[1])
        for i in range(1, len(self.labels)):
            if self.labels[i][1] - self.labels[i - 1][1] < 14:
                self.labels[i][1] = self.labels[i - 1][1] + 14
        for x, y, note in self.labels:
            self.out.append(f'<text x="{x:.1f}" y="{y:.1f}" fill="{INK}" '
                            f'font-size="11" {FONT}>{note}</text>')


def nice(v):
    e = 10 ** math.floor(math.log10(v))
    for m in (1, 2, 5, 10):
        if m * e >= v:
            return m * e
    return 10 * e


def fmt(v):
    if v >= 1e6:
        return f"{v / 1e6:.3g}M"
    if v >= 1000:
        return f"{v / 1000:.3g}k" if v < 10000 else f"{v / 1000:.0f}k"
    if v >= 100:
        return f"{v:.0f}"
    if v >= 1:
        return f"{v:.3g}"
    return f"{v:.2g}"


def svg(width, height, panels, title, subtitle, legend_sides):
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
         f'height="{height}" viewBox="0 0 {width} {height}">',
         f'<rect width="{width}" height="{height}" fill="{SURFACE}"/>',
         f'<text x="24" y="30" fill="{INK}" font-size="18" font-weight="600" '
         f'{FONT}>{title}</text>',
         f'<text x="24" y="50" fill="{INK2}" font-size="12" {FONT}>{subtitle}'
         f'</text>']
    x = 24
    for key, label, colour in legend_sides:
        o.append(f'<rect x="{x}" y="64" width="12" height="12" rx="2" '
                 f'fill="{colour}"/>')
        o.append(f'<text x="{x + 18}" y="74" fill="{INK}" font-size="12" '
                 f'{FONT}>{label}</text>')
        x += 18 + 7 * len(label) + 24
    for p in panels:
        p.end_labels()
        o.extend(p.out)
    o.append("</svg>")
    return "\n".join(o)


def sides_in(runs, test):
    return [s for s in SIDES if (test, s[0]) in runs]


def chart_noisy(runs, out):
    """Quiet tenants' p95 against the heavy count, writers and scanners,
    and the writers' own throughput."""
    sides = sides_in(runs, "noisy")
    if not sides:
        return
    heavies = sorted({int(r["heavy"]) for s in sides
                      for r in runs[("noisy", s[0])]["rows"]})
    xpos = {h: i for i, h in enumerate(heavies)}
    W, PW, PH = 1060, 760, 220
    panels = []
    specs = [("write", "p95_ms", "Quiet tenants' p95 beside heavy writers",
              "ms", True),
             ("cpu", "p95_ms", "Quiet tenants' p95 beside heavy scanners",
              "ms", True),
             ("write", "heavy_done", "The heavy writers' own statements in "
              "the minute", "statements", True),
             ("write", "write_mib", "Disk written over the writers' step",
              "MiB", True)]
    for i, (kind, col, title, ylabel, ylog) in enumerate(specs):
        ys = [num(r.get(col)) for s in sides
              for r in runs[("noisy", s[0])]["rows"]
              if r["kind"] == kind and num(r.get(col))]
        if not ys:
            continue
        p = Panel(90, 110 + i * (PH + 70), PW, PH, list(xpos.values()), ys,
                  False, ylog, title, ylabel, "heavy tenants beside 50 quiet")
        p.frame(list(xpos.values()), lambda x: str(heavies[int(x)]))
        for key, label, colour in sides:
            pts = [(xpos[int(r["heavy"])], num(r.get(col)),
                    f'{label}, {r["heavy"]} heavy: {r.get(col)} {ylabel}')
                   for r in runs[("noisy", key)]["rows"]
                   if r["kind"] == kind and num(r.get(col))]
            p.line(pts, colour, label)
        panels.append(p)
    sub = ("50 quiet tenants each send a point read a second; the heavy ones"
           " run one statement flat out on 4 connections. Lower is better.")
    open(os.path.join(out, "noisy.svg"), "w").write(
        svg(W, 110 + len(panels) * (PH + 70), panels,
            "Noisy neighbours: does a busy tenant slow a quiet one?", sub,
            sides))


def chart_footprint(runs, out):
    """Memory held and point-read p95 against the tenant count."""
    sides = sides_in(runs, "footprint")
    if not sides:
        return
    W, PW, PH = 1060, 760, 240
    rows = {s[0]: [r for r in runs[("footprint", s[0])]["rows"]
                   if r["state"] == "loaded" and int(r["tenants"]) > 0]
            for s in sides}
    xs = [int(r["tenants"]) for v in rows.values() for r in v]
    budget = None
    for s in sides:
        st = runs[("footprint", s[0])]["settings"]
        if "MEM_GIB" in st:
            budget = int(st["MEM_GIB"]) * int(st.get("BUDGET_PCT", 90)) / 100
    # The shared Postgres runs in docker's cgroup, not the test's, so
    # its memory is its processes' PSS; the other sides' is what their
    # cgroup holds, file cache they pin included.
    def mem(key, r):
        col = "pss_mib" if key == "pgshared" else "held_mib"
        return num(r[col]) / 1024

    panels = []
    ys = [mem(k, r) for k, v in rows.items() for r in v]
    if budget:
        ys.append(budget)
    p = Panel(90, 110, PW, PH, xs, ys, True, True,
              "Memory held, all tenants loaded", "GiB", "tenants")
    p.frame(sorted(set(xs)), fmt)
    if budget:
        p.rule(budget, f"the {fmt(budget)} GiB budget")
    for key, label, colour in sides:
        p.line([(int(r["tenants"]), mem(key, r),
                 f'{label}, {r["tenants"]} tenants: {mem(key, r):.2f} GiB')
                for r in rows[key]], colour, label)
    panels.append(p)
    ys = [num(r.get("p95_ms")) for v in rows.values() for r in v
          if num(r.get("p95_ms"))]
    p = Panel(90, 110 + PH + 70, PW, PH, xs, ys, True, True,
              "Point read p95 at each count, a fresh connection each", "ms",
              "tenants")
    p.frame(sorted(set(xs)), fmt)
    for key, label, colour in sides:
        p.line([(int(r["tenants"]), num(r.get("p95_ms")),
                 f'{label}, {r["tenants"]} tenants: p95 {r.get("p95_ms")} ms')
                for r in rows[key] if num(r.get("p95_ms"))], colour, label)
    panels.append(p)
    sub = ("Tenants created through SQL, 1,000 rows each, then point reads at"
           " random tenants. The line ends at the largest count the side ran.")
    open(os.path.join(out, "footprint.svg"), "w").write(
        svg(W, 110 + 2 * (PH + 70), panels,
            "Footprint: what a tenant costs, and how many fit", sub, sides))


def chart_agents(runs, out):
    """The agents workload against the tenant count: the stop rules'
    measures, the first statement from idle, and the memory."""
    sides = sides_in(runs, "agents")
    if not sides:
        return
    W, PW, PH = 1060, 760, 200
    rows = {s[0]: [r for r in runs[("agents", s[0])]["rows"]
                   if int(r["tenants"]) > 0] for s in sides}
    xs = [int(r["tenants"]) for v in rows.values() for r in v]
    st = runs[("agents", sides[0][0])]["settings"]
    specs = [("point_p95_ms", "Active tenants' point read p95", "ms",
              num(st.get("STOP_P95_MS")), "the stop, {} ms"),
             # Runs before 2026-11-19 wrote the column as arrive_p95_ms.
             ("onboard_p95_ms|arrive_p95_ms",
              "An onboarding's time to its first row, p95",
              "ms", num(st.get("STOP_ONBOARD_MS")), "the stop, {} ms"),
             ("first_p95_ms", "A session's first statement from an idle "
              "tenant, p95 (a wake included)", "ms", None, None),
             ("held_mib", "Memory held", "GiB", None, None),
             ("write_mib_s", "Disk written during the window", "MiB/s",
              None, None)]
    budget = None
    if "MEM_GIB" in st:
        budget = int(st["MEM_GIB"]) * int(st.get("BUDGET_PCT", 90)) / 100
    panels = []
    for i, (cols, title, ylabel, stop, stop_label) in enumerate(specs):
        col = next((c for c in cols.split("|")
                    if any(c in r for v in rows.values() for r in v)),
                   cols.split("|")[0])
        div = 1024 if ylabel == "GiB" else 1
        ys = [num(r.get(col)) / div for v in rows.values() for r in v
              if num(r.get(col))]
        if not ys:
            continue
        if stop:
            ys.append(stop)
        if col == "held_mib" and budget:
            ys.append(budget)
        p = Panel(90, 110 + i * (PH + 70), PW, PH, xs, ys, True, True, title,
                  ylabel, "tenants")
        p.frame(sorted(set(xs)), fmt)
        if stop:
            p.rule(stop, stop_label.format(fmt(stop)))
        if col == "held_mib" and budget:
            p.rule(budget, f"the {fmt(budget)} GiB budget")
        for key, label, colour in sides:
            pts = [(int(r["tenants"]), num(r[col]) / div,
                    f'{label}, {r["tenants"]} tenants: '
                    f'{fmt(num(r[col]) / div)} {ylabel}')
                   for r in rows[key] if num(r.get(col))]
            p.line(pts, colour, label)
        panels.append(p)
    active = st.get("ACTIVE", "0.10")
    sub = (f"Tenants onboard at {st.get('ONBOARD', '?')} a second; "
           f"{float(active) * 100:.0f}% are in a session at any moment, the"
           " rest idle. Each line ends at the last count its side was"
           " measured at.")
    open(os.path.join(out, "agents.svg"), "w").write(
        svg(W, 110 + len(panels) * (PH + 70), panels,
            "Agents: tenants onboarding and working", sub, sides))


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    runs = pick_runs(sys.argv[2:])
    for (test, backend), run in sorted(runs.items()):
        print(f"{test:10} {backend:9} {run['dir']}", file=sys.stderr)
    chart_noisy(runs, out)
    chart_footprint(runs, out)
    chart_agents(runs, out)


if __name__ == "__main__":
    main()
