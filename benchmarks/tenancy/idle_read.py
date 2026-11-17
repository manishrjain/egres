#!/usr/bin/env python3
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Read latency of single 4 KiB O_DIRECT reads of a file, after idle
# gaps from 0 to 2 s: a drive that sleeps when idle shows it here.
#   python3 idle_read.py <file on the drive>
import mmap, os, random, sys, time
path = sys.argv[1]
fd = os.open(path, os.O_RDONLY | os.O_DIRECT)
size = os.fstat(fd).st_size
buf = mmap.mmap(-1, 4096)
def rd():
    off = random.randrange(0, size // 4096) * 4096
    t = time.perf_counter()
    os.preadv(fd, [buf], off)
    return (time.perf_counter() - t) * 1000
for idle in (0.0, 0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0):
    xs = []
    for _ in range(12):
        time.sleep(idle)
        xs.append(rd())
    xs.sort()
    print("idle %5.2fs  min %.2f  median %.2f  max %.2f ms" % (
        idle, xs[0], xs[len(xs) // 2], xs[-1]))
