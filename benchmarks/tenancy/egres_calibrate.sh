#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Measures the drive once, the way Egres does at start-up, and keeps
# the result in run/egres_io.flags; every later start (egres_lib.sh)
# is given it and probes nothing. Run on an idle machine, before the
# Egres tests. A probe that lands in a drive stall (writes under a
# tenth of reads) is thrown away and taken again.
#   ./egres_calibrate.sh
source "$(dirname "$0")/env.sh"
export EDATA=$WORK/egres-data-calibrate
source "$(dirname "$0")/egres_lib.sh"
trap 'egres_stop; rm -rf "$EDATA"' EXIT
rm -f "$RUN/egres_io.flags"
for try in 1 2 3 4 5; do
    egres_wipe
    PROBE=1 egres_launch
    egres_wait
    line=$(grep -a "disk calibrated" "$ELOG" | head -1)
    stalled=$(grep -ac "look like a drive stall" "$ELOG" || true)
    egres_stop
    echo "probe $try: ${line#*disk calibrated: }" >&2
    if [ -z "$line" ] || [ "$stalled" != 0 ]; then
        sleep 20
        continue
    fi
    mib=$(echo "$line" | sed -n 's/.*bandwidth_bytes=\([0-9]*\)MiB.*/\1/p')
    cost=$(echo "$line" | sed -n 's/.*write_cost=\([0-9]*\).*/\1/p')
    printf '%s;%s\n' "auto-calibrate=false;bandwidth-bytes=$((mib << 20))" \
        "write-cost=$cost" >"$RUN/egres_io.flags"
    echo "kept: $(cat "$RUN/egres_io.flags")"
    exit 0
done
echo "no clean probe in 5 tries; nothing kept" >&2
exit 1
