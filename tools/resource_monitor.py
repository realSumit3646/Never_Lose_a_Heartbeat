"""Measure the pipeline's resource use on Linux (criterion 8).

  python3 tools/resource_monitor.py --seconds 3600 --iface eth0 > resources.json

Samples every 5 s: CPU time of all `heartbeat run` processes (restarts are
followed), peak resident memory, bytes written to disk by the pipeline, and
bytes sent on the network interface. Prints one JSON summary at the end and
writes per-sample lines to stderr.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

CLK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100


def pipeline_pids():
    out = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            cmd = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ")
        except OSError:
            continue
        if b"heartbeat" in cmd and b" run" in cmd and b"resource_monitor" not in cmd:
            out.append(int(pid))
    return out


def proc_sample(pid):
    try:
        stat = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        cpu = (int(stat[11]) + int(stat[12])) / CLK
        status = dict(l.split(":", 1) for l in open(f"/proc/{pid}/status") if ":" in l)
        hwm_kb = int(status.get("VmHWM", "0 kB").split()[0])
        io = dict(l.split(":", 1) for l in open(f"/proc/{pid}/io"))
        return cpu, hwm_kb, int(io["write_bytes"])
    except (OSError, KeyError, IndexError, ValueError):
        return None


def net_tx(iface):
    for line in open("/proc/net/dev"):
        if line.strip().startswith(iface + ":"):
            return int(line.split(":", 1)[1].split()[8])
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=3600)
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--iface", default="eth0")
    a = ap.parse_args()
    last = {}       # pid -> (cpu, write_bytes) at last sample
    cpu_total = 0.0
    write_total = 0
    peak_kb = 0
    tx0 = net_tx(a.iface)
    t0 = time.monotonic()
    while time.monotonic() - t0 < a.seconds:
        for pid in pipeline_pids():
            s = proc_sample(pid)
            if not s:
                continue
            cpu, hwm, wb = s
            pc, pw = last.get(pid, (0.0, 0))
            cpu_total += cpu - pc
            write_total += wb - pw
            last[pid] = (cpu, wb)
            peak_kb = max(peak_kb, hwm)
        elapsed = time.monotonic() - t0
        print(json.dumps({"t": round(elapsed), "cpu_s": round(cpu_total, 2), "peak_mb": round(peak_kb / 1024, 1),
                          "disk_mb": round(write_total / 1e6, 2),
                          "net_tx_mb": round((net_tx(a.iface) - tx0) / 1e6, 2)}), file=sys.stderr, flush=True)
        time.sleep(a.interval)
    elapsed = time.monotonic() - t0
    print(json.dumps({
        "duration_s": round(elapsed),
        "cpus": os.cpu_count(),
        "avg_cpu_percent_of_one_core": round(100 * cpu_total / elapsed, 2),
        "peak_memory_mb": round(peak_kb / 1024, 1),
        "disk_written_mb": round(write_total / 1e6, 2),
        "network_sent_mb": round((net_tx(a.iface) - tx0) / 1e6, 2),
        "interface": a.iface,
    }, indent=2))


if __name__ == "__main__":
    main()
