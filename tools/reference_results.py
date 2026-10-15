"""Independent batch implementation of section 3, used to cross-check the pipeline.

It deliberately shares no code with heartbeat/processing.py: it loads a whole
export into memory and applies the rules literally, one window at a time.

Usage:
  python tools/reference_results.py EXPORT.jsonl > reference.jsonl
  python tools/reference_results.py EXPORT.jsonl --compare RESULTS.jsonl
"""
from __future__ import annotations

import argparse
import json
import math
import sys

FIELDS = ["missing_1", "missing_2", "complete", "mean_1", "min_1", "max_1",
          "mean_2", "min_2", "max_2", "magnitude", "heart_rate"]


def load_export(path):
    s1, s2 = {}, {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            d = json.loads(line)
            if "index" in d:
                (s1 if d["sensor"] == 1 else s2)[d["index"]] = tuple(d["values"])
    return s1, s2


def find_peaks(s1, upto):
    """All peaks of sensor 1 channel 2 with index < upto, scanning in order."""
    peaks, prev = [], None
    for i in range(800, upto):
        if i not in s1 or (i + 1) not in s1:
            continue
        if not all(j in s1 for j in range(i - 800, i)):
            continue
        v = s1[i][1]
        if not (v > s1[i - 1][1] and v > s1[i + 1][1]):
            continue
        if not v > max(s1[j][1] for j in range(i - 800, i)) / 2:
            continue
        if prev is not None and i - prev < 100:
            continue
        peaks.append(i)
        prev = i
    return peaks


def compute(s1, s2, last_window):
    peaks = find_peaks(s1, 400 * (last_window + 1))
    out = []
    for w in range(last_window + 1):
        r = {"window": w}
        for s, data, nch in ((1, s1, 3), (2, s2, 2)):
            rows = [data[i] for i in range(400 * w, 400 * w + 400) if i in data]
            r[f"missing_{s}"] = 400 - len(rows)
            r[f"mean_{s}"] = [round(sum(row[c] for row in rows) / len(rows), 1) if rows else None
                              for c in range(nch)]
            r[f"min_{s}"] = [min(row[c] for row in rows) if rows else None for c in range(nch)]
            r[f"max_{s}"] = [max(row[c] for row in rows) if rows else None for c in range(nch)]
            if s == 2:
                r["magnitude"] = (round(sum(math.sqrt(a ** 2 + b ** 2) for a, b in rows) / len(rows), 1)
                                  if rows else None)
        r["complete"] = r["missing_1"] == 0 and r["missing_2"] == 0
        hr = None
        a, b = 400 * (w - 4), 400 * (w + 1) - 1
        if w >= 4 and all(i in s1 for i in range(a, b + 1)):
            ps = [p for p in peaks if a <= p <= b]
            if len(ps) >= 2:
                mean_gap = sum((ps[k + 1] - ps[k]) / 400 for k in range(len(ps) - 1)) / (len(ps) - 1)
                hr = round(60 / mean_gap, 1)
        r["heart_rate"] = hr
        out.append(r)
    return out


def compare(reference, results_path, mode=None):
    got = {}
    with open(results_path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                d = json.loads(line)
                if mode is None or d.get("mode") == mode:
                    got.setdefault(d["window"], []).append(d)
    bad = 0
    for r in reference:
        for g in got.get(r["window"], []):
            diffs = {k: (r[k], g.get(k)) for k in FIELDS if r[k] != g.get(k)}
            if diffs:
                bad += 1
                if bad <= 20:
                    print(f"window {r['window']} ({g.get('mode')}): {diffs}")
    checked = sum(len(v) for v in got.values())
    print(f"compared {checked} result lines against {len(reference)} reference windows: "
          f"{bad} mismatching")
    return bad


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("export")
    ap.add_argument("--compare", help="results JSONL from `pipeline results`")
    ap.add_argument("--mode", choices=["online", "offline"], help="only compare this mode")
    args = ap.parse_args()
    s1, s2 = load_export(args.export)
    last = max(list(s1) + list(s2) + [-1]) // 400
    ref = compute(s1, s2, last)
    if args.compare:
        sys.exit(1 if compare(ref, args.compare, args.mode) else 0)
    for r in ref:
        print(json.dumps(r))


if __name__ == "__main__":
    main()
