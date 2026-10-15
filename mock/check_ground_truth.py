"""check_ground_truth: compares a pipeline export with what the mock generated.

  python -m mock.check_ground_truth --truth mock_truth.jsonl --export export.jsonl [--session ID]

For every sensor and every index the mock generated, the export must contain
the sample exactly once with the right values, or list it as lost. Reports:
  silent_loss  generated, not exported, not listed as lost   (judges deduct)
  duplicates   exported more than once
  wrong_values exported with values the mock never produced
  invented     exported although the mock never generated that index
  contradicted exported and also listed as lost
Exit code 0 only when all of these are zero.
"""
from __future__ import annotations

import argparse
import json
import sys

from mock.signals import values


def load_truth(path):
    sessions = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            e = json.loads(line)
            sid = e.get("session_id")
            if e["event"] == "session_start":
                sessions[sid] = {"mode": e["mode"], "seed": e["seed"], "generated": 0,
                                 "total": int(e["duration"] * 400)}
            elif e["event"] == "progress" and sid in sessions:
                sessions[sid]["generated"] = max(sessions[sid]["generated"], e["generated"])
    return sessions


def check(truth, export_path, session):
    t = truth[session]
    seen = {1: {}, 2: {}}
    losses = {1: [], 2: []}
    report = {"session": session, "generated_per_sensor": t["generated"], "exported": 0,
              "listed_lost": 0, "silent_loss": 0, "duplicates": 0, "wrong_values": 0,
              "invented": 0, "contradicted": 0}
    examples = []
    with open(export_path, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            d = json.loads(line)
            s = d["sensor"]
            if "index" in d:
                i = d["index"]
                report["exported"] += 1
                if i in seen[s]:
                    report["duplicates"] += 1
                    examples.append(f"duplicate sensor {s} index {i}")
                seen[s][i] = d["values"]
            else:
                losses[s].append((d["from_index"], d["to_index"]))
    for s in (1, 2):
        def is_lost(i, ranges=losses[s]):
            return any(a <= i and (b is None or i <= b) for a, b in ranges)
        for i, v in seen[s].items():
            if i >= t["total"] or i < 0:
                report["invented"] += 1
                examples.append(f"invented sensor {s} index {i}")
            elif v != values(t["mode"], t["seed"], s, i):
                report["wrong_values"] += 1
                examples.append(f"wrong values sensor {s} index {i}: {v}")
            if is_lost(i):
                report["contradicted"] += 1
        ranges = sorted(losses[s])
        for i in range(t["generated"]):
            if i in seen[s]:
                continue
            if is_lost(i, ranges):
                report["listed_lost"] += 1
            else:
                report["silent_loss"] += 1
                if report["silent_loss"] <= 5:
                    examples.append(f"silent loss sensor {s} index {i}")
    report["ok"] = not any(report[k] for k in
                           ("silent_loss", "duplicates", "wrong_values", "invented", "contradicted"))
    return report, examples[:20]


def main():
    ap = argparse.ArgumentParser(description="check_ground_truth: compare an export with the mock ground truth")
    ap.add_argument("--truth", required=True)
    ap.add_argument("--export", required=True)
    ap.add_argument("--session", help="default: every session in the truth log")
    args = ap.parse_args()
    truth = load_truth(args.truth)
    if not args.session and len(truth) > 1:
        sys.exit(f"several sessions in the truth log; pass --session (one of {', '.join(truth)})")
    sessions = [args.session] if args.session else list(truth)
    ok = True
    for sid in sessions:
        report, examples = check(truth, args.export, sid)
        print(json.dumps(report))
        for e in examples:
            print("  " + e)
        ok &= report["ok"]
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
