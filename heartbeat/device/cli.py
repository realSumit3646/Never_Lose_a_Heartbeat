"""The `pipeline` command.

  pipeline run                                   start the device service
  pipeline status                                one JSON line: mode, saved, in cloud, backlog
  pipeline export --session ID --from device     samples then losses, JSON lines
  pipeline export --session ID --from cloud
  pipeline results --session ID                  one JSON line per window and mode
  pipeline sessions                              sessions known to the device
  pipeline events [--last N]                     mode switches, restarts, faults (evidence)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time

from ..codec import from_wire, unpack
from . import config
from .cloudclient import CloudClient, CloudError
from .store import Store


def _store(cfg) -> Store:
    if not os.path.exists(cfg.db_path):
        sys.exit(f"no data yet: {cfg.db_path} does not exist (is `pipeline run` running?)")
    return Store(cfg.db_path)


def cmd_status(cfg, args):
    st = _store(cfg)
    out = {"mode": st.get_meta("mode", "unknown")}
    out.update(st.status())
    hb = st.get_meta("heartbeat_mono")
    running = hb is not None and 0 <= time.monotonic() - hb < 12
    out["service"] = "running" if running else "not running"
    out["mode_since"] = st.get_meta("mode_since")
    print(json.dumps(out))


def cmd_export(cfg, args):
    write = sys.stdout.write
    if args.source == "device":
        _store(cfg).export(args.session, write)
        return
    client = CloudClient(cfg)
    for sensor in (1, 2):
        after = -1
        while True:
            page = client.get("/chunks", timeout=5, session=args.session, sensor=sensor, after=after,
                              limit=100)
            for first, count, wire in page["chunks"]:
                for k, row in enumerate(unpack(from_wire(wire), sensor)):
                    write(json.dumps({"sensor": sensor, "index": first + k, "values": list(row)}) + "\n")
                after = first
            if not page["chunks"]:
                break
    for l in client.get("/losses", session=args.session)["losses"]:
        write(json.dumps(l) + "\n")


def cmd_results(cfg, args):
    merged = {}
    if os.path.exists(cfg.db_path):
        for r in Store(cfg.db_path).offline_results(args.session):
            r["mode"] = "offline"
            merged[(r["window"], "offline")] = r
    try:
        client = CloudClient(cfg)
        after, step = -1, 2000
        while True:  # pages are window ranges (a window can have two modes)
            page = client.get("/results", session=args.session, after=after, limit=step)
            for r in page["results"]:
                merged.setdefault((r["window"], r["mode"]), r)
            after += step
            if page.get("max_window") is None or after >= page["max_window"]:
                break
    except CloudError as e:
        print(f"warning: cloud unreachable, showing device results only ({e})", file=sys.stderr)
    for key in sorted(merged):
        print(json.dumps(merged[key]))


def cmd_sessions(cfg, args):
    st = _store(cfg)
    cols = ["session", "seq", "cursor_1", "cursor_2", "end_1", "end_2", "closed", "close_reason",
            "last_window", "cloud_next_window"]
    for row in st.conn.execute(f"SELECT {', '.join(cols)} FROM sessions ORDER BY seq"):
        print(json.dumps(dict(zip(cols, row))))


def cmd_events(cfg, args):
    st = _store(cfg)
    rows = st.conn.execute("SELECT wall, kind, detail FROM events ORDER BY id DESC LIMIT ?",
                           (args.last,)).fetchall()
    for wall, kind, detail in reversed(rows):
        print(json.dumps({"time": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(wall)),
                          "wall": round(wall, 3), "event": kind, "detail": detail}))


def main(argv=None):
    ap = argparse.ArgumentParser(prog="pipeline", description="Never Lose a Heartbeat device pipeline")
    ap.add_argument("--config", help="INI file (default /etc/heartbeat/pipeline.ini or ./pipeline.ini)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run")
    sub.add_parser("status")
    p = sub.add_parser("export")
    p.add_argument("--session", required=True)
    p.add_argument("--from", dest="source", choices=["device", "cloud"], default="device")
    p = sub.add_parser("results")
    p.add_argument("--session", required=True)
    sub.add_parser("sessions")
    p = sub.add_parser("events")
    p.add_argument("--last", type=int, default=50)
    args = ap.parse_args(argv)
    cfg = config.load(args.config)
    if args.cmd == "run":
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
        from .service import run
        run(cfg)
        return
    try:
        {"status": cmd_status, "export": cmd_export, "results": cmd_results,
         "sessions": cmd_sessions, "events": cmd_events}[args.cmd](cfg, args)
    except BrokenPipeError:
        pass
    except CloudError as e:
        sys.exit(f"cloud error: {e}")


if __name__ == "__main__":
    main()
