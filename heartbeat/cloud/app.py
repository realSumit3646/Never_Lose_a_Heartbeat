"""Cloud side: receives the device's data, stores it once, processes it online.

A plain WSGI application (standard library only), so it runs anywhere that
hosts Python: `python -m heartbeat.cloud.app` for a VM, or any WSGI host.

  GET  /health                         no auth; connectivity probe
  POST /ingest                         device upload (chunks, losses, results, session info)
  GET  /sessions
  GET  /status?session=ID
  GET  /chunks?session=&sensor=&after=&limit=
  GET  /losses?session=
  GET  /results?session=&after=&limit=

Exactly-once: a chunk is keyed by (session, sensor, first_index) and the
device never produces overlapping chunks, so a re-sent chunk is ignored.

Environment: HEARTBEAT_CLOUD_DB (default ./cloud.db), HEARTBEAT_API_KEY,
HEARTBEAT_CLOUD_JOURNAL (WAL | DELETE | TRUNCATE | PERSIST), HEARTBEAT_CLOUD_SYNC (FULL | NORMAL);
see tools/sqlite_bench.py to pick them on a network filesystem.
"""
from __future__ import annotations

import argparse
import hmac
import json
import logging
import os
import sqlite3
import threading
import time
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from ..codec import from_wire, to_wire
from ..processing import State, advance
from ..sqlsource import SqliteSource

log = logging.getLogger("cloud")

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions(
  session TEXT PRIMARY KEY, seq INTEGER, closed INTEGER NOT NULL DEFAULT 0, last_window INTEGER,
  end_1 INTEGER, end_2 INTEGER, first_seen REAL, updated REAL);
CREATE TABLE IF NOT EXISTS chunks(
  session TEXT NOT NULL, sensor INTEGER NOT NULL, first_index INTEGER NOT NULL,
  count INTEGER NOT NULL, data BLOB NOT NULL, received REAL,
  UNIQUE(session, sensor, first_index));
CREATE TABLE IF NOT EXISTS losses(
  session TEXT NOT NULL, sensor INTEGER NOT NULL, from_index INTEGER NOT NULL, to_index INTEGER,
  reason TEXT, received REAL, UNIQUE(session, sensor, from_index));
CREATE TABLE IF NOT EXISTS results(
  session TEXT NOT NULL, win INTEGER NOT NULL, mode TEXT NOT NULL, result TEXT NOT NULL,
  processed_at REAL, received REAL, PRIMARY KEY(session, win, mode)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS proc_state(session TEXT PRIMARY KEY, state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS conflicts(at REAL, detail TEXT);
"""

PROCESS_BUDGET_S = 2.0


class CloudStore:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
        # WAL needs shared memory; on network filesystems (e.g. PythonAnywhere) use DELETE.
        journal = os.environ.get("HEARTBEAT_CLOUD_JOURNAL", "WAL").upper()
        sync = os.environ.get("HEARTBEAT_CLOUD_SYNC", "FULL").upper()
        if journal not in ("WAL", "DELETE", "TRUNCATE", "PERSIST") or sync not in ("FULL", "NORMAL"):
            raise ValueError("HEARTBEAT_CLOUD_JOURNAL / HEARTBEAT_CLOUD_SYNC not recognised")
        self.conn.execute(f"PRAGMA journal_mode={journal}")
        self.conn.execute(f"PRAGMA synchronous={sync}")
        self.conn.executescript(SCHEMA)
        self.lock = threading.Lock()

    def ingest(self, p: dict):
        """Store a device upload, then process what became ready.

        Returns (per-session next window, timing in ms) - the timing lets the
        device log where a slow request spent its time.
        """
        t0 = time.monotonic()
        now = time.time()
        touched = set()
        with self.lock:
            t_lock = time.monotonic()
            c = self.conn
            c.execute("BEGIN IMMEDIATE")
            try:
                for s in p.get("sessions", []):
                    touched.add(s["session"])
                    c.execute("INSERT INTO sessions(session, seq, first_seen) VALUES(?,?,?) "
                              "ON CONFLICT(session) DO NOTHING", (s["session"], s.get("seq"), now))
                    c.execute("UPDATE sessions SET seq=?, closed=?, last_window=?, end_1=?, end_2=?, "
                              "updated=? WHERE session=?",
                              (s.get("seq"), int(bool(s.get("closed"))), s.get("last_window"),
                               s.get("end_1"), s.get("end_2"), now, s["session"]))
                for sid, sensor, first, count, wire in p.get("chunks", []):
                    touched.add(sid)
                    self._ensure_session(sid, now)
                    data = from_wire(wire)
                    cur = c.execute("INSERT OR IGNORE INTO chunks(session, sensor, first_index, count, "
                                    "data, received) VALUES(?,?,?,?,?,?)",
                                    (sid, sensor, first, count, data, now))
                    if cur.rowcount == 0:
                        old = c.execute("SELECT count, data FROM chunks WHERE session=? AND sensor=? "
                                        "AND first_index=?", (sid, sensor, first)).fetchone()
                        if old != (count, data):
                            c.execute("INSERT INTO conflicts VALUES(?,?)",
                                      (now, f"chunk {sid}/{sensor}/{first} re-sent with different content"))
                for l in p.get("losses", []):
                    touched.add(l["session"])
                    self._ensure_session(l["session"], now)
                    c.execute("INSERT OR IGNORE INTO losses(session, sensor, from_index, to_index, reason, "
                              "received) VALUES(?,?,?,?,?,?)",
                              (l["session"], l["sensor"], l["from_index"], l["to_index"], l["reason"], now))
                for r in p.get("results", []):
                    touched.add(r["session"])
                    res = dict(r["result"], mode="offline")
                    c.execute("INSERT OR IGNORE INTO results(session, win, mode, result, processed_at, "
                              "received) VALUES(?,?,?,?,?,?)",
                              (r["session"], r["window"], "offline", json.dumps(res),
                               r.get("offline_wall"), now))
                for sid, st in (p.get("resumes") or {}).items():
                    touched.add(sid)
                    cur_state = self._state(sid)
                    if st["next_window"] > cur_state.next_window:
                        c.execute("INSERT OR REPLACE INTO proc_state(session, state) VALUES(?,?)",
                                  (sid, json.dumps(st)))
                        log.info("session %s: windows %d..%d were processed offline; resuming at %d",
                                 sid, cur_state.next_window, st["next_window"] - 1, st["next_window"])
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise
            t_commit = time.monotonic()
            open_sessions = {r[0] for r in c.execute("SELECT session FROM sessions WHERE closed=0")}
            deadline = time.monotonic() + PROCESS_BUDGET_S
            windows = 0
            for sid in sorted(touched | open_sessions):
                if time.monotonic() > deadline:
                    break
                windows += self._process(sid, deadline)
            t_end = time.monotonic()
            timing = {"lock_ms": round((t_lock - t0) * 1000), "store_ms": round((t_commit - t_lock) * 1000),
                      "process_ms": round((t_end - t_commit) * 1000), "windows": windows}
            return {sid: {"next_window": self._state(sid).next_window} for sid in touched}, timing

    def _ensure_session(self, sid, now):
        self.conn.execute("INSERT INTO sessions(session, first_seen) VALUES(?,?) "
                          "ON CONFLICT(session) DO NOTHING", (sid, now))

    def _state(self, sid) -> State:
        row = self.conn.execute("SELECT state FROM proc_state WHERE session=?", (sid,)).fetchone()
        return State.from_dict(json.loads(row[0])) if row else State()

    def _process(self, sid, deadline):
        row = self.conn.execute("SELECT closed, last_window, end_1, end_2 FROM sessions WHERE session=?",
                                (sid,)).fetchone()
        if not row:
            return 0
        closed, last_window, e1, e2 = row
        ends = {k: v for k, v in ((1, e1), (2, e2)) if v is not None}
        src = SqliteSource(self.conn, sid, ends)
        state = self._state(sid)
        done = 0
        while time.monotonic() < deadline:
            out, state = advance(src, sid, state, last_window if closed else None, max_windows=100)
            if not out:
                return done
            done += len(out)
            now = time.time()
            c = self.conn
            c.execute("BEGIN IMMEDIATE")
            for res, _ in out:
                res = dict(res, mode="online")
                c.execute("INSERT OR IGNORE INTO results(session, win, mode, result, processed_at, received) "
                          "VALUES(?,?,?,?,?,?)", (sid, res["window"], "online", json.dumps(res), now, now))
            c.execute("INSERT OR REPLACE INTO proc_state(session, state) VALUES(?,?)",
                      (sid, json.dumps(state.to_dict())))
            c.execute("COMMIT")
        return done

    # ---- reads -------------------------------------------------------------
    def sessions(self):
        cols = ["session", "seq", "closed", "last_window", "end_1", "end_2", "first_seen", "updated"]
        return [dict(zip(cols, r)) for r in self.conn.execute(
            f"SELECT {', '.join(cols)} FROM sessions ORDER BY seq")]

    def status(self, sid):
        c = self.conn
        samples = {s: n for s, n in c.execute(
            "SELECT sensor, COALESCE(SUM(count), 0) FROM chunks WHERE session=? GROUP BY sensor", (sid,))}
        results = {m: n for m, n in c.execute(
            "SELECT mode, COUNT(*) FROM results WHERE session=? GROUP BY mode", (sid,))}
        return {"session": sid, "samples": samples, "losses": c.execute(
            "SELECT COUNT(*) FROM losses WHERE session=?", (sid,)).fetchone()[0],
            "results": results, "next_window": self._state(sid).next_window,
            "conflicts": c.execute("SELECT COUNT(*) FROM conflicts").fetchone()[0]}

    def chunks(self, sid, sensor, after, limit):
        return [[f, n, to_wire(d)] for f, n, d in self.conn.execute(
            "SELECT first_index, count, data FROM chunks WHERE session=? AND sensor=? AND first_index>? "
            "ORDER BY first_index LIMIT ?", (sid, sensor, after, limit))]

    def losses(self, sid):
        return [{"sensor": s, "from_index": a, "to_index": b, "reason": r} for s, a, b, r in
                self.conn.execute("SELECT sensor, from_index, to_index, reason FROM losses "
                                  "WHERE session=? ORDER BY sensor, from_index", (sid,))]

    def results(self, sid, after, limit):
        rows = self.conn.execute("SELECT result, processed_at FROM results WHERE session=? AND win>? "
                                 "AND win<=? ORDER BY win, mode", (sid, after, after + limit)).fetchall()
        out = []
        for r, t in rows:
            d = json.loads(r)
            d["processed_at"] = t
            out.append(d)
        top = self.conn.execute("SELECT MAX(win) FROM results WHERE session=?", (sid,)).fetchone()[0]
        return out, top


_store = None
_store_lock = threading.Lock()


def get_store():
    global _store
    with _store_lock:
        if _store is None:
            _store = CloudStore(os.environ.get("HEARTBEAT_CLOUD_DB", "cloud.db"))
        return _store


def _json(start_response, obj, status="200 OK"):
    body = json.dumps(obj).encode()
    start_response(status, [("Content-Type", "application/json"), ("Content-Length", str(len(body)))])
    return [body]


def application(environ, start_response):
    method = environ["REQUEST_METHOD"]
    path = environ.get("PATH_INFO", "")
    q = {k: v[0] for k, v in parse_qs(environ.get("QUERY_STRING", "")).items()}
    if path == "/health":
        return _json(start_response, {"ok": True, "time": time.time()})
    key = os.environ.get("HEARTBEAT_API_KEY", "")
    if key and not hmac.compare_digest(environ.get("HTTP_X_API_KEY", ""), key):
        return _json(start_response, {"error": "unauthorized"}, "401 Unauthorized")
    store = get_store()
    try:
        if method == "POST" and path == "/ingest":
            n = int(environ.get("CONTENT_LENGTH") or 0)
            payload = json.loads(environ["wsgi.input"].read(n))
            sessions, timing = store.ingest(payload)
            return _json(start_response, {"ok": True, "sessions": sessions, "timing": timing})
        with store.lock:  # reads must not interleave with an ingest transaction
            return _read(store, method, path, q, start_response)
    except (KeyError, ValueError) as e:
        return _json(start_response, {"error": f"bad request: {e}"}, "400 Bad Request")


def _read(store, method, path, q, start_response):
    if method == "GET" and path == "/sessions":
        return _json(start_response, {"sessions": store.sessions()})
    if method == "GET" and path == "/status":
        return _json(start_response, store.status(q["session"]))
    if method == "GET" and path == "/chunks":
        return _json(start_response, {"chunks": store.chunks(
            q["session"], int(q["sensor"]), int(q.get("after", -1)), min(int(q.get("limit", 500)), 2000))})
    if method == "GET" and path == "/losses":
        return _json(start_response, {"losses": store.losses(q["session"])})
    if method == "GET" and path == "/results":
        res, top = store.results(q["session"], int(q.get("after", -1)), min(int(q.get("limit", 2000)), 5000))
        return _json(start_response, {"results": res, "max_window": top})
    return _json(start_response, {"error": "not found"}, "404 Not Found")


class _ThreadingServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


class _QuietHandler(WSGIRequestHandler):
    def log_message(self, *args):
        pass


def main():
    ap = argparse.ArgumentParser(description="Heartbeat cloud service")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8080)))
    ap.add_argument("--db", help="SQLite file (or HEARTBEAT_CLOUD_DB)")
    args = ap.parse_args()
    if args.db:
        os.environ["HEARTBEAT_CLOUD_DB"] = args.db
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    get_store()
    srv = make_server(args.host, args.port, application, server_class=_ThreadingServer,
                      handler_class=_QuietHandler)
    log.info("cloud service on %s:%d", args.host, args.port)
    srv.serve_forever()


if __name__ == "__main__":
    main()
