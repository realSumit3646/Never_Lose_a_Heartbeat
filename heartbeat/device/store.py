"""Durable on-device storage (SQLite, WAL journal, synchronous=FULL).

Every write that matters (samples, losses, the read cursor) happens in one
transaction, and a transaction is on disk (fsync'd) when commit returns. So
after a crash or power cut the database holds exactly the committed replies:
no torn chunks, no duplicates, and the cursor agrees with the data.

Invariant per session and sensor: every index below `cursor` is either in
`chunks` or covered by a row in `losses`. Chunks never overlap.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import time
from contextlib import contextmanager

from ..codec import MAX_CHUNK, pack, unpack
from ..processing import State, last_window_for

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions(
  session TEXT PRIMARY KEY,
  seq INTEGER NOT NULL,
  opened_wall REAL,
  cursor_1 INTEGER NOT NULL DEFAULT 0,
  cursor_2 INTEGER NOT NULL DEFAULT 0,
  end_1 INTEGER,
  end_2 INTEGER,
  closed INTEGER NOT NULL DEFAULT 0,
  close_reason TEXT,
  last_window INTEGER,
  cloud_next_window INTEGER NOT NULL DEFAULT 0,
  info_version INTEGER NOT NULL DEFAULT 1,
  info_sent INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS chunks(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session TEXT NOT NULL,
  sensor INTEGER NOT NULL,
  first_index INTEGER NOT NULL,
  count INTEGER NOT NULL,
  data BLOB NOT NULL,
  received_wall REAL,
  UNIQUE(session, sensor, first_index)
);
CREATE TABLE IF NOT EXISTS pending(
  id INTEGER PRIMARY KEY,
  session TEXT NOT NULL,
  sensor INTEGER NOT NULL,
  first_index INTEGER NOT NULL,
  count INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS losses(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session TEXT NOT NULL,
  sensor INTEGER NOT NULL,
  from_index INTEGER NOT NULL,
  to_index INTEGER,
  reason TEXT NOT NULL,
  recorded_wall REAL,
  uploaded INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS losses_by_range ON losses(session, sensor, from_index);
CREATE TABLE IF NOT EXISTS results(
  session TEXT NOT NULL,
  win INTEGER NOT NULL,
  result TEXT NOT NULL,
  state TEXT NOT NULL,
  offline INTEGER NOT NULL DEFAULT 0,
  offline_wall REAL,
  uploaded INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(session, win)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS proc_state(session TEXT PRIMARY KEY, state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, wall REAL, mono REAL, kind TEXT, detail TEXT);
"""


class DiskFull(Exception):
    pass


def is_disk_full(e: Exception, path: str = None) -> bool:
    """SQLite reports a full disk as "database or disk is full", but when the WAL
    cannot grow it may say "disk I/O error"; treat that as full if space is gone."""
    if not isinstance(e, sqlite3.OperationalError):
        return False
    msg = str(e).lower()
    if "full" in msg:
        return True
    if "i/o error" in msg and path:
        try:
            return shutil.disk_usage(os.path.dirname(os.path.abspath(path))).free < 1024 * 1024
        except OSError:
            return False
    return False


class Store:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.executescript(SCHEMA)

    def close(self):
        self.conn.close()

    @contextmanager
    def tx(self):
        c = self.conn
        c.execute("BEGIN IMMEDIATE")
        try:
            yield c
            c.execute("COMMIT")
        except BaseException as e:
            try:
                c.execute("ROLLBACK")
            except sqlite3.Error:
                pass  # SQLite already rolled back (it does so itself on a full disk)
            if is_disk_full(e, self.path):
                raise DiskFull(str(e)) from e
            raise

    # ---- meta and events -------------------------------------------------
    def set_meta(self, **kv):
        with self.tx() as c:
            for k, v in kv.items():
                c.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", (k, json.dumps(v)))

    def get_meta(self, key, default=None):
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def event(self, kind: str, detail=""):
        try:
            with self.tx() as c:
                c.execute("INSERT INTO events(wall, mono, kind, detail) VALUES(?,?,?,?)",
                          (time.time(), time.monotonic(), kind, str(detail)))
        except DiskFull:
            pass  # events are evidence only; never let them block data

    # ---- collector -------------------------------------------------------
    def open_session(self):
        """The session the collector was last writing (not closed), or None."""
        return self.conn.execute(
            "SELECT session, cursor_1, cursor_2, end_1, end_2 FROM sessions "
            "WHERE closed=0 ORDER BY seq DESC LIMIT 1").fetchone()

    def is_closed(self, session) -> bool:
        row = self.conn.execute("SELECT closed FROM sessions WHERE session=?", (session,)).fetchone()
        return bool(row and row[0])

    def switch_session(self, new_session: str, reason: str):
        """Close every open session except `new_session`, listing unread data as lost."""
        now = time.time()
        with self.tx() as c:
            for sid, c1, c2, e1, e2 in c.execute(
                    "SELECT session, cursor_1, cursor_2, end_1, end_2 FROM sessions "
                    "WHERE closed=0 AND session<>?", (new_session,)).fetchall():
                for sensor, cur, end in ((1, c1, e1), (2, c2, e2)):
                    if end is None:
                        c.execute("INSERT INTO losses(session, sensor, from_index, to_index, reason, "
                                  "recorded_wall) VALUES(?,?,?,NULL,?,?)", (sid, sensor, cur, reason, now))
                c.execute("UPDATE sessions SET closed=1, close_reason=?, last_window=?, "
                          "info_version=info_version+1 WHERE session=?",
                          (reason, last_window_for(max(c1, c2)), sid))
            if not c.execute("SELECT 1 FROM sessions WHERE session=?", (new_session,)).fetchone():
                seq = c.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM sessions").fetchone()[0]
                c.execute("INSERT INTO sessions(session, seq, opened_wall) VALUES(?,?,?)",
                          (new_session, seq, now))

    def commit_reply(self, session, sensor, losses, first_index, rows, new_cursor, ended):
        """Atomically store losses + new samples and advance the cursor."""
        self.commit_replies(session, [(sensor, losses, first_index, rows, new_cursor, ended)])

    def commit_replies(self, session, entries):
        """Store several replies (sensor, losses, first_index, rows, new_cursor, ended) in one transaction."""
        with self.tx() as c:
            for entry in entries:
                self._write_reply(c, session, *entry)

    def _write_reply(self, c, session, sensor, losses, first_index, rows, new_cursor, ended):
        now = time.time()
        for a, b, reason in losses:
            c.execute("INSERT INTO losses(session, sensor, from_index, to_index, reason, "
                      "recorded_wall) VALUES(?,?,?,?,?,?)", (session, sensor, a, b, reason, now))
        for k in range(0, len(rows), MAX_CHUNK):
            part = rows[k:k + MAX_CHUNK]
            cur = c.execute("INSERT INTO chunks(session, sensor, first_index, count, data, "
                            "received_wall) VALUES(?,?,?,?,?,?)",
                            (session, sensor, first_index + k, len(part), pack(part), now))
            c.execute("INSERT INTO pending(id, session, sensor, first_index, count) "
                      "VALUES(?,?,?,?,?)", (cur.lastrowid, session, sensor, first_index + k, len(part)))
        col = f"cursor_{sensor}"
        c.execute(f"UPDATE sessions SET {col}=? WHERE session=?", (new_cursor, session))
        if ended:
            c.execute(f"UPDATE sessions SET end_{sensor}=?, info_version=info_version+1 "
                      f"WHERE session=?", (new_cursor, session))
            row = c.execute("SELECT end_1, end_2, cursor_1, cursor_2 FROM sessions WHERE session=?",
                            (session,)).fetchone()
            if row[0] is not None and row[1] is not None:
                c.execute("UPDATE sessions SET closed=1, close_reason='session_end', last_window=? "
                          "WHERE session=?", (last_window_for(max(row[0], row[1])), session))

    # ---- processing -------------------------------------------------------
    def sessions_to_process(self):
        out = []
        for sid, closed, last_window, e1, e2 in self.conn.execute(
                "SELECT session, closed, last_window, end_1, end_2 FROM sessions ORDER BY seq").fetchall():
            st = self.load_state(sid)
            if closed and last_window is not None and st.next_window > last_window:
                continue
            ends = {k: v for k, v in ((1, e1), (2, e2)) if v is not None}
            out.append((sid, st, last_window if closed else None, ends))
        return out

    def load_state(self, session) -> State:
        row = self.conn.execute("SELECT state FROM proc_state WHERE session=?", (session,)).fetchone()
        return State.from_dict(json.loads(row[0])) if row else State()

    def save_results(self, session, out, state: State):
        with self.tx() as c:
            for res, st in out:
                c.execute("INSERT OR REPLACE INTO results(session, win, result, state) VALUES(?,?,?,?)",
                          (session, res["window"], json.dumps(res), json.dumps(st.to_dict())))
            c.execute("INSERT OR REPLACE INTO proc_state(session, state) VALUES(?,?)",
                      (session, json.dumps(state.to_dict())))

    def emit_offline(self) -> int:
        """Mark computed windows the cloud has not processed as offline results."""
        with self.tx() as c:
            cur = c.execute(
                "UPDATE results SET offline=1, offline_wall=? WHERE offline=0 AND win >= "
                "(SELECT cloud_next_window FROM sessions s WHERE s.session=results.session)",
                (time.time(),))
            return cur.rowcount

    # ---- upload -----------------------------------------------------------
    def build_batch(self, budget_bytes: int, max_results=2000):
        c = self.conn
        sessions = {}
        for (sid, seq, closed, last_window, e1, e2, ver, sent, cnw) in c.execute(
                "SELECT session, seq, closed, last_window, end_1, end_2, info_version, info_sent, "
                "cloud_next_window FROM sessions").fetchall():
            sessions[sid] = {"seq": seq, "closed": bool(closed), "last_window": last_window,
                             "end_1": e1, "end_2": e2, "version": ver, "sent": sent, "cloud_next": cnw}
        losses = [dict(zip(("id", "session", "sensor", "from_index", "to_index", "reason"), r))
                  for r in c.execute("SELECT id, session, sensor, from_index, to_index, reason "
                                     "FROM losses WHERE uploaded=0 ORDER BY id LIMIT 5000")]
        results = [{"session": s, "window": w, "result": json.loads(r), "offline_wall": t}
                   for s, w, r, t in c.execute(
                       "SELECT session, win, result, offline_wall FROM results "
                       "WHERE offline=1 AND uploaded=0 ORDER BY session, win LIMIT ?", (max_results,))]
        # Chunks the cloud needs to move its processing forward go first; the
        # rest of the backlog fills the remaining budget, oldest first.
        chosen, used = [], 0
        seen = set()
        for sid, info in sessions.items():
            floor = 400 * (info["cloud_next"] - 6)
            for row in c.execute("SELECT id, sensor, count FROM pending WHERE session=? AND "
                                 "first_index + count > ? ORDER BY first_index, sensor",
                                 (sid, floor)):
                if used >= budget_bytes:
                    break
                chosen.append(row[0])
                seen.add(row[0])
                used += row[2] * (12 if row[1] == 1 else 8)
        if used < budget_bytes:
            for row in c.execute("SELECT id, sensor, count FROM pending ORDER BY id"):
                if used >= budget_bytes:
                    break
                if row[0] not in seen:
                    chosen.append(row[0])
                    used += row[2] * (12 if row[1] == 1 else 8)
        chunks = []
        for i in range(0, len(chosen), 500):
            ids = chosen[i:i + 500]
            q = ",".join("?" * len(ids))
            chunks += c.execute(f"SELECT id, session, sensor, first_index, count, data FROM chunks "
                                f"WHERE id IN ({q})", ids).fetchall()
        resumes = self._resumes(sessions)
        return {"sessions": sessions, "losses": losses, "results": results, "chunks": chunks,
                "resumes": resumes, "bytes": used, "full": used >= budget_bytes}

    def _resumes(self, sessions):
        """Where the cloud can skip windows the device already processed offline."""
        out = {}
        for sid, info in sessions.items():
            nxt = info["cloud_next"]
            last = None
            for (w,) in self.conn.execute("SELECT win FROM results WHERE session=? AND win>=? "
                                          "AND offline=1 ORDER BY win", (sid, nxt)):
                if w != nxt:
                    break
                last, nxt = w, w + 1
            if last is not None:
                st = self.conn.execute("SELECT state FROM results WHERE session=? AND win=?",
                                       (sid, last)).fetchone()[0]
                out[sid] = json.loads(st)
        return out

    def ack(self, batch, cloud_sessions: dict):
        with self.tx() as c:
            ids = [ch[0] for ch in batch["chunks"]]
            for i in range(0, len(ids), 500):
                part = ids[i:i + 500]
                c.execute(f"DELETE FROM pending WHERE id IN ({','.join('?' * len(part))})", part)
            for l in batch["losses"]:
                c.execute("UPDATE losses SET uploaded=1 WHERE id=?", (l["id"],))
            for r in batch["results"]:
                c.execute("UPDATE results SET uploaded=1 WHERE session=? AND win=?",
                          (r["session"], r["window"]))
            for sid, info in batch["sessions"].items():
                c.execute("UPDATE sessions SET info_sent=? WHERE session=? AND info_sent<?",
                          (info["version"], sid, info["version"]))
            for sid, info in cloud_sessions.items():
                c.execute("UPDATE sessions SET cloud_next_window=MAX(cloud_next_window, ?) "
                          "WHERE session=?", (int(info["next_window"]), sid))

    # ---- reporting --------------------------------------------------------
    def status(self):
        c = self.conn
        saved = c.execute("SELECT COALESCE(SUM(count), 0) FROM chunks").fetchone()[0]
        backlog = c.execute("SELECT COALESCE(SUM(count), 0) FROM pending").fetchone()[0]
        lost = c.execute("SELECT COUNT(*) FROM losses").fetchone()[0]
        cur = c.execute("SELECT session FROM sessions ORDER BY seq DESC LIMIT 1").fetchone()
        return {"session": cur[0] if cur else None, "samples_saved": saved,
                "samples_in_cloud": saved - backlog, "backlog": backlog, "loss_records": lost}

    def export(self, session, write):
        for sensor, first, count, data in self.conn.execute(
                "SELECT sensor, first_index, count, data FROM chunks WHERE session=? "
                "ORDER BY sensor, first_index", (session,)):
            for k, row in enumerate(unpack(data, sensor)):
                write(json.dumps({"sensor": sensor, "index": first + k, "values": list(row)}) + "\n")
        for sensor, a, b, reason in self.conn.execute(
                "SELECT sensor, from_index, to_index, reason FROM losses WHERE session=? "
                "ORDER BY sensor, from_index", (session,)):
            write(json.dumps({"sensor": sensor, "from_index": a, "to_index": b, "reason": reason}) + "\n")

    def offline_results(self, session):
        return [json.loads(r) for (r,) in self.conn.execute(
            "SELECT result FROM results WHERE session=? AND offline=1 ORDER BY win", (session,))]
