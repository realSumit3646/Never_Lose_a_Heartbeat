"""Reads both sensors from the mock service and saves every sample.

Replies are not trusted to arrive once, in order, or intact:
  * a reply whose crc32 does not match its raw data text is dropped and the
    data is re-read with from_index;
  * data already saved (duplicate replies, re-reads) is trimmed by index;
  * an index range that is skipped is only accepted as lost when the mock
    reports it lost, or after re-reading it twice did not bring it back;
  * a new session_id means the mock restarted: the old session's unread
    data is listed as lost with to_index null.
"""
from __future__ import annotations

import json
import logging
import os
import socket
import time
import zlib

from ..codec import valid_row
from .store import DiskFull

log = logging.getLogger("collector")

MAX_GAP_RETRIES = 2


def raw_value(line: bytes, key: bytes = b"data"):
    """Raw bytes of the top-level array value of `key`, exactly as sent."""
    depth, i, n = 0, 0, len(line)
    while i < n:
        ch = line[i]
        if ch == 0x22:  # string
            j = i + 1
            while j < n and line[j] != 0x22:
                j += 2 if line[j] == 0x5C else 1
            text = line[i + 1:j]
            i = j + 1
            if depth == 1 and text == key:
                k = i
                while k < n and line[k] in b" \t\r\n":
                    k += 1
                if k < n and line[k] == 0x3A:  # ':'
                    k += 1
                    while k < n and line[k] in b" \t\r\n":
                        k += 1
                    if k < n and line[k] == 0x5B:  # '['
                        return _array_span(line, k)
            continue
        if ch in b"{[":
            depth += 1
        elif ch in b"}]":
            depth -= 1
        i += 1
    return None


def _array_span(line: bytes, start: int):
    depth, i, n, in_str = 0, start, len(line), False
    while i < n:
        ch = line[i]
        if in_str:
            if ch == 0x5C:
                i += 1
            elif ch == 0x22:
                in_str = False
        elif ch == 0x22:
            in_str = True
        elif ch == 0x5B:
            depth += 1
        elif ch == 0x5D:
            depth -= 1
            if depth == 0:
                return line[start:i + 1]
        i += 1
    return None


def crc_ok(line: bytes, crc: str) -> bool:
    raw = raw_value(line)
    return raw is not None and isinstance(crc, str) and \
        "%08x" % (zlib.crc32(raw) & 0xFFFFFFFF) == crc.lower()


class LineConn:
    def __init__(self, host, port, timeout):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.buf = b""

    def send(self, obj):
        self.sock.sendall((json.dumps(obj) + "\n").encode())

    def has_line(self) -> bool:
        return b"\n" in self.buf

    def readline(self, timeout: float):
        deadline = time.monotonic() + timeout
        while b"\n" not in self.buf:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            self.sock.settimeout(remaining)
            try:
                chunk = self.sock.recv(1 << 16)
            except socket.timeout:
                return None
            if not chunk:
                raise ConnectionError("mock closed the connection")
            self.buf += chunk
        line, _, self.buf = self.buf.partition(b"\n")
        return line

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


class Collector:
    def __init__(self, cfg, store, stop_event):
        self.cfg = cfg
        self.store = store
        self.stop = stop_event
        self.session = None
        self.cursor = {1: 0, 2: 0}
        self.ended = {1: False, 2: False}
        self.reread = {1: False, 2: False}
        self.gap_retries = {1: 0, 2: 0}
        self.disk_loss = {}  # sensor -> [from, to] dropped while the disk was full
        self.batch = []      # replies of the current read cycle, not yet committed
        self.last_reply = time.monotonic()
        self.stats = {"replies": 0, "corrupt": 0, "duplicates": 0, "rereads": 0}
        row = store.open_session()
        if row:
            self.session = row[0]
            self.cursor = {1: row[1], 2: row[2]}
            self.ended = {1: row[3] is not None, 2: row[4] is not None}
            self.reread = {1: True, 2: True}  # resume exactly where the disk says

    # ---- reply handling (no I/O; unit-testable) ----------------------------
    def handle_line(self, line: bytes) -> bool:
        """Process one reply line. Returns True when the reply was a full second."""
        try:
            msg = json.loads(line)
        except ValueError:
            self._corrupt(None, "unparseable reply")
            return False
        if not isinstance(msg, dict) or "session_id" not in msg:
            log.warning("ignoring reply: %s", line[:200])
            return False
        sensor = msg.get("sensor")
        if sensor not in (1, 2):
            self._corrupt(None, "bad sensor")
            return False
        rows = msg.get("data")
        if not crc_ok(line, msg.get("crc32")) or not isinstance(rows, list) or \
                msg.get("count", len(rows)) != len(rows) or not all(valid_row(r, sensor) for r in rows):
            self._corrupt(sensor, "crc32 mismatch or malformed data")
            return False
        self.stats["replies"] += 1
        sid = msg["session_id"]
        if sid != self.session:
            if self.store.is_closed(sid):
                return False  # stale reply from a session already closed
            self._switch(sid)
        self._apply(sensor, msg)
        return len(rows) >= 400

    def _corrupt(self, sensor, why):
        self.stats["corrupt"] += 1
        log.warning("corrupt reply (%s); re-reading", why)
        for s in ((1, 2) if sensor is None else (sensor,)):
            self.reread[s] = True
        self.store.event("corrupt_reply", why)

    def _switch(self, sid):
        self.flush()  # what we have of the old session goes to disk first
        reason = "mock restarted (new session); unread data of this session was lost"
        self.store.switch_session(sid, reason)
        old = self.session
        self.session = sid
        self.cursor = {1: 0, 2: 0}
        self.ended = {1: False, 2: False}
        self.reread = {1: True, 2: True}  # ask for index 0 explicitly, whatever the mock's cursor
        self.gap_retries = {1: 0, 2: 0}
        self.disk_loss = {}
        log.info("session %s -> %s", old, sid)
        self.store.event("session_switch", f"{old} -> {sid}")

    def _apply(self, sensor, msg):
        first = int(msg.get("first_index", 0))
        rows = msg["data"]
        pos = self.cursor[sensor]
        losses = []
        new_rows, new_first = [], None
        events = []
        for l in msg.get("lost") or []:
            a, b = l.get("from_index"), l.get("to_index")
            if b is None:  # end unknown: it ends where the delivered data resumes
                if rows and first > a:
                    b = first - 1
                else:
                    continue
            events.append((int(a), 0, int(b), str(l.get("reason") or "reported lost by mock")))
        if rows:
            events.append((first, 1, first + len(rows) - 1, None))
        events.sort()
        for a, kind, b, reason in events:
            if b < pos:
                if kind == 1:
                    self.stats["duplicates"] += 1
                continue
            if a > pos:  # nothing explains [pos, a-1]
                if self.gap_retries[sensor] < MAX_GAP_RETRIES:
                    self.gap_retries[sensor] += 1
                    self.reread[sensor] = True
                    self.stats["rereads"] += 1
                    break
                losses.append((pos, a - 1, "not delivered by mock (gap persisted after re-reads)"))
                pos = a
            if kind == 0:
                losses.append((pos, b, reason))
            else:
                new_first = pos
                new_rows = rows[pos - first:]
            pos = b + 1
        else:
            self.gap_retries[sensor] = 0
            # An empty reply positioned elsewhere (e.g. answering an old session's
            # from_index after a restart) does not mean we are in step: keep asking
            # explicitly from our cursor. Never record a loss from an empty reply.
            self.reread[sensor] = not rows and first != pos
        ended = bool(msg.get("session_end")) and pos >= first + len(rows) and not self.reread[sensor]
        if pos == self.cursor[sensor] and (not ended or self.ended[sensor]):
            return
        self._commit(sensor, losses, new_first, new_rows, pos, ended)

    def _commit(self, sensor, losses, first, rows, new_cursor, ended):
        """Queue a reply's effect; flush() writes the whole read cycle in one transaction."""
        self.batch.append((sensor, losses, first, rows, self.cursor[sensor], new_cursor, ended))
        self.cursor[sensor] = new_cursor

    def flush(self):
        """Commit everything received in this read cycle atomically (one fsync)."""
        entries = list(self.batch)
        for sensor, (a, b) in self.disk_loss.items():  # losses the full disk forced on us
            entries.insert(0, (sensor, [(a, b, "disk full: sample could not be saved")], None, [],
                               a, b + 1, False))
        if not entries:
            return
        self.batch = []
        try:
            self.store.commit_replies(self.session, [
                (s, losses, first if first is not None else 0, rows, new_cursor, ended)
                for s, losses, first, rows, _, new_cursor, ended in entries])
        except DiskFull:
            for s in {e[0] for e in entries}:
                own = [e for e in entries if e[0] == s]
                start = self.disk_loss[s][0] if s in self.disk_loss else own[0][4]
                if self.cursor[s] > start:
                    self.disk_loss[s] = [start, self.cursor[s] - 1]
            self._free_reserve()
            return
        self.disk_loss = {}
        for s, _, _, _, _, new_cursor, ended in entries:
            if ended and not self.ended[s]:
                self.ended[s] = True
                self.store.event("session_end", f"{self.session} sensor {s} at {new_cursor}")

    def _free_reserve(self):
        path = os.path.join(self.cfg.data_dir, "reserve.bin")
        if os.path.exists(path):
            os.remove(path)
            log.error("disk full: released reserve file to record the loss")
        self.store.event("disk_full", "samples are being listed as lost")

    # ---- network loop ------------------------------------------------------
    def run(self):
        while not self.stop.is_set():
            conn = None
            try:
                conn = LineConn(self.cfg.mock_host, self.cfg.mock_port, timeout=3)
                log.info("connected to mock %s:%s", self.cfg.mock_host, self.cfg.mock_port)
                self.store.event("mock_connected", f"{self.cfg.mock_host}:{self.cfg.mock_port}")
                if self.session:
                    self.reread = {1: True, 2: True}
                self.last_reply = time.monotonic()
                self._loop(conn)
            except (OSError, ConnectionError) as e:
                log.warning("mock connection: %s", e)
                self.store.event("mock_disconnected", e)
            finally:
                if conn:
                    conn.close()
                self.flush()
            self.stop.wait(1.0)

    def _loop(self, conn):
        while not self.stop.is_set():
            started = time.monotonic()
            behind = False
            # After the session ends keep polling sensor 1, to notice a restarted mock.
            for sensor in [s for s in (1, 2) if not self.ended[s]] or [1]:
                req = {"cmd": "read", "sensor": sensor}
                if self.reread[sensor] and self.session is not None:
                    req["from_index"] = self.cursor[sensor]
                conn.send(req)
                line = conn.readline(self.cfg.reply_timeout)
                if line is None:
                    if time.monotonic() - self.last_reply > self.cfg.silent_reconnect:
                        raise ConnectionError("mock silent; reconnecting")
                    continue
                self.last_reply = time.monotonic()
                behind |= self.handle_line(line)
                while conn.has_line():  # extra lines, e.g. duplicated replies
                    behind |= self.handle_line(conn.readline(0))
            self.flush()
            if self.ended[1] and self.ended[2]:
                self.stop.wait(1.0)
            elif not behind:
                self.stop.wait(max(0.0, self.cfg.poll_interval - (time.monotonic() - started)))
