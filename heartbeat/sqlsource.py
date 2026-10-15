"""Read access to stored samples, shared by the device and cloud stores.

Both stores keep the same two tables:
  chunks(session, sensor, first_index, count, data)       data = codec.pack(rows)
  losses(session, sensor, from_index, to_index, reason)   to_index NULL = to the end

A sample index is *final* once it is stored, listed as lost, or beyond the
known end of the session. Processing only reads final ranges, so a window is
never computed from data that may still arrive.
"""
from __future__ import annotations

from .codec import MAX_CHUNK, unpack


class SqliteSource:
    def __init__(self, conn, session: str, ends=None):
        self.conn = conn
        self.session = session
        self.ends = ends or {}  # sensor -> end index (exclusive) of a normally ended session

    def _chunks(self, sensor, start, stop):
        rows = self.conn.execute(
            "SELECT first_index, count, data FROM chunks WHERE session=? AND sensor=? "
            "AND first_index > ? AND first_index < ? ORDER BY first_index",
            (self.session, sensor, start - MAX_CHUNK, stop)).fetchall()
        return [r for r in rows if r[0] + r[1] > start]

    def _losses(self, sensor, start, stop):
        return self.conn.execute(
            "SELECT from_index, to_index FROM losses WHERE session=? AND sensor=? "
            "AND from_index < ? AND (to_index IS NULL OR to_index >= ?)",
            (self.session, sensor, stop, start)).fetchall()

    def is_final(self, sensor, start, stop) -> bool:
        end = self.ends.get(sensor)
        if end is not None:
            stop = min(stop, end)
        if stop <= start:
            return True
        spans = [(f, f + c) for f, c, _ in self._chunks(sensor, start, stop)]
        spans += [(a, stop if b is None else b + 1) for a, b in self._losses(sensor, start, stop)]
        spans.sort()
        pos = start
        for a, b in spans:
            if a > pos:
                return False
            pos = max(pos, b)
            if pos >= stop:
                return True
        return pos >= stop

    def read(self, sensor, start, stop) -> list:
        out = [None] * (stop - start)
        for first, count, data in self._chunks(sensor, start, stop):
            for k, row in enumerate(unpack(data, sensor)):
                i = first + k
                if start <= i < stop:
                    out[i - start] = row
        return out
