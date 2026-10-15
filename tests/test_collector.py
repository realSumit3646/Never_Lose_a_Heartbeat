"""Reply handling: crc32, duplicates, losses, gaps, restarts, resume after a crash."""
import io
import json
import shutil
import sys
import tempfile
import threading
import unittest
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from heartbeat.device.collector import Collector, crc_ok, raw_value  # noqa: E402
from heartbeat.device.config import Config  # noqa: E402
from heartbeat.device.store import Store  # noqa: E402

EXAMPLE = (b'{"session_id": "5f0c2a9e", "sensor": 1, "first_index": 120000, "count": 3, "lost": [], '
           b'"session_end": false, "crc32": "f9a5bf62", "data": [[720, 1200, 1560], [708, 1180, 1534], '
           b'[726, 1210, 1573]]}')


def reply(session, sensor, first, n, lost=(), end=False, corrupt=False):
    nch = 3 if sensor == 1 else 2
    rows = [[(i * 10 + c) for c in range(nch)] for i in range(first, first + n)]
    text = json.dumps(rows)
    crc = "%08x" % (zlib.crc32(text.encode()) & 0xFFFFFFFF)
    if corrupt:
        text = text.replace("1", "9", 1)
    head = json.dumps({"session_id": session, "sensor": sensor, "first_index": first, "count": n,
                       "lost": list(lost), "session_end": end, "crc32": crc})
    return (head[:-1] + ', "data": ' + text + "}").encode()


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.cfg = Config(data_dir=self.dir)
        self.store = Store(self.cfg.db_path)
        self.col = Collector(self.cfg, self.store, threading.Event())

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def feed(self, line):
        self.col.handle_line(line)
        self.col.flush()  # the service flushes once per read cycle

    def export(self, session):
        buf = io.StringIO()
        self.store.export(session, buf.write)
        return [json.loads(l) for l in buf.getvalue().splitlines()]

    def test_crc_matches_brief_example(self):
        self.assertTrue(crc_ok(EXAMPLE, "f9a5bf62"))
        self.assertEqual(raw_value(EXAMPLE)[:6], b"[[720,")
        self.assertFalse(crc_ok(EXAMPLE.replace(b"708", b"709"), "f9a5bf62"))

    def test_duplicates_stored_once(self):
        r = reply("s1", 1, 0, 100)
        self.feed(r)
        self.feed(r)
        self.feed(reply("s1", 1, 50, 100))  # overlapping re-read
        rows = [d for d in self.export("s1") if "index" in d]
        self.assertEqual([d["index"] for d in rows], list(range(150)))

    def test_corrupt_reply_is_rejected(self):
        self.feed(reply("s1", 2, 0, 50))
        self.feed(reply("s1", 2, 50, 50, corrupt=True))
        self.assertTrue(self.col.reread[2])
        self.assertEqual(self.col.cursor[2], 50)
        self.feed(reply("s1", 2, 50, 50))
        rows = [d for d in self.export("s1") if "index" in d]
        self.assertEqual(len(rows), 100)
        self.assertEqual(rows[60]["values"], [600, 601])

    def test_reported_loss_is_listed(self):
        self.feed(reply("s1", 1, 0, 100))
        lost = [{"from_index": 100, "to_index": 199, "reason": "buffer_overflow"}]
        self.feed(reply("s1", 1, 200, 100, lost=lost))
        out = self.export("s1")
        self.assertEqual(len([d for d in out if "index" in d]), 200)
        self.assertIn({"sensor": 1, "from_index": 100, "to_index": 199, "reason": "buffer_overflow"}, out)

    def test_unexplained_gap_retried_then_listed(self):
        self.feed(reply("s1", 1, 0, 100))
        for _ in range(2):
            self.feed(reply("s1", 1, 300, 100))
            self.assertEqual(self.col.cursor[1], 100)
            self.assertTrue(self.col.reread[1])
        self.feed(reply("s1", 1, 300, 100))
        self.assertEqual(self.col.cursor[1], 400)
        losses = [d for d in self.export("s1") if "from_index" in d]
        self.assertEqual((losses[0]["from_index"], losses[0]["to_index"]), (100, 299))

    def test_restart_lists_unread_data_with_null_end(self):
        self.feed(reply("old", 1, 0, 100))
        self.feed(reply("old", 2, 0, 80))
        self.feed(reply("new", 1, 0, 100))
        losses = [d for d in self.export("old") if "from_index" in d]
        self.assertIn({"sensor": 1, "from_index": 100, "to_index": None,
                       "reason": losses[0]["reason"]}, losses)
        self.assertIn(80, [l["from_index"] for l in losses if l["sensor"] == 2])
        self.assertTrue(self.store.is_closed("old"))
        self.assertEqual(self.col.cursor[1], 100)
        self.feed(reply("old", 1, 100, 100))  # stale old-session reply is ignored
        self.assertEqual(len([d for d in self.export("old") if "index" in d]), 180)

    def test_empty_reply_elsewhere_keeps_rereading(self):
        self.feed(reply("old", 1, 0, 100))
        # After a mock restart, the answer to the old session's from_index is empty
        # and positioned far ahead; we must keep asking for index 0 explicitly.
        self.feed(reply("new", 1, 31871, 0))
        self.assertEqual(self.col.cursor[1], 0)
        self.assertTrue(self.col.reread[1])
        self.feed(reply("new", 1, 0, 50))
        self.assertFalse(self.col.reread[1])
        self.assertEqual(self.col.cursor[1], 50)

    def test_resume_after_crash(self):
        self.feed(reply("s1", 1, 0, 120))
        col2 = Collector(self.cfg, Store(self.cfg.db_path), threading.Event())
        self.assertEqual(col2.session, "s1")
        self.assertEqual(col2.cursor[1], 120)
        self.assertTrue(col2.reread[1])

    def test_session_end_closes_session(self):
        self.feed(reply("s1", 1, 0, 400, end=True))
        self.feed(reply("s1", 2, 0, 400, end=True))
        row = self.store.conn.execute("SELECT closed, close_reason, last_window FROM sessions").fetchone()
        self.assertEqual(row, (1, "session_end", 0))


if __name__ == "__main__":
    unittest.main()
