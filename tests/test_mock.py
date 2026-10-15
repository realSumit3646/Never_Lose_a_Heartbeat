"""The mock follows section 6: values, buffer, losses, re-reads, crc32, session end, faults."""
import argparse
import json
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from heartbeat.device.collector import crc_ok  # noqa: E402
from mock.biosignal_mock import Mock, Session  # noqa: E402
from mock.signals import values  # noqa: E402


def args(**kw):
    a = dict(mode="ramp", seed=7, duration=60, host="127.0.0.1", port=0, speed=1.0,
             truth=str(Path(tempfile.mkdtemp()) / "truth.jsonl"), fault_rate=0.0, fault_seed=1,
             pause_every=0, pause_for=4, dropout_every=0, dropout_for=3, restart_every=0)
    a.update(kw)
    return argparse.Namespace(**a)


class FixedClockSession(Session):
    """Session whose clock we set by hand."""
    now = 0.0

    def elapsed(self):
        return self.now


class MockProtocol(unittest.TestCase):
    def test_ramp_values(self):
        self.assertEqual(values("ramp", 7, 1, 10), [31, 32, 33])
        self.assertEqual(values("ramp", 7, 2, 10), [21, 22])
        self.assertEqual(values("ramp", 7, 1, 2_796_203), [(3 * 2_796_203 + c) % 8_388_608 for c in (1, 2, 3)])

    def test_signal_is_deterministic_and_in_range(self):
        for i in (0, 12345, 400 * 200):
            self.assertEqual(values("signal", 7, 1, i), values("signal", 7, 1, i))
            v1, v2, v3 = values("signal", 7, 1, i)
            self.assertEqual(v1, round(v2 * 0.6))
            self.assertEqual(v3, round(v2 * 1.3))
        ppg = [values("signal", 7, 2, i) for i in range(0, 4000, 50)]
        self.assertTrue(all(380_000 < a < 420_000 and 140_000 < b < 160_000 for a, b in ppg))

    def test_at_most_one_second_and_cursor(self):
        s = FixedClockSession(args())
        s.now = 2.5
        r = s.read(1)
        self.assertEqual((r["first_index"], r["count"]), (0, 400))
        r = s.read(1)
        self.assertEqual((r["first_index"], r["count"]), (400, 400))
        r = s.read(1)
        self.assertEqual((r["first_index"], r["count"]), (800, 200))
        self.assertEqual(s.read(1)["count"], 0)

    def test_buffer_overflow_is_reported(self):
        s = FixedClockSession(args())
        s.now = 15.0  # 6000 samples generated; only the last 10 s are kept
        r = s.read(2)
        self.assertEqual(r["lost"], [{"from_index": 0, "to_index": 1999, "reason": "buffer_overflow"}])
        self.assertEqual(r["first_index"], 2000)

    def test_reread_from_index(self):
        s = FixedClockSession(args())
        s.now = 3.0
        s.read(1)
        s.read(1)
        r = s.read(1, from_index=100)
        self.assertEqual((r["first_index"], r["count"]), (100, 400))
        self.assertEqual(r["rows"][0], values("ramp", 7, 1, 100))

    def test_session_end(self):
        s = FixedClockSession(args(duration=1))
        s.now = 5.0
        r = s.read(1)
        self.assertEqual(r["count"], 400)
        self.assertTrue(r["session_end"])

    def test_dropout_reported_as_lost(self):
        s = FixedClockSession(args(dropout_every=2, dropout_for=1))
        s.now = 2.0
        s.read(1)
        s.read(1)
        s.now = 4.0
        r = s.read(1)  # sensor 1 drops samples 800..1199
        self.assertEqual(r["lost"], [{"from_index": 800, "to_index": 1199, "reason": "sensor_dropout"}])
        self.assertEqual(r["first_index"], 1200)


class MockOverTcp(unittest.TestCase):
    def test_reply_line_and_crc(self):
        a = args(fault_rate=0.0)
        m = Mock(a)
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        threading.Thread(target=lambda: m.serve_client(srv.accept()[0]), daemon=True).start()
        time.sleep(0.3)
        c = socket.create_connection(("127.0.0.1", port), timeout=5)
        c.sendall(b'{"cmd": "read", "sensor": 1}\n')
        buf = b""
        while b"\n" not in buf:
            buf += c.recv(65536)
        line = buf.split(b"\n")[0]
        msg = json.loads(line)
        self.assertEqual(set(msg), {"session_id", "sensor", "first_index", "count", "lost",
                                    "session_end", "crc32", "data"})
        self.assertTrue(crc_ok(line, msg["crc32"]))
        self.assertEqual(msg["data"][0], values("ramp", 7, 1, 0))
        c.close()

    def test_corrupt_replies_fail_crc(self):
        m = Mock(args(fault_rate=1.0))
        m.session.t0 -= 1.0  # one second of data available
        line = m.reply_line({"cmd": "read", "sensor": 1})[0].encode().strip()
        msg = json.loads(line)
        self.assertFalse(crc_ok(line, msg["crc32"]))


if __name__ == "__main__":
    unittest.main()
