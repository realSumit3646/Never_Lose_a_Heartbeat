"""biosignal-mock: imitates the device's two sensors (section 6 of the brief).

Newline-delimited JSON over TCP, one client at a time:
  {"cmd": "read", "sensor": 1}                     -> next data (at most 1 s)
  {"cmd": "read", "sensor": 1, "from_index": N}    -> re-read from index N
The mock keeps only the last 10 seconds of each sensor.

Faults (all optional) imitate what the judges inject: pauses, corrupt replies,
duplicated replies, sensor dropouts and restarts. Every generated sample is
logged to a ground-truth file for mock/check_ground_truth.py.

Example:
  python -m mock.biosignal_mock --mode signal --seed 7 --duration 3600 --port 7100
  python -m mock.biosignal_mock --mode ramp --duration 600 --fault-rate 0.02 \
      --pause-every 60 --pause-for 4 --dropout-every 90 --dropout-for 3 --restart-every 300
"""
from __future__ import annotations

import argparse
import json
import os
import random
import socket
import threading
import time
import zlib

from mock.signals import values

RATE = 400
BUFFER_S = 10


class Session:
    def __init__(self, args):
        self.args = args
        self.id = os.urandom(4).hex()
        self.t0 = time.monotonic()
        self.total = int(args.duration * RATE)
        self.cursor = {1: 0, 2: 0}
        self.dropouts = []
        if args.dropout_every > 0:
            k = 1
            while k * args.dropout_every < args.duration:
                a = int(k * args.dropout_every * RATE)
                self.dropouts.append((1 if k % 2 else 2, a, a + int(args.dropout_for * RATE) - 1))
                k += 1

    def elapsed(self) -> float:
        """Session time in seconds of signal (scaled by --speed)."""
        return (time.monotonic() - self.t0) * self.args.speed

    def available(self) -> int:
        return min(self.total, int(self.elapsed() * RATE))

    def read(self, sensor: int, from_index=None) -> dict:
        avail = self.available()
        oldest = max(0, avail - BUFFER_S * RATE)
        pos = self.cursor[sensor] if from_index is None else max(0, int(from_index))
        lost = []
        if pos < oldest:
            lost.append({"from_index": pos, "to_index": oldest - 1, "reason": "buffer_overflow"})
            pos = oldest
        drops = sorted((a, b) for s, a, b in self.dropouts if s == sensor)
        for a, b in drops:
            if a <= pos <= b and pos < avail:
                end = min(b, avail - 1)
                lost.append({"from_index": pos, "to_index": end, "reason": "sensor_dropout"})
                pos = end + 1
        stop = min(avail, pos + RATE)
        for a, _ in drops:
            if pos < a < stop:
                stop = a
        rows = [values(self.args.mode, self.args.seed, sensor, i) for i in range(pos, stop)]
        self.cursor[sensor] = max(pos, stop)
        return {"session_id": self.id, "sensor": sensor, "first_index": pos, "count": len(rows),
                "lost": lost, "session_end": self.cursor[sensor] >= self.total, "rows": rows}


class Mock:
    def __init__(self, args):
        self.args = args
        self.rng = random.Random(args.fault_seed)
        self.truth = open(args.truth, "a", encoding="utf-8")
        self.lock = threading.Lock()
        self.session = None
        self.new_session()
        threading.Thread(target=self._progress_loop, daemon=True).start()

    def log(self, **event):
        with self.lock:
            event["wall"] = time.time()
            self.truth.write(json.dumps(event) + "\n")
            self.truth.flush()
            if event["event"] != "dropout":
                os.fsync(self.truth.fileno())  # the ground truth must survive a power cut too

    def new_session(self):
        if self.session:
            self.log(event="progress", session_id=self.session.id, generated=self.session.available())
            self.log(event="restart", session_id=self.session.id)
        self.session = Session(self.args)
        s = self.session
        self.log(event="session_start", session_id=s.id, mode=self.args.mode, seed=self.args.seed,
                 duration=self.args.duration, speed=self.args.speed)
        for sensor, a, b in s.dropouts:
            self.log(event="dropout", session_id=s.id, sensor=sensor, from_index=a, to_index=b)
        print(f"[mock] session {s.id} started", flush=True)

    def _progress_loop(self):
        while True:
            s = self.session
            self.log(event="progress", session_id=s.id, generated=s.available())
            time.sleep(1.0)

    def restart_due(self) -> bool:
        return self.args.restart_every > 0 and self.session.elapsed() >= self.args.restart_every

    def pause_left(self) -> float:
        a = self.args
        if a.pause_every <= 0:
            return 0.0
        e = self.session.elapsed()
        k = int(e // a.pause_every)
        if k >= 1 and e - k * a.pause_every < a.pause_for:
            return (a.pause_for - (e - k * a.pause_every)) / a.speed
        return 0.0

    def reply_line(self, req: dict) -> list:
        if req.get("cmd") != "read" or req.get("sensor") not in (1, 2):
            return [json.dumps({"error": "unknown command"}) + "\n"]
        r = self.session.read(req["sensor"], req.get("from_index"))
        data_text = json.dumps(r.pop("rows"))
        r["crc32"] = "%08x" % (zlib.crc32(data_text.encode()) & 0xFFFFFFFF)
        if self.rng.random() < self.args.fault_rate:  # corrupt: data no longer matches crc32
            data_text = data_text.replace("1", "7", 1) if "1" in data_text else data_text
            if data_text == "[]":
                r["crc32"] = "00000000"
            print(f"[mock] fault: corrupt reply sensor {req['sensor']}", flush=True)
        line = json.dumps(r)[:-1] + ', "data": ' + data_text + "}\n"
        if self.rng.random() < self.args.fault_rate:
            print(f"[mock] fault: duplicate reply sensor {req['sensor']}", flush=True)
            return [line, line]
        return [line]

    def serve_client(self, conn):
        conn.settimeout(0.2)
        buf = b""
        while True:
            if self.restart_due():
                print("[mock] fault: restart", flush=True)
                self.new_session()
                return
            try:
                chunk = conn.recv(65536)
            except socket.timeout:
                continue
            if not chunk:
                return
            buf += chunk
            while b"\n" in buf:
                raw, _, buf = buf.partition(b"\n")
                try:
                    req = json.loads(raw)
                except ValueError:
                    conn.sendall(b'{"error": "bad json"}\n')
                    continue
                wait = self.pause_left()
                if wait > 0:
                    print(f"[mock] fault: not answering for {wait:.1f} s", flush=True)
                    time.sleep(wait)
                for line in self.reply_line(req):
                    conn.sendall(line.encode())

    def serve(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.args.host, self.args.port))
        srv.listen(1)
        srv.settimeout(0.5)
        print(f"[mock] listening on {self.args.host}:{self.args.port} mode={self.args.mode}", flush=True)
        while True:
            if self.restart_due():
                self.new_session()
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            print(f"[mock] client {addr} connected", flush=True)
            try:
                self.serve_client(conn)
            except OSError as e:
                print(f"[mock] client error: {e}", flush=True)
            finally:
                conn.close()
                print("[mock] client disconnected", flush=True)


def main():
    ap = argparse.ArgumentParser(description="biosignal-mock: two-sensor mock service (section 6 protocol)")
    ap.add_argument("--mode", choices=["signal", "ramp"], default="signal")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--duration", type=float, default=3600, help="session length in seconds")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7100)
    ap.add_argument("--speed", type=float, default=1.0, help="time acceleration for quick tests")
    ap.add_argument("--truth", default="mock_truth.jsonl", help="ground-truth log (appended)")
    ap.add_argument("--fault-rate", type=float, default=0.0, help="chance per reply of corrupt / duplicate")
    ap.add_argument("--fault-seed", type=int, default=1)
    ap.add_argument("--pause-every", type=float, default=0, help="stop answering every N s")
    ap.add_argument("--pause-for", type=float, default=4)
    ap.add_argument("--dropout-every", type=float, default=0, help="drop one sensor every N s")
    ap.add_argument("--dropout-for", type=float, default=3)
    ap.add_argument("--restart-every", type=float, default=0, help="restart (new session) every N s")
    Mock(ap.parse_args()).serve()


if __name__ == "__main__":
    main()
