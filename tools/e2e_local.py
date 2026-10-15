"""End-to-end failure test on one machine (mock service, local cloud, network-cut proxy).

  python tools/e2e_local.py [--workdir DIR] [--short]

Timeline (seconds since start):
   0  start cloud, proxy, mock (faults on), pipeline
  20  network cut (blackhole) ........ expect mode offline within 10 s
  45  network restored ............... expect mode online within 10 s
  60  pipeline killed (kill -9), restarted 2 s later
  80  mock killed and restarted ...... new session; unread data listed as lost
 ~145 mock session ends; wait for an empty backlog
Then, for every session: export from device and cloud, check against the
ground truth, and compare results with the batch reference implementation.
A JSON report is written to WORKDIR/report.json.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))

import reference_results  # noqa: E402
from mock.check_ground_truth import check, load_truth  # noqa: E402

PY = sys.executable
CLOUD, PROXY, MOCK = 18080, 18081, 17100


class Run:
    def __init__(self, workdir: Path, short: bool, slow: int = 0):
        self.w = workdir
        self.short = short
        self.link = f"slow:{slow}" if slow else "pass"  # network state when not cut
        self.t0 = time.monotonic()
        self.procs = {}
        self.timeline = []
        self.env = dict(os.environ, PYTHONPATH=str(ROOT), HEARTBEAT_CONFIG=str(workdir / "none.ini"),
                        HEARTBEAT_DATA_DIR=str(workdir / "device"),
                        HEARTBEAT_CLOUD_URL=f"http://127.0.0.1:{PROXY}",
                        HEARTBEAT_MOCK_PORT=str(MOCK), HEARTBEAT_RESERVE_MB="4",
                        HEARTBEAT_CLOUD_DB=str(workdir / "cloud.db"))

    def now(self):
        return round(time.monotonic() - self.t0, 1)

    def note(self, what, **kw):
        e = dict(t=self.now(), event=what, **kw)
        self.timeline.append(e)
        print(json.dumps(e), flush=True)

    def start(self, name, args):
        log = open(self.w / f"{name}.log", "ab")
        self.procs[name] = subprocess.Popen([PY, *args], cwd=ROOT, env=self.env, stdout=log,
                                            stderr=subprocess.STDOUT)

    def kill(self, name):
        p = self.procs.pop(name)
        p.kill()
        p.wait()

    def cli(self, *args):
        r = subprocess.run([PY, "-m", "heartbeat", *args], cwd=ROOT, env=self.env,
                           capture_output=True, text=True, timeout=300)
        return r.stdout

    def status(self):
        try:
            return json.loads(self.cli("status").strip().splitlines()[-1])
        except (ValueError, IndexError):
            return {}

    def netmode(self, mode):
        (self.w / "netmode.txt").write_text(mode)

    def start_mock(self, duration):
        self.start("mock", ["-m", "mock.biosignal_mock", "--mode", "signal", "--seed", "7",
                            "--duration", str(duration), "--port", str(MOCK),
                            "--truth", str(self.w / "truth.jsonl"), "--fault-rate", "0.02",
                            "--fault-seed", str(int(time.time())), "--pause-every", "25",
                            "--pause-for", "3", "--dropout-every", "30", "--dropout-for", "2"])

    def wait_mode(self, want, limit):
        start = time.monotonic()
        while time.monotonic() - start < limit:
            if self.status().get("mode") == want:
                return round(time.monotonic() - start, 1)
            time.sleep(0.5)
        return None

    def sleep_until(self, t):
        while self.now() < t:
            time.sleep(0.2)

    def main(self):
        self.w.mkdir(parents=True, exist_ok=True)
        self.netmode(self.link)
        self.start("cloud", ["-m", "heartbeat.cloud.app", "--port", str(CLOUD), "--host", "127.0.0.1"])
        self.start("proxy", ["tools/netcut_proxy.py", "--listen", str(PROXY), "--target",
                             f"127.0.0.1:{CLOUD}", "--mode-file", str(self.w / "netmode.txt")])
        self.start_mock(100000)  # long session 1; we kill the mock at t=80
        time.sleep(1.0)
        self.start("pipeline", ["-m", "heartbeat", "run"])
        self.note("started")
        report = {"timeline": self.timeline}

        self.sleep_until(20)
        self.netmode("blackhole")
        self.note("network_cut")
        report["offline_after_s"] = self.wait_mode("offline", 20)
        self.note("mode_offline", after_s=report["offline_after_s"])
        self.sleep_until(45)
        self.netmode(self.link)
        self.note("network_restored")
        report["online_after_s"] = self.wait_mode("online", 20)
        self.note("mode_online", after_s=report["online_after_s"])

        self.sleep_until(60)
        self.kill("pipeline")
        self.note("pipeline_killed")
        time.sleep(2)
        self.start("pipeline", ["-m", "heartbeat", "run"])
        self.note("pipeline_restarted")

        self.sleep_until(80)
        self.kill("mock")
        self.note("mock_killed")
        time.sleep(1)
        self.start_mock(30 if self.short else 60)
        self.note("mock_restarted")

        # wait for the new session to end and the backlog to drain
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            sessions = [json.loads(l) for l in self.cli("sessions").splitlines() if l.strip()]
            s = self.status()
            if sessions and all(x["closed"] for x in sessions) and s.get("backlog") == 0:
                break
            time.sleep(2)
        time.sleep(5)  # last uploads and processing
        self.note("drained", status=self.status())

        truth = load_truth(self.w / "truth.jsonl")
        report["sessions"] = {}
        for sid in truth:
            sr = report["sessions"][sid] = {}
            for src in ("device", "cloud"):
                path = self.w / f"export_{sid}_{src}.jsonl"
                path.write_text(self.cli("export", "--session", sid, "--from", src))
                sr[src] = check(truth, path, sid)[0]
            res_path = self.w / f"results_{sid}.jsonl"
            res_path.write_text(self.cli("results", "--session", sid))
            results = [json.loads(l) for l in res_path.read_text().splitlines() if l.strip()]
            sr["results_by_mode"] = {m: sum(r["mode"] == m for r in results) for m in ("online", "offline")}
            s1, s2 = reference_results.load_export(self.w / f"export_{sid}_device.jsonl")
            last = max(list(s1) + list(s2) + [-1]) // 400
            ref = {r["window"]: r for r in reference_results.compute(s1, s2, last)}
            mism = [r for r in results if any(ref.get(r["window"], {}).get(k) != r.get(k)
                                              for k in reference_results.FIELDS)]
            sr["results_mismatching_reference"] = len(mism)
            sr["windows_without_result"] = sorted(set(ref) - {r["window"] for r in results})[:20]
            start = next(json.loads(l)["wall"] for l in (self.w / "truth.jsonl").read_text().splitlines()
                         if json.loads(l).get("event") == "session_start" and json.loads(l)["session_id"] == sid)
            late = sorted(r["processed_at"] - (start + r["window"] + 1) for r in results
                          if r["mode"] == "online" and r.get("processed_at"))
            if late:
                sr["online_latency_s"] = {"median": round(late[len(late) // 2], 2),
                                          "p95": round(late[int(len(late) * 0.95)], 2),
                                          "over_5s": sum(x > 5 for x in late)}
        for name in list(self.procs):
            self.kill(name)
        (self.w / "report.json").write_text(json.dumps(report, indent=2))
        print(json.dumps(report, indent=2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", default="e2e_run")
    ap.add_argument("--short", action="store_true")
    ap.add_argument("--slow", type=int, default=0, help="limit the link to N bytes/s (poor mobile network)")
    a = ap.parse_args()
    run = Run(Path(a.workdir).resolve(), a.short, a.slow)
    try:
        run.main()
    finally:
        for name in list(run.procs):
            run.kill(name)


if __name__ == "__main__":
    main()
