"""Failure tests on the ARM64 device container (mock + pipeline), against the real cloud.

  python tools/docker_fault_test.py --env-file hb.env [--phase main|diskfull|both]

main:     network cut (container disconnected) -> reconnected -> kill -9 of the
          pipeline -> "power cut" (container hard-killed and restarted; the mock
          restarts too, so a new session begins) -> wait for the end.
diskfull: the pipeline's data folder is a 150 MB disk; it is filled for 30 s,
          then freed.
Afterwards every session is exported from the device and the cloud and
checked against the mock's ground truth, and the results are compared with
tools/reference_results.py. The report goes to evidence/docker_<phase>_report.json.

Note: `docker kill` stops every process at once like a power cut, but the
Docker VM's kernel still flushes their written data, so this does not prove
fsync behaviour. Pull the plug on a real Pi (or hard-reset a VM) for that.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import reference_results  # noqa: E402

IMAGE = "heartbeat-device"
MOCK_ARGS = ("--mode signal --seed 7 --duration {d} --port 7100 --fault-rate 0.02 "
             "--pause-every 50 --pause-for 3 --dropout-every 70 --dropout-for 2")


def sh(*args, check=True, timeout=300):
    r = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout


class Test:
    def __init__(self, name, env_file, workdir):
        self.name = name
        self.env_file = env_file
        self.w = workdir
        self.t0 = time.time()
        self.timeline = []

    def note(self, event, **kw):
        e = {"t": round(time.time() - self.t0, 1), "wall": round(time.time(), 3), "event": event, **kw}
        self.timeline.append(e)
        print(json.dumps(e), flush=True)

    def exec(self, *cmd, check=True, timeout=300):
        return sh("exec", self.name, *cmd, check=check, timeout=timeout)

    def status(self):
        try:
            return json.loads(self.exec("pipeline", "status", check=False, timeout=60).strip().splitlines()[-1])
        except (ValueError, IndexError):
            return {}

    def start(self, duration, extra=()):
        sh("rm", "-f", self.name, check=False)
        for v in (f"{self.name}-data", f"{self.name}-mock"):
            sh("volume", "rm", v, check=False)
        data = ["--mount", f"type=tmpfs,destination=/var/lib/heartbeat,tmpfs-size={150 * 2**20}"] \
            if "tmpfs" in extra else ["-v", f"{self.name}-data:/var/lib/heartbeat"]
        sh("run", "-d", "--name", self.name, "--platform", "linux/arm64", "--cpus", "4", "--memory", "2g",
           "--env-file", str(self.env_file), "-e", f"HEARTBEAT_DEVICE_ID={self.name}",
           "-e", "MOCK_ARGS=" + MOCK_ARGS.format(d=duration), *data,
           "-v", f"{self.name}-mock:/var/lib/biosignal-mock", IMAGE)
        self.note("container_started")

    def wait_until(self, t):
        while time.time() - self.t0 < t:
            time.sleep(0.5)

    def wait_mode(self, want, limit):
        start = time.time()
        while time.time() - start < limit:
            if self.status().get("mode") == want:
                return
            time.sleep(0.5)

    def drain(self, limit=600):
        deadline = time.time() + limit
        while time.time() < deadline:
            sessions = [json.loads(l) for l in self.exec("pipeline", "sessions", check=False).splitlines() if l.strip()]
            s = self.status()
            if sessions and all(x["closed"] for x in sessions) and s.get("backlog") == 0:
                break
            time.sleep(5)
        time.sleep(8)
        self.note("drained", status=self.status())

    def verify(self):
        report = {"sessions": {}}
        sessions = [json.loads(l)["session"] for l in self.exec("pipeline", "sessions").splitlines() if l.strip()]
        for sid in sessions:
            r = report["sessions"][sid] = {}
            for src in ("device", "cloud"):
                path = f"/tmp/export_{sid}_{src}.jsonl"
                self.exec("sh", "-c", f"pipeline export --session {sid} --from {src} > {path}")
                out = self.exec("check_ground_truth", "--truth", "/var/lib/biosignal-mock/mock_truth.jsonl",
                                "--export", path, "--session", sid, check=False)
                r[src] = json.loads(out.splitlines()[0])
            local = self.w / f"export_{sid}_device.jsonl"
            sh("cp", f"{self.name}:/tmp/export_{sid}_device.jsonl", str(local))
            results = [json.loads(l) for l in self.exec("pipeline", "results", "--session", sid).splitlines()
                       if l.strip()]
            s1, s2 = reference_results.load_export(local)
            last = max(list(s1) + list(s2) + [-1]) // 400
            ref = {x["window"]: x for x in reference_results.compute(s1, s2, last)}
            r["results_by_mode"] = {m: sum(x["mode"] == m for x in results) for m in ("online", "offline")}
            r["results_mismatching_reference"] = sum(
                any(ref.get(x["window"], {}).get(k) != x.get(k) for k in reference_results.FIELDS) for x in results)
            r["windows_without_result"] = sorted(set(ref) - {x["window"] for x in results})[:20]
            loss_reasons = {}
            for l in local.read_text().splitlines():
                d = json.loads(l)
                if "reason" in d:
                    loss_reasons[d["reason"]] = loss_reasons.get(d["reason"], 0) + 1
            r["loss_reasons"] = loss_reasons
        report["events"] = [json.loads(l) for l in self.exec("pipeline", "events", "--last", "200").splitlines()
                            if l.strip()]
        return report

    def switch_delay(self, events, after_wall, kind):
        for e in events:
            if e["event"] == kind and e["wall"] >= after_wall:
                return round(e["wall"] - after_wall, 1)
        return None


def phase_main(t: Test):
    t.start(240)
    t.wait_until(35)
    sh("network", "disconnect", "bridge", t.name)
    t.note("network_cut")
    cut = time.time()
    t.wait_mode("offline", 30)
    t.wait_until(80)
    sh("network", "connect", "bridge", t.name)
    t.note("network_restored")
    restored = time.time()
    t.wait_mode("online", 30)
    t.wait_until(110)
    t.exec("pkill", "-9", "-f", "heartbeat run", check=False)
    t.note("pipeline_kill_9")
    t.wait_until(140)
    sh("kill", t.name)
    t.note("power_cut")
    time.sleep(3)
    sh("start", t.name)
    t.note("power_restored")
    t.drain()
    report = t.verify()
    ev = report["events"]
    report["offline_after_cut_s"] = t.switch_delay(ev, cut, "mode_offline")
    report["online_after_restore_s"] = t.switch_delay(ev, restored, "mode_online")
    return report


def phase_diskfull(t: Test):
    t.start(150, extra=("tmpfs",))
    t.wait_until(40)
    t.exec("sh", "-c", "dd if=/dev/zero of=/var/lib/heartbeat/filler bs=1M 2>/dev/null; "
                       "df -h /var/lib/heartbeat | tail -1", check=False)
    t.note("disk_filled")
    t.wait_until(75)
    t.exec("rm", "-f", "/var/lib/heartbeat/filler")
    t.note("disk_freed")
    t.drain()
    return t.verify()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", required=True, help="HEARTBEAT_CLOUD_URL / HEARTBEAT_API_KEY")
    ap.add_argument("--phase", choices=["main", "diskfull", "both"], default="both")
    ap.add_argument("--workdir", default=str(ROOT / "evidence"))
    a = ap.parse_args()
    w = Path(a.workdir)
    w.mkdir(parents=True, exist_ok=True)
    for phase in (["main", "diskfull"] if a.phase == "both" else [a.phase]):
        t = Test(f"hb-{phase}", a.env_file, w)
        try:
            report = (phase_main if phase == "main" else phase_diskfull)(t)
        finally:
            sh("rm", "-f", t.name, check=False)
        report["timeline"] = t.timeline
        out = ROOT / "evidence" / f"docker_{phase}_report.json"
        out.write_text(json.dumps(report, indent=2))
        summary = {sid: {"device_ok": s["device"]["ok"], "cloud_ok": s["cloud"]["ok"],
                         "silent_loss": (s["device"]["silent_loss"], s["cloud"]["silent_loss"]),
                         "results": s["results_by_mode"], "mismatch": s["results_mismatching_reference"],
                         "losses": s["loss_reasons"]}
                   for sid, s in report["sessions"].items()}
        print(json.dumps({"phase": phase, "offline_after_cut_s": report.get("offline_after_cut_s"),
                          "online_after_restore_s": report.get("online_after_restore_s"),
                          "sessions": summary}, indent=2))


if __name__ == "__main__":
    main()
