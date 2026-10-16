"""Narrated demo for the video (about 5 minutes): network cut, process kill, power cut.

  python tools/demo.py --env-file hb.env                # real cloud
  python tools/demo.py --env-file hb.env --local-cloud  # cloud service on this laptop (no internet needed)

Runs the ARM64 device container (mock + pipeline), prints `pipeline status`
every second with a caption for each step, then checks the device and cloud
copies against the mock's ground truth. Captions make it readable without voice.
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
NAME = "hb-demo"
MOCK_ARGS = "--mode signal --seed 7 --duration 150 --port 7100 --fault-rate 0.02"


def docker(*args, check=True):
    r = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=600)
    if check and r.returncode != 0:
        raise RuntimeError(r.stderr.strip())
    return r.stdout


def caption(text, pause=3):
    print("\n" + "=" * 78 + f"\n  {text}\n" + "=" * 78, flush=True)
    time.sleep(pause)  # time to read it in the video


def status_line(t0):
    try:
        s = json.loads(docker("exec", NAME, "pipeline", "status", check=False).strip().splitlines()[-1])
    except (ValueError, IndexError, RuntimeError):
        print(f"  t={time.time() - t0:6.1f}s  (device restarting...)", flush=True)
        return {}
    print(f"  t={time.time() - t0:6.1f}s  mode={s['mode']:<8} session={s['session']}  saved={s['samples_saved']:>7}"
          f"  in_cloud={s['samples_in_cloud']:>7}  backlog={s['backlog']:>5}  loss_records={s['loss_records']}",
          flush=True)
    return s


def watch(t0, seconds, until=None):
    end = time.time() + seconds
    while time.time() < end:
        s = status_line(t0)
        if until and until(s):
            return
        time.sleep(1)


def all_closed():
    try:
        return all(json.loads(l)["closed"] for l in docker("exec", NAME, "pipeline", "sessions").splitlines())
    except (RuntimeError, ValueError):
        return False


def run_demo(a, extra_env, cloud_desc):
    caption("NEVER LOSE A HEARTBEAT - failure demo (MayaCardio Software Systems Engineer case)\n"
            "  Device: ARM64 Raspberry Pi-class container (4 CPUs, 2 GB): biosignal-mock + pipeline\n"
            f"  Cloud:  {cloud_desc}\n"
            "  Shown:  network cut, process kill (kill -9), power cut, then every sample is verified\n"
            "  Each line below is `pipeline status`, printed once per second", pause=8)
    docker("rm", "-f", NAME, check=False)
    for v in (f"{NAME}-data", f"{NAME}-mock"):
        docker("volume", "rm", v, check=False)

    caption("1. Start the ARM64 device (Raspberry Pi limits: 4 CPUs, 2 GB) with mock + pipeline")
    docker("run", "-d", "--name", NAME, "--platform", "linux/arm64", "--cpus", "4", "--memory", "2g",
           "--env-file", a.env_file, *extra_env, "-e", "HEARTBEAT_DEVICE_ID=demo", "-e", "MOCK_ARGS=" + MOCK_ARGS,
           "-v", f"{NAME}-data:/var/lib/heartbeat", "-v", f"{NAME}-mock:/var/lib/biosignal-mock",
           "heartbeat-device")
    print("  CPU architecture inside the device: " + docker("exec", NAME, "uname", "-m").strip())
    t0 = time.time()
    watch(t0, 15)

    caption("2. NETWORK CUT: device disconnected. Expect mode=offline within 10 s; saving continues")
    docker("network", "disconnect", "bridge", NAME)
    watch(t0, 25)

    caption("3. NETWORK BACK: expect mode=online within 10 s, then the backlog drains to 0")
    docker("network", "connect", "bridge", NAME)
    watch(t0, 25)

    caption("4. PROCESS KILL: kill -9 the pipeline. It restarts and continues from disk; nothing lost")
    docker("exec", NAME, "pkill", "-9", "-f", "heartbeat run", check=False)
    watch(t0, 15)

    caption("5. POWER CUT: the whole device is killed at once, then powered on again.\n"
            "  The mock restarts too (new session); the old session's unread tail is listed as lost")
    docker("kill", NAME)
    time.sleep(2)
    docker("start", NAME)
    watch(t0, 20)

    caption("6. Events recorded by the device (evidence)")
    for line in docker("exec", NAME, "pipeline", "events", "--last", "30").splitlines():
        e = json.loads(line)
        if e["event"] != "corrupt_reply":
            print(f"  {e['time']}  {e['event']:<16} {e['detail'][:70]}")
    time.sleep(6)

    caption("7. Waiting for the second session to end and the backlog to reach 0")
    watch(t0, 200, until=lambda s: s.get("backlog") == 0 and s.get("mode") == "online" and all_closed())

    caption("8. VERIFY: device and cloud copies vs the mock's ground truth (every sample exactly once)")
    for line in docker("exec", NAME, "pipeline", "sessions").splitlines():
        sid = json.loads(line)["session"]
        for src in ("device", "cloud"):
            for _ in range(4):  # the cloud export needs a working network
                try:
                    docker("exec", NAME, "sh", "-c",
                           f"pipeline export --session {sid} --from {src} > /tmp/{sid}_{src}.jsonl")
                    break
                except RuntimeError as e:
                    print(f"  export from {src} failed ({str(e)[:60]}); retrying", flush=True)
                    time.sleep(5)
            else:
                print(f"  session {sid} {src:<6}: could not export (network); run again later")
                continue
            out = docker("exec", NAME, "check_ground_truth", "--truth", "/var/lib/biosignal-mock/mock_truth.jsonl",
                         "--export", f"/tmp/{sid}_{src}.jsonl", "--session", sid, check=False)
            r = json.loads(out.splitlines()[0])
            print(f"  session {sid} {src:<6}: exported={r['exported']:>6} listed_lost={r['listed_lost']:>5} "
                  f"silent_loss={r['silent_loss']} duplicates={r['duplicates']} wrong={r['wrong_values']} "
                  f"-> {'OK' if r['ok'] else 'FAIL'}", flush=True)
    time.sleep(4)

    caption("SUMMARY\n"
            "  - Network cut: switched to offline, kept saving and processing on the device\n"
            "  - Network back: switched to online, uploaded the backlog exactly once\n"
            "  - kill -9 and power cut: restarted from disk; unreadable data listed as lost, never invented\n"
            "  - Device and cloud copies checked against the mock's ground truth: OK means no silent loss,\n"
            "    no duplicates, no wrong values", pause=10)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", required=True)
    ap.add_argument("--local-cloud", action="store_true",
                    help="run the cloud service on this machine instead of the real one")
    ap.add_argument("--ca-bundle", help="extra CA bundle for the container (e.g. when an antivirus "
                                        "re-signs HTTPS traffic on this laptop)")
    a = ap.parse_args()
    extra_env, cloud_proc = [], None
    if a.ca_bundle:
        extra_env += ["-v", f"{os.path.abspath(a.ca_bundle)}:/certs/bundle.pem:ro",
                      "-e", "SSL_CERT_FILE=/certs/bundle.pem"]
    cloud_desc = "PythonAnywhere (free tier) over the internet"
    if a.local_cloud:
        db = ROOT / "demo_cloud.db"
        for s in ("", "-wal", "-shm"):
            if os.path.exists(str(db) + s):
                os.remove(str(db) + s)
        env = dict(os.environ, PYTHONPATH=str(ROOT), HEARTBEAT_CLOUD_DB=str(db), HEARTBEAT_API_KEY="demo-key")
        cloud_proc = subprocess.Popen([sys.executable, "-m", "heartbeat.cloud.app", "--port", "18080"],
                                      cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        extra_env += ["-e", "HEARTBEAT_CLOUD_URL=http://host.docker.internal:18080",
                      "-e", "HEARTBEAT_API_KEY=demo-key"]
        cloud_desc = "the same cloud service, run on this laptop (recorded on a train without internet)"
        time.sleep(2)
    try:
        run_demo(a, extra_env, cloud_desc)
    finally:
        docker("rm", "-f", NAME, check=False)
        if cloud_proc:
            cloud_proc.terminate()


if __name__ == "__main__":
    main()
