"""Narrated demo for the video (about 4 minutes): network cut, process kill, power cut.

  python tools/demo.py --env-file hb.env

Runs the ARM64 device container (mock + pipeline) against the real cloud,
prints `pipeline status` every second with a caption for each step, then
checks the device and cloud copies against the mock's ground truth.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import time

NAME = "hb-demo"
MOCK_ARGS = "--mode signal --seed 7 --duration 150 --port 7100 --fault-rate 0.02"


def docker(*args, check=True):
    r = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=600)
    if check and r.returncode != 0:
        raise RuntimeError(r.stderr.strip())
    return r.stdout


def caption(text):
    print("\n" + "=" * 78 + f"\n  {text}\n" + "=" * 78, flush=True)


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", required=True)
    a = ap.parse_args()
    docker("rm", "-f", NAME, check=False)
    for v in (f"{NAME}-data", f"{NAME}-mock"):
        docker("volume", "rm", v, check=False)

    caption("1. Start the ARM64 device (Raspberry Pi limits: 4 CPUs, 2 GB) with mock + pipeline")
    docker("run", "-d", "--name", NAME, "--platform", "linux/arm64", "--cpus", "4", "--memory", "2g",
           "--env-file", a.env_file, "-e", "HEARTBEAT_DEVICE_ID=demo", "-e", "MOCK_ARGS=" + MOCK_ARGS,
           "-v", f"{NAME}-data:/var/lib/heartbeat", "-v", f"{NAME}-mock:/var/lib/biosignal-mock",
           "heartbeat-device")
    print("  " + docker("exec", NAME, "uname", "-m").strip() + " (ARM64)")
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

    caption("6. Recent events recorded by the device (evidence)")
    for line in docker("exec", NAME, "pipeline", "events", "--last", "25").splitlines():
        e = json.loads(line)
        if e["event"] != "corrupt_reply":
            print(f"  {e['time']}  {e['event']:<16} {e['detail'][:70]}")

    caption("7. Waiting for the second session to end and the backlog to reach 0")
    watch(t0, 200, until=lambda s: s.get("backlog") == 0 and s.get("mode") == "online"
          and all(json.loads(l)["closed"] for l in docker("exec", NAME, "pipeline", "sessions").splitlines()))

    caption("8. VERIFY: device and cloud copies vs the mock's ground truth (every sample exactly once)")
    for line in docker("exec", NAME, "pipeline", "sessions").splitlines():
        sid = json.loads(line)["session"]
        for src in ("device", "cloud"):
            for attempt in range(4):  # the cloud export needs a working network
                try:
                    docker("exec", NAME, "sh", "-c",
                           f"pipeline export --session {sid} --from {src} > /tmp/{sid}_{src}.jsonl")
                    break
                except RuntimeError as e:
                    print(f"  export from {src} failed ({e}); retrying", flush=True)
                    time.sleep(5)
            else:
                print(f"  session {sid} {src:<6}: could not export (network); run again later")
                continue
            out = docker("exec", NAME, "check_ground_truth", "--truth", "/var/lib/biosignal-mock/mock_truth.jsonl",
                         "--export", f"/tmp/{sid}_{src}.jsonl", "--session", sid, check=False)
            r = json.loads(out.splitlines()[0])
            print(f"  session {sid} {src:<6}: exported={r['exported']:>6} listed_lost={r['listed_lost']:>5} "
                  f"silent_loss={r['silent_loss']} duplicates={r['duplicates']} wrong={r['wrong_values']} "
                  f"-> {'OK' if r['ok'] else 'FAIL'}")
    docker("rm", "-f", NAME, check=False)
    caption("Done")


if __name__ == "__main__":
    main()
