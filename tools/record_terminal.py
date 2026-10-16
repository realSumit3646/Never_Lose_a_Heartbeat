"""Run a command and record its output lines with timestamps (for rendering a video).

  python tools/record_terminal.py OUT.jsonl -- python -u tools/demo.py --env-file ../hb.env
"""
import json
import subprocess
import sys
import time

out_path, cmd = sys.argv[1], sys.argv[sys.argv.index("--") + 1:]
t0 = time.time()
with open(out_path, "w", encoding="utf-8") as out:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    for line in p.stdout:
        line = line.rstrip("\n")
        out.write(json.dumps({"t": round(time.time() - t0, 3), "line": line}) + "\n")
        out.flush()
        print(line, flush=True)
    p.wait()
