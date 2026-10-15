"""Time small SQLite transactions with each journal / sync mode, in a given directory.

Run where the cloud database lives (e.g. PythonAnywhere): python3 sqlite_bench.py ~/data
"""
import os
import sqlite3
import sys
import time

d = os.path.expanduser(sys.argv[1] if len(sys.argv) > 1 else ".")
blob = os.urandom(4000)
for journal in ("DELETE", "TRUNCATE", "PERSIST", "WAL"):
    for sync in ("FULL", "NORMAL"):
        path = os.path.join(d, "bench.db")
        for suffix in ("", "-wal", "-shm", "-journal"):
            if os.path.exists(path + suffix):
                os.remove(path + suffix)
        try:
            c = sqlite3.connect(path, isolation_level=None)
            c.execute(f"PRAGMA journal_mode={journal}")
            c.execute(f"PRAGMA synchronous={sync}")
            c.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b BLOB)")
            times = []
            for i in range(20):
                t = time.time()
                c.execute("BEGIN IMMEDIATE")
                c.execute("INSERT INTO t(b) VALUES(?)", (blob,))
                c.execute("INSERT INTO t(b) VALUES(?)", (blob,))
                c.execute("COMMIT")
                times.append(time.time() - t)
            c.close()
            times.sort()
            print(f"{journal:8} {sync:6} median {times[10] * 1000:7.1f} ms   max {times[-1] * 1000:7.1f} ms")
        except sqlite3.Error as e:
            print(f"{journal:8} {sync:6} ERROR {e}")
for suffix in ("", "-wal", "-shm", "-journal"):
    if os.path.exists(os.path.join(d, "bench.db") + suffix):
        os.remove(os.path.join(d, "bench.db") + suffix)
