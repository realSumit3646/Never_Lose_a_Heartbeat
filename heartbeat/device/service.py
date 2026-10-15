"""`pipeline run`: the long-running device service (collector, uploader, processor).

Each part runs in its own thread with its own database connection. If any
thread dies unexpectedly the whole process exits non-zero, and systemd
restarts it; on start, everything resumes from what is on disk.
"""
from __future__ import annotations

import logging
import os
import shutil
import signal
import threading
import time

from .collector import Collector
from .processor import Processor
from .store import DiskFull, Store
from .uploader import Uploader

log = logging.getLogger("service")


def ensure_reserve(cfg):
    """Keep a reserve file that is deleted when the disk fills, so losses can still be written."""
    path = os.path.join(cfg.data_dir, "reserve.bin")
    size = cfg.reserve_mb * 1024 * 1024
    if os.path.exists(path) or size <= 0:
        return
    if shutil.disk_usage(cfg.data_dir).free < 4 * size:
        return
    with open(path, "wb") as f:
        block = b"\0" * (1024 * 1024)
        for _ in range(cfg.reserve_mb):
            f.write(block)
        f.flush()
        os.fsync(f.fileno())


def run(cfg):
    os.makedirs(cfg.data_dir, exist_ok=True)
    ensure_reserve(cfg)
    stop = threading.Event()
    main_store = Store(cfg.db_path)
    main_store.event("service_start", f"pid {os.getpid()}")
    collector = Collector(cfg, Store(cfg.db_path), stop)
    uploader = Uploader(cfg, Store(cfg.db_path), stop)
    processor = Processor(Store(cfg.db_path), uploader, stop)
    threads = [threading.Thread(target=t.run, name=n, daemon=True)
               for n, t in (("collector", collector), ("uploader", uploader), ("processor", processor))]

    def on_signal(signum, _frame):
        log.info("signal %s: stopping", signum)
        stop.set()

    signal.signal(signal.SIGINT, on_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, on_signal)
    for t in threads:
        t.start()
    log.info("pipeline running (data in %s, cloud %s)", cfg.data_dir, cfg.cloud_url or "not set")
    last_reserve_check = time.monotonic()
    last_heartbeat = 0.0
    while not stop.is_set():
        for t in threads:
            if not t.is_alive():
                log.critical("thread %s died; exiting so the service manager restarts us", t.name)
                main_store.event("thread_died", t.name)
                os._exit(1)
        if time.monotonic() - last_heartbeat >= 5:  # every write costs an fsync; 5 s is enough
            last_heartbeat = time.monotonic()
            try:
                main_store.set_meta(heartbeat_mono=time.monotonic(), heartbeat_wall=time.time(),
                                    pid=os.getpid(), collector=collector.stats)
            except DiskFull:
                pass
        if time.monotonic() - last_reserve_check > 60:
            last_reserve_check = time.monotonic()
            try:
                ensure_reserve(cfg)
            except OSError:
                pass
        stop.wait(1.0)
    for t in threads:
        t.join(timeout=5)
    main_store.event("service_stop", "clean shutdown")
