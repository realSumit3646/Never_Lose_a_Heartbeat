"""Streams saved data to the cloud and decides online / offline mode.

Online:  every ~0.3 s, send everything not yet confirmed (chunks the cloud
         needs to keep processing first, then the oldest backlog), plus loss
         records, offline results and session info. A chunk is only marked as
         in the cloud after the cloud commits it, and the cloud ignores chunks
         it already has, so a lost response just means a harmless re-send.
Offline: probe the cloud every ~1.5 s; the processor produces results locally.

Switching: a failed upload -> offline (at most request_timeout + upload_interval,
about 5 s); a successful probe -> online (about 1.5-5 s).
"""
from __future__ import annotations

import logging
import time

from ..codec import to_wire
from .cloudclient import CloudClient, CloudError
from .store import DiskFull

log = logging.getLogger("uploader")

MIN_BATCH = 32 * 1024


class Uploader:
    def __init__(self, cfg, store, stop_event):
        self.cfg = cfg
        self.store = store
        self.stop = stop_event
        self.client = CloudClient(cfg)
        # "starting" until the first probe: neither the cloud nor the device emits results
        # as offline just because the service restarted.
        self.mode = "starting"
        self.budget = cfg.batch_bytes
        self.last_ok = 0.0
        self.store.set_meta(mode="starting", mode_since=time.time(), mode_reason="service start")

    def set_mode(self, mode, reason):
        if mode == self.mode:
            return
        log.warning("mode %s -> %s (%s)", self.mode, mode, reason)
        self.mode = mode
        try:
            self.store.set_meta(mode=mode, mode_since=time.time(), mode_reason=reason)
        except DiskFull:
            pass
        self.store.event("mode_" + mode, reason)

    def probe(self) -> bool:
        try:
            self.client.get("/health", timeout=self.cfg.probe_timeout, retries=1)
            return True
        except CloudError:
            return False

    def run(self):
        while not self.stop.is_set():
            if self.mode == "online":
                self._online_tick()
            elif self.probe():
                self.set_mode("online", "cloud reachable")
            else:
                self.set_mode("offline", "cloud unreachable")
                self.stop.wait(1.5)

    def _online_tick(self):
        batch = self.store.build_batch(self.budget)
        body = self._encode(batch)
        started = time.monotonic()
        try:
            resp = self.client.request("POST", "/ingest", body, timeout=self.cfg.request_timeout)
        except CloudError as e:
            # No answer within request_timeout: treat the cloud as unreachable now
            # (the 10 s switch limit leaves no room for a second, slower check).
            # Send less next time in case the request was just too big for the link.
            self.budget = max(MIN_BATCH, self.budget // 2)
            self.set_mode("offline", f"upload failed: {e}")
            return
        took = time.monotonic() - started
        if took > 2.0:
            log.warning("slow upload: %.1f s for %d chunks (%d bytes); cloud timing %s",
                        took, len(batch["chunks"]), batch["bytes"], resp.get("timing"))
        try:
            self.store.ack(batch, resp.get("sessions", {}))
        except DiskFull:
            pass  # chunks are re-sent later; the cloud ignores what it already has
        self.last_ok = time.monotonic()
        self.budget = min(self.cfg.batch_bytes, self.budget * 2)
        if not batch["full"]:
            self.stop.wait(self.cfg.upload_interval)

    def _encode(self, batch):
        sessions = [
            {"session": sid, "seq": i["seq"], "closed": i["closed"], "last_window": i["last_window"],
             "end_1": i["end_1"], "end_2": i["end_2"]}
            for sid, i in batch["sessions"].items() if i["version"] > i["sent"] or not i["closed"]]
        return {
            "device_id": self.cfg.device_id,
            "sent_wall": time.time(),
            "sessions": sessions,
            "chunks": [[s, sensor, first, count, to_wire(data)]
                       for _, s, sensor, first, count, data in batch["chunks"]],
            "losses": [{k: l[k] for k in ("session", "sensor", "from_index", "to_index", "reason")}
                       for l in batch["losses"]],
            "results": batch["results"],
            "resumes": batch["resumes"],
        }
