"""On-device processing (section 3) for offline mode.

The device keeps its window results up to date at all times (cheap: about
400 samples per second), so it can take over instantly when the network
drops. Results only become *offline results* when the device is offline and
the cloud has not processed that window; in online mode the cloud's results
are the output.
"""
from __future__ import annotations

import logging

from ..processing import advance
from ..sqlsource import SqliteSource
from .store import DiskFull

log = logging.getLogger("processor")


class Processor:
    def __init__(self, store, uploader, stop_event):
        self.store = store
        self.uploader = uploader
        self.stop = stop_event

    def run(self):
        while not self.stop.is_set():
            busy = False
            try:
                for sid, state, last_window, ends in self.store.sessions_to_process():
                    src = SqliteSource(self.store.conn, sid, ends)
                    out, new_state = advance(src, sid, state, last_window, max_windows=200)
                    if out:
                        self.store.save_results(sid, out, new_state)
                        busy |= len(out) == 200
                if self.uploader.mode == "offline":
                    n = self.store.emit_offline()
                    if n:
                        log.info("%d window(s) processed offline", n)
            except DiskFull:
                log.error("disk full: results delayed")
                self.stop.wait(2.0)
            if not busy:
                self.stop.wait(0.25)
