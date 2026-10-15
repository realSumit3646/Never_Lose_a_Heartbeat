"""TCP proxy that can simulate a network cut, for testing on one machine.

  python tools/netcut_proxy.py --listen 18081 --target 127.0.0.1:18080 --mode-file netmode.txt

The mode file holds one word, re-read every 0.2 s:
  pass       forward traffic normally
  slow:N     forward at most N bytes per second (a poor mobile link)
  blackhole  stop forwarding (like Wi-Fi dropping: the client sees timeouts)
On the real device, cut the network instead (see deploy/fault_tests.sh).
"""
from __future__ import annotations

import argparse
import socket
import threading
import time


class Proxy:
    def __init__(self, listen, target, mode_file):
        self.listen, self.target, self.mode_file = listen, target, mode_file
        self.mode = "pass"
        self.generation = 0  # bumped on every cut; connections from an older generation die

    def watch(self):
        while True:
            try:
                with open(self.mode_file) as f:
                    m = f.read().strip() or "pass"
            except OSError:
                m = "pass"
            if m != self.mode:
                print(f"[proxy] {self.mode} -> {m}", flush=True)
                if not self.passing(m):
                    self.generation += 1
                self.mode = m
            time.sleep(0.2)

    @staticmethod
    def passing(mode):
        return mode == "pass" or mode.startswith("slow:")

    def pipe(self, src, dst, gen, done):
        """Copy src -> dst. On EOF half-close dst; close both once both directions end."""
        cut = False
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                while not self.passing(self.mode) and gen == self.generation:
                    time.sleep(0.1)  # hold traffic: the peer sees a stall
                if gen != self.generation:
                    cut = True  # a cut happened while this connection was open
                    break
                if self.mode.startswith("slow:"):
                    time.sleep(len(data) / max(1, int(self.mode[5:])))
                dst.sendall(data)
            if not cut:
                dst.shutdown(socket.SHUT_WR)
        except OSError:
            cut = True
        with done["lock"]:
            done["n"] += 1
            last = done["n"] == 2
        if cut or last:
            for s in (src, dst):
                try:
                    s.close()
                except OSError:
                    pass

    def handle(self, client):
        gen = self.generation
        while not self.passing(self.mode):
            time.sleep(0.1)  # accepted but silent, like a dead network
            if gen != self.generation:
                gen = self.generation
        if gen != self.generation:
            client.close()
            return
        try:
            upstream = socket.create_connection(self.target, timeout=5)
        except OSError:
            client.close()
            return
        done = {"n": 0, "lock": threading.Lock()}
        threading.Thread(target=self.pipe, args=(client, upstream, gen, done), daemon=True).start()
        threading.Thread(target=self.pipe, args=(upstream, client, gen, done), daemon=True).start()

    def serve(self):
        threading.Thread(target=self.watch, daemon=True).start()
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", self.listen))
        srv.listen(50)
        print(f"[proxy] 127.0.0.1:{self.listen} -> {self.target[0]}:{self.target[1]}", flush=True)
        while True:
            client, _ = srv.accept()
            threading.Thread(target=self.handle, args=(client,), daemon=True).start()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", type=int, required=True)
    ap.add_argument("--target", required=True, help="host:port")
    ap.add_argument("--mode-file", required=True)
    a = ap.parse_args()
    host, port = a.target.rsplit(":", 1)
    Proxy(a.listen, (host, int(port)), a.mode_file).serve()


if __name__ == "__main__":
    main()
