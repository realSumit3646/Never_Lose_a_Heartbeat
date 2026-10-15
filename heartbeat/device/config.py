"""Pipeline configuration: INI file, overridable by HEARTBEAT_* environment variables."""
from __future__ import annotations

import configparser
import os
from dataclasses import dataclass, fields

DEFAULT_PATHS = ["/etc/heartbeat/pipeline.ini", "pipeline.ini"]


@dataclass
class Config:
    mock_host: str = "127.0.0.1"
    mock_port: int = 7100
    data_dir: str = "/var/lib/heartbeat"
    cloud_url: str = ""
    api_key: str = ""
    device_id: str = "pi-1"
    poll_interval: float = 0.2       # seconds between read cycles
    reply_timeout: float = 2.0       # wait for one mock reply
    silent_reconnect: float = 20.0   # reconnect after this long without replies
    upload_interval: float = 0.3     # seconds between uploads when not catching up
    request_timeout: float = 4.5     # one upload request
    probe_timeout: float = 3.5       # connectivity probe (a fresh TLS handshake can take seconds)
    batch_bytes: int = 256 * 1024    # raw sample bytes per upload request (about 30 s of data)
    tls_ignore_clock: bool = True    # keep TLS working if the clock is changed
    reserve_mb: int = 16             # disk kept free to record losses when the disk fills

    @property
    def db_path(self) -> str:
        return os.path.join(self.data_dir, "pipeline.db")


def load(path: str = None) -> Config:
    cfg = Config()
    candidates = [path] if path else ([os.environ["HEARTBEAT_CONFIG"]]
                                      if os.environ.get("HEARTBEAT_CONFIG") else DEFAULT_PATHS)
    parser = configparser.ConfigParser()
    for p in candidates:
        if p and os.path.exists(p):
            parser.read(p)
            break
    raw = dict(parser["pipeline"]) if parser.has_section("pipeline") else {}
    for f in fields(Config):
        env = os.environ.get("HEARTBEAT_" + f.name.upper())
        value = env if env is not None else raw.get(f.name)
        if value is None:
            continue
        if f.type in ("bool", bool):
            value = str(value).strip().lower() in ("1", "true", "yes", "on")
        elif f.type in ("int", int):
            value = int(value)
        elif f.type in ("float", float):
            value = float(value)
        setattr(cfg, f.name, value)
    return cfg
