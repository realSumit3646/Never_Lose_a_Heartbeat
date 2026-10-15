"""Small HTTP(S) client for the cloud service (standard library only).

One connection is kept open and reused: a new TLS handshake costs two or
more extra round trips, which matters for the 5 s online deadline.
"""
from __future__ import annotations

import http.client
import json
import ssl
import urllib.parse

X509_V_FLAG_NO_CHECK_TIME = 0x200000  # OpenSSL flag: verify the chain but not the dates


class CloudError(Exception):
    pass


class CloudClient:
    def __init__(self, cfg):
        self.base = cfg.cloud_url.rstrip("/")
        self.api_key = cfg.api_key
        u = urllib.parse.urlsplit(self.base)
        self.https = u.scheme == "https"
        self.host = u.hostname
        self.port = u.port
        self.prefix = u.path
        self.ctx = ssl.create_default_context()
        if cfg.tls_ignore_clock:
            # The judges change the system clock. Certificate dates are then
            # "wrong" and every upload would fail; chain and hostname are still checked.
            self.ctx.verify_flags |= X509_V_FLAG_NO_CHECK_TIME
        self.conn = None

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except OSError:
                pass
            self.conn = None

    def _connection(self, timeout):
        if self.conn is None:
            if self.https:
                self.conn = http.client.HTTPSConnection(self.host, self.port, timeout=timeout,
                                                        context=self.ctx)
            else:
                self.conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
            return self.conn, False
        self.conn.timeout = timeout
        if self.conn.sock is not None:
            self.conn.sock.settimeout(timeout)
        return self.conn, True

    def request(self, method, path, body=None, timeout=5.0, params=None):
        if not self.base:
            raise CloudError("cloud_url is not configured")
        url = self.prefix + path + ("?" + urllib.parse.urlencode(params) if params else "")
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json", "Connection": "keep-alive"}
        if self.api_key:
            headers["X-Api-Key"] = self.api_key
        for _ in range(2):
            conn, reused = self._connection(timeout)
            try:
                conn.request(method, url, body=data, headers=headers)
                resp = conn.getresponse()
                payload = resp.read()
            except (http.client.RemoteDisconnected, ConnectionResetError, BrokenPipeError) as e:
                self.close()
                if reused:
                    continue  # the server closed an idle kept-alive connection; ingest is idempotent
                raise CloudError(str(e)) from e
            except (OSError, http.client.HTTPException) as e:
                self.close()
                raise CloudError(str(e) or type(e).__name__) from e
            if resp.will_close:
                self.close()
            if resp.status >= 400:
                raise CloudError(f"HTTP {resp.status}: {payload[:200]!r}")
            try:
                return json.loads(payload)
            except ValueError as e:
                raise CloudError(f"bad response: {payload[:200]!r}") from e
        raise CloudError("connection closed by server")

    def get(self, path, timeout=10.0, retries=3, **params):
        """GETs are idempotent, so a timed-out read is simply retried."""
        for attempt in range(retries):
            try:
                return self.request("GET", path, timeout=timeout, params=params)
            except CloudError:
                if attempt == retries - 1:
                    raise
