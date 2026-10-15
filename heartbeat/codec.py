"""Compact packing of sample values for storage and upload.

A chunk is a run of consecutive samples of one sensor. Its values are stored
as little-endian int32 (signed 24-bit values fit), row after row.
"""
from __future__ import annotations

import base64
import sys
import zlib
from array import array

CHANNELS = {1: 3, 2: 2}
INT24_MIN, INT24_MAX = -(1 << 23), (1 << 23) - 1
MAX_CHUNK = 400  # samples per stored chunk (one mock reply holds at most 1 s = 400)

assert array("i").itemsize == 4


def pack(rows) -> bytes:
    a = array("i", [v for row in rows for v in row])
    if sys.byteorder == "big":
        a.byteswap()
    return a.tobytes()


def unpack(blob: bytes, sensor: int) -> list:
    a = array("i")
    a.frombytes(blob)
    if sys.byteorder == "big":
        a.byteswap()
    n = CHANNELS[sensor]
    return [tuple(a[i:i + n]) for i in range(0, len(a), n)]


def to_wire(blob: bytes) -> str:
    return base64.b64encode(zlib.compress(blob, 6)).decode("ascii")


def from_wire(text: str) -> bytes:
    return zlib.decompress(base64.b64decode(text))


def valid_row(row, sensor: int) -> bool:
    return (isinstance(row, list) and len(row) == CHANNELS[sensor]
            and all(type(v) is int and INT24_MIN <= v <= INT24_MAX for v in row))
