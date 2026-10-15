"""Section 3 processing logic, shared by the device (offline) and the cloud (online).

Both sides run exactly this code over the same stored data, so a window
gives the same numbers wherever it is processed.

Processing is incremental: `step` turns one window into a result once all the
data it depends on is final (stored, or known to be lost). The only state
carried between windows is the peak detector's (`State`), which is small and
serialisable so the cloud can resume from the device's state after an outage.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional

WINDOW = 400        # samples per one-second window (400 Hz)
RATE = 400          # samples per second
LOOKBACK = 800      # samples before a candidate peak that must be available
REFRACTORY = 100    # minimum samples between consecutive peaks
HR_WINDOWS = 5      # heart rate uses the last 5 seconds
CHANNELS = {1: 3, 2: 2}
PEAK_CHANNEL = 1    # sensor 1, channel 2 (0-based column 1)


def round1(x):
    """Rounding used for every decimal output (Python's round-half-even)."""
    return None if x is None else round(x, 1)


@dataclass
class State:
    next_window: int = 0               # next window to produce
    scan_next: int = 0                 # next sample index to test as a peak
    last_peak: Optional[int] = None    # most recent peak (for the 100-sample rule)
    peaks: List[int] = field(default_factory=list)  # peaks still inside a 5 s span

    def to_dict(self) -> dict:
        return {"next_window": self.next_window, "scan_next": self.scan_next,
                "last_peak": self.last_peak, "peaks": list(self.peaks)}

    @classmethod
    def from_dict(cls, d) -> "State":
        if not d:
            return cls()
        return cls(d["next_window"], d["scan_next"], d["last_peak"], list(d["peaks"]))


def sensor1_range(state: State):
    """Sensor-1 index range [start, stop) that window `state.next_window` depends on."""
    w = state.next_window
    start = max(0, min(state.scan_next - LOOKBACK, WINDOW * (w - (HR_WINDOWS - 1))))
    return start, WINDOW * (w + 1) + 1  # +1: the sample after a peak must be known


def step(state: State, source, session: str):
    """Process window `state.next_window`.

    `source` provides is_final(sensor, start, stop) and read(sensor, start, stop);
    read returns one tuple per index, or None where the sample is not available.
    Returns (result, new_state), or None if the data is not final yet.
    """
    w = state.next_window
    lo, hi = WINDOW * w, WINDOW * (w + 1)
    s1_start, s1_stop = sensor1_range(state)
    if not (source.is_final(1, s1_start, s1_stop) and source.is_final(2, lo, hi)):
        return None
    s1 = source.read(1, s1_start, s1_stop)
    s2 = source.read(2, lo, hi)
    off = s1_start

    x = [None if r is None else r[PEAK_CHANNEL] for r in s1]
    missing_prefix = [0]
    for v in x:
        missing_prefix.append(missing_prefix[-1] + (v is None))

    def missing_in(a, b):  # missing sensor-1 samples in [a, b)
        return missing_prefix[b - off] - missing_prefix[a - off]

    # Peaks: sample i is a peak when the 800 samples before it and the sample
    # after it are available, it is larger than both neighbours, larger than
    # half the maximum of the 800 samples before it, and at least 100 samples
    # after the previous peak.
    last_peak = state.last_peak
    peaks = [p for p in state.peaks if p >= WINDOW * (w - (HR_WINDOWS - 1))]
    for i in range(max(state.scan_next, LOOKBACK), hi):
        xi = x[i - off]
        if xi is None:
            continue
        before, after = x[i - 1 - off], x[i + 1 - off]
        if before is None or after is None or not (xi > before and xi > after):
            continue
        if missing_in(i - LOOKBACK, i):
            continue
        if not 2 * xi > max(x[i - LOOKBACK - off:i - off]):  # exact "> max / 2"
            continue
        if last_peak is not None and i - last_peak < REFRACTORY:
            continue
        peaks.append(i)
        last_peak = i

    win1 = [r for r in s1[lo - off:hi - off] if r is not None]
    win2 = [r for r in s2 if r is not None]
    res = {"session": session, "window": w,
           "missing_1": WINDOW - len(win1), "missing_2": WINDOW - len(win2)}
    res["complete"] = res["missing_1"] == 0 and res["missing_2"] == 0
    for s, rows in ((1, win1), (2, win2)):
        if rows:
            cols = list(zip(*rows))
            res[f"mean_{s}"] = [round1(sum(c) / len(c)) for c in cols]
            res[f"min_{s}"] = [min(c) for c in cols]
            res[f"max_{s}"] = [max(c) for c in cols]
        else:
            res[f"mean_{s}"] = res[f"min_{s}"] = res[f"max_{s}"] = [None] * CHANNELS[s]
    res["magnitude"] = (round1(sum(math.sqrt(a * a + b * b) for a, b in win2) / len(win2))
                        if win2 else None)

    heart_rate = None
    if w >= HR_WINDOWS - 1:
        a = WINDOW * (w - (HR_WINDOWS - 1))
        if missing_in(a, hi) == 0:
            ps = [p for p in peaks if a <= p < hi]
            if len(ps) >= 2:
                gaps = [(q - p) / RATE for p, q in zip(ps, ps[1:])]
                heart_rate = round1(60 / (sum(gaps) / len(gaps)))
    res["heart_rate"] = heart_rate

    keep_from = WINDOW * (w + 1 - (HR_WINDOWS - 1))
    return res, State(w + 1, hi, last_peak, [p for p in peaks if p >= keep_from])


def advance(source, session: str, state: State, last_window=None, max_windows=200):
    """Process as many consecutive windows as the data allows.

    Returns ([(result, state_after), ...], final_state).
    """
    out = []
    while len(out) < max_windows and (last_window is None or state.next_window <= last_window):
        r = step(state, source, session)
        if r is None:
            break
        out.append(r)
        state = r[1]
    return out, state


def last_window_for(max_index_exclusive: int) -> int:
    """Last window of a closed session whose known indices end before the given index."""
    return (max_index_exclusive - 1) // WINDOW if max_index_exclusive > 0 else -1
