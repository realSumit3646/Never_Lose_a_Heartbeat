"""Section 3 logic: the incremental processor must match the batch reference."""
import json
import random
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))

from heartbeat.processing import State, advance, step  # noqa: E402
import reference_results  # noqa: E402


class MemorySource:
    """Samples in dicts; everything below `final_upto[sensor]` is final."""

    def __init__(self, s1, s2, final_upto):
        self.data = {1: s1, 2: s2}
        self.final_upto = final_upto

    def is_final(self, sensor, start, stop):
        return stop <= self.final_upto[sensor]

    def read(self, sensor, start, stop):
        return [self.data[sensor].get(i) for i in range(start, stop)]


def synthetic(n, period, seed, gaps=(), spikes=0.01):
    """ECG-like spikes every `period` samples (with jitter) plus noise, minus gaps."""
    rng = random.Random(seed)
    s1, s2 = {}, {}
    next_beat = rng.randint(50, period)
    for i in range(n):
        if i == next_beat:
            next_beat += period + rng.randint(-15, 15)
        d = min(abs(i - (next_beat - period)), abs(i - next_beat))
        v = 1000 + int(6000 * max(0.0, 1 - d / 6)) + rng.randint(-40, 40)
        if rng.random() < spikes:
            v += rng.randint(2000, 9000)  # occasional spikes test the 100-sample rule
        s1[i] = (round(v * 0.6), v, round(v * 1.3))
        s2[i] = (400000 + rng.randint(-500, 500), 150000 + rng.randint(-300, 300))
    for sensor, a, b in gaps:
        for i in range(a, b + 1):
            (s1 if sensor == 1 else s2).pop(i, None)
    return s1, s2


def strip(r):
    return {k: v for k, v in r.items() if k not in ("session", "mode")}


class ProcessingMatchesReference(unittest.TestCase):
    def check(self, s1, s2, n):
        last = (n - 1) // 400
        ref = reference_results.compute(s1, s2, last)
        # Feed data in irregular increments and resume from serialised state,
        # as the device and cloud do.
        state, got, upto = State(), [], 0
        rng = random.Random(1)
        while state.next_window <= last:
            upto = min(n + 10**6, upto + rng.randint(1, 900))
            src = MemorySource(s1, s2, {1: upto if upto < n else 10**9, 2: upto if upto < n else 10**9})
            out, state = advance(src, "t", state, last_window=last)
            got += [strip(r) for r, _ in out]
            state = State.from_dict(json.loads(json.dumps(state.to_dict())))
        self.assertEqual(len(got), len(ref))
        for g, r in zip(got, ref):
            self.assertEqual(g, r, f"window {r['window']}")
        return got

    def test_clean_60_bpm(self):
        n = 400 * 30
        s1, s2 = synthetic(n, 400, seed=1, spikes=0)
        got = self.check(s1, s2, n)
        rates = [g["heart_rate"] for g in got if g["heart_rate"] is not None]
        self.assertTrue(rates and all(50 < r < 70 for r in rates), rates[:10])

    def test_gaps_and_faster_rate(self):
        n = 400 * 40
        gaps = [(1, 4800, 5199), (2, 7000, 7100), (1, 12000, 12000), (2, 14000, 15999)]
        s1, s2 = synthetic(n, 267, seed=2, gaps=gaps)
        got = self.check(s1, s2, n)
        self.assertFalse(got[12]["complete"])
        self.assertEqual(got[12]["missing_1"], 400)
        self.assertIsNone(got[12]["mean_1"][0])
        self.assertIsNone(got[14]["heart_rate"])  # a missing sample in the last 5 s

    def test_random_noise(self):
        n = 400 * 15
        rng = random.Random(3)
        s1 = {i: (rng.randint(-9, 9), rng.randint(-1000, 1000), 0) for i in range(n)}
        s2 = {i: (rng.randint(-5, 5), rng.randint(-5, 5)) for i in range(n) if i % 97}
        self.check(s1, s2, n)

    def test_not_ready_until_final(self):
        s1, s2 = synthetic(2000, 400, seed=4)
        src = MemorySource(s1, s2, {1: 400, 2: 400})
        self.assertIsNone(step(State(), src, "t"))  # needs sensor 1 sample 400
        src.final_upto[1] = 401
        self.assertIsNotNone(step(State(), src, "t"))

    def test_first_windows_have_no_rate(self):
        s1, s2 = synthetic(400 * 6, 300, seed=5)
        got = self.check(s1, s2, 400 * 6)
        self.assertTrue(all(g["heart_rate"] is None for g in got[:4]))


if __name__ == "__main__":
    unittest.main()
