"""Deterministic sample values, as a pure function of (mode, seed, sensor, index).

Same seed -> same values, so the checker can regenerate the ground truth.

signal mode, modelled on real recordings:
  sensor 1  ECG (lead-II-like P, QRS, T morphology), channel 2; channels 1 and 3
            are channel 2 x 0.6 and x 1.3. Heart rate 60, 90, 72 bpm for two
            minutes each, repeating.
  sensor 2  two-wavelength PPG (like a pulse oximeter's red / infrared light
            levels): large DC levels near 400,000 and 150,000 drifting slowly,
            with a ~0.5 % pulse wave that follows each R peak by ~0.2 s
            (pulse transit time), with a dicrotic notch.
"""
from __future__ import annotations

import math
import random
from functools import lru_cache

RATE = 400
MOD = 8_388_608  # 2**23
CYCLE_S = 360.0
BEATS_PER_CYCLE = 120 + 180 + 144  # 60 bpm x 2 min, 90 bpm x 2 min, 72 bpm x 2 min


def _beats(t: float) -> float:
    """Number of beats since t=0 (heart rate 60, 90, 72 bpm for 2 minutes each)."""
    k, r = divmod(t, CYCLE_S)
    if r < 120:
        b = r
    elif r < 240:
        b = 120 + (r - 120) * 1.5
    else:
        b = 300 + (r - 240) * 1.2
    return k * BEATS_PER_CYCLE + b


def _beat_time(n: int) -> float:
    k, q = divmod(n, BEATS_PER_CYCLE)
    if q < 120:
        r = q
    elif q < 300:
        r = 120 + (q - 120) / 1.5
    else:
        r = 240 + (q - 300) / 1.2
    return k * CYCLE_S + r


def _ecg(dt: float) -> float:
    """One heartbeat, R peak at dt = 0 seconds."""
    g = math.exp
    return (1.0 * g(-(dt / 0.012) ** 2)            # R
            - 0.12 * g(-((dt + 0.03) / 0.010) ** 2)  # Q
            - 0.20 * g(-((dt - 0.03) / 0.012) ** 2)  # S
            + 0.30 * g(-((dt - 0.25) / 0.050) ** 2)  # T
            + 0.12 * g(-((dt + 0.16) / 0.030) ** 2))  # P


@lru_cache(maxsize=256)
def _noise_block(seed: int, sensor: int, block: int):
    rng = random.Random(f"{seed}:{sensor}:{block}")
    return [rng.gauss(0.0, 1.0) for _ in range(RATE * 2)]


def values(mode: str, seed: int, sensor: int, i: int) -> list:
    if mode == "ramp":
        if sensor == 1:
            return [(3 * i + c) % MOD for c in (1, 2, 3)]
        return [(2 * i + c) % MOD for c in (1, 2)]
    t = i / RATE
    noise = _noise_block(seed, sensor, i // RATE)
    k = (i % RATE) * 2
    if sensor == 1:
        n = math.floor(_beats(t))
        v = _ecg(t - _beat_time(n)) + _ecg(t - _beat_time(n + 1))
        ch2 = round(1000 + 6000 * v + 40 * noise[k])
        return [round(ch2 * 0.6), ch2, round(ch2 * 1.3)]
    phase = (seed % 97) / 97 * 2 * math.pi
    n = math.floor(_beats(t - PULSE_DELAY_S)) if t >= PULSE_DELAY_S else -1
    pulse = _ppg(t - PULSE_DELAY_S - _beat_time(n)) if n >= 0 else 0.0
    return [round(400_000 + 20_000 * math.sin(2 * math.pi * t / 90 + phase)
                  - 2_000 * pulse + 200 * noise[k]),
            round(150_000 + 8_000 * math.sin(2 * math.pi * t / 70 + phase + 1)
                  - 800 * pulse + 100 * noise[k + 1])]


PULSE_DELAY_S = 0.2


def _ppg(dt: float) -> float:
    """One PPG pulse (blood volume), dt seconds after it arrives: upstroke, then dicrotic wave.

    More blood absorbs more light, so the sensor's light level dips by this amount.
    """
    if dt < 0:
        return 0.0
    return math.exp(-((dt - 0.12) / 0.07) ** 2) + 0.35 * math.exp(-((dt - 0.38) / 0.09) ** 2)
