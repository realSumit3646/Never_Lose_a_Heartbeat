"""Render a recorded terminal session (tools/record_terminal.py) to an MP4 video.

  python tools/render_cast.py demo_cast.jsonl demo.mp4 [--max-seconds 290]

Lines appear at their recorded times. Waiting in step 7 (and any long silent
gap) is fast-forwarded, with an on-screen label, so the video fits the limit.
Needs: pillow, imageio-ffmpeg.
"""
from __future__ import annotations

import argparse
import json
import subprocess

import imageio_ffmpeg
from PIL import Image, ImageDraw, ImageFont

W, H, FPS = 1280, 720, 4
FONT_SIZE, LINE_H = 15, 19
TOP, LEFT = 12, 14
FOOTER = ("Terminal output of tools/demo.py, real run against the real cloud, real timing  |  "
          "github.com/realSumit3646/Never_Lose_a_Heartbeat")
BG, FG, DIM = (18, 20, 26), (220, 224, 230), (130, 138, 150)
CYAN, GREEN, ORANGE, RED, YELLOW = (100, 210, 230), (110, 220, 130), (250, 170, 70), (240, 90, 90), (240, 220, 110)


def font(bold=False):
    for name in (("consolab.ttf" if bold else "consola.ttf"), "cour.ttf"):
        try:
            return ImageFont.truetype("C:/Windows/Fonts/" + name, FONT_SIZE)
        except OSError:
            continue
    return ImageFont.load_default()


def colour(line, in_box):
    if in_box or line.startswith("====="):
        return CYAN
    if "FAIL" in line:
        return RED
    if "-> OK" in line:
        return GREEN
    if "mode=offline" in line or "mode_offline" in line:
        return ORANGE
    if "mode=online" in line or "mode_online" in line:
        return GREEN
    if "restarting" in line or "session_switch" in line or "service_start" in line:
        return YELLOW
    return FG


def timeline(events, max_seconds):
    """Map recorded times to video times; fast-forward step 7 and long gaps."""
    def build(ff):
        out, prev_t, prev_v, in7, label = [], 0.0, 0.0, False, []
        for e in events:
            dt = e["t"] - prev_t
            if e["line"].strip().startswith("7."):
                in7 = True
            if e["line"].strip().startswith("8."):
                in7 = False
            speed = ff if in7 else 1.0
            if not in7 and dt > 8:  # a long silent wait (e.g. exporting): show 2 s of it
                speed = dt / 2
            v = prev_v + dt / speed
            out.append(v)
            label.append(speed if speed > 1.01 else 0)
            prev_t, prev_v = e["t"], v
        return out, label
    ff = 1.0
    while True:
        vt, label = build(ff)
        if vt[-1] + 12 <= max_seconds or ff > 64:
            return vt, label, ff
        ff *= 1.25


def render(cast_path, out_path, max_seconds):
    events = [json.loads(l) for l in open(cast_path, encoding="utf-8") if l.strip()]
    vt, ff_flags, ff = timeline(events, max_seconds)
    total = vt[-1] + 12  # hold the summary on screen
    fn, fb = font(), font(True)
    rows = (H - TOP - 40) // LINE_H
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    proc = subprocess.Popen([ffmpeg, "-y", "-loglevel", "error", "-f", "rawvideo", "-pix_fmt", "rgb24",
                             "-s", f"{W}x{H}", "-r", str(FPS), "-i", "-", "-c:v", "libx264", "-pix_fmt", "yuv420p",
                             "-crf", "26", "-preset", "medium", "-movflags", "+faststart", out_path],
                            stdin=subprocess.PIPE)
    shown, cache_key, cache = 0, None, None
    n_frames = int(total * FPS) + 1
    for f in range(n_frames):
        now = f / FPS
        while shown < len(events) and vt[shown] <= now:
            shown += 1
        fast = ff_flags[shown] if 0 < shown < len(events) else 0
        key = (shown, fast)
        if key != cache_key:
            img = Image.new("RGB", (W, H), BG)
            d = ImageDraw.Draw(img)
            lines = [e["line"] for e in events[:shown]]
            in_box_flags, in_box = [], False
            for line in lines:  # caption text sits between two ==== lines
                if line.startswith("====="):
                    in_box = not in_box
                    in_box_flags.append(True)
                else:
                    in_box_flags.append(in_box)
            view = list(zip(lines, in_box_flags))[-rows:]
            for i, (line, box) in enumerate(view):
                d.text((LEFT, TOP + i * LINE_H), line[:150], font=fb if box else fn, fill=colour(line, box))
            d.rectangle([0, H - 28, W, H], fill=(30, 34, 44))
            d.text((LEFT, H - 23), FOOTER, font=fn, fill=DIM)
            if fast:
                d.rectangle([W - 290, 8, W - 8, 34], fill=(60, 50, 20))
                d.text((W - 280, 13), f">> fast-forward x{fast:.1f} (waiting)", font=fb, fill=YELLOW)
            cache, cache_key = img.tobytes(), key
        proc.stdin.write(cache)
    proc.stdin.close()
    proc.wait()
    print(f"wrote {out_path}: {total:.0f} s, step 7 fast-forward x{ff:.1f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cast")
    ap.add_argument("out")
    ap.add_argument("--max-seconds", type=float, default=290)
    a = ap.parse_args()
    render(a.cast, a.out, a.max_seconds)


if __name__ == "__main__":
    main()
