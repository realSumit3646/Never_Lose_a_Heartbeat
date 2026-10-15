# Design note: Never Lose a Heartbeat

## Architecture

```
 biosignal-mock ──TCP──► collector ──► SQLite on the device (WAL, synchronous=FULL)
                         (crc32,       │  chunks · losses · cursors · results · events
                          dedupe,      │
                          gaps)        ├──► processor (section 3, always running; its
                                       │    results become output only when offline)
                                       └──► uploader ──HTTPS──► cloud (WSGI + SQLite)
                                            (mode switch,        stores once, processes
                                             catch-up)           online, serves exports
```

There is one process on the device (`pipeline run`, a systemd unit with
`Restart=always`) running three threads, each with its own database connection.

* **Collector.** Polls both sensors every 0.2 s. A reply is used only if the
  CRC-32 of its raw `data` text matches; otherwise the data is re-read with
  `from_index`. Data at indexes already saved (duplicate replies, re-reads) is
  trimmed. A skipped range is recorded as lost only when the mock reports it
  lost, or when two re-reads did not bring it back. A new `session_id` closes
  the old session and lists its unread data as lost, with `to_index: null`.
* **Store.** For each read cycle (both sensors' replies), the new samples,
  any loss records and the per-sensor cursors are written in one transaction. Invariant: every index
  below the cursor is either stored or listed as lost. Chunks never overlap.
* **Uploader.** Sends everything the cloud has not confirmed. It sends first
  the chunks the cloud needs to keep processing, then the oldest backlog.
  Chunks are marked as uploaded only after the cloud commits them. The cloud
  keys chunks by (session, sensor, first index), so a re-sent chunk is
  ignored. The result is exactly-once delivery on top of at-least-once
  sending.
* **Processing.** One module, used by both the device and the cloud. A window
  is processed only when every sample it depends on is *final* (stored, lost,
  or past the end). The heart-rate rule needs the sample after a peak, so
  window *w* waits for sensor-1 sample 400(*w*+1). The only state carried from
  one window to the next is the peak detector's (next index, last peak,
  recent peaks). After an outage the device sends this state, so the cloud
  resumes live processing right away instead of waiting for the backlog.

## Modes and switching

| Event | Detection | Bound |
|---|---|---|
| Network lost | an upload gets no answer within 4.5 s | ≤ ~5 s (measured 4.1 s on ARM64) |
| Network back | probe every 1.5 s succeeds (one kept-alive HTTPS connection) | ≤ ~5 s (measured 0.8–3.5 s) |

While offline, every window the cloud has not processed gets
`mode: "offline"` on the device. Those results are uploaded later. The cloud
skips those windows and keeps processing live data. A window can appear in
both modes. For example, an upload sent just before a cut may reach the cloud
only after the network returns, after the device has already processed that
window offline. `pipeline results` prints one line per window and mode, and
each line carries `processed_at`.

## What is guaranteed

* A sample on disk stays on disk, unchanged and exactly once, through crashes,
  `kill -9`, reboots and power cuts. This holds because transactions are
  fsync'd at commit and recovery comes from the WAL.
* No silent loss. Every index the device does not hold is covered by a loss
  record with a reason: `buffer_overflow`/dropout (reported by the mock), mock
  restart (end unknown), disk full, or a gap that re-reads did not fill.
  Values are never invented.
* The cloud copy converges to the device copy exactly once. A conflicting
  re-send is logged in the cloud's `conflicts` table.
* Results are deterministic and identical on device and cloud, because both
  run the same code over the same data. They were checked against an
  independent batch implementation (`tools/reference_results.py`).

## What is not guaranteed

* Data the mock no longer holds cannot be recovered. This covers its 10 s
  buffer while the pipeline is down for longer than that, and a mock restart.
  Such data is listed as lost.
* If the disk is full, samples are dropped and listed as lost. A 16 MB reserve
  file is released so that the loss records can still be written.
* The 5 s online deadline assumes enough uplink bandwidth. During catch-up,
  live data is sent before the backlog.
* Rounding follows Python's `round` (half-to-even). This is an assumption
  until the reference implementation is confirmed.

## Maximum data loss on a power cut

**At most about 0.5 s per sensor.** A reply is durable once its commit
returns. What can be lost is:

1. samples that reached the device but were not committed yet: one poll
   cycle, 0.2 s, plus the fsync time;
2. samples the mock had generated but not yet sent. The mock dies in the same
   power cut, and its 10 s buffer goes with it: up to one poll cycle, 0.2 s.

After a reboot the restarted mock begins a new session. The old session's
unread tail is listed as lost (`to_index: null`), because its exact end is
unknown. This relies on the storage honouring fsync. Cheap SD cards can lie
about this; an SD card with power-loss protection, or an SSD, is recommended.

## Other faults

* **Clock changes:** timeouts use the monotonic clock, and data is keyed by
  sample index, never by time. TLS certificate *dates* are not checked
  (chain and hostname still are), so a wrong clock does not cut the device
  off from the cloud.
* **Mock silent:** requests time out after 2 s. The device reconnects after
  20 s of silence.
* **Disk full:** the failed transaction is rolled back. A 16 MB reserve file is
  released, and the dropped range is recorded as `disk full`. Tested by filling
  the data disk for 35 s: no crash, no silent loss.
* **Slow or poor network:** catch-up batch size adapts to the measured upload
  speed, about 1.5 s per request (32 KB to 1 MB of samples). A slow link
  therefore does not look like an outage. The whole failure test also passed
  over a 20 KB/s link.

## Evidence and resources

The mock service (`mock/`) was built as part of this work, following section 6:
MayaCardio confirmed that it is not provided. Test reports are in `evidence/`:
`tools/e2e_local.py` (laptop) and `tools/docker_fault_test.py` (ARM64 container
and real cloud). They cover network cut, `kill -9`, power cut, mock restart and
disk full. In all of them the device and cloud exports passed
`check_ground_truth` with no silent loss, and results matched the reference.

**Resources, 1 hour online.** Environment: Docker `linux/arm64` image
(Python 3.11, Debian bookworm) limited with `--cpus 4 --memory 2g`. It ran
under QEMU emulation on an x86-64 laptop and uploaded to PythonAnywhere
(free tier).

| Average CPU | Peak memory | Disk written | Network sent |
|---|---|---|---|
| 9.2 % of one core | 92 MB | 837 MB | 30.5 MB |

All 2,880,000 samples were in the cloud at the end, with no loss records.
Online results took 0.9 s median and 2.9 s p95 from data generation, with
1.3 % over 5 s. The cloud did not answer within 4.5 s 64 times in the hour,
so about a third of windows were processed offline. Server-side timing shows
the cloud spends under 0.2 s per upload. The delays came from the test
network, a mobile connection on a train, where uploads to a neutral server
(httpbin.org) were equally slow. This is the situation the offline mode is
for: no data was lost.
The CPU figure includes emulation overhead, so a native Pi 4 should use less.
The disk figure is dominated by one fsync'd transaction per 0.2 s read cycle
(SQLite WAL frames and checkpoints). Setting `poll_interval = 0.5` roughly
halves it, at the cost of a ~1 s power-cut bound.
