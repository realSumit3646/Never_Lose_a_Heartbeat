# Never Lose a Heartbeat

A device-to-cloud pipeline for a two-sensor bedside device. It saves every
sample on the device and survives crashes, reboots and power cuts. It streams
to a free cloud service and processes the data there when the network is up,
and processes on the device when it is down. After an outage it catches up so
the cloud holds every sample exactly once.

Only Python 3.9+ and its standard library are needed, on both the device and
the cloud. Nothing is installed with pip.

```
heartbeat/processing.py   section 3 logic, shared by device (offline) and cloud (online)
heartbeat/sqlsource.py    reads stored samples; "final" = stored, lost, or past the end
heartbeat/codec.py        int32 packing of samples
heartbeat/device/         collector, store (SQLite), uploader, processor, service, CLI
heartbeat/cloud/app.py    WSGI cloud service (ingest, online processing, export)
mock/                     biosignal-mock (sensor simulator) + check_ground_truth
tools/                    reference implementation, end-to-end test, resource monitor
deploy/                   systemd units, install script, ARM64 Docker image, fault tests
docs/design_note.md       architecture, guarantees, power-cut bound
```

## Install on a fresh board (Raspberry Pi OS / Debian, ARM64)

```sh
git clone <this repo> && cd heartbeat-pipeline
sudo sh deploy/install.sh                     # commands + systemd units (mock and pipeline)
sudo nano /etc/heartbeat/pipeline.ini         # set cloud_url and api_key
sudo systemctl restart heartbeat-pipeline
pipeline status
```

Both services start on boot and restart within 1 s if they die
(`Restart=always`). The mock runs as
`biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100`; edit
`/etc/systemd/system/biosignal-mock.service` to change its options or add faults.

## Commands

```sh
pipeline status                                  # one JSON line: mode, saved, in cloud, backlog
pipeline export --session ID --from device       # samples, then losses (JSON lines)
pipeline export --session ID --from cloud
pipeline results --session ID                    # one JSON line per window and mode
pipeline sessions                                # sessions seen by the device
pipeline events --last 50                        # mode switches, restarts, faults (evidence)
```

## Cloud deployment (free)

The cloud side is a single WSGI app with SQLite. Any free host that runs
Python with a persistent disk works, such as an always-free VM (Oracle Cloud,
Google Cloud e2-micro) or PythonAnywhere (WSGI file:
`from heartbeat.cloud.app import application`).

```sh
export HEARTBEAT_API_KEY=<long random string>   # same value as api_key on the device
export HEARTBEAT_CLOUD_DB=/var/lib/heartbeat-cloud/cloud.db
python3 -m heartbeat.cloud.app --port 8080      # put HTTPS (e.g. Caddy) in front
```

## Testing

```sh
python -m unittest discover -s tests            # section 3 vs reference, reply handling, mock protocol
python tools/e2e_local.py --short               # full failure test on one machine (about 3 min)
python tools/e2e_local.py --short --slow 20000  # the same over a 20 KB/s link
python tools/docker_fault_test.py --env-file hb.env   # ARM64 container + real cloud (network cut,
                                                 # kill -9, power cut, disk full)
python tools/demo.py --env-file hb.env            # narrated 4-minute demo (video)
```

`tools/e2e_local.py` runs the cloud, a network-cut proxy, the mock (with
corrupt and duplicate replies, pauses and sensor dropouts) and the pipeline.
It then cuts the network, kills the pipeline and restarts the mock. Finally it
checks the device and cloud exports against the ground truth, and the results
against the independent batch implementation in `tools/reference_results.py`.

On the ARM64 device, `deploy/fault_tests.sh` runs these tests: network cut,
`kill -9`, clock change and full disk. It records evidence. Pull the power by
hand.

### ARM64 environment without a Pi

```sh
docker buildx build --platform linux/arm64 -t heartbeat-device -f deploy/docker/Dockerfile .
docker run -d --name hb --platform linux/arm64 --cpus 4 --memory 2g \
  -e HEARTBEAT_CLOUD_URL=https://<cloud> -e HEARTBEAT_API_KEY=<key> \
  -v hbdata:/var/lib/heartbeat heartbeat-device
docker exec hb pipeline status
```

`docker kill` does not emulate a power cut, because the host kernel still
flushes cached writes. Use a VM hard reset or a real Pi for that test.

## The mock service

MayaCardio confirmed that the mock is not provided, so `mock/` implements
section 6 of the brief.

```sh
biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100
biosignal-mock --mode ramp --duration 600 --fault-rate 0.02     --pause-every 60 --pause-for 4 --dropout-every 90 --dropout-for 3 --restart-every 300
check_ground_truth --truth mock_truth.jsonl --export export.jsonl --session ID
```

* **Protocol:** newline-delimited JSON over TCP, one client at a time,
  `read` and re-read with `from_index`. At most 1 s per reply; only the last
  10 s are kept, and older unread data is reported in `lost`.
  `session_end` is set in the last reply. `crc32` is the zlib CRC-32 of the
  `data` text exactly as sent; this matches the example in the brief.
* **Signal mode:** sensor 1 is an ECG with P, QRS and T waves. Channel 2
  peaks once per beat at 60 / 90 / 72 bpm for 2 minutes each; channels 1 and
  3 are channel 2 x 0.6 and x 1.3. Sensor 2 is a two-wavelength PPG, like a
  pulse oximeter's red and infrared light levels: around 400,000 and 150,000,
  drifting slowly, with a small pulse wave about 0.2 s after each heartbeat.
  The same seed always gives the same values.
* **Ramp mode:** sensor 1 channel c = (3i + c) mod 8,388,608 and sensor 2
  channel c = (2i + c) mod 8,388,608, with channels numbered from 1.
* **Faults:** stop answering (`--pause-every/--pause-for`), corrupt replies
  (wrong crc32) and duplicated replies (`--fault-rate`), one sensor stopping
  (`--dropout-every/--dropout-for`, reported in `lost`), and restart with a new
  session (`--restart-every`, or kill and start the process).
* **Ground truth:** every session, every second's generated count and every
  dropout go to `mock_truth.jsonl`, fsync'd. `check_ground_truth` regenerates
  the values from the seed. It then reports silent loss, duplicates, wrong
  values, invented samples, and samples both exported and listed as lost.

## AI disclosure

This code was written with an AI assistant (Claude), under my direction and
review. I verified it by running the unit tests and the end-to-end failure
test, and by comparing the section 3 results against an independent batch
implementation.
