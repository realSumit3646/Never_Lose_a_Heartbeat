#!/bin/sh
# Failure tests on the ARM64 device, recording evidence in ./evidence/.
# Run as root on the board while the mock and the pipeline are running:
#   sudo sh deploy/fault_tests.sh <cloud-host> [network-interface]
# The power-cut test cannot be scripted: pull the plug (or hard-reset the VM),
# then run `sh deploy/fault_tests.sh <cloud-host> <iface> after-power-cut`.
set -u
CLOUD_HOST=${1:?cloud host name, e.g. myapp.example.com}
IFACE=${2:-eth0}
STEP=${3:-all}
mkdir -p evidence
LOG=evidence/fault_tests_$(date +%Y%m%d_%H%M%S).log

say() { echo "$(date -u +%H:%M:%S) $*" | tee -a "$LOG"; }
status() { pipeline status | tee -a "$LOG"; }
watch_mode() {  # print status every second for $1 seconds
    i=0; while [ $i -lt "$1" ]; do status; sleep 1; i=$((i + 1)); done
}

if [ "$STEP" = "after-power-cut" ]; then
    say "after power cut: service state and last events"
    systemctl is-active heartbeat-pipeline | tee -a "$LOG"
    pipeline events --last 30 | tee -a "$LOG"
    watch_mode 15
    exit 0
fi

say "baseline"; watch_mode 5

say "TEST 1: network cut (drop traffic to $CLOUD_HOST for 60 s)"
IPS=$(getent ahostsv4 "$CLOUD_HOST" | awk '{print $1}' | sort -u)
for ip in $IPS; do iptables -I OUTPUT -d "$ip" -j DROP; done
watch_mode 60
say "network restored"
for ip in $IPS; do iptables -D OUTPUT -d "$ip" -j DROP; done
watch_mode 30

say "TEST 2: kill -9 the pipeline"
pkill -9 -f "heartbeat run"
watch_mode 15

say "TEST 3: system clock set 3 years ahead for 60 s"
NOW=$(date -u +%s)
date -u -s "@$((NOW + 94608000))" >/dev/null
watch_mode 60
date -u -s "@$((NOW + 60))" >/dev/null
say "clock restored (NTP will finish correcting it)"

say "TEST 4: disk full for 30 s"
DATA_DIR=/var/lib/heartbeat
fallocate -l "$(df --output=avail -B1 "$DATA_DIR" | tail -1)" "$DATA_DIR/../fill.bin" 2>/dev/null \
    || dd if=/dev/zero of="$DATA_DIR/../fill.bin" bs=1M 2>/dev/null
watch_mode 30
rm -f "$DATA_DIR/../fill.bin"
say "disk freed"; watch_mode 15

say "events recorded by the pipeline:"
pipeline events --last 60 | tee -a "$LOG"
say "done; evidence in $LOG. Next: pull the power, then run with 'after-power-cut'."
