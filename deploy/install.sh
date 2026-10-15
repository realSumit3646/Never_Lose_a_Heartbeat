#!/bin/sh
# Install the mock service and the pipeline on a fresh Raspberry Pi OS / Debian ARM64 board.
# Needs only python3 (3.9+), which Raspberry Pi OS ships with. Run from the repo root:
#   sudo sh deploy/install.sh
set -eu

PREFIX=/opt/heartbeat
mkdir -p "$PREFIX" /etc/heartbeat /var/lib/heartbeat /var/lib/biosignal-mock
rm -rf "$PREFIX/heartbeat" "$PREFIX/mock" "$PREFIX/tools"
cp -r heartbeat mock tools "$PREFIX/"

sh deploy/make_wrappers.sh "$PREFIX"

if [ ! -f /etc/heartbeat/pipeline.ini ]; then
    cp deploy/pipeline.ini.example /etc/heartbeat/pipeline.ini
    echo "Edit /etc/heartbeat/pipeline.ini (cloud_url, api_key), then:"
    echo "  sudo systemctl restart heartbeat-pipeline"
fi

cp deploy/biosignal-mock.service deploy/heartbeat-pipeline.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now biosignal-mock heartbeat-pipeline
echo "Installed. Check with: pipeline status"
