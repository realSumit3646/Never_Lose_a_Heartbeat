#!/bin/sh
# Stands in for the board's init: start the mock, then keep the pipeline
# running (restart on exit, like systemd Restart=always).
# The mock keeps its ground truth outside the pipeline's data folder, so a
# full pipeline disk does not stop the mock.
mkdir -p /var/lib/biosignal-mock && cd /var/lib/biosignal-mock
biosignal-mock $MOCK_ARGS --host 127.0.0.1 >> mock.log 2>&1 &

while true; do
    pipeline run
    echo "pipeline exited ($?); restarting in 1 s" >&2
    sleep 1
done
