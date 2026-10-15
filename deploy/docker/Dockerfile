# ARM64 "Raspberry Pi-class" test device: mock + pipeline in one container.
#
# Build (from the repo root):
#   docker buildx build --platform linux/arm64 -t heartbeat-device -f deploy/docker/Dockerfile .
# Run limited to Pi 4 resources:
#   docker run -d --name hb --platform linux/arm64 --cpus 4 --memory 2g \
#     -e HEARTBEAT_CLOUD_URL=https://YOUR-CLOUD -e HEARTBEAT_API_KEY=... \
#     -v hbdata:/var/lib/heartbeat heartbeat-device
FROM python:3.11-slim-bookworm

WORKDIR /opt/heartbeat
COPY heartbeat ./heartbeat
COPY mock ./mock
COPY tools ./tools
COPY deploy/docker/entrypoint.sh /entrypoint.sh
COPY deploy/make_wrappers.sh /tmp/make_wrappers.sh
RUN sh /tmp/make_wrappers.sh /opt/heartbeat && chmod +x /entrypoint.sh     && apt-get update && apt-get install -y --no-install-recommends procps iproute2     && rm -rf /var/lib/apt/lists/*

ENV HEARTBEAT_DATA_DIR=/var/lib/heartbeat \
    MOCK_ARGS="--mode signal --seed 7 --duration 3600 --port 7100"
VOLUME /var/lib/heartbeat /var/lib/biosignal-mock
ENTRYPOINT ["/entrypoint.sh"]
