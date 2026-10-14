# syntax=docker/dockerfile:1.7
FROM python:3.12-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    ETLWIFI_DATA_ROOT=/data \
    LOG_FORMAT=json

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install .


# Passive sniffer. Needs the host network namespace and NET_ADMIN/NET_RAW to drive the
# USB adapter in monitor mode, so it runs as root inside the container.
FROM base AS capture
RUN apt-get update \
    && apt-get install -y --no-install-recommends iw iproute2 libpcap0.8 \
    && rm -rf /var/lib/apt/lists/* \
    && pip install ".[capture]"
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD ["etlwifi", "health", "capture", "--max-age", "60"]
CMD ["etlwifi", "capture"]


# Scheduler: pipeline every PIPELINE interval, live snapshot every LIVE interval.
FROM base AS worker
RUN useradd --uid 1000 --create-home --shell /usr/sbin/nologin etlwifi \
    && mkdir -p /data \
    && chown etlwifi:etlwifi /data
USER etlwifi
HEALTHCHECK --interval=60s --timeout=10s --start-period=120s --retries=3 \
    CMD ["etlwifi", "health", "worker", "--max-age", "180"]
CMD ["etlwifi", "worker"]
