#!/usr/bin/env bash
# One-time host preparation for the capture + worker containers. Safe to re-run.
#   sudo ./deploy/host-setup.sh [interface] [data-dir] [owner]
set -euo pipefail

INTERFACE="${1:-wlx001cf09e26ce}"
DATA_DIR="${2:-/mnt/TOPHDD/etlwifi}"
OWNER="${3:-${SUDO_USER:-luwe}}"

if [[ $EUID -ne 0 ]]; then
    echo "run with sudo" >&2
    exit 1
fi

if ! ip link show "$INTERFACE" >/dev/null 2>&1; then
    echo "interface $INTERFACE not found; plug in the adapter or pass its name" >&2
    exit 1
fi

# NetworkManager must leave the monitor-mode adapter alone, or it flips it back to managed.
NM_CONF=/etc/NetworkManager/conf.d/99-etlwifi-unmanaged.conf
if command -v nmcli >/dev/null 2>&1; then
    cat >"$NM_CONF" <<EOF
[keyfile]
unmanaged-devices=interface-name:${INTERFACE}
EOF
    systemctl reload NetworkManager
    echo "NetworkManager: $INTERFACE unmanaged ($NM_CONF)"
fi

install -d -o "$OWNER" -g "$OWNER" -m 0755 "$DATA_DIR"
for sub in landing landing/capture landing/telemetry bronze silver gold reference serving state; do
    install -d -o "$OWNER" -g "$OWNER" -m 0755 "$DATA_DIR/$sub"
done
echo "data directory ready: $DATA_DIR (owner $OWNER)"

echo "next: cp .env.example .env, fill in secrets, then: docker compose up -d --build worker"
