#!/bin/bash
# FIU Minute Bar Generator — 生产环境停止脚本
set -euo pipefail

APP_DIR="/home/rpeng/fiu_minute_bar"
PID_FILE="${APP_DIR}/fiu_minute_bar.pid"

if [ ! -f "${PID_FILE}" ]; then
    echo "Not running (no PID file)"
    exit 0
fi

PID=$(cat "${PID_FILE}")
if ! kill -0 "${PID}" 2>/dev/null; then
    echo "Process ${PID} not found, cleaning PID file"
    rm -f "${PID_FILE}"
    exit 0
fi

echo "Stopping FIU Minute Bar Generator (PID=${PID})..."
kill -TERM "${PID}"

# Wait up to 30s for graceful shutdown
for i in $(seq 1 30); do
    if ! kill -0 "${PID}" 2>/dev/null; then
        echo "Stopped gracefully"
        rm -f "${PID_FILE}"
        exit 0
    fi
    sleep 1
done

echo "Graceful shutdown timeout, force killing..."
kill -KILL "${PID}" 2>/dev/null || true
rm -f "${PID_FILE}"
echo "Force stopped"
