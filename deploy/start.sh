#!/bin/bash
# FIU Minute Bar Generator — 生产环境启动脚本
set -euo pipefail

APP_DIR="/home/rpeng/fiu_minute_bar"
CONFIG="${APP_DIR}/config/production.ini"
PID_FILE="${APP_DIR}/fiu_minute_bar.pid"
LOG_DIR="${APP_DIR}/logs"
PYTHON="/home/prod/anaconda3/bin/python"

mkdir -p "${LOG_DIR}"

if [ -f "${PID_FILE}" ]; then
    OLD_PID=$(cat "${PID_FILE}")
    if kill -0 "${OLD_PID}" 2>/dev/null; then
        echo "Already running (PID=${OLD_PID})"
        exit 1
    fi
    rm -f "${PID_FILE}"
fi

echo "Starting FIU Minute Bar Generator..."
cd "${APP_DIR}"
# Pre-flight: ensure the Rust extension is importable (build via setup.sh if missing)
PYTHONPATH=src ${PYTHON} -c "from minute_bar import _order_accel" || { echo "ERROR: Rust extension _order_accel missing — run setup.sh first"; exit 1; }
PYTHONPATH=src nohup ${PYTHON} main.py --config "${CONFIG}" \
    >> "${LOG_DIR}/startup.log" 2>&1 &
echo $! > "${PID_FILE}"
echo "Started (PID=$(cat ${PID_FILE}))"
