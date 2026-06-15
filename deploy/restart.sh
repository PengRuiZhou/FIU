#!/bin/bash
# FIU Minute Bar Generator — 生产环境重启脚本
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
"${SCRIPT_DIR}/stop.sh"
sleep 2
"${SCRIPT_DIR}/start.sh"
