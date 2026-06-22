#!/bin/bash
# FIU Minute Bar Generator — 生产环境首次部署脚本
set -euo pipefail

APP_DIR="/home/rpeng/fiu_minute_bar"
DATA_DIR="/home/rpeng/fiu_minute_bar"
PYTHON="/home/prod/anaconda3/bin/python"

echo "=== FIU Minute Bar Generator — Production Setup ==="

# 1. Create directories
echo "Creating directories..."
mkdir -p "${DATA_DIR}/output"
mkdir -p "${DATA_DIR}/checkpoint"
mkdir -p "${DATA_DIR}/logs"

# 2. Copy project files
echo "Copying project to ${APP_DIR}..."
mkdir -p "${APP_DIR}"
rsync -av --exclude='test/' \
    --exclude='input/' \
    --exclude='.pytest_cache/' \
    --exclude='__pycache__/' \
    --exclude='.hypothesis/' \
    --exclude='docs/' \
    ./ "${APP_DIR}/"

# 3. Set permissions
chmod +x "${APP_DIR}/deploy/start.sh"
chmod +x "${APP_DIR}/deploy/stop.sh"
chmod +x "${APP_DIR}/deploy/restart.sh"

# 4. Verify Python
echo "Verifying Python..."
${PYTHON} -c "import sys; print(f'Python {sys.version}')"

# 5. Build Rust extension (_order_accel) — engine imports minute_bar._order_accel
echo "Building Rust extension (_order_accel)..."
pip install setuptools-rust || { echo "ERROR: setuptools-rust install failed"; exit 1; }
pip install . || { echo "ERROR: Rust extension build failed (pip install .)"; exit 1; }
PYTHONPATH=src ${PYTHON} -c "from minute_bar import _order_accel; print('Rust ext OK')" || { echo "ERROR: _order_accel not importable after build"; exit 1; }

# 6. Verify config
echo "Verifying config..."
cd "${APP_DIR}"
PYTHONPATH=src ${PYTHON} -c "from minute_bar.config import load_config; load_config('config/production.ini'); print('Config OK')"

# 7. Optional: install as systemd service
read -p "Install as systemd service? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    sudo cp "${APP_DIR}/deploy/fiu-minute-bar.service" /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable fiu-minute-bar
    echo "Service installed. Start with: sudo systemctl start fiu-minute-bar"
fi

echo ""
echo "=== Setup Complete ==="
echo "Start manually:  ${APP_DIR}/deploy/start.sh"
echo "Stop manually:   ${APP_DIR}/deploy/stop.sh"
echo "Restart:         ${APP_DIR}/deploy/restart.sh"
echo "View logs:       tail -f ${DATA_DIR}/logs/$(date +%Y%m%d)_errors.log"
echo "Input data:      /home/rpeng/FIU/log (FIU 接收服务写入)"
echo "Output data:     ${DATA_DIR}/output/"
