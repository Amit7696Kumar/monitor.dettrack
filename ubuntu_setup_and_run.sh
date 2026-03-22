#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
RUNTIME_DIR="$PROJECT_DIR/.runtime"

cd "$PROJECT_DIR"

echo "Meter OCR Ubuntu setup + run"
echo "Project: $PROJECT_DIR"

if ! command -v sudo >/dev/null 2>&1; then
  echo "This script expects sudo to be available on Ubuntu."
  exit 1
fi

if ! command -v apt-get >/dev/null 2>&1; then
  echo "This script is intended for Ubuntu/Debian systems with apt-get."
  exit 1
fi

echo
echo "[1/5] Installing Ubuntu packages"
sudo apt-get update
sudo apt-get install -y \
  python3 \
  python3-venv \
  python3-pip \
  tesseract-ocr \
  libgl1 \
  libglib2.0-0 \
  libsm6 \
  libxext6 \
  libxrender1 \
  libgomp1 \
  curl

echo
echo "[2/5] Creating virtual environment"
if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

python -m pip install --upgrade pip setuptools wheel

echo
echo "[3/5] Installing Python dependencies"
pip install --no-cache-dir -r server/requirements.txt
pip install --no-cache-dir \
  scipy==1.11.4 \
  scikit-image==0.21.0 \
  torch==2.3.1 \
  torchvision==0.18.1 \
  torchaudio==2.3.1 \
  pillow

echo
echo "[4/5] Applying Ubuntu compatibility fix for pytesseract path"
TESSERACT_BIN="$(command -v tesseract)"
if [ -z "$TESSERACT_BIN" ]; then
  echo "tesseract was not found after installation."
  exit 1
fi

sudo mkdir -p /opt/homebrew/bin
sudo ln -sf "$TESSERACT_BIN" /opt/homebrew/bin/tesseract

mkdir -p "$RUNTIME_DIR/ultralytics" "$RUNTIME_DIR/matplotlib"

export OCR_BACKEND="${OCR_BACKEND:-gcv_then_tesseract}"
export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-8000}"
export UVICORN_RELOAD="${UVICORN_RELOAD:-0}"
export RUN_STARTUP_HEALTHCHECKS="${RUN_STARTUP_HEALTHCHECKS:-0}"
export WATCHFILES_FORCE_POLLING="${WATCHFILES_FORCE_POLLING:-1}"
export YOLO_CONFIG_DIR="${YOLO_CONFIG_DIR:-$RUNTIME_DIR/ultralytics}"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$RUNTIME_DIR/matplotlib}"

if [ -f "$PROJECT_DIR/.env" ]; then
  echo
  echo "Loading .env"
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_DIR/.env"
  set +a
fi

echo
echo "[5/5] Starting server"
echo "URL: http://localhost:$PORT"
exec uvicorn server.app:app --host "$HOST" --port "$PORT"
