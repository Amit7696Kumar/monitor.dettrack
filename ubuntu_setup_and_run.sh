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
echo "[4/5] Verifying tesseract"
TESSERACT_BIN="$(command -v tesseract || true)"
if [ -z "$TESSERACT_BIN" ]; then
  echo "tesseract was not found after installation."
  exit 1
fi
echo "tesseract: $TESSERACT_BIN"

mkdir -p "$RUNTIME_DIR/ultralytics" "$RUNTIME_DIR/matplotlib"

echo
echo "[5/5] Handing off to Ubuntu run script"
exec "$PROJECT_DIR/run_project_ubuntu.sh"
