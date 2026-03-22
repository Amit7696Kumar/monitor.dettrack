#!/usr/bin/env bash
set -euo pipefail

echo "==> 0) Move to project root: $(pwd)"

# 1) Backup old venv (in case you want to inspect later)
if [ -d ".venv" ]; then
  TS=$(date +"%Y%m%d_%H%M%S")
  echo "==> 1) Found existing .venv. Backing up to .venv_bak_$TS"
  mv .venv ".venv_bak_$TS"
fi

# 2) Create fresh venv using Homebrew python3.9 (you already have it)
echo "==> 2) Creating fresh venv (.venv) with python3.9"
python3.9 -m venv .venv

# 3) Activate and repair pip tooling
echo "==> 3) Activating venv and upgrading pip tooling"
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel

# 4) Install backend deps first (fastapi stack)
echo "==> 4) Installing backend deps"
python -m pip install "fastapi==0.128.0" "uvicorn[standard]==0.39.0" itsdangerous "passlib[bcrypt]" python-multipart aiofiles jinja2

# 5) Install computer vision + OCR stack
# NOTE: pytesseract is small; the engine is installed via brew (next step)
echo "==> 5) Installing CV/OCR deps"
python -m pip install --upgrade "numpy==1.26.4"
python -m pip install "opencv-python-headless==4.10.0.84" "scipy==1.12.0" "scikit-image==0.24.0"
python -m pip install "easyocr==1.7.1" "pytesseract==0.3.10"

# 6) Install YOLO/Ultralytics (you already used it, but install cleanly anyway)
echo "==> 6) Installing ultralytics"
python -m pip install "ultralytics==8.4.21"

echo "==> 6b) Installing OpenAI SDK"
python -m pip install "openai==2.26.0"

# 7) Verify imports
echo "==> 7) Verifying imports"
python - <<'PY'
import sys, numpy, cv2, scipy, skimage
print("PY:", sys.version.split()[0], sys.executable)
print("numpy:", numpy.__version__)
print("cv2:", cv2.__version__)
print("scipy:", scipy.__version__)
print("skimage:", skimage.__version__)

from ultralytics import YOLO
print("ultralytics: OK")

import easyocr
print("easyocr import: OK")

import pytesseract
print("pytesseract import: OK")
PY

echo "==> DONE. Venv rebuilt successfully."
echo ""
echo "Next:"
echo "1) Install Tesseract engine:  brew install tesseract"
echo "2) Run server:  source .venv/bin/activate && python -m uvicorn server.app:app --host 127.0.0.1 --port 8000 --reload"
