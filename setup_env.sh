#!/bin/bash
set -e

echo "Creating clean ML environment for meter-ocr-app"

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"

echo "Project dir: $PROJECT_DIR"
cd "$PROJECT_DIR"

# ----------------------------
# 1. Remove old venv if exists
# ----------------------------
if [ -d ".venv" ]; then
  echo "🧹 Removing existing venv"
  rm -rf .venv
fi

# ----------------------------
# 2. Create fresh venv
# ----------------------------
echo "Creating Python 3.9 venv"
python3.9 -m venv .venv

source .venv/bin/activate

python -m pip install --upgrade pip setuptools wheel

# ----------------------------
# 3. Install pinned scientific stack
# ----------------------------
echo "Installing NumPy"
pip install --no-cache-dir numpy==1.26.4

echo "Installing SciPy"
pip install --no-cache-dir scipy==1.11.4

echo "Installing scikit-image"
pip install --no-cache-dir scikit-image==0.21.0

echo "Installing OpenCV (headless)"
pip install --no-cache-dir opencv-python-headless==4.10.0.84

# ----------------------------
# 4. Install PyTorch (CPU + MPS safe)
# ----------------------------
echo "Installing PyTorch stack"
pip install --no-cache-dir \
  torch==2.3.1 \
  torchvision==0.18.1 \
  torchaudio==2.3.1

# ----------------------------
# 5. Install YOLO + EasyOCR
# ----------------------------
echo "Installing Ultralytics YOLO"
pip install --no-cache-dir ultralytics==8.4.21

echo "Installing OpenAI SDK"
pip install --no-cache-dir openai==2.26.0

echo "Installing EasyOCR"
pip install --no-cache-dir easyocr==1.7.1

# ----------------------------
# 6. Verification block
# ----------------------------
echo "Verifying installation"

python - <<'PY'
import numpy, scipy, skimage, cv2, torch
from ultralytics import YOLO
import easyocr

print("numpy:", numpy.__version__)
print("scipy:", scipy.__version__)
print("skimage:", skimage.__version__)
print("cv2:", cv2.__version__)
print("torch:", torch.__version__)
print("ultralytics OK")
print("Initializing EasyOCR Reader...")

r = easyocr.Reader(['en'], gpu=False)
print("EasyOCR Reader ready")
PY

echo "ENVIRONMENT READY SUCCESSFULLY"
