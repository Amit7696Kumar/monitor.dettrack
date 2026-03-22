#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
ENV_FILE="$PROJECT_DIR/.env"
RUNTIME_DIR="$PROJECT_DIR/.runtime"

cd "$PROJECT_DIR"

fail() {
  echo "ERROR: $1" >&2
  exit 1
}

check_command() {
  local cmd="$1"
  local help_text="$2"
  command -v "$cmd" >/dev/null 2>&1 || fail "$help_text"
}

echo "Meter OCR Ubuntu runner"
echo "Project: $PROJECT_DIR"

if [ ! -f /etc/os-release ]; then
  fail "Cannot verify OS. This script is intended for Ubuntu."
fi

# shellcheck disable=SC1091
source /etc/os-release
if [ "${ID:-}" != "ubuntu" ] && [ "${ID_LIKE:-}" != *debian* ]; then
  fail "Detected '${PRETTY_NAME:-unknown}'. Use this script only on Ubuntu or Ubuntu-compatible systems."
fi

[ -d "$VENV_DIR" ] || fail "Missing virtual environment at $VENV_DIR. Run ./ubuntu_setup_and_run.sh first."
[ -f "$VENV_DIR/bin/activate" ] || fail "Virtual environment is incomplete. Rebuild it before starting."

mkdir -p "$RUNTIME_DIR/ultralytics" "$RUNTIME_DIR/matplotlib"

if [ -f "$ENV_FILE" ]; then
  echo "Loading .env"
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo ".env not found, continuing with shell environment"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

check_command python "Python is unavailable inside the virtual environment."
check_command uvicorn "uvicorn is unavailable inside the virtual environment. Reinstall Python dependencies."
check_command tesseract "tesseract is not installed. Run: sudo apt-get install -y tesseract-ocr"

export TESSERACT_CMD="${TESSERACT_CMD:-$(command -v tesseract)}"
export OCR_BACKEND="${OCR_BACKEND:-gcv_then_tesseract}"
export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-8000}"
export UVICORN_RELOAD="${UVICORN_RELOAD:-0}"
export RUN_STARTUP_HEALTHCHECKS="${RUN_STARTUP_HEALTHCHECKS:-1}"
export WATCHFILES_FORCE_POLLING="${WATCHFILES_FORCE_POLLING:-1}"
export YOLO_CONFIG_DIR="${YOLO_CONFIG_DIR:-$RUNTIME_DIR/ultralytics}"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$RUNTIME_DIR/matplotlib}"

echo "Checking Python runtime"
python - <<'PY'
from pathlib import Path
import importlib
import os
import sys

required_modules = [
    "fastapi",
    "uvicorn",
    "jinja2",
    "multipart",
    "numpy",
    "cv2",
    "pytesseract",
    "ultralytics",
    "openai",
]

missing = []
for name in required_modules:
    try:
        importlib.import_module(name)
    except Exception as exc:
        missing.append(f"{name}: {exc}")

base = Path.cwd() / "server" / "models"
required_files = [
    base / "lcd_best.pt",
    base / "firfightingpoint_best.pt",
]
missing_files = [str(path) for path in required_files if not path.exists()]

print(f"Python: {sys.version.split()[0]} ({sys.executable})")
print(f"Tesseract: {os.environ.get('TESSERACT_CMD', '')}")

if missing:
    raise SystemExit("Missing Python dependencies:\n- " + "\n- ".join(missing))
if missing_files:
    raise SystemExit("Missing model files:\n- " + "\n- ".join(missing_files))
PY

echo "Host: $HOST"
echo "Port: $PORT"
echo "OCR_BACKEND: $OCR_BACKEND"
echo "UVICORN_RELOAD: $UVICORN_RELOAD"
echo "RUN_STARTUP_HEALTHCHECKS: $RUN_STARTUP_HEALTHCHECKS"
echo "Google OAuth: $( [ -n "${GOOGLE_CLIENT_ID:-}" ] && echo configured || echo missing )"
echo "Google Vision: $( [ -n "${GCV_API_KEY:-}" ] && echo configured || echo missing )"
echo "OpenAI: $( [ -n "${OPENAI_API_KEY:-}" ] && echo configured || echo missing )"

if [ "$RUN_STARTUP_HEALTHCHECKS" = "1" ]; then
  echo "Running YOLO health check"
  python - <<'PY'
import os
from pathlib import Path

import numpy as np
from ultralytics import YOLO

base = Path.cwd() / "server" / "models"
lcd_model_path = Path(os.getenv("YOLO_MODEL_PATH", str(base / "lcd_best.pt")))
fire_model_path = Path(os.getenv("FIREPOINT_MODEL_PATH", str(base / "firfightingpoint_best.pt")))

dummy = np.zeros((640, 640, 3), dtype=np.uint8)

lcd = YOLO(str(lcd_model_path))
lcd.predict(dummy, imgsz=640, conf=0.25, verbose=False)
print("[YOLO CHECK] LCD model loaded and inference passed")

fire = YOLO(str(fire_model_path))
fire.predict(dummy, imgsz=640, conf=0.15, verbose=False)
print("[YOLO CHECK] Fire-point model loaded and inference passed")
PY
else
  echo "Skipping YOLO health check"
fi

echo "Starting server at http://localhost:$PORT"
if [ "$UVICORN_RELOAD" = "1" ]; then
  exec uvicorn server.app:app --reload --reload-dir "$PROJECT_DIR/server" --host "$HOST" --port "$PORT"
else
  exec uvicorn server.app:app --host "$HOST" --port "$PORT"
fi
