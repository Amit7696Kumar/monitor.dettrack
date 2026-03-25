#!/bin/bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${VENV_DIR:-$PROJECT_DIR/.venv}"
ENV_FILE="$PROJECT_DIR/.env"
RUNTIME_DIR="$PROJECT_DIR/.runtime"

cd "$PROJECT_DIR"
mkdir -p "$RUNTIME_DIR/ultralytics" "$RUNTIME_DIR/matplotlib"

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

if [ ! -d "$VENV_DIR" ]; then
  echo "Virtual environment not found at $VENV_DIR"
  echo "Create it first, then rerun this script."
  exit 1
fi

PYTHON_BIN=""
for candidate in "$VENV_DIR/bin/python" "$VENV_DIR/bin/python3" "$VENV_DIR/bin/python3.11" "$VENV_DIR/bin/python3.9"; do
  if [ -x "$candidate" ]; then
    PYTHON_BIN="$candidate"
    break
  fi
done

if [ -z "$PYTHON_BIN" ]; then
  echo "No Python interpreter found in $VENV_DIR/bin"
  exit 1
fi

# Keep defaults predictable if they are not set in .env.
export OCR_BACKEND="${OCR_BACKEND:-gcv}"
export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-8000}"
export UVICORN_RELOAD="${UVICORN_RELOAD:-0}"
export RUN_STARTUP_HEALTHCHECKS="${RUN_STARTUP_HEALTHCHECKS:-0}"
export WATCHFILES_FORCE_POLLING="${WATCHFILES_FORCE_POLLING:-1}"
export YOLO_CONFIG_DIR="${YOLO_CONFIG_DIR:-$RUNTIME_DIR/ultralytics}"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$RUNTIME_DIR/matplotlib}"

echo "Starting DET Monitoring"
echo "Project: $PROJECT_DIR"
echo "Venv: $VENV_DIR"
echo "Python: $PYTHON_BIN"
echo "Host: $HOST"
echo "Port: $PORT"
echo "OCR_BACKEND: $OCR_BACKEND"
echo "UVICORN_RELOAD: $UVICORN_RELOAD"
echo "WATCHFILES_FORCE_POLLING: $WATCHFILES_FORCE_POLLING"
echo "RUN_STARTUP_HEALTHCHECKS: $RUN_STARTUP_HEALTHCHECKS"

if [ -n "${GOOGLE_CLIENT_ID:-}" ]; then
  echo "Google OAuth: configured"
else
  echo "Google OAuth: missing GOOGLE_CLIENT_ID"
fi

if [ -n "${GCV_API_KEY:-}" ]; then
  echo "Google Vision: configured"
else
  echo "Google Vision: missing GCV_API_KEY"
fi

if [ -n "${OPENAI_API_KEY:-}" ]; then
  echo "OpenAI: configured"
else
  echo "OpenAI: missing OPENAI_API_KEY"
fi

if [ "$RUN_STARTUP_HEALTHCHECKS" = "1" ]; then
echo "Running YOLO health check"
"$PYTHON_BIN" - <<'PY'
import os
from pathlib import Path

import numpy as np

base = Path.cwd() / "server" / "models"
lcd_model_path = Path(os.getenv("YOLO_MODEL_PATH", str(base / "lcd_best.pt")))
fire_model_path = Path(os.getenv("FIREPOINT_MODEL_PATH", str(base / "firfightingpoint_best.pt")))

print(f"[YOLO CHECK] LCD model: {lcd_model_path}")
print(f"[YOLO CHECK] Fire-point model: {fire_model_path}")

if not lcd_model_path.exists():
    raise SystemExit(f"[YOLO CHECK] Missing LCD model: {lcd_model_path}")
if not fire_model_path.exists():
    raise SystemExit(f"[YOLO CHECK] Missing fire-point model: {fire_model_path}")

from ultralytics import YOLO

dummy = np.zeros((640, 640, 3), dtype=np.uint8)

lcd = YOLO(str(lcd_model_path))
lcd.predict(dummy, imgsz=640, conf=0.25, verbose=False)
print("[YOLO CHECK] LCD model loaded and inference passed")

fire = YOLO(str(fire_model_path))
fire.predict(dummy, imgsz=640, conf=0.15, verbose=False)
print("[YOLO CHECK] Fire-point model loaded and inference passed")
print("[YOLO CHECK] YOLO OK")
PY
else
  echo "Skipping YOLO health check for faster startup"
fi

if [ "$UVICORN_RELOAD" = "1" ]; then
  exec "$PYTHON_BIN" -m uvicorn server.app:app --reload --reload-dir "$PROJECT_DIR/server" --host "$HOST" --port "$PORT"
else
  exec "$PYTHON_BIN" -m uvicorn server.app:app --host "$HOST" --port "$PORT"
fi
