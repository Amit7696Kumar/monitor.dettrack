# server/lcd_detector.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

import cv2
import numpy as np
from ultralytics import YOLO

# Load once
MODEL_PATH = Path(__file__).parent / "models" / "lcd_best.pt"
_model = YOLO(str(MODEL_PATH))

def detect_lcd_box(
    image_bgr: np.ndarray,
    conf: float = 0.50,
    iou: float = 0.35,
) -> Optional[Tuple[int, int, int, int]]:
    """
    Returns best LCD box as (x, y, w, h) in pixel coords, or None.
    Uses max_det=1 (best box only).
    """
    if image_bgr is None or image_bgr.size == 0:
        return None

    # Ultralytics expects RGB
    rgb = cv2.cvtColor(image_bgr, cv2.COLOR_BGR2RGB)

    results = _model.predict(
        source=rgb,
        conf=conf,
        iou=iou,
        max_det=1,
        verbose=False,
    )

    if not results:
        return None

    r = results[0]
    if r.boxes is None or len(r.boxes) == 0:
        return None

    # xyxy in pixels
    xyxy = r.boxes.xyxy[0].cpu().numpy().tolist()
    x1, y1, x2, y2 = [int(round(v)) for v in xyxy]

    h, w = image_bgr.shape[:2]
    x1 = max(0, min(x1, w - 1))
    x2 = max(0, min(x2, w - 1))
    y1 = max(0, min(y1, h - 1))
    y2 = max(0, min(y2, h - 1))

    if x2 <= x1 or y2 <= y1:
        return None

    return (x1, y1, x2 - x1, y2 - y1)
