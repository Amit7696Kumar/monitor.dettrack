from __future__ import annotations

import base64
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List


def log(msg: str) -> None:
    print(msg, flush=True, file=sys.stdout)


_fire_point_yolo = None
BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = BASE_DIR.parent / ".runtime"
YOLO_CONFIG_DIR = RUNTIME_DIR / "ultralytics"
MPLCONFIGDIR = RUNTIME_DIR / "matplotlib"
YOLO_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
MPLCONFIGDIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("YOLO_CONFIG_DIR", str(YOLO_CONFIG_DIR))
os.environ.setdefault("MPLCONFIGDIR", str(MPLCONFIGDIR))


def _get_gcv_api_key() -> str:
    api_key = (os.getenv("GCV_API_KEY", "") or "").strip()
    if not api_key:
        raise RuntimeError("GCV_API_KEY is not set")
    return api_key


def get_fire_point_model():
    global _fire_point_yolo
    if _fire_point_yolo is not None:
        return _fire_point_yolo

    model_path = os.getenv(
        "FIREPOINT_MODEL_PATH",
        str(BASE_DIR / "models" / "firfightingpoint_best.pt"),
    )
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Fire-point model not found: {model_path}")

    os.environ.setdefault("TORCHDYNAMO_DISABLE", "1")
    os.environ.setdefault("TORCHINDUCTOR_DISABLE", "1")
    os.environ.setdefault("TORCH_COMPILE_DISABLE", "1")
    os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
    os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")

    log(f"[OBJECT_DETECTION][FIRE_POINT][YOLO] Loading model: {model_path}")
    from ultralytics import YOLO  # lazy import

    _fire_point_yolo = YOLO(model_path)
    return _fire_point_yolo


def _fire_point_detect_with_model(image_path: str) -> Dict[str, Any]:
    try:
        import cv2
    except Exception as e:
        raise RuntimeError(f"cv2 unavailable for fire-point model inference: {e}") from e

    model = get_fire_point_model()
    img = cv2.imread(image_path)
    if img is None:
        raise RuntimeError("Failed to decode image for fire-point model inference")

    results = model.predict(img, imgsz=640, conf=0.15, verbose=False)
    if not results:
        return {"present": False, "confidence": 0.0, "detections": 0, "source": "firepoint_model"}

    r0 = results[0]
    boxes = getattr(r0, "boxes", None)
    if boxes is None or boxes.conf is None or len(boxes) == 0:
        return {"present": False, "confidence": 0.0, "detections": 0, "source": "firepoint_model"}

    confs = boxes.conf.cpu().numpy().tolist()
    class_ids = boxes.cls.cpu().numpy().tolist() if getattr(boxes, "cls", None) is not None else []
    names = getattr(model, "names", {}) or {}
    labels: List[str] = []
    for class_id in class_ids:
        try:
            labels.append(str(names.get(int(class_id), int(class_id))))
        except Exception:
            continue

    best_conf = max([float(c) for c in confs], default=0.0)
    return {
        "present": bool(len(confs) > 0),
        "confidence": float(best_conf),
        "detections": int(len(confs)),
        "labels": labels,
        "source": "firepoint_model",
    }


def _gcv_fire_point_annotations(image_bytes: bytes) -> Dict[str, Any]:
    api_key = _get_gcv_api_key()
    url = f"https://vision.googleapis.com/v1/images:annotate?key={api_key}"
    payload = {
        "requests": [
            {
                "image": {"content": base64.b64encode(image_bytes).decode("ascii")},
                "features": [
                    {"type": "OBJECT_LOCALIZATION", "maxResults": 20},
                    {"type": "LABEL_DETECTION", "maxResults": 20},
                    {"type": "TEXT_DETECTION"},
                ],
            }
        ]
    }
    req = urllib.request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            resp_obj = json.loads(body)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GCV HTTP {e.code}: {body}") from e
    except Exception as e:
        raise RuntimeError(f"GCV request failed: {e}") from e

    responses = resp_obj.get("responses", []) if isinstance(resp_obj, dict) else []
    r0 = responses[0] if responses else {}
    if isinstance(r0, dict) and r0.get("error"):
        raise RuntimeError(f"GCV error: {r0.get('error')}")
    return r0 if isinstance(r0, dict) else {}


def _collect_keyword_matches(items: List[Dict[str, Any]], field: str) -> List[Dict[str, Any]]:
    strong_keywords = [
        "fire extinguisher",
        "extinguisher",
        "hose reel",
        "fire hose",
        "hydrant",
        "fire bucket",
        "bucket",
        "sprinkler",
    ]
    weak_keywords = [
        "alarm",
        "smoke detector",
        "safety equipment",
        "emergency equipment",
        "safety sign",
        "emergency sign",
        "cabinet",
    ]

    matches: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        raw = (item.get(field) or "").strip()
        name = raw.lower()
        if not name:
            continue
        score = float(item.get("score") or 0.0)
        for keyword in strong_keywords + weak_keywords:
            if keyword in name:
                matches.append({"name": raw, "keyword": keyword, "score": score})
                break
    return matches


def _fire_point_detect_with_local_text(image_path: str) -> Dict[str, Any]:
    try:
        import cv2
        import pytesseract
    except Exception as e:
        raise RuntimeError(f"cv2/pytesseract unavailable for local fire-point OCR: {e}") from e

    img = cv2.imread(image_path)
    if img is None:
        raise RuntimeError("Failed to decode image for local fire-point OCR")

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.equalizeHist(gray)
    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    text = pytesseract.image_to_string(thresh, config="--oem 3 --psm 6")
    text = (text or "").lower()
    hits = [
        token for token in [
            "fire",
            "extinguisher",
            "extinguishers",
            "bucket",
            "buckets",
            "hydrant",
            "hose",
            "sprinkler",
            "alarm",
        ]
        if token in text
    ]
    present = "fire" in hits and len(hits) >= 2 or len(hits) >= 3
    confidence = 0.78 if present else (0.45 if hits else 0.0)
    return {
        "present": bool(present),
        "confidence": float(confidence),
        "matched_keywords": hits,
        "ocr_excerpt": text[:300],
        "source": "local_text",
    }


def _fire_point_detect_with_cv_heuristic(image_path: str) -> Dict[str, Any]:
    try:
        import cv2
        import numpy as np
    except Exception as e:
        raise RuntimeError(f"cv2/numpy unavailable for heuristic fire-point detection: {e}") from e

    img = cv2.imread(image_path)
    if img is None:
        raise RuntimeError("Failed to decode image for heuristic fire-point detection")

    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    lower_red_1 = np.array([0, 70, 50], dtype=np.uint8)
    upper_red_1 = np.array([12, 255, 255], dtype=np.uint8)
    lower_red_2 = np.array([165, 70, 50], dtype=np.uint8)
    upper_red_2 = np.array([180, 255, 255], dtype=np.uint8)
    mask = cv2.inRange(hsv, lower_red_1, upper_red_1) | cv2.inRange(hsv, lower_red_2, upper_red_2)

    kernel = np.ones((5, 5), np.uint8)
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel)
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel)

    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    h, w = img.shape[:2]
    image_area = float(h * w)
    candidate_components: List[Dict[str, Any]] = []
    red_area = 0.0

    for contour in contours:
        area = float(cv2.contourArea(contour))
        if area <= 0:
            continue
        red_area += area
        if area < image_area * 0.003 or area > image_area * 0.45:
            continue
        x, y, bw, bh = cv2.boundingRect(contour)
        aspect = bw / float(max(bh, 1))
        if 0.25 <= aspect <= 3.5:
            candidate_components.append(
                {
                    "box": [int(x), int(y), int(bw), int(bh)],
                    "area_ratio": area / image_area,
                    "aspect_ratio": aspect,
                }
            )

    red_ratio = red_area / image_area if image_area else 0.0
    component_count = len(candidate_components)
    present = bool(
        component_count >= 3
        or (component_count >= 2 and red_ratio >= 0.06)
        or red_ratio >= 0.12
    )
    confidence = min(0.9, max(component_count * 0.18, red_ratio * 3.0, 0.15))

    return {
        "present": bool(present),
        "confidence": float(confidence),
        "component_count": int(component_count),
        "red_area_ratio": float(round(red_ratio, 4)),
        "components": candidate_components[:12],
        "source": "cv_heuristic",
    }


def detect_fire_fighting_equipment(image_path: str) -> Dict[str, Any]:
    with open(image_path, "rb") as f:
        image_bytes = f.read()
    if not image_bytes:
        raise RuntimeError("Failed to read image bytes for fire-point detection")

    model_error = ""
    model_result: Dict[str, Any] | None = None
    try:
        model_result = _fire_point_detect_with_model(image_path)
        if model_result.get("present"):
            model_result["model_error"] = None
            model_result["gcv_error"] = None
            return model_result
        log("[OBJECT_DETECTION][FIRE_POINT] Model returned no detections, continuing to fallback detectors")
    except Exception as e:
        model_error = str(e)
        log(f"[OBJECT_DETECTION][FIRE_POINT] Model detection failed, falling back to GCV: {e}")

    gcv_error = ""
    try:
        response = _gcv_fire_point_annotations(image_bytes)
        localized = response.get("localizedObjectAnnotations", []) if isinstance(response, dict) else []
        labels = response.get("labelAnnotations", []) if isinstance(response, dict) else []
        text_annotations = response.get("textAnnotations", []) if isinstance(response, dict) else []

        object_matches = _collect_keyword_matches(localized, "name")
        label_matches = _collect_keyword_matches(labels, "description")
        text = ""
        if text_annotations and isinstance(text_annotations[0], dict):
            text = (text_annotations[0].get("description") or "").lower()
        text_hits = [phrase for phrase in ["fire extinguisher", "hose reel", "hydrant", "fire bucket", "sprinkler"] if phrase in text]

        best_object_score = max([m["score"] for m in object_matches], default=0.0)
        best_label_score = max([m["score"] for m in label_matches], default=0.0)
        present = bool(
            best_object_score >= 0.30
            or best_label_score >= 0.55
            or text_hits
        )
        confidence = max(best_object_score, best_label_score, 0.88 if text_hits else 0.0, 0.2)

        return {
            "present": bool(present),
            "confidence": float(confidence),
            "object_matches": object_matches[:10],
            "label_matches": label_matches[:10],
            "matched_keywords": text_hits,
            "ocr_excerpt": text[:800],
            "source": "google_vision",
            "model_error": model_error or None,
            "model_result": model_result,
            "gcv_error": None,
        }
    except Exception as e:
        gcv_error = str(e)
        log(f"[OBJECT_DETECTION][FIRE_POINT] GCV detection failed, falling back to CV heuristic: {e}")

    text_error = ""
    try:
        local_text_result = _fire_point_detect_with_local_text(image_path)
        if local_text_result.get("present"):
            local_text_result["model_error"] = model_error or None
            local_text_result["model_result"] = model_result
            local_text_result["gcv_error"] = gcv_error or None
            return local_text_result
    except Exception as e:
        text_error = str(e)
        log(f"[OBJECT_DETECTION][FIRE_POINT] Local text detection failed, falling back to CV heuristic: {e}")

    heuristic_result = _fire_point_detect_with_cv_heuristic(image_path)
    heuristic_result["model_error"] = model_error or None
    heuristic_result["model_result"] = model_result
    heuristic_result["gcv_error"] = gcv_error or None
    heuristic_result["text_error"] = text_error or None
    return heuristic_result


def detect_task_objects(task_type: str, image_path: str) -> Dict[str, Any]:
    normalized = (task_type or "").strip().lower()
    if normalized == "fire_point":
        return detect_fire_fighting_equipment(image_path)
    raise ValueError(f"Unsupported object detection task type: {task_type}")
