from __future__ import annotations

import os
import re
import sys
import time
import base64
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Lazy-load cv2 to avoid interpreter crashes at import time on broken envs.
cv2 = None
import numpy as np

try:
    import pytesseract
except Exception:
    pytesseract = None

# DO NOT import ultralytics / torch at top-level.
# Must be lazy-loaded inside functions to prevent server import crash/hang.

def log(msg: str) -> None:
    print(msg, flush=True, file=sys.stdout)


def _ensure_cv2_loaded() -> bool:
    global cv2
    if cv2 is not None:
        return True
    try:
        import cv2 as _cv2
        cv2 = _cv2
        return True
    except Exception as e:
        log(f"[OCR] cv2 import failed: {e}")
        cv2 = None
        return False


BASE_DIR = os.path.dirname(__file__)
RUNTIME_DIR = Path(BASE_DIR).parent / ".runtime"
YOLO_CONFIG_DIR = RUNTIME_DIR / "ultralytics"
MPLCONFIGDIR = RUNTIME_DIR / "matplotlib"
YOLO_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
MPLCONFIGDIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("YOLO_CONFIG_DIR", str(YOLO_CONFIG_DIR))
os.environ.setdefault("MPLCONFIGDIR", str(MPLCONFIGDIR))

YOLO_MODEL_PATH = os.getenv(
    "YOLO_MODEL_PATH",
    os.path.join(BASE_DIR, "models", "lcd_best.pt"),
)

DEBUG_DIR = Path(os.path.join(BASE_DIR, "static", "debug"))
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

_yolo = None
_fire_point_yolo = None
# Homebrew path on Apple Silicon (Tesseract)
if pytesseract is not None:
    pytesseract.pytesseract.tesseract_cmd = "/opt/homebrew/bin/tesseract"


def _get_pytesseract():
    if pytesseract is None:
        raise RuntimeError("pytesseract is not installed")
    return pytesseract


def _clean_digits(s: str) -> str:
    """Fix common OCR confusions and keep only digits + dot."""
    if not s:
        return ""
    s = s.strip()

    trans = str.maketrans({
        "O": "0", "o": "0",
        "I": "1", "l": "1", "|": "1",
        "S": "5", "s": "5",
        "B": "8",
        "Z": "2",
        "G": "6",
        "q": "9",
        ",": ".",  # sometimes comma becomes decimal
    })
    s = s.translate(trans)
    s = re.sub(r"[^0-9.]", "", s)

    # Only keep first dot if multiple
    if s.count(".") > 1:
        first = s.find(".")
        s = s[:first + 1] + s[first + 1:].replace(".", "")

    if s == ".":
        return ""
    if s.startswith(".") and len(s) > 1:
        s = "0" + s
    if s.endswith(".") and len(s) > 1:
        s = s[:-1]

    return s


def _best_number_from_text(text: str) -> Optional[str]:
    """Pick the longest numeric token."""
    if not text:
        return None
    nums = re.findall(r"\d+(?:\.\d+)?", text)
    if not nums:
        return None
    nums.sort(key=lambda x: len(x.replace(".", "")), reverse=True)
    return nums[0]


def _ocr_backend_mode() -> str:
    # gcv_then_tesseract (default), gcv, tesseract
    return (os.getenv("OCR_BACKEND", "gcv_then_tesseract") or "").strip().lower()


def _get_gcv_api_key() -> str:
    api_key = (os.getenv("GCV_API_KEY", "") or "").strip()
    if not api_key:
        raise RuntimeError("GCV_API_KEY is not set")
    return api_key


def _gcv_lines_and_numeric(crop_bgr: np.ndarray) -> Tuple[List[Dict[str, Any]], Optional[str], float]:
    if cv2 is None:
        raise RuntimeError("cv2 is not available")
    ok, encoded = cv2.imencode(".jpg", crop_bgr)
    if not ok:
        raise RuntimeError("Failed to encode crop as JPEG for GCV")
    return _gcv_lines_and_numeric_from_bytes(encoded.tobytes())


def _gcv_lines_and_numeric_from_bytes(image_bytes: bytes) -> Tuple[List[Dict[str, Any]], Optional[str], float]:
    api_key = _get_gcv_api_key()
    url = f"https://vision.googleapis.com/v1/images:annotate?key={api_key}"
    log(f"[OCR][GCV] Hitting Vision API (bytes={len(image_bytes)})")
    payload = {
        "requests": [
            {
                "image": {"content": base64.b64encode(image_bytes).decode("ascii")},
                "features": [{"type": "TEXT_DETECTION"}],
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
            log(f"[OCR][GCV] Vision API response status={getattr(resp, 'status', 'unknown')}")
            resp_obj = json.loads(body)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        log(f"[OCR][GCV] Vision API HTTP error status={e.code}")
        raise RuntimeError(f"GCV HTTP {e.code}: {body}") from e
    except Exception as e:
        log(f"[OCR][GCV] Vision API request error: {e}")
        raise RuntimeError(f"GCV request failed: {e}") from e

    responses = resp_obj.get("responses", []) if isinstance(resp_obj, dict) else []
    r0 = responses[0] if responses else {}
    if isinstance(r0, dict) and r0.get("error"):
        raise RuntimeError(f"GCV error: {r0.get('error')}")

    anns = r0.get("textAnnotations", []) if isinstance(r0, dict) else []
    lines: List[Dict[str, Any]] = []
    best_value: Optional[str] = None
    best_score = -1.0
    best_conf01 = 0.0

    if anns:
        full = anns[0].get("description", "") or ""
        for ln in [x.strip() for x in full.splitlines() if x.strip()]:
            lines.append({"text": ln, "confidence": 0.0})

    for ann in anns[1:] if len(anns) > 1 else []:
        if not isinstance(ann, dict):
            continue
        text = (ann.get("description") or "").strip()
        if not text:
            continue
        candidate = _best_number_from_text(_clean_digits(text))
        if not candidate:
            continue
        conf01 = 0.0
        score = _score_candidate(candidate, conf01)
        if score > best_score:
            best_score = score
            best_value = candidate
            best_conf01 = conf01

    log(f"[OCR][GCV] Parsed lines={len(lines)} best_value={best_value or 'None'}")
    return lines, best_value, best_conf01


def _gcv_full_text_from_bytes(image_bytes: bytes) -> str:
    """
    Fetch full OCR text from Google Vision API.
    Returns empty string if no text is detected.
    """
    api_key = _get_gcv_api_key()
    url = f"https://vision.googleapis.com/v1/images:annotate?key={api_key}"
    payload = {
        "requests": [
            {
                "image": {"content": base64.b64encode(image_bytes).decode("ascii")},
                "features": [{"type": "TEXT_DETECTION"}],
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

    anns = r0.get("textAnnotations", []) if isinstance(r0, dict) else []
    if not anns:
        return ""
    full = anns[0].get("description", "") if isinstance(anns[0], dict) else ""
    return (full or "").strip()


def _gcv_fire_point_annotations(image_bytes: bytes) -> Dict[str, Any]:
    """
    Ask Google Vision for visual signals that indicate fire-fighting equipment.
    TEXT_DETECTION alone is too weak because many valid photos contain no readable text.
    """
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


def _pad_box(x0: int, y0: int, x1: int, y1: int, w: int, h: int, pad: float = 0.10) -> Tuple[int, int, int, int]:
    bw = x1 - x0
    bh = y1 - y0
    px = int(bw * pad)
    py = int(bh * pad)
    x0 = max(0, x0 - px)
    y0 = max(0, y0 - py)
    x1 = min(w - 1, x1 + px)
    y1 = min(h - 1, y1 + py)
    return x0, y0, x1, y1


def get_yolo():
    """
    Lazy-load YOLO ONLY when needed.
    Disables TorchDynamo/Inductor to avoid slow import paths.
    """
    global _yolo
    if _yolo is not None:
        return _yolo

    os.environ.setdefault("TORCHDYNAMO_DISABLE", "1")
    os.environ.setdefault("TORCHINDUCTOR_DISABLE", "1")
    os.environ.setdefault("TORCH_COMPILE_DISABLE", "1")
    os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
    os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")

    if not os.path.exists(YOLO_MODEL_PATH):
        raise FileNotFoundError(f"YOLO model not found: {YOLO_MODEL_PATH}")

    log(f"[YOLO] Loading model: {YOLO_MODEL_PATH}")

    from ultralytics import YOLO  # lazy import

    t0 = time.time()
    _yolo = YOLO(YOLO_MODEL_PATH)
    log(f"[YOLO] Loaded in {time.time() - t0:.2f}s")

    # One dummy inference to warm up execution graph
    try:
        warm = np.zeros((640, 640, 3), dtype=np.uint8)
        t1 = time.time()
        _yolo.predict(warm, imgsz=640, conf=0.25, verbose=False)
        log(f"[YOLO] Warmup inference in {time.time() - t1:.2f}s")
    except Exception as e:
        log(f"[YOLO] Warmup inference failed: {e}")

    return _yolo


def get_fire_point_model():
    """
    Lazy-load fire-point detection model.
    Falls back to caller-side logic when model is missing/unavailable.
    """
    global _fire_point_yolo
    if _fire_point_yolo is not None:
        return _fire_point_yolo

    model_path = os.getenv(
        "FIREPOINT_MODEL_PATH",
        os.path.join(BASE_DIR, "models", "firfightingpoint_best.pt"),
    )
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Fire-point model not found: {model_path}")

    os.environ.setdefault("TORCHDYNAMO_DISABLE", "1")
    os.environ.setdefault("TORCHINDUCTOR_DISABLE", "1")
    os.environ.setdefault("TORCH_COMPILE_DISABLE", "1")
    os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
    os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")

    log(f"[FIRE_POINT][YOLO] Loading model: {model_path}")
    from ultralytics import YOLO  # lazy import
    _fire_point_yolo = YOLO(model_path)
    return _fire_point_yolo


def _fire_point_detect_with_model(image_path: str) -> Dict[str, Any]:
    model = get_fire_point_model()
    if not _ensure_cv2_loaded() or cv2 is None:
        raise RuntimeError("cv2 is unavailable for fire-point model inference")
    img = cv2.imread(image_path)
    if img is None:
        raise RuntimeError("Failed to decode image for fire-point model inference")

    results = model.predict(img, imgsz=640, conf=0.25, verbose=False)
    if not results:
        return {"present": False, "confidence": 0.0, "detections": 0, "source": "firepoint_model"}

    r0 = results[0]
    boxes = getattr(r0, "boxes", None)
    if boxes is None or boxes.conf is None or len(boxes) == 0:
        return {"present": False, "confidence": 0.0, "detections": 0, "source": "firepoint_model"}

    confs = boxes.conf.cpu().numpy().tolist()
    best_conf = max([float(c) for c in confs], default=0.0)
    detections = len(confs)
    present = detections > 0 and best_conf >= 0.25
    return {
        "present": bool(present),
        "confidence": float(best_conf),
        "detections": int(detections),
        "source": "firepoint_model",
    }


def _fire_point_detect_with_gcv(image_bytes: bytes) -> Dict[str, Any]:
    response = _gcv_fire_point_annotations(image_bytes)
    text_annotations = response.get("textAnnotations", []) if isinstance(response, dict) else []
    localized = response.get("localizedObjectAnnotations", []) if isinstance(response, dict) else []
    labels = response.get("labelAnnotations", []) if isinstance(response, dict) else []

    text = ""
    if text_annotations and isinstance(text_annotations[0], dict):
        text = (text_annotations[0].get("description") or "").lower()

    strong_keywords = [
        "fire extinguisher",
        "extinguisher",
        "hose reel",
        "fire hose",
        "hydrant",
        "fire bucket",
        "fire alarm",
        "fire point",
        "sprinkler",
    ]
    weak_keywords = [
        "alarm",
        "smoke detector",
        "safety equipment",
        "emergency equipment",
        "safety sign",
        "emergency sign",
    ]

    def _collect_matches(items: List[Dict[str, Any]], field: str) -> List[Dict[str, Any]]:
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

    object_matches = _collect_matches(localized, "name")
    label_matches = _collect_matches(labels, "description")
    text_matches = [k for k in strong_keywords if k in text]

    best_object_score = max([m["score"] for m in object_matches], default=0.0)
    best_label_score = max([m["score"] for m in label_matches], default=0.0)
    weak_match_count = sum(1 for m in object_matches + label_matches if m["keyword"] in weak_keywords)

    present = bool(
        best_object_score >= 0.35
        or best_label_score >= 0.60
        or text_matches
        or weak_match_count >= 2
    )
    confidence = max(
        best_object_score,
        best_label_score,
        0.88 if text_matches else 0.0,
        0.66 if weak_match_count >= 2 else 0.0,
        0.2,
    )
    return {
        "present": bool(present),
        "confidence": float(confidence),
        "matched_keywords": text_matches,
        "object_matches": object_matches[:10],
        "label_matches": label_matches[:10],
        "weak_match_count": int(weak_match_count),
        "ocr_excerpt": text[:800],
        "source": "google_vision",
    }


def save_yolo_debug(img_bgr: np.ndarray, box: Tuple[int, int, int, int], out_path: str) -> None:
    x0, y0, x1, y1 = box
    dbg = img_bgr.copy()
    cv2.rectangle(dbg, (x0, y0), (x1, y1), (0, 255, 0), 3)
    cv2.putText(dbg, "LCD", (x0, max(20, y0 - 10)), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)
    cv2.imwrite(out_path, dbg)


def detect_lcd(img_bgr: np.ndarray) -> Optional[Tuple[int, int, int, int, float]]:
    model = get_yolo()
    results = model.predict(img_bgr, imgsz=640, conf=0.25, verbose=False)
    if not results:
        return None

    r0 = results[0]
    boxes = getattr(r0, "boxes", None)
    if boxes is None or boxes.xyxy is None or len(boxes) == 0:
        return None

    xyxy = boxes.xyxy.cpu().numpy()
    confs = boxes.conf.cpu().numpy() if boxes.conf is not None else np.zeros((len(xyxy),), dtype=float)
    best_i = int(np.argmax(confs)) if len(confs) else 0

    x0, y0, x1, y1 = xyxy[best_i].tolist()
    conf = float(confs[best_i]) if len(confs) else 0.0

    h, w = img_bgr.shape[:2]
    x0 = max(0, min(w - 1, int(x0)))
    y0 = max(0, min(h - 1, int(y0)))
    x1 = max(0, min(w - 1, int(x1)))
    y1 = max(0, min(h - 1, int(y1)))

    if x1 <= x0 or y1 <= y0:
        return None

    x0, y0, x1, y1 = _pad_box(x0, y0, x1, y1, w, h, pad=0.10)
    return x0, y0, x1, y1, conf


def _unsharp(gray: np.ndarray) -> np.ndarray:
    blur = cv2.GaussianBlur(gray, (0, 0), 1.0)
    return cv2.addWeighted(gray, 1.6, blur, -0.6, 0)


def _gamma(gray: np.ndarray, gamma: float) -> np.ndarray:
    inv = 1.0 / max(0.1, gamma)
    table = (np.linspace(0, 1, 256) ** inv * 255).astype(np.uint8)
    return cv2.LUT(gray, table)


def _normalize_gray(gray: np.ndarray) -> np.ndarray:
    # Contrast stretch using robust percentiles.
    lo, hi = np.percentile(gray, (2, 98))
    if hi <= lo:
        return gray
    out = np.clip((gray - lo) * (255.0 / (hi - lo)), 0, 255).astype(np.uint8)
    return out


def _clahe_gray(gray: np.ndarray) -> np.ndarray:
    clahe = cv2.createCLAHE(clipLimit=2.5, tileGridSize=(8, 8))
    return clahe.apply(gray)


def _to_lab_l(bgr: np.ndarray) -> np.ndarray:
    lab = cv2.cvtColor(bgr, cv2.COLOR_BGR2LAB)
    l = lab[:, :, 0]
    return l


def build_variants(crop_bgr: np.ndarray) -> List[Tuple[str, np.ndarray]]:
    variants: List[Tuple[str, np.ndarray]] = []

    gray = cv2.cvtColor(crop_bgr, cv2.COLOR_BGR2GRAY)
    gray = _normalize_gray(gray)
    up = cv2.resize(gray, None, fx=4.0, fy=4.0, interpolation=cv2.INTER_CUBIC)

    den = cv2.fastNlMeansDenoising(up, None, 12, 7, 21)
    cl = _clahe_gray(den)
    un = _unsharp(cl)
    lab_l = _clahe_gray(cv2.resize(_normalize_gray(_to_lab_l(crop_bgr)), None, fx=4.0, fy=4.0, interpolation=cv2.INTER_CUBIC))

    bases = [
        ("cl", cl),
        ("unsharp", un),
        ("lab_l", lab_l),
    ]

    for bname, base in bases:
        _, otsu = cv2.threshold(base, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        variants.append((f"{bname}_otsu", otsu))
        variants.append((f"{bname}_otsu_inv", cv2.bitwise_not(otsu)))

    # One adaptive variant (stable)
    adap = cv2.adaptiveThreshold(
        cl, 255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31, 7,
    )
    variants.append(("cl_adaptive_31", adap))
    variants.append(("cl_adaptive_31_inv", cv2.bitwise_not(adap)))

    return variants


def _score_candidate(text: str, conf01: float, target_len: Optional[int] = None) -> float:
    if not text:
        return -1.0
    digits = text.replace(".", "")
    score = conf01 + min(0.6, 0.08 * len(digits))
    if len(digits) == 1 and (not target_len or target_len > 1):
        score -= 0.35
    # Penalize very short outputs when we have a target length
    if target_len:
        if len(digits) < max(2, target_len - 1):
            score -= 0.25
        elif len(digits) > target_len + 1:
            score -= 0.15
    return score


def _tesseract_pass(gray: np.ndarray, psm: int, allow_dot: bool) -> Tuple[str, float]:
    pt = _get_pytesseract()
    whitelist = "0123456789." if allow_dot else "0123456789"
    config = (
        f"--oem 1 --psm {psm} "
        f"-c tessedit_char_whitelist={whitelist} "
        "-c classify_bln_numeric_mode=1"
    )
    data = pt.image_to_data(gray, config=config, output_type=pt.Output.DICT)

    best_text = ""
    best_conf = -1.0
    n = len(data.get("text", []))
    for i in range(n):
        txt = (data["text"][i] or "").strip()
        if not txt:
            continue
        cleaned = _clean_digits(txt)
        if not cleaned:
            continue
        try:
            conf = float(data["conf"][i])
        except Exception:
            conf = -1.0
        if conf > best_conf:
            best_conf = conf
            best_text = cleaned

    # Fallback: try full string parse
    if not best_text:
        raw = pt.image_to_string(gray, config=config)
        best_text = _best_number_from_text(_clean_digits(raw)) or ""
        best_conf = 30.0 if best_text else -1.0

    if best_conf < 0 and best_text:
        # Treat unknown confidence as neutral, not zero.
        best_conf = 35.0
    conf01 = 0.0 if best_conf < 0 else max(0.0, min(1.0, best_conf / 100.0))
    return best_text, conf01


def tesseract_digits(img_any: np.ndarray) -> Tuple[str, float]:
    """
    Returns (text, confidence_0_to_1)
    """
    if img_any.ndim == 3:
        gray = cv2.cvtColor(img_any, cv2.COLOR_BGR2GRAY)
    else:
        gray = img_any

    if gray.dtype != np.uint8:
        gray = cv2.normalize(gray, None, 0, 255, cv2.NORM_MINMAX).astype(np.uint8)

    best_text = ""
    best_score = -1.0
    best_conf = 0.0

    for psm in (7, 8, 6, 13):
        for allow_dot in (True, False):
            txt, conf01 = _tesseract_pass(gray, psm, allow_dot=allow_dot)
            txt = _clean_digits(txt)
            if not txt or not re.search(r"\d", txt):
                continue
            score = _score_candidate(txt, conf01)
            if score > best_score:
                best_score = score
                best_text = txt
                best_conf = conf01

    return best_text, best_conf


def _largest_component_crop(bin_img: np.ndarray) -> Optional[Tuple[int, int, int, int]]:
    contours, _ = cv2.findContours(bin_img, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return None
    h, w = bin_img.shape[:2]
    best = None
    best_area = 0
    for c in contours:
        x, y, cw, ch = cv2.boundingRect(c)
        area = cw * ch
        if area < (w * h * 0.03):
            continue
        if area > best_area:
            best_area = area
            best = (x, y, cw, ch)
    if not best:
        return None
    x, y, cw, ch = best
    pad = int(max(cw, ch) * 0.08)
    x0 = max(0, x - pad)
    y0 = max(0, y - pad)
    x1 = min(w - 1, x + cw + pad)
    y1 = min(h - 1, y + ch + pad)
    return x0, y0, x1, y1


def _ensure_white_fg(bin_img: np.ndarray) -> np.ndarray:
    if bin_img.mean() > 127:
        return cv2.bitwise_not(bin_img)
    return bin_img


def _infer_decimal_from_binary(bin_img: np.ndarray, digits: str) -> Optional[str]:
    if not digits or "." in digits:
        return digits
    if not re.search(r"\d", digits):
        return digits

    fg = _ensure_white_fg(bin_img)
    h, w = fg.shape[:2]

    # Find small dot-like components near the bottom.
    num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(fg, connectivity=8)
    min_area = max(6, int(0.0002 * w * h))
    max_area = max(min_area + 2, int(0.02 * w * h))
    dot_candidates: List[Tuple[float, int, int, int, int]] = []
    for i in range(1, num_labels):
        x, y, cw, ch, area = stats[i]
        if area < min_area or area > max_area:
            continue
        ar = cw / max(1, ch)
        if ar < 0.3 or ar > 3.2:
            continue
        cy = y + ch / 2.0
        if cy < h * 0.55:
            continue
        cx = x + cw / 2.0
        dot_candidates.append((area, int(cx), x, y, cw))

    if not dot_candidates:
        return digits

    # Choose the most dot-like (smallest area) candidate.
    dot_candidates.sort(key=lambda t: t[0])
    _, dot_x, _, _, _ = dot_candidates[0]

    # Estimate digit runs from vertical projection.
    col_sum = np.sum(fg > 0, axis=0)
    ink = col_sum > (0.10 * h)
    runs: List[Tuple[int, int]] = []
    start = None
    for i, on in enumerate(ink.tolist() + [False]):
        if on and start is None:
            start = i
        elif (not on) and start is not None:
            runs.append((start, i - 1))
            start = None

    min_digit_w = max(2, int(0.02 * w))
    digit_runs = [r for r in runs if (r[1] - r[0] + 1) >= min_digit_w]
    if not digit_runs:
        return digits
    if len(digit_runs) < 2 and len(digits.replace(".", "")) >= 2:
        return digits

    # Keep the widest runs if there are too many.
    digit_runs.sort(key=lambda r: (r[1] - r[0]), reverse=True)
    if len(digit_runs) > len(digits):
        digit_runs = digit_runs[: len(digits)]
    digit_runs.sort(key=lambda r: r[0])
    centers = [int((r[0] + r[1]) / 2) for r in digit_runs]

    insert_at = None
    for i in range(len(centers) - 1):
        if centers[i] < dot_x < centers[i + 1]:
            insert_at = i + 1
            break
    if insert_at is None:
        if dot_x > centers[-1]:
            insert_at = len(centers)
        elif dot_x < centers[0]:
            return f"0.{digits}"
        else:
            return digits

    if insert_at <= 0:
        return f"0.{digits}"
    if insert_at >= len(digits):
        return f"{digits}."
    return digits[:insert_at] + "." + digits[insert_at:]


def _normalize_fixed_decimals(value: str, decimals: int = 2) -> Optional[str]:
    if not value:
        return None
    s = _clean_digits(value)
    if not s:
        return None
    if "." in s:
        left, right = s.split(".", 1)
    else:
        left, right = s, ""

    left = left if left else "0"
    right = re.sub(r"[^0-9]", "", right)

    if not right:
        digits = re.sub(r"[^0-9]", "", s)
        if len(digits) >= decimals + 1:
            left = digits[:-decimals]
            right = digits[-decimals:]
        elif len(digits) == decimals:
            left = "0"
            right = digits
        else:
            return None

    if len(right) < decimals:
        right = right.ljust(decimals, "0")
    elif len(right) > decimals:
        right = right[:decimals]

    left = re.sub(r"[^0-9]", "", left) or "0"
    return f"{left}.{right}"


def _is_earthing_format(value: str) -> bool:
    return bool(re.fullmatch(r"\d+\.\d{2}", value or ""))


def _best_conf_for_value(candidates: List[Tuple[str, float, str, float]], value: str) -> float:
    best = 0.0
    for cand, _total, _name, conf01 in candidates:
        if cand == value and conf01 > best:
            best = conf01
    return best


def _consensus_pick(candidates: List[Tuple[str, float, str, float]], normalize=None) -> Optional[str]:
    if not candidates:
        return None
    counts: Dict[str, int] = {}
    best_score: Dict[str, float] = {}
    for cand, total, _name, _conf in candidates:
        key = normalize(cand) if normalize else cand
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1
        best_score[key] = max(best_score.get(key, -1.0), total)
    if not counts:
        return None
    # Prefer most frequent; tie-break by best score
    return max(counts.keys(), key=lambda k: (counts[k], best_score.get(k, -1.0)))


def warmup_models() -> None:
    backend = _ocr_backend_mode()
    if backend == "gcv":
        log("[WARMUP] Skipping YOLO warmup (GCV-only mode or cv2 unavailable)")
        return
    if not _ensure_cv2_loaded():
        log("[WARMUP] Skipping YOLO warmup (cv2 unavailable)")
        return
    try:
        get_yolo()
    except Exception as e:
        log(f"[WARMUP] ⚠️ YOLO warmup failed: {e}")


def run_ocr(image_path: str, debug_id: Optional[str] = None, meter_type: Optional[str] = None) -> Dict[str, Any]:
    log(f"[OCR] Processing image: {image_path}")
    backend = _ocr_backend_mode()
    log(f"[OCR] Backend={backend} GCV_API_KEY_set={'yes' if bool((os.getenv('GCV_API_KEY', '') or '').strip()) else 'no'}")
    _ensure_cv2_loaded()

    if cv2 is None:
        if backend not in {"gcv", "gcv_then_tesseract"}:
            return {
                "numeric": None,
                "text": "",
                "lines": [],
                "used_crop": False,
                "crop_box": None,
                "debug": {"error": "cv2 is not available and OCR_BACKEND is not GCV"},
            }
        try:
            with open(image_path, "rb") as f:
                image_bytes = f.read()
            gcv_lines, gcv_value, gcv_conf = _gcv_lines_and_numeric_from_bytes(image_bytes)
            if meter_type == "earthing" and gcv_value:
                normalized = _normalize_fixed_decimals(gcv_value, decimals=2)
                if normalized and _is_earthing_format(normalized):
                    gcv_value = normalized
            if gcv_value:
                return {
                    "numeric": {"value": gcv_value, "confidence": float(gcv_conf)},
                    "text": gcv_value,
                    "lines": gcv_lines,
                    "used_crop": False,
                    "crop_box": None,
                    "debug": {"provider": "google_vision", "ocr_conf": gcv_conf, "cv2": "unavailable"},
                }
            return {
                "numeric": None,
                "text": "",
                "lines": gcv_lines,
                "used_crop": False,
                "crop_box": None,
                "debug": {"provider": "google_vision", "ocr_conf": 0.0, "cv2": "unavailable"},
            }
        except Exception as e:
            return {
                "numeric": None,
                "text": "",
                "lines": [],
                "used_crop": False,
                "crop_box": None,
                "debug": {"provider": "google_vision", "ocr_conf": 0.0, "cv2": "unavailable", "error": str(e)},
            }

    img = cv2.imread(image_path)
    if img is None:
        return {"numeric": None, "text": "", "lines": [], "used_crop": False, "crop_box": None, "debug": {"error": "cv2.imread failed"}}

    det = None
    yolo_error: Optional[str] = None
    try:
        det = detect_lcd(img)
    except Exception as e:
        yolo_error = str(e)
        log(f"[YOLO] Detection failed: {e}")

    if det is None:
        h, w = img.shape[:2]
        x0, y0, x1, y1, yconf = 0, 0, w, h, 0.0
        if meter_type == "earthing":
            log("[YOLO] No LCD detected for earthing; refusing full-image OCR")
            return {
                "numeric": None,
                "text": "",
                "lines": [],
                "used_crop": False,
                "crop_box": None,
                "debug": {
                    "provider": "meter_guard",
                    "meter_detected": False,
                    "yolo_conf": 0.0,
                    "ocr_conf": 0.0,
                    "reason": "no_lcd_detected",
                },
            }
        if backend in {"gcv", "gcv_then_tesseract"}:
            log("[YOLO] No LCD detected; using full image for GCV")
        else:
            log("[YOLO] No LCD detected; using full image for Tesseract")
    else:
        x0, y0, x1, y1, yconf = det
        log(f"[YOLO] LCD detected at ({x0},{y0},{x1},{y1}) conf={yconf:.2f}")

    crop = img[y0:y1, x0:x1].copy()

    debug_urls = {}
    if debug_id:
        yolo_path = DEBUG_DIR / f"yolo_{debug_id}.jpg"
        crop_path = DEBUG_DIR / f"crop_{debug_id}.jpg"

        if det is not None:
            save_yolo_debug(img, (x0, y0, x1, y1), str(yolo_path))
        else:
            cv2.imwrite(str(yolo_path), img)
        cv2.imwrite(str(crop_path), crop)

        debug_urls = {
            "yolo": f"/static/debug/yolo_{debug_id}.jpg",
            "crop": f"/static/debug/crop_{debug_id}.jpg",
        }

    if backend in {"gcv", "gcv_then_tesseract"}:
        try:
            gcv_lines, gcv_value, gcv_conf = _gcv_lines_and_numeric(crop)
            if meter_type == "earthing" and gcv_value:
                normalized = _normalize_fixed_decimals(gcv_value, decimals=2)
                if normalized and _is_earthing_format(normalized):
                    gcv_value = normalized
            if gcv_value:
                log(f"[OCR][GCV] FINAL VALUE = {gcv_value} (conf={gcv_conf:.2f})")
                return {
                    "numeric": {"value": gcv_value, "confidence": float(gcv_conf)},
                    "text": gcv_value,
                    "lines": gcv_lines,
                    "used_crop": True,
                    "crop_box": [x0, y0, x1 - x0, y1 - y0],
                    "debug_urls": debug_urls,
                    "debug": {"provider": "google_vision", "meter_detected": True, "yolo_conf": yconf, "ocr_conf": gcv_conf},
                }
            log("[OCR][GCV] No numeric value extracted")
            if backend == "gcv":
                return {
                    "numeric": None,
                    "text": "",
                    "lines": gcv_lines,
                    "used_crop": True,
                    "crop_box": [x0, y0, x1 - x0, y1 - y0],
                    "debug_urls": debug_urls,
                    "debug": {"provider": "google_vision", "meter_detected": True, "yolo_conf": yconf, "ocr_conf": 0.0},
                }
        except Exception as e:
            log(f"[OCR][GCV] Failed: {e}")
            if backend == "gcv":
                return {
                    "numeric": None,
                    "text": "",
                    "lines": [],
                    "used_crop": True,
                    "crop_box": [x0, y0, x1 - x0, y1 - y0],
                    "debug_urls": debug_urls,
                    "debug": {"provider": "google_vision", "meter_detected": True, "yolo_conf": yconf, "ocr_conf": 0.0, "error": str(e)},
                }

    if pytesseract is None:
        log("[OCR][Tesseract] pytesseract unavailable; skipping local OCR")
        return {
            "numeric": None,
            "text": "",
            "lines": [],
            "used_crop": True,
            "crop_box": [x0, y0, x1 - x0, y1 - y0],
            "debug_urls": debug_urls,
            "debug": {"meter_detected": True, "yolo_conf": yconf, "ocr_conf": 0.0, "error": "pytesseract not installed"},
        }

    variants = build_variants(crop)

    best_value: Optional[str] = None
    best_score: float = -1.0
    best_variant: str = ""
    best_img: Optional[np.ndarray] = None
    all_candidates: List[Tuple[str, float, str, float]] = []
    dot_bins: List[Tuple[float, str, np.ndarray]] = []
    variant_candidate: Dict[str, str] = {}
    variant_img: Dict[str, np.ndarray] = {}
    length_counts: Dict[int, int] = {}

    for name, vimg in variants:
        log(f"[OCR] Trying variant: {name}")
        candidate, score = tesseract_digits(vimg)
        if not candidate or not re.search(r"\d", candidate):
            log("[OCR]   (no digits)")
            continue

        log(f"[OCR]   candidate='{candidate}' score={score:.2f}")

        # Prefer thresholded variants slightly
        bonus = 0.10 if "otsu" in name or "adaptive" in name else 0.0
        total = score + bonus

        # Ignore weak single-digit hits
        if len(candidate.replace(".", "")) == 1 and total < 0.5:
            continue

        clen = len(candidate.replace(".", ""))
        length_counts[clen] = length_counts.get(clen, 0) + 1

        if total > best_score:
            best_score = total
            best_value = candidate
            best_variant = name
            best_img = vimg
        all_candidates.append((candidate, total, name, score))
        variant_candidate[name] = candidate
        variant_img[name] = vimg
        if ("otsu" in name or "adaptive" in name) and "blackhat" not in name and vimg.ndim == 2:
            dot_bins.append((total, name, vimg))

    target_len = None
    if length_counts:
        target_len = max(length_counts.keys(), key=lambda k: length_counts[k])

    if target_len and all_candidates:
        # Re-score candidates against the consensus length to avoid "300" -> "1"
        for cand, _total, name, conf01 in all_candidates:
            bonus = 0.15 if "otsu" in name or "adaptive" in name else 0.0
            adj = _score_candidate(cand, conf01, target_len=target_len) + bonus
            if adj > best_score:
                best_score = adj
                best_value = cand
                best_variant = f"{name}+len"
                best_img = variant_img.get(name)

    if best_value and len(best_value.replace(".", "")) < 2:
        # Prefer any longer candidate within a reasonable score delta
        best_len = len(best_value.replace(".", ""))
        for cand, total, name, _ in all_candidates:
            clen = len(cand.replace(".", ""))
            if clen >= 2 and total >= (best_score - 0.1) and clen > best_len:
                best_score = total
                best_value = cand
                best_variant = f"{name}+len2"
                best_img = variant_img.get(name)

    if not best_value and all_candidates:
        # Majority vote fallback if confidences are poor but consistent
        counts: Dict[str, int] = {}
        best_by_score: Dict[str, float] = {}
        for cand, total, _, _ in all_candidates:
            if len(cand.replace(".", "")) < 2:
                continue
            counts[cand] = counts.get(cand, 0) + 1
            best_by_score[cand] = max(best_by_score.get(cand, -1.0), total)
        if counts:
            best_value = max(counts.keys(), key=lambda c: (counts[c], best_by_score.get(c, -1.0)))
            best_score = best_by_score.get(best_value, -1.0)
            best_variant = "vote"
        else:
            # As a last resort, pick the longest candidate
            cand_long = max(all_candidates, key=lambda t: len(t[0].replace(".", "")))
            best_value = cand_long[0]
            best_score = cand_long[1]
            best_variant = f"{cand_long[2]}+long"

    if best_value and "." not in best_value and len(best_value.replace(".", "")) >= 2 and dot_bins:
        dot_bins.sort(key=lambda t: t[0], reverse=True)
        _, dot_name, dot_img = dot_bins[0]
        base_cand = variant_candidate.get(dot_name, "")
        if base_cand and len(base_cand.replace(".", "")) < 2:
            dot_name = ""
        if dot_name:
            base_digits = best_value.replace(".", "")
            if base_cand and len(base_cand.replace(".", "")) != len(base_digits):
                dot_name = ""
        if not dot_name:
            pass
        else:
            with_dot = _infer_decimal_from_binary(dot_img, best_value)
            if with_dot and with_dot != best_value:
                best_value = with_dot
                best_variant = f"{best_variant}+dot({dot_name})"

    # If still weak, try a second-pass crop around the largest component
    if (not best_value) or best_score < 0.25:
        try:
            gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
            scale = 4.0
            up = cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
            _, otsu = cv2.threshold(up, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            box2 = _largest_component_crop(otsu)
            if box2:
                x2, y2, x3, y3 = box2
                # Map back to original crop scale before building variants
                x2o = max(0, int(x2 / scale))
                y2o = max(0, int(y2 / scale))
                x3o = min(gray.shape[1] - 1, int(x3 / scale))
                y3o = min(gray.shape[0] - 1, int(y3 / scale))
                if x3o > x2o and y3o > y2o:
                    crop2 = crop[y2o:y3o, x2o:x3o].copy()
                else:
                    crop2 = crop.copy()
                for name, vimg in build_variants(crop2):
                    log(f"[OCR] Trying variant: {name}_pass2")
                    variant_img[f"{name}_pass2"] = vimg
                    candidate, score = tesseract_digits(vimg)
                    if not candidate:
                        log("[OCR]   (no digits)")
                        continue
                    log(f"[OCR]   candidate='{candidate}' score={score:.2f}")
                    variant_candidate[f"{name}_pass2"] = candidate
                    bonus = 0.15 if "otsu" in name or "adaptive" in name else 0.0
                    total = score + bonus
                    if len(candidate.replace(".", "")) == 1 and total < 0.4:
                        continue
                    if total > best_score:
                        best_score = total
                        best_value = candidate
                        best_variant = f"{name}_pass2"
                        best_img = vimg
                    variant_img[f"{name}_pass2"] = vimg
                    if ("otsu" in name or "adaptive" in name) and "blackhat" not in name and vimg.ndim == 2:
                        dot_bins.append((total, f"{name}_pass2", vimg))
        except Exception as e:
            log(f"[OCR] pass2 failed: {e}")

    # Re-run earthing consensus after pass2
    if meter_type == "earthing":
        consensus = _consensus_pick(all_candidates, normalize=lambda v: _normalize_fixed_decimals(v, decimals=2))
        if consensus and _is_earthing_format(consensus):
            best_value = consensus
            best_variant = "consensus+norm2"

    if best_value and "." not in best_value and len(best_value.replace(".", "")) >= 2 and dot_bins:
        dot_bins.sort(key=lambda t: t[0], reverse=True)
        _, dot_name, dot_img = dot_bins[0]
        base_cand = variant_candidate.get(dot_name, "")
        if base_cand and len(base_cand.replace(".", "")) < 2:
            dot_name = ""
        if dot_name:
            base_digits = best_value.replace(".", "")
            if base_cand and len(base_cand.replace(".", "")) != len(base_digits):
                dot_name = ""
        if not dot_name:
            pass
        else:
            with_dot = _infer_decimal_from_binary(dot_img, best_value)
            if with_dot and with_dot != best_value:
                best_value = with_dot
                best_variant = f"{best_variant}+dot({dot_name})"

    if not best_value:
        log("[OCR]  No numeric value extracted")
        return {
            "numeric": None,
            "text": "",
            "lines": [],
            "used_crop": True,
            "crop_box": [x0, y0, x1 - x0, y1 - y0],
            "debug_urls": debug_urls,
            "debug": {"meter_detected": True, "yolo_conf": yconf, "ocr_conf": 0.0},
        }

    if meter_type == "earthing":
        # Enforce format during selection: pick most frequent normalized X.XX.
        consensus = _consensus_pick(all_candidates, normalize=lambda v: _normalize_fixed_decimals(v, decimals=2))
        if consensus and _is_earthing_format(consensus):
            best_value = consensus
            best_variant = "consensus+norm2"
        else:
            normalized = _normalize_fixed_decimals(best_value, decimals=2)
            if normalized and _is_earthing_format(normalized):
                best_value = normalized
            else:
                # Find first candidate that normalizes to valid format
                ranked = sorted(all_candidates, key=lambda t: t[1], reverse=True)
                for cand, _total, name, _ in ranked:
                    normalized = _normalize_fixed_decimals(cand, decimals=2)
                    if normalized and _is_earthing_format(normalized):
                        best_value = normalized
                        best_variant = f"{name}+norm2"
                        break

    ocr_conf = _best_conf_for_value(all_candidates, best_value) if best_value else 0.0

    if debug_id and best_img is not None:
        prep_path = DEBUG_DIR / f"prep_{debug_id}.jpg"
        try:
            cv2.imwrite(str(prep_path), best_img)
            debug_urls["prep"] = f"/static/debug/prep_{debug_id}.jpg"
        except Exception as e:
            log(f"[OCR]  Failed to save preprocess image: {e}")

    log(f"[OCR]  FINAL VALUE = {best_value} (variant={best_variant}, score={best_score:.2f})")

    return {
        "numeric": {"value": best_value, "confidence": float(ocr_conf)},
        "text": best_value,
        "lines": [{"text": best_value, "confidence": float(ocr_conf)}],
        "used_crop": True,
        "crop_box": [x0, y0, x1 - x0, y1 - y0],
        "debug_urls": debug_urls,
        "debug": {"meter_detected": True, "yolo_conf": yconf, "ocr_conf": ocr_conf, "best_variant": best_variant},
    }


def detect_fire_fighting_equipment(image_path: str) -> Dict[str, Any]:
    """
    Detect fire-point/safety equipment with model-first strategy:
    1) fire-point YOLO model
    2) fallback to Google Vision OCR keyword detection
    """
    with open(image_path, "rb") as f:
        image_bytes = f.read()
    if not image_bytes:
        raise RuntimeError("Failed to read image bytes for fire-point detection")

    model_error = ""
    try:
        model_result = _fire_point_detect_with_model(image_path)
        model_result["model_error"] = None
        model_result["gcv_error"] = None
        return model_result
    except Exception as e:
        model_error = str(e)
        log(f"[FIRE_POINT] Model detection failed, falling back to GCV: {e}")

    try:
        gcv_result = _fire_point_detect_with_gcv(image_bytes)
        gcv_result["model_error"] = model_error or None
        gcv_result["gcv_error"] = None
        return gcv_result
    except Exception as e:
        gcv_error = str(e)
        log(f"[FIRE_POINT] GCV fallback failed: {e}")
        raise RuntimeError(
            f"Fire-point detection failed (model + GCV). model_error={model_error}; gcv_error={gcv_error}"
        ) from e
