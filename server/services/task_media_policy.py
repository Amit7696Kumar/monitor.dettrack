from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
import re
from typing import Any, Dict, List, Optional

from PIL import Image


ODOMETER_ALLOWED_SOURCES = {"camera", "gallery"}
CAMERA_ONLY_SOURCES = {"camera"}
KNOWN_MEDIA_SOURCES = {"camera", "gallery", "file", "upload"}
_DATETIME_KEYWORDS = {
    "datetime",
    "datecreated",
    "date:create",
    "date:modify",
    "createdate",
    "modifydate",
    "created",
    "captured",
    "timestamp",
    "timecreated",
}
_DATETIME_PATTERNS = [
    re.compile(r"\d{4}:\d{2}:\d{2} \d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:?\d{2})?"),
    re.compile(r"\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?"),
]


@dataclass
class MediaUploadEvidence:
    field_name: str
    filename: str
    media_type: str
    media_source: str
    raw_bytes: bytes


def normalize_media_source(raw: Optional[str]) -> str:
    value = str(raw or "").strip().lower()
    return value if value in KNOWN_MEDIA_SOURCES else "unknown"


def build_task_media_policy(*, task_kind: str, allowed_types: Optional[List[str]] = None) -> Dict[str, Any]:
    media_types = [str(item).strip().lower() for item in (allowed_types or []) if str(item).strip()]
    has_camera_media = any(item in {"photo", "video"} for item in media_types)
    if task_kind == "odometer":
        return {
            "policy_name": "odometer_metadata_today",
            "camera_only": False,
            "allowed_sources": set(ODOMETER_ALLOWED_SOURCES),
            "metadata_required": True,
            "metadata_today_required": True,
            "alert_on_violation": True,
            "enforce_for_media_types": {"photo"},
            "allow_missing_metadata": False,
        }
    if has_camera_media:
        return {
            "policy_name": "camera_only",
            "camera_only": True,
            "allowed_sources": set(CAMERA_ONLY_SOURCES),
            "metadata_required": False,
            "metadata_today_required": False,
            "alert_on_violation": False,
            "enforce_for_media_types": {"photo", "video"},
            "allow_missing_metadata": True,
        }
    return {
        "policy_name": "no_media_restriction",
        "camera_only": False,
        "allowed_sources": set(),
        "metadata_required": False,
        "metadata_today_required": False,
        "alert_on_violation": False,
        "enforce_for_media_types": set(),
        "allow_missing_metadata": True,
    }


def extract_image_capture_datetime(raw_bytes: bytes) -> Optional[datetime]:
    try:
        with Image.open(BytesIO(raw_bytes)) as img:
            for value in _iter_capture_metadata_values(img):
                dt = _normalize_exif_datetime(value)
                if dt is not None:
                    return dt
    except Exception:
        pass
    for value in _extract_datetime_candidates_from_text(raw_bytes.decode("utf-8", errors="ignore")):
        dt = _normalize_exif_datetime(value)
        if dt is not None:
            return dt
    return None


def format_capture_datetime(raw: Any) -> Optional[str]:
    dt = raw if isinstance(raw, datetime) else _normalize_exif_datetime(raw)
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _iter_capture_metadata_values(img: Image.Image) -> List[Any]:
    values: List[Any] = []

    try:
        exif = img.getexif()
    except Exception:
        exif = None
    if exif:
        for tag_id in (36867, 36868, 306):
            value = exif.get(tag_id)
            if value:
                values.append(value)
        for _tag_id, value in exif.items():
            if value:
                values.append(value)

    info = getattr(img, "info", {}) or {}
    for key, value in info.items():
        key_text = str(key or "").strip().lower()
        if any(token in key_text for token in _DATETIME_KEYWORDS) or key_text in {"exif", "xmp", "xml:com.adobe.xmp"}:
            values.extend(_flatten_metadata_values(value))

    text_map = getattr(img, "text", None)
    if isinstance(text_map, dict):
        for key, value in text_map.items():
            key_text = str(key or "").strip().lower()
            if any(token in key_text for token in _DATETIME_KEYWORDS):
                values.extend(_flatten_metadata_values(value))

    try:
        xmp = img.getxmp()
    except Exception:
        xmp = None
    if xmp:
        values.extend(_collect_datetime_like_values(xmp))

    return values


def _flatten_metadata_values(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, dict):
        out: List[Any] = []
        for item in value.values():
            out.extend(_flatten_metadata_values(item))
        return out
    if isinstance(value, (list, tuple, set)):
        out: List[Any] = []
        for item in value:
            out.extend(_flatten_metadata_values(item))
        return out
    if isinstance(value, bytes):
        return [value.decode("utf-8", errors="ignore")]
    return [value]


def _collect_datetime_like_values(payload: Any, key_hint: str = "") -> List[Any]:
    values: List[Any] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            next_hint = str(key or "").strip().lower()
            if any(token in next_hint for token in _DATETIME_KEYWORDS):
                values.extend(_flatten_metadata_values(value))
            values.extend(_collect_datetime_like_values(value, next_hint))
        return values
    if isinstance(payload, (list, tuple, set)):
        for item in payload:
            values.extend(_collect_datetime_like_values(item, key_hint))
        return values
    if key_hint and any(token in key_hint for token in _DATETIME_KEYWORDS):
        values.extend(_flatten_metadata_values(payload))
    return values


def _extract_datetime_candidates_from_text(text: str) -> List[str]:
    haystack = str(text or "")
    if not haystack:
        return []
    candidates: List[str] = []
    for pattern in _DATETIME_PATTERNS:
        candidates.extend(match.group(0) for match in pattern.finditer(haystack))
    return candidates


def _normalize_exif_datetime(raw: Any) -> Optional[datetime]:
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="ignore").strip()
    else:
        text = str(raw or "").strip()
    if not text:
        return None
    text = text.replace("t", "T")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    for fmt in (
        "%Y:%m:%d %H:%M:%S",
        "%Y:%m:%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    ):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def validate_task_media_uploads(
    *,
    task_kind: str,
    allowed_types: Optional[List[str]],
    uploads: List[MediaUploadEvidence],
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    current_dt = now or datetime.now()
    policy = build_task_media_policy(task_kind=task_kind, allowed_types=allowed_types)
    enforced_types = set(policy.get("enforce_for_media_types") or set())
    metadata_by_field: Dict[str, Optional[str]] = {}
    violations: List[Dict[str, Any]] = []

    for upload in uploads:
        if upload.media_type not in enforced_types:
            continue
        source = normalize_media_source(upload.media_source)
        if policy["camera_only"] and source not in policy["allowed_sources"]:
            violations.append(
                {
                    "code": "camera_only_required",
                    "field_name": upload.field_name,
                    "media_type": upload.media_type,
                    "source": source,
                    "message": "Only live photo/video capture is allowed for this task.",
                }
            )
            continue
        if task_kind == "odometer":
            if source not in policy["allowed_sources"]:
                violations.append(
                    {
                        "code": "invalid_media_source",
                        "field_name": upload.field_name,
                        "media_type": upload.media_type,
                        "source": source,
                        "message": "Only camera capture or same-day odometer photos are allowed for this task.",
                    }
                )
                continue
            captured_at = extract_image_capture_datetime(upload.raw_bytes)
            metadata_by_field[upload.field_name] = captured_at.isoformat() if captured_at else None
            if captured_at is None:
                violations.append(
                    {
                        "code": "odometer_metadata_missing",
                        "field_name": upload.field_name,
                        "media_type": upload.media_type,
                        "source": source,
                        "message": "This odometer photo does not contain valid capture date information, so it cannot be uploaded.",
                    }
                )
                continue
            capture_date = captured_at.date()
            allowed_date = current_dt.date()
            if capture_date != allowed_date:
                violations.append(
                    {
                        "code": "odometer_metadata_date_mismatch",
                        "field_name": upload.field_name,
                        "media_type": upload.media_type,
                        "source": source,
                        "captured_at": captured_at.isoformat(),
                        "message": "Odometer photo must be captured today. Older photos are not allowed.",
                    }
                )

    accepted = not violations
    return {
        "accepted": accepted,
        "policy": policy,
        "violations": violations,
        "metadata_by_field": metadata_by_field,
        "validated_at": current_dt.isoformat(),
    }
