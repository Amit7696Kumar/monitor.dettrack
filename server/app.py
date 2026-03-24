import os
import json
import uuid
import time
import re
import hashlib
import hmac
import secrets
import threading
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Optional, Any, Dict, List

from fastapi import FastAPI, Request, UploadFile, Form, File
from fastapi.exceptions import RequestValidationError
from fastapi import HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.concurrency import run_in_threadpool
from starlette.responses import Response

import bcrypt as pybcrypt
import numpy as np
from passlib.hash import pbkdf2_sha256
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from PIL import Image

from server.db import (
    init_db,
    create_user,
    get_user_by_username,
    get_user_by_id,
    get_user_by_email,
    get_user_by_google_id,
    update_user_identity,
    update_user_username,
    update_user_password,
    set_user_force_password_change,
    update_user_team,
    insert_reading,
    fetch_readings_all,
    fetch_readings_by_team,
    fetch_readings_by_user,
    create_alert,
    fetch_alerts_for_admin,
    fetch_alerts_for_coadmin,
    mark_alert_read,
    clear_alerts_admin,
    clear_alerts_coadmin,
    count_unread_admin,
    count_unread_coadmin,
    get_latest_reading_id_all,
    get_latest_reading_id_team,
    create_message,
    fetch_messages_for_user,
    mark_message_read,
    fetch_users_all,
    fetch_users_by_team,
    fetch_users_without_team,
    chat_list_users_for_picker,
    chat_get_or_create_direct,
    chat_create_group,
    chat_list_conversations,
    chat_fetch_messages,
    chat_fetch_reactions,
    chat_create_message,
    chat_get_message,
    chat_mark_read,
    chat_toggle_reaction,
    chat_edit_message,
    chat_soft_delete_message,
    chat_is_member,
    chat_fetch_members,
    chat_set_member_flags,
    chat_add_block,
    chat_create_report,
    chat_is_blocked_between,
    task_create_form,
    task_get_form,
    task_update_form,
    task_delete_form,
    task_list_forms_for_actor,
    task_create_instance,
    task_get_instance,
    task_list_instances_for_form,
    task_update_instance_assignment,
    task_delete_instance,
    task_list_instances_for_user,
    task_list_instances_for_scope,
    task_mark_instance_status,
    task_get_submission,
    task_get_question,
    task_upsert_submission,
    task_list_due_for_overdue,
    task_mark_overdue_sent,
    task_list_repeat_forms,
    task_get_latest_instance_for_user,
    task_create_notification,
    task_log_activity,
    task_upsert_ai_result,
)

#  use warmup + run_ocr from ocr_engine
from server.ocr_engine import run_ocr, warmup_models
from server.object_detection import detect_task_objects
from server.openai_ai import analyze_task_image_with_openai, openai_available
from server.logging_utils import (
    setup_logging,
    get_logger,
    log_event,
    set_request_context,
    clear_request_context,
)
from server.services.dashboard_service import (
    build_admin_dashboard_context,
    build_coadmin_dashboard_context,
)
from server.services.readings_view import (
    augment_readings_for_view,
    filter_dashboard_readings_scope,
)


def _load_local_env_files() -> None:
    # Load local env files without overriding already-exported values.
    base_dir = Path(__file__).resolve().parent
    candidates = [base_dir.parent / ".env", base_dir / ".env"]
    for env_path in candidates:
        if not env_path.exists():
            continue
        try:
            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key or key in os.environ:
                    continue
                val = value.strip().strip('"').strip("'")
                os.environ[key] = val
        except Exception:
            # Environment file parsing should never block app startup.
            continue


def _normalize_image_taken_at(raw: Any) -> Optional[str]:
    text = str(raw or "").strip()
    if not text:
        return None
    for fmt in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y:%m:%d %H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
    return text


def _extract_image_taken_at(image_path: str) -> Optional[str]:
    try:
        with Image.open(image_path) as img:
            exif = img.getexif()
            if not exif:
                return None
            for tag_id in (36867, 36868, 306):
                normalized = _normalize_image_taken_at(exif.get(tag_id))
                if normalized:
                    return normalized
    except Exception:
        return None
    return None


_load_local_env_files()

setup_logging()
system_logger = get_logger("system")
api_logger = get_logger("api")
auth_logger = get_logger("auth")
security_logger = get_logger("security")
admin_logger = get_logger("admin")
jobs_logger = get_logger("jobs")
audit_logger = get_logger("audit")
chat_logger = get_logger("chat")
error_logger = get_logger("errors")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
TASK_RULES_PATH = os.path.join(BASE_DIR, "task_processing_rules.json")


@lru_cache(maxsize=1)
def _load_task_processing_rules() -> List[Dict[str, Any]]:
    try:
        with open(TASK_RULES_PATH, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception:
        return []
    rules = payload.get("rules") if isinstance(payload, dict) else payload
    if not isinstance(rules, list):
        return []
    normalized: List[Dict[str, Any]] = []
    for idx, raw in enumerate(rules):
        if isinstance(raw, dict):
            item = dict(raw)
            item.setdefault("id", f"rule_{idx + 1}")
            item.setdefault("priority", idx + 1)
            normalized.append(item)
    normalized.sort(key=lambda item: int(item.get("priority") or 0))
    return normalized


def _task_rule_matches(task_text: str, rule: Dict[str, Any]) -> bool:
    haystack = f" {task_text.lower()} "
    match_all = [str(token).strip().lower() for token in (rule.get("match_all") or []) if str(token).strip()]
    match_any = [str(token).strip().lower() for token in (rule.get("match_any") or []) if str(token).strip()]
    excludes = [str(token).strip().lower() for token in (rule.get("exclude_any") or []) if str(token).strip()]
    if excludes and any(token in haystack for token in excludes):
        return False
    if match_all and not all(token in haystack for token in match_all):
        return False
    if match_any and not any(token in haystack for token in match_any):
        return False
    return bool(match_all or match_any)


def _task_find_processing_rule(task_text: str) -> Optional[Dict[str, Any]]:
    for rule in _load_task_processing_rules():
        if _task_rule_matches(task_text, rule):
            return rule
    return None
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")
PREVIEW_DIR = os.path.join(UPLOAD_DIR, "_previews")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(PREVIEW_DIR, exist_ok=True)

IDEAL_PATH = os.path.join(BASE_DIR, "ideal_values.json")

DEBUG_DIR = Path(os.path.join(BASE_DIR, "static", "debug"))
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="DET MONITORING APPLICATION")


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _session_cookie_samesite() -> str:
    raw = (os.getenv("SESSION_COOKIE_SAMESITE", "lax") or "").strip().lower()
    if raw in {"lax", "strict", "none"}:
        return raw
    return "lax"


SESSION_SECRET_KEY = (os.getenv("SESSION_SECRET_KEY", "") or "").strip() or "CHANGE_ME_TO_A_RANDOM_LONG_SECRET"
SESSION_MAX_AGE = max(300, _env_int("SESSION_MAX_AGE", 60 * 60 * 24 * 7))
SESSION_COOKIE_HTTPS_ONLY = _env_bool("SESSION_COOKIE_HTTPS_ONLY", default=False)
SESSION_COOKIE_SAMESITE = _session_cookie_samesite()
PASSWORD_RESET_TOKEN_TTL_SECONDS = max(300, _env_int("PASSWORD_RESET_TOKEN_TTL_SECONDS", 30 * 60))

# Session cookies
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET_KEY,
    max_age=SESSION_MAX_AGE,
    same_site=SESSION_COOKIE_SAMESITE,
    https_only=SESSION_COOKIE_HTTPS_ONLY,
)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

templates = Jinja2Templates(directory=TEMPLATE_DIR)


def _current_static_version() -> str:
    static_root = Path(STATIC_DIR)
    paths = [
        static_root / "app.css",
        static_root / "app.js",
        static_root / "sw.js",
    ]
    latest_mtime = 0
    for path in paths:
        try:
            latest_mtime = max(latest_mtime, int(path.stat().st_mtime))
        except OSError:
            continue
    return str(latest_mtime or int(time.time()))


templates.env.globals["static_version"] = _current_static_version()

_RATE_LIMIT_STATE: Dict[str, List[float]] = {}
_RATE_LIMIT_LOCK = threading.Lock()
_CSRF_EXEMPT_PATHS = {
    "/auth/google/callback",
}
_UPLOAD_MAX_BYTES = max(1024 * 1024, _env_int("TASK_MAX_FILE_MB", 30) * 1024 * 1024)


def _client_ip(request: Request) -> str:
    forwarded = (request.headers.get("x-forwarded-for", "") or "").split(",")[0].strip()
    if forwarded:
        return forwarded
    return request.client.host if request.client else "unknown"


def _issue_csrf_token(request: Request) -> str:
    try:
        token = request.session.get("csrf_token")
    except AssertionError:
        return ""
    if token:
        return str(token)
    token = secrets.token_urlsafe(32)
    try:
        request.session["csrf_token"] = token
    except AssertionError:
        return ""
    return token


def _csrf_token(request: Request) -> str:
    return _issue_csrf_token(request)


templates.env.globals["csrf_token"] = _csrf_token


async def _require_csrf(request: Request) -> Optional[Response]:
    if request.method.upper() not in {"POST", "PUT", "PATCH", "DELETE"}:
        return None
    if request.url.path in _CSRF_EXEMPT_PATHS:
        return None
    try:
        session_token = str(request.session.get("csrf_token") or "")
    except AssertionError:
        return None
    if not session_token:
        _issue_csrf_token(request)
        return JSONResponse({"error": "csrf_missing"}, status_code=403)
    header_token = (request.headers.get("x-csrf-token", "") or "").strip()
    form_token = ""
    ctype = (request.headers.get("content-type", "") or "").lower()
    if "application/x-www-form-urlencoded" in ctype or "multipart/form-data" in ctype:
        try:
            form = await request.form()
            try:
                form_token = str(form.get("csrf_token") or "").strip()
            except Exception:
                form_token = ""
        except Exception:
            form_token = ""
    token = header_token or form_token
    if token and hmac.compare_digest(token, session_token):
        return None
    log_event(
        security_logger,
        "WARNING",
        "SECURITY_CSRF_REJECTED",
        "Request rejected due to invalid CSRF token",
        path=request.url.path,
        ip=_client_ip(request),
    )
    return JSONResponse({"error": "csrf_invalid"}, status_code=403)


def _check_rate_limit(bucket: str, key: str, *, limit: int, window_seconds: int) -> Optional[int]:
    now = time.time()
    state_key = f"{bucket}:{key}"
    with _RATE_LIMIT_LOCK:
        entries = [ts for ts in _RATE_LIMIT_STATE.get(state_key, []) if (now - ts) < window_seconds]
        if len(entries) >= limit:
            retry_after = max(1, int(window_seconds - (now - entries[0])))
            _RATE_LIMIT_STATE[state_key] = entries
            return retry_after
        entries.append(now)
        _RATE_LIMIT_STATE[state_key] = entries
    return None


def _auth_rate_limit(request: Request, bucket: str, subject: str = "", *, limit: int = 8, window_seconds: int = 300) -> Optional[int]:
    ip = _client_ip(request)
    base = f"{ip}|{subject.strip().lower()}"
    return _check_rate_limit(bucket, base, limit=limit, window_seconds=window_seconds)


def _clean_rel_upload_path(raw: str) -> Optional[str]:
    rel = (raw or "").strip().lstrip("/")
    if not rel or rel.startswith("_previews/"):
        return None
    return rel


def _resolve_upload_relative_path(rel: str) -> Optional[Path]:
    cleaned = _clean_rel_upload_path(rel)
    if not cleaned:
        return None
    candidate = (Path(UPLOAD_DIR) / cleaned).resolve()
    uploads_root = Path(UPLOAD_DIR).resolve()
    try:
        candidate.relative_to(uploads_root)
    except ValueError:
        return None
    return candidate if candidate.is_file() else None


def _image_bytes_look_valid(raw: bytes) -> bool:
    if not raw:
        return False
    try:
        arr = np.frombuffer(raw, dtype=np.uint8)
        try:
            import cv2
        except Exception:
            return False
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        return img is not None and getattr(img, "size", 0) > 0
    except Exception:
        return False


def _video_bytes_look_valid(raw: bytes, ext: str) -> bool:
    head = raw[:64]
    ext = (ext or "").lower()
    if ext == ".webm":
        return head.startswith(b"\x1a\x45\xdf\xa3")
    if ext in {".mp4", ".mov"}:
        return len(head) >= 12 and head[4:8] == b"ftyp"
    if ext == ".avi":
        return len(head) >= 12 and head[:4] == b"RIFF" and head[8:12] == b"AVI "
    if ext == ".mkv":
        return head.startswith(b"\x1a\x45\xdf\xa3")
    return False


def _validate_upload_payload(raw: bytes, file_type: str, ext: str) -> Optional[str]:
    if not raw:
        return "Uploaded file is empty."
    if len(raw) > _UPLOAD_MAX_BYTES and file_type != "video":
        return "File exceeds the allowed size."
    if file_type == "pdf":
        if not raw.startswith(b"%PDF-"):
            return "Uploaded PDF content is invalid."
        return None
    if file_type == "photo":
        if ext not in {".jpg", ".jpeg", ".png", ".webp"}:
            return "Unsupported image format."
        if not _image_bytes_look_valid(raw):
            return "Uploaded image content is invalid or unreadable."
        return None
    if file_type == "video":
        if not _video_bytes_look_valid(raw, ext):
            return "Uploaded video content does not match the selected format."
        return None
    return "Unsupported file type."


def _upload_error_redirect(path: str, message: str) -> RedirectResponse:
    return RedirectResponse(f"{path}{'&' if '?' in path else '?'}err={urllib.parse.quote_plus(message)}", status_code=303)


def _resolve_upload_url_path(src: str) -> Optional[Path]:
    raw = (src or "").strip()
    if not raw:
        return None
    parsed = urllib.parse.urlparse(raw)
    path = urllib.parse.unquote(parsed.path or "")
    if not path.startswith("/uploads/"):
        return None
    rel = path[len("/uploads/"):].lstrip("/")
    if not rel or rel.startswith("_previews/"):
        return None
    candidate = (Path(UPLOAD_DIR) / rel).resolve()
    uploads_root = Path(UPLOAD_DIR).resolve()
    try:
        candidate.relative_to(uploads_root)
    except ValueError:
        return None
    return candidate if candidate.is_file() else None


def _build_preview_cache_path(source_path: Path) -> Path:
    rel = source_path.resolve().relative_to(Path(UPLOAD_DIR).resolve())
    digest = hashlib.sha1(str(rel).encode("utf-8")).hexdigest()[:12]
    preview_name = f"{source_path.stem}.{digest}.jpg"
    return (Path(PREVIEW_DIR) / rel.parent / preview_name).resolve()


def _generate_cached_preview(source_path: Path) -> Optional[Path]:
    if source_path.suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp"}:
        return None

    preview_path = _build_preview_cache_path(source_path)
    preview_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if preview_path.exists() and preview_path.stat().st_mtime >= source_path.stat().st_mtime:
            return preview_path
    except OSError:
        pass

    try:
        import cv2  # Lazy import to avoid startup-time dependency failures.
    except Exception:
        return None

    image = cv2.imread(str(source_path))
    if image is None:
        return None

    height, width = image.shape[:2]
    max_dim = 1400
    largest_side = max(height, width)
    if largest_side > max_dim:
        scale = max_dim / float(largest_side)
        image = cv2.resize(
            image,
            (max(1, int(width * scale)), max(1, int(height * scale))),
            interpolation=cv2.INTER_AREA,
        )

    ok, encoded = cv2.imencode(".jpg", image, [int(cv2.IMWRITE_JPEG_QUALITY), 72])
    if not ok:
        return None

    tmp_path = preview_path.with_suffix(".tmp")
    tmp_path.write_bytes(encoded.tobytes())
    os.replace(tmp_path, preview_path)
    return preview_path


def _warm_cached_preview(path_like: Optional[str]) -> None:
    if not path_like:
        return
    source_path = Path(path_like)
    if source_path.suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp"}:
        return

    def _runner() -> None:
        try:
            _generate_cached_preview(source_path)
        except Exception:
            return

    threading.Thread(target=_runner, daemon=True).start()


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    templates.env.globals["static_version"] = _current_static_version()
    path = request.url.path
    session = request.scope.get("session") or {}
    uid = session.get("uid")
    if uid:
        user = get_user_by_id(int(uid))
        if _must_rotate_password(user):
            allowed_paths = {"/force-password-change", "/logout", "/login"}
            if not (path in allowed_paths or path.startswith("/static/")):
                return RedirectResponse("/force-password-change", status_code=303)
    request_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex
    method = request.method
    route = path
    ip = request.client.host if request.client else None
    started = time.perf_counter()
    user_agent = request.headers.get("user-agent", "")

    set_request_context(
        {
            "request_id": request_id,
            "method": method,
            "route": route,
            "ip": ip,
        }
    )
    log_event(
        api_logger,
        "INFO",
        "API_REQUEST_START",
        "Request started",
        user_agent=user_agent,
    )

    response: Optional[Response] = None
    try:
        response = await call_next(request)
        return response
    except Exception:
        duration_ms = round((time.perf_counter() - started) * 1000.0, 2)
        log_event(
            error_logger,
            "ERROR",
            "API_REQUEST_FAIL",
            "Request failed with unhandled exception",
            exc_info=True,
            duration_ms=duration_ms,
        )
        raise
    finally:
        status_code = response.status_code if response is not None else 500
        duration_ms = round((time.perf_counter() - started) * 1000.0, 2)
        user_id = None
        try:
            user_id = request.session.get("uid")
        except Exception:
            user_id = None
        set_request_context(
            {
                "request_id": request_id,
                "method": method,
                "route": route,
                "ip": ip,
                "user_id": user_id,
                "duration_ms": duration_ms,
            }
        )
        log_event(
            api_logger,
            "INFO",
            "API_REQUEST_END",
            "Request completed",
            status_code=status_code,
            duration_ms=duration_ms,
        )
        if response is not None:
            response.headers["X-Request-Id"] = request_id
        clear_request_context()


@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault(
        "Permissions-Policy",
        "camera=(), microphone=(), geolocation=()",
    )

    forwarded_proto = (request.headers.get("x-forwarded-proto", "") or "").split(",")[0].strip().lower()
    is_https = request.url.scheme == "https" or forwarded_proto == "https"
    if is_https:
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")

    if request.url.path in {"/login", "/force-password-change"}:
        response.headers.setdefault("Cache-Control", "no-store")
        response.headers.setdefault("Pragma", "no-cache")

    return response


@app.middleware("http")
async def csrf_protection_middleware(request: Request, call_next):
    if "session" in request.scope:
        _issue_csrf_token(request)
    rejection = await _require_csrf(request)
    if rejection is not None:
        return rejection
    return await call_next(request)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    log_event(
        error_logger,
        "WARN",
        "API_VALIDATION_ERROR",
        "Request validation failed",
        errors=exc.errors(),
        body=exc.body if isinstance(exc.body, dict) else None,
    )
    return JSONResponse(
        status_code=422,
        content={"error": "validation_error", "detail": exc.errors()},
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    level = "WARN" if int(exc.status_code) < 500 else "ERROR"
    log_event(
        error_logger,
        level,
        "API_HTTP_EXCEPTION",
        "HTTP exception raised",
        status_code=exc.status_code,
        detail=str(exc.detail),
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": "http_error", "detail": exc.detail},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log_event(
        error_logger,
        "ERROR",
        "UNHANDLED_EXCEPTION",
        "Unhandled server exception",
        exc_info=True,
        error_type=exc.__class__.__name__,
    )
    return JSONResponse(
        status_code=500,
        content={"error": "internal_error", "detail": "Internal server error"},
    )


# -----------------------
# Ideals + helper utils
# -----------------------
def load_ideals():
    if not os.path.exists(IDEAL_PATH):
        log_event(system_logger, "WARN", "IDEALS_FILE_MISSING", "Ideals file missing, using no rules", path=IDEAL_PATH)
        return {}

    try:
        with open(IDEAL_PATH, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                log_event(system_logger, "WARN", "IDEALS_FILE_EMPTY", "Ideals file empty, using no rules", path=IDEAL_PATH)
                return {}
            data = json.loads(raw)
            if not isinstance(data, dict):
                log_event(system_logger, "WARN", "IDEALS_FILE_INVALID", "Ideals file invalid type, using no rules", path=IDEAL_PATH)
                return {}
            return data
    except Exception as e:
        log_event(system_logger, "ERROR", "IDEALS_READ_FAIL", "Failed to parse ideals file", path=IDEAL_PATH, error=str(e))
        return {}


def current_user(request: Request):
    uid = request.session.get("uid")
    if not uid:
        return None
    return get_user_by_id(int(uid))


def require_role(user, roles):
    return user and user.get("role") in roles


def _user_team_int(user: Optional[Dict[str, Any]]) -> Optional[int]:
    if not user:
        return None
    team = user.get("team")
    if team is None:
        return None
    try:
        return int(team)
    except Exception:
        return None


def _is_effectively_unassigned_user(user: Optional[Dict[str, Any]]) -> bool:
    if not user or user.get("role") != "user":
        return False
    team = user.get("team")
    if team is None or str(team).strip() in {"", "0"}:
        return True
    # Legacy self-registered accounts were earlier stored with team=1.
    team_txt = str(team).strip()
    username = str(user.get("username") or "")
    email = str(user.get("email") or "")
    auth_provider = str(user.get("auth_provider") or "")
    if team_txt == "1" and ("@" in username or "@" in email) and auth_provider in {"password", "google"}:
        return True
    return False


def _auth_page(request: Request, error: Optional[str] = None, info: Optional[str] = None):
    _issue_csrf_token(request)
    google_enabled = bool((os.getenv("GOOGLE_CLIENT_ID", "") or "").strip() and (os.getenv("GOOGLE_CLIENT_SECRET", "") or "").strip())
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": error or request.query_params.get("error"),
            "info": info or request.query_params.get("info"),
            "google_enabled": google_enabled,
        },
    )


def _must_rotate_password(user: Optional[Dict[str, Any]]) -> bool:
    if not user:
        return False
    return user.get("role") in {"admin", "coadmin"} and bool(int(user.get("force_password_change") or 0))


def _password_change_page(request: Request, user: Dict[str, Any], error: Optional[str] = None, info: Optional[str] = None):
    _issue_csrf_token(request)
    return templates.TemplateResponse(
        "force_password_change.html",
        {
            "request": request,
            "user": user,
            "error": error or request.query_params.get("error"),
            "info": info or request.query_params.get("info"),
        },
    )


def _password_reset_page(request: Request, token: str, error: Optional[str] = None, info: Optional[str] = None):
    _issue_csrf_token(request)
    return templates.TemplateResponse(
        "reset_password.html",
        {
            "request": request,
            "token": token,
            "error": error or request.query_params.get("error"),
            "info": info or request.query_params.get("info"),
        },
    )


def _password_reset_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(SESSION_SECRET_KEY, salt="password-reset")


def _password_reset_stamp(user: Dict[str, Any]) -> str:
    raw = str(user.get("password_hash") or "")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _create_password_reset_token(user: Dict[str, Any]) -> str:
    payload = {
        "uid": int(user["id"]),
        "stamp": _password_reset_stamp(user),
    }
    return _password_reset_serializer().dumps(payload)


def _validate_password_reset_token(token: str) -> Optional[Dict[str, Any]]:
    raw = (token or "").strip()
    if not raw:
        return None
    try:
        data = _password_reset_serializer().loads(raw, max_age=PASSWORD_RESET_TOKEN_TTL_SECONDS)
    except (BadSignature, SignatureExpired):
        return None
    try:
        user_id = int(data.get("uid"))
    except Exception:
        return None
    user = get_user_by_id(user_id)
    if not user:
        return None
    if data.get("stamp") != _password_reset_stamp(user):
        return None
    return user


def _find_user_for_password_reset(identifier: str) -> Optional[Dict[str, Any]]:
    ident = (identifier or "").strip()
    if not ident:
        return None
    user = get_user_by_email(ident)
    if user:
        return user
    return get_user_by_username(ident)


def _google_redirect_uri(request: Request) -> str:
    env_uri = (os.getenv("GOOGLE_REDIRECT_URI", "") or "").strip()
    if env_uri:
        return env_uri
    base = str(request.base_url).rstrip("/")
    return f"{base}/auth/google/callback"


def compare_against_ideal(meter_type: str, value_str: Optional[str]):
    ideals = load_ideals()
    rule = ideals.get(meter_type)
    if not rule:
        return (False, None, None)

    try:
        v = float(value_str)
    except Exception:
        return (False, None, None)

    mn = rule.get("min")
    mx = rule.get("max")

    if mn is not None and v < float(mn):
        return (True, "low", f"{meter_type.upper()} is LOW: {v} (min ideal {mn})")
    if mx is not None and v > float(mx):
        return (True, "high", f"{meter_type.upper()} is HIGH: {v} (max ideal {mx})")

    return (False, None, None)


TASK_ALLOWED_TYPES = {"pdf", "photo", "video"}
TASK_REPEAT_TYPES = {"daily", "weekly", "monthly", "interval"}


def _task_parse_deadline(date_str: str, time_str: str) -> Optional[datetime]:
    try:
        return datetime.strptime(f"{(date_str or '').strip()} {(time_str or '').strip()}", "%Y-%m-%d %H:%M")
    except Exception:
        return None


def _task_cycle_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M")


def _task_next_deadline(current_deadline: datetime, repeat_type: str, interval_days: Optional[int]) -> datetime:
    rt = (repeat_type or "").strip().lower()
    if rt == "daily":
        return current_deadline + timedelta(days=1)
    if rt == "weekly":
        return current_deadline + timedelta(days=7)
    if rt == "monthly":
        return current_deadline + timedelta(days=30)
    days = int(interval_days or 1)
    return current_deadline + timedelta(days=max(1, days))


def _task_allowed_ext(file_type: str) -> set:
    if file_type == "pdf":
        return {".pdf"}
    if file_type == "photo":
        return {".jpg", ".jpeg", ".png", ".webp"}
    if file_type == "video":
        return {".mp4", ".mov", ".avi", ".mkv", ".webm"}
    return set()


def _task_detect_file_type(filename: str) -> Optional[str]:
    ext = os.path.splitext((filename or "").lower())[1]
    if ext in {".pdf"}:
        return "pdf"
    if ext in {".jpg", ".jpeg", ".png", ".webp"}:
        return "photo"
    if ext in {".mp4", ".mov", ".avi", ".mkv", ".webm"}:
        return "video"
    return None


def _task_parse_json_list(raw: Optional[str]) -> List[Any]:
    try:
        obj = json.loads(raw or "[]")
        return obj if isinstance(obj, list) else []
    except Exception:
        return []


async def _task_process_ai(
    instance: Dict[str, Any],
    upload_full_path: str,
    file_type: str,
    ai_engine_type: str = "auto",
) -> Dict[str, Any]:
    engine = (ai_engine_type or "auto").strip().lower()
    if file_type == "photo":
        try:
            question = None
            form_id = instance.get("form_id")
            try:
                if form_id is not None:
                    question = task_get_question(int(form_id))
            except Exception:
                question = None
            spec = _task_build_spec(instance, question)
            mode_hint = str(spec.get("mode_hint") or _task_ai_mode_hint(instance))
            meter_hint = "earthing" if spec.get("task_kind") == "earthing" else None
            is_earthing_task = str(spec.get("task_kind") or "") == "earthing"
            allow_local_ocr_fallback = bool(spec.get("allow_local_ocr_fallback"))
            openai_payload = None
            extracted_text = ""
            value = None
            validation_status = "failed"
            validation_reason = ""
            engine_used = "openai"
            ocr = {}
            local_confidence = 0.0
            local_missing_required_object = False

            if is_earthing_task:
                debug_id = uuid.uuid4().hex
                ocr = await run_in_threadpool(run_ocr, upload_full_path, debug_id, meter_hint)
                extracted_text = str(ocr.get("text") or "").strip()
                ocr_debug = (ocr.get("debug") or {}) if isinstance(ocr, dict) else {}
                meter_detected = bool(ocr_debug.get("meter_detected"))
                num = (ocr.get("numeric") or {}) if isinstance(ocr, dict) else {}
                try:
                    local_confidence = float(num.get("confidence") or 0.0)
                except Exception:
                    local_confidence = 0.0

                if not meter_detected:
                    local_missing_required_object = True
                    validation_status = "missing_required_object"
                    validation_reason = "No meter detected in image. Expected earthing meter display."
                    engine_used = "local_meter_guard"

                if isinstance(num, dict) and num.get("value") not in {None, ""}:
                    ocr_validation = _task_validate_ai_result(
                        spec,
                        {
                            "relevant": True,
                            "readable": True,
                            "value": num.get("value"),
                            "present": True,
                            "confidence": num.get("confidence") or 0.0,
                            "summary": extracted_text,
                            "evidence": extracted_text,
                        },
                    )
                    validation_status = str(ocr_validation.get("status") or "failed")
                    validation_reason = str(ocr_validation.get("reason") or "")
                    if ocr_validation.get("accepted"):
                        value = ocr_validation.get("value")
                        engine_used = "local_meter_ocr"
                        return {
                            "status": "completed",
                            "summary": f"AI ({engine_used}) completed; value={value}",
                            "extracted_text": extracted_text,
                            "extracted_values": {"value": value},
                            "engine_used": engine_used,
                            "local_result": ocr,
                            "openai_result": None,
                            "task_spec": spec,
                            "validation_status": validation_status,
                            "validation_reason": validation_reason,
                            "confidence": local_confidence,
                        }

                if meter_detected:
                    validation_status = "unreadable"
                    validation_reason = "Earthing meter detected but the display value could not be read clearly."

            if openai_available():
                try:
                    openai_payload = await run_in_threadpool(
                        analyze_task_image_with_openai,
                        image_path=upload_full_path,
                        task_title=str(instance.get("title") or ""),
                        task_description=str(instance.get("description") or ""),
                        local_result=ocr if is_earthing_task else {},
                        task_spec=spec,
                        mode_hint=mode_hint,
                    )
                    validation = _task_validate_ai_result(spec, openai_payload)
                    validation_status = str(validation.get("status") or "failed")
                    validation_reason = str(validation.get("reason") or "")
                    if validation.get("accepted"):
                        value = validation.get("value")
                    extracted_text = str(openai_payload.get("summary") or "").strip()
                except Exception as e:
                    openai_payload = {"error": str(e), "engine_used": "openai"}

            if is_earthing_task and value in {None, ""} and local_missing_required_object and not openai_payload:
                return {
                    "status": "completed",
                    "summary": "AI (local_meter_guard) completed; no meter detected",
                    "extracted_text": extracted_text,
                    "extracted_values": {},
                    "engine_used": "local_meter_guard",
                    "local_result": ocr,
                    "openai_result": None,
                    "task_spec": spec,
                    "validation_status": validation_status,
                    "validation_reason": validation_reason,
                    "confidence": 0.0,
                }

            openai_relevant = (openai_payload or {}).get("relevant")
            openai_confidence = float((openai_payload or {}).get("confidence") or 0.0)
            should_skip_local_fallback = bool(
                openai_payload
                and allow_local_ocr_fallback is False
                and (
                    openai_relevant is False
                    or (
                        value in {None, ""}
                        and openai_confidence <= 0.35
                    )
                )
            )

            if value in {None, ""} and not should_skip_local_fallback and allow_local_ocr_fallback:
                debug_id = uuid.uuid4().hex
                ocr = await run_in_threadpool(run_ocr, upload_full_path, debug_id, meter_hint)
                engine_used = "local_fallback"
                if isinstance(ocr, dict):
                    num = (ocr.get("numeric") or {})
                    if isinstance(num, dict):
                        ocr_validation = _task_validate_ai_result(
                            spec,
                            {
                                "relevant": True,
                                "readable": bool(num.get("value")),
                                "value": num.get("value"),
                                "present": None,
                                "confidence": num.get("confidence") or 0.0,
                                "summary": ocr.get("text") or "",
                                "evidence": ocr.get("text") or "",
                            },
                        )
                        validation_status = str(ocr_validation.get("status") or validation_status)
                        validation_reason = str(ocr_validation.get("reason") or validation_reason)
                        if ocr_validation.get("accepted"):
                            value = ocr_validation.get("value")
                        try:
                            local_confidence = float(num.get("confidence") or 0.0)
                        except Exception:
                            local_confidence = 0.0
                    extracted_text = extracted_text or (ocr.get("text") or "").strip()

            if should_skip_local_fallback and value in {None, ""}:
                engine_used = "openai"

            summary = f"AI ({engine_used}) completed; value={value}" if value else f"AI ({engine_used}) completed; no value extracted"
            return {
                "status": "completed",
                "summary": summary,
                "extracted_text": extracted_text,
                "extracted_values": {"value": value} if value is not None else {},
                "engine_used": engine_used,
                "local_result": ocr,
                "openai_result": openai_payload,
                "task_spec": spec,
                "validation_status": validation_status,
                "validation_reason": validation_reason,
                "confidence": max(openai_confidence, local_confidence),
            }
        except Exception as e:
            return {"status": "failed", "summary": f"AI ({engine}) failed: {e}", "engine_used": engine}
    if file_type == "video":
        return {
            "status": "queued",
            "summary": f"Video AI ({engine}) queued for later processing.",
            "extracted_text": "",
            "extracted_values": {},
            "engine_used": engine,
        }
    return {"status": "not_requested", "summary": "No AI processing for this file type.", "engine_used": engine}


def _task_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _task_ai_mode_hint(instance: Dict[str, Any]) -> str:
    task_text = f"{instance.get('title') or ''} {instance.get('description') or ''}".strip().lower()
    qtype = (instance.get("question_type") or "upload").strip().lower()
    if qtype in {"number", "upload_number"}:
        return "numeric"
    if any(token in task_text for token in ["fire point", "firepoint", "fire fighting", "firefighting", "extinguisher", "hydrant", "fire bucket"]):
        return "present_absent"
    if (
        ("socket" in task_text and "plug" in task_text)
        or "correct" in task_text
        or "incorrect" in task_text
        or "properly plugged" in task_text
        or "proper connection" in task_text
    ):
        return "correct_incorrect"
    return "auto"


def _task_parse_json_object(raw: Optional[str]) -> Dict[str, Any]:
    try:
        obj = json.loads(raw or "{}")
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _task_text_blob(instance: Dict[str, Any], question: Optional[Dict[str, Any]] = None) -> str:
    return " ".join(
        [
            str(instance.get("title") or "").strip(),
            str(instance.get("description") or "").strip(),
            str(instance.get("extraction_hints") or "").strip(),
            str((question or {}).get("question_text") or "").strip(),
            str((question or {}).get("parsing_instructions") or "").strip(),
        ]
    ).strip()


def _task_build_spec(instance: Dict[str, Any], question: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    title = str(instance.get("title") or "").strip()
    description = str(instance.get("description") or "").strip()
    extraction_hints = str(instance.get("extraction_hints") or "").strip()
    question_text = str((question or {}).get("question_text") or "").strip()
    parsing_instructions = str((question or {}).get("parsing_instructions") or "").strip()
    threshold_rules = _task_parse_json_object((question or {}).get("threshold_rules_json") or instance.get("threshold_rules_json"))
    task_text = _task_text_blob(instance, question).lower()
    mode_hint = _task_ai_mode_hint(instance)
    qtype = (instance.get("question_type") or "upload").strip().lower()
    unit = str((question or {}).get("unit") or instance.get("number_unit") or "").strip() or None
    min_v = _task_float((question or {}).get("ideal_min"))
    max_v = _task_float((question or {}).get("ideal_max"))
    if min_v is None:
        min_v = _task_float(instance.get("number_min"))
    if max_v is None:
        max_v = _task_float(instance.get("number_max"))

    expected_object = title or question_text or "task evidence"
    task_kind = "custom_query"
    expected_output_type = "text"
    allowed_labels: List[str] = []
    allow_local_ocr_fallback = False
    require_object_presence_for_value = False
    alert_when_required_object_missing = False
    requires_ai = False
    response_rule = _task_find_processing_rule(task_text)

    if response_rule:
        task_kind = str(response_rule.get("task_kind") or task_kind)
        mode_hint = str(response_rule.get("mode_hint") or mode_hint)
        expected_output_type = str(response_rule.get("expected_output_type") or expected_output_type)
        expected_object = str(response_rule.get("expected_object") or expected_object)
        allowed_labels = [str(label).strip() for label in (response_rule.get("allowed_labels") or []) if str(label).strip()]
        allow_local_ocr_fallback = bool(response_rule.get("allow_local_ocr_fallback"))
        require_object_presence_for_value = bool(response_rule.get("require_object_presence_for_value"))
        alert_when_required_object_missing = bool(response_rule.get("alert_when_required_object_missing"))
        requires_ai = bool(response_rule.get("requires_ai"))
        unit = str(response_rule.get("unit") or unit or "").strip() or None

    if response_rule:
        pass
    else:
        task_kind = "unsupported_notice"
        expected_output_type = "text"
        requires_ai = False

    if task_kind == "earthing":
        allow_local_ocr_fallback = False
        require_object_presence_for_value = True
        alert_when_required_object_missing = True

    return {
        "rule_id": str((response_rule or {}).get("id") or ""),
        "task_kind": task_kind,
        "mode_hint": mode_hint,
        "expected_output_type": expected_output_type,
        "expected_object": expected_object,
        "allowed_labels": allowed_labels,
        "unit": unit,
        "min_value": min_v,
        "max_value": max_v,
        "return_null_if_missing": True,
        "strict_relevance": True,
        "allow_local_ocr_fallback": allow_local_ocr_fallback,
        "require_object_presence_for_value": require_object_presence_for_value,
        "alert_when_required_object_missing": alert_when_required_object_missing,
        "requires_ai": requires_ai,
        "inspection_points": list((response_rule or {}).get("inspection_points") or []),
        "return_label_when": dict((response_rule or {}).get("return_label_when") or {}),
        "alert_threshold": dict((response_rule or {}).get("alert_threshold") or {}),
        "alert_on_values": list((response_rule or {}).get("alert_on_values") or []),
        "alert_message": str((response_rule or {}).get("alert_message") or "").strip() or None,
        "unsupported_message": str((response_rule or {}).get("unsupported_message") or "").strip() or ("currently not supported" if not response_rule else None),
        "use_image_timestamp_as_value": bool((response_rule or {}).get("use_image_timestamp_as_value")),
        "missing_value_message": str((response_rule or {}).get("missing_value_message") or "").strip() or None,
        "question_text": question_text or None,
        "extraction_hints": extraction_hints or None,
        "parsing_instructions": parsing_instructions or None,
        "threshold_rules": threshold_rules,
        "custom_query": " ".join([x for x in [title, description, question_text, extraction_hints] if x]).strip() or title or "task",
    }


def _task_validate_ai_result(spec: Dict[str, Any], payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    relevant = data.get("relevant")
    readable = data.get("readable")
    raw_value = data.get("value")
    present = data.get("present")
    summary = str(data.get("summary") or "").strip()
    evidence = str(data.get("evidence") or "").strip()
    confidence = float(data.get("confidence") or 0.0)
    expected_type = str(spec.get("expected_output_type") or "text")
    task_kind = str(spec.get("task_kind") or "custom_query")
    require_object_presence = bool(spec.get("require_object_presence_for_value"))

    if expected_type == "number" and require_object_presence and present is not True:
        object_name = str(spec.get("expected_object") or "required meter").strip() or "required meter"
        return {
            "accepted": False,
            "status": "missing_required_object",
            "reason": f"No meter detected in image. Expected {object_name}.",
        }
    if relevant is False:
        return {"accepted": False, "status": "irrelevant", "reason": "Image is not relevant to the task"}
    if readable is False and expected_type == "number":
        return {"accepted": False, "status": "unreadable", "reason": "Required numeric display is not readable"}

    def _normalize_numeric(value: Any) -> Optional[float]:
        if value is None or str(value).strip() == "":
            return None
        try:
            return float(str(value).strip())
        except Exception:
            m = re.search(r"-?\d+(?:\.\d+)?", str(value))
            if not m:
                return None
            try:
                return float(m.group(0))
            except Exception:
                return None

    if expected_type == "number":
        num = _normalize_numeric(raw_value)
        if num is None:
            return {"accepted": False, "status": "no_numeric_value", "reason": "No numeric value returned"}
        normalized_value = f"{num:.2f}" if task_kind == "earthing" else str(raw_value).strip() or str(num)
        return {
            "accepted": True,
            "status": "validated",
            "value": normalized_value,
            "numeric_value": num,
            "reason": "",
            "confidence": confidence,
        }

    if expected_type == "present_absent":
        normalized = None
        if present is not None:
            normalized = "Present" if bool(present) else "Absent"
        else:
            value_text = str(raw_value or "").strip().lower()
            if value_text in {"present", "yes", "detected", "available"}:
                normalized = "Present"
            elif value_text in {"absent", "no", "not present", "missing"}:
                normalized = "Absent"
        if not normalized:
            return {"accepted": False, "status": "invalid_label", "reason": "Expected Present or Absent"}
        return {"accepted": True, "status": "validated", "value": normalized, "reason": "", "confidence": confidence}

    if expected_type == "correct_incorrect":
        value_text = str(raw_value or "").strip().lower()
        if value_text in {"correct", "proper", "ok", "compliant"}:
            normalized = "Correct"
        elif value_text in {"incorrect", "wrong", "improper", "non-compliant", "non compliant"}:
            normalized = "Incorrect"
        else:
            return {"accepted": False, "status": "invalid_label", "reason": "Expected Correct or Incorrect"}
        return {"accepted": True, "status": "validated", "value": normalized, "reason": "", "confidence": confidence}

    if expected_type == "label":
        value_text = str(raw_value or summary or evidence).strip()
        allowed_labels = [str(label).strip() for label in (spec.get("allowed_labels") or []) if str(label).strip()]
        if not value_text:
            return {"accepted": False, "status": "no_label_value", "reason": "No label returned"}
        if not allowed_labels:
            return {"accepted": True, "status": "validated", "value": value_text, "reason": "", "confidence": confidence}
        normalized_lookup = {label.lower(): label for label in allowed_labels}
        canonical = normalized_lookup.get(value_text.lower())
        if canonical:
            return {"accepted": True, "status": "validated", "value": canonical, "reason": "", "confidence": confidence}
        keyword_map = {
            "Area maintained": ["maintained", "clean", "organized", "proper layout"],
            "Area Not maintained": ["not maintained", "over hanging", "overhanging", "dirty", "messed", "cluttered", "improper"],
            "Serviceable": ["serviceable", "working", "spray coming", "functional"],
            "Unserviceable": ["unserviceable", "not working", "no spray", "faulty"],
            "ON": ["on", "flame visible", "flame present", "lit"],
            "OFF": ["off", "no flame", "flame not visible", "not lit"],
        }
        lower_value = value_text.lower()
        for label in allowed_labels:
            for token in keyword_map.get(label, []):
                if token in lower_value:
                    return {"accepted": True, "status": "validated", "value": label, "reason": "", "confidence": confidence}
        return {"accepted": False, "status": "invalid_label", "reason": f"Expected one of: {', '.join(allowed_labels)}"}

    text_value = str(raw_value or summary or evidence).strip()
    if not text_value:
        return {"accepted": False, "status": "no_text_value", "reason": "No task result returned"}
    if confidence < 0.15 and relevant is not True:
        return {"accepted": False, "status": "low_confidence", "reason": "Low-confidence text result"}
    return {"accepted": True, "status": "validated", "value": text_value, "reason": "", "confidence": confidence}


def _task_semantic_alert_reason(
    instance: Dict[str, Any],
    response_value: Any,
    spec: Optional[Dict[str, Any]] = None,
    ai_payload: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    if response_value is None:
        return None
    value = str(response_value).strip().lower()
    if not value:
        return None
    mode_hint = _task_ai_mode_hint(instance)
    title = str(instance.get("title") or "").strip() or "task"
    evidence = str(((ai_payload or {}).get("openai_result") or {}).get("evidence") or (ai_payload or {}).get("evidence") or (ai_payload or {}).get("summary") or "").strip()
    active_spec = spec or {}
    alert_values = {str(item).strip().lower() for item in (active_spec.get("alert_on_values") or []) if str(item).strip()}
    if alert_values and value in alert_values:
        template = str(active_spec.get("alert_message") or "").strip()
        if template:
            return template.format(title=title, value=response_value, evidence=evidence or str(response_value))
        return f"Task alert: Rule triggered for task '{title}'."
    threshold = active_spec.get("alert_threshold") or {}
    operator = str(threshold.get("operator") or "").strip().lower()
    threshold_value = _task_float(threshold.get("value"))
    numeric_value = _task_float(response_value)
    if operator and threshold_value is not None and numeric_value is not None:
        triggered = (
            (operator == "gt" and numeric_value > threshold_value)
            or (operator == "gte" and numeric_value >= threshold_value)
            or (operator == "lt" and numeric_value < threshold_value)
            or (operator == "lte" and numeric_value <= threshold_value)
        )
        if triggered:
            template = str(threshold.get("message") or "").strip()
            if template:
                return template.format(title=title, value=numeric_value, threshold=threshold_value, evidence=evidence)
            return f"Task alert: Value {numeric_value} triggered threshold for task '{title}'."
    if mode_hint == "correct_incorrect" and value == "incorrect":
        return f"Task alert: Response marked Incorrect for task '{title}'."
    if mode_hint == "present_absent" and value == "absent":
        return f"Task alert: Required item absent for task '{title}'."
    return None


def _task_missing_object_alert_reason(
    instance: Dict[str, Any],
    spec: Optional[Dict[str, Any]] = None,
    ai_payload: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    active_spec = spec or {}
    if not active_spec.get("alert_when_required_object_missing"):
        return None
    payload = ai_payload or {}
    status = str(payload.get("validation_status") or "").strip().lower()
    reason = str(payload.get("validation_reason") or "").strip()
    if status != "missing_required_object" and "no meter detected" not in reason.lower():
        return None
    title = str(instance.get("title") or "").strip() or "task"
    return reason or f"Task alert: No meter detected in image for task '{title}', so no value was recorded."


def _task_numeric_from_ocr(ocr_payload: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(ocr_payload, dict):
        return None
    try:
        raw = (ocr_payload.get("numeric") or {}).get("value")
        if raw is not None and str(raw).strip() != "":
            return float(raw)
    except Exception:
        pass
    txt = str(ocr_payload.get("text") or "")
    m = re.search(r"-?\d+(?:\.\d+)?", txt)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


async def _task_openai_numeric_fallback(
    *,
    image_path: str,
    task_title: str,
    task_description: str,
    local_result: Optional[Dict[str, Any]] = None,
) -> Optional[float]:
    if not openai_available():
        return None
    try:
        payload = await run_in_threadpool(
            analyze_task_image_with_openai,
            image_path=image_path,
            task_title=task_title,
            task_description=task_description,
            local_result=local_result,
            mode_hint="numeric",
        )
        raw = payload.get("value")
        if raw is None or str(raw).strip() == "":
            return None
        return float(raw)
    except Exception:
        return None


async def _task_openai_numeric_first(
    *,
    image_path: str,
    task_title: str,
    task_description: str,
    local_result: Optional[Dict[str, Any]] = None,
) -> Optional[float]:
    if not openai_available():
        return None
    try:
        payload = await run_in_threadpool(
            analyze_task_image_with_openai,
            image_path=image_path,
            task_title=task_title,
            task_description=task_description,
            local_result=local_result,
            mode_hint="numeric",
        )
        raw = payload.get("value")
        if raw is None or str(raw).strip() == "":
            return None
        return float(raw)
    except Exception:
        return None


def _task_validate_threshold(question: Optional[Dict[str, Any]], extracted_value: Optional[float]) -> Dict[str, Any]:
    if not question:
        return {"alert": False, "reason": "", "status": "no_rule"}
    if extracted_value is None:
        return {"alert": False, "reason": "No numeric value extracted", "status": "no_value"}
    condition = (question.get("alert_condition") or "outside_range").strip().lower()
    min_v = _task_float(question.get("ideal_min"))
    max_v = _task_float(question.get("ideal_max"))
    if min_v is None and max_v is None:
        return {"alert": False, "reason": "", "status": "no_rule"}
    reason = ""
    alert = False
    if condition in {"outside_range", "between"}:
        if min_v is not None and extracted_value < min_v:
            alert = True
            reason = f"Value {extracted_value} is below min {min_v}"
        if max_v is not None and extracted_value > max_v:
            alert = True
            reason = f"Value {extracted_value} is above max {max_v}"
    elif condition == "greater_than" and max_v is not None:
        alert = extracted_value > max_v
        if alert:
            reason = f"Value {extracted_value} is greater than {max_v}"
    elif condition == "less_than" and min_v is not None:
        alert = extracted_value < min_v
        if alert:
            reason = f"Value {extracted_value} is less than {min_v}"
    elif condition == "equal_to":
        target = max_v if max_v is not None else min_v
        if target is not None:
            alert = abs(extracted_value - target) > 1e-9
            if alert:
                reason = f"Value {extracted_value} is not equal to {target}"
    return {"alert": alert, "reason": reason, "status": "checked"}


def _task_send_overdue_alert(instance: Dict[str, Any]):
    assigned_user_id = int(instance.get("assigned_user_id") or 0)
    assignee = get_user_by_id(assigned_user_id)
    if not assignee:
        return
    team_id = _user_team_int(assignee)
    rid = insert_reading(
        user_id=assigned_user_id,
        team=int(team_id or 0),
        meter_type="task",
        label=f"TASK-{instance.get('id')}",
        value="OVERDUE",
        filename="task://system",
        ocr_json=json.dumps({"task_id": instance.get("id"), "form_id": instance.get("form_id"), "event": "overdue"}),
    )
    msg = (
        f"Task overdue: {instance.get('title')} | User: {assignee.get('username')} | "
        f"Deadline: {instance.get('deadline_at')} | Status: OVERDUE"
    )
    if team_id is not None:
        create_alert(
            reading_id=rid,
            target_role="coadmin",
            target_team=int(team_id),
            message=msg,
            severity="high",
        )
        task_create_notification(
            task_instance_id=int(instance["id"]),
            recipient_role="coadmin",
            recipient_user_id=None,
            recipient_team=int(team_id),
            alert_type="overdue",
            message=msg,
        )
    create_alert(
        reading_id=rid,
        target_role="admin",
        target_team=None,
        message=msg,
        severity="high",
    )
    task_create_notification(
        task_instance_id=int(instance["id"]),
        recipient_role="admin",
        recipient_user_id=None,
        recipient_team=None,
        alert_type="overdue",
        message=msg,
    )


def _task_send_submission_alert(instance: Dict[str, Any], message: str, severity: str = "high"):
    assigned_user_id = int(instance.get("assigned_user_id") or 0)
    assignee = get_user_by_id(assigned_user_id)
    team_id = _user_team_int(assignee)
    rid = insert_reading(
        user_id=assigned_user_id,
        team=int(team_id or 0),
        meter_type="task",
        label=f"TASK-{instance.get('id')}",
        value="ALERT",
        filename="task://system",
        ocr_json=json.dumps({"task_id": instance.get("id"), "event": "submission_alert", "reason": message}),
    )
    if team_id is not None:
        create_alert(
            reading_id=rid,
            target_role="coadmin",
            target_team=int(team_id),
            message=message,
            severity=severity,
        )
        task_create_notification(
            task_instance_id=int(instance["id"]),
            recipient_role="coadmin",
            recipient_user_id=None,
            recipient_team=int(team_id),
            alert_type="submission_alert",
            message=message,
        )
    create_alert(
        reading_id=rid,
        target_role="admin",
        target_team=None,
        message=message,
        severity=severity,
    )
    task_create_notification(
        task_instance_id=int(instance["id"]),
        recipient_role="admin",
        recipient_user_id=None,
        recipient_team=None,
        alert_type="submission_alert",
        message=message,
    )


def _task_scheduler_cycle():
    now = datetime.now()
    now_iso = now.isoformat()
    overdue_items = task_list_due_for_overdue(now_iso)
    for item in overdue_items:
        task_mark_overdue_sent(int(item["id"]))
        _task_send_overdue_alert(item)
        task_log_activity(
            task_instance_id=int(item["id"]),
            action="task_overdue",
            actor_user_id=None,
            actor_role="system",
            meta={"deadline_at": item.get("deadline_at")},
        )

    repeat_forms = task_list_repeat_forms()
    for form in repeat_forms:
        try:
            assigned_scope = (form.get("assigned_scope") or "").strip()
            assignee_ids: List[int] = []
            if assigned_scope == "users":
                for val in _task_parse_json_list(form.get("assigned_user_ids_json")):
                    try:
                        assignee_ids.append(int(val))
                    except Exception:
                        continue
            elif assigned_scope == "team":
                t = form.get("assigned_team_id")
                if t is not None:
                    assignee_ids = [int(u["id"]) for u in fetch_users_by_team(int(t)) if u.get("role") == "user"]
            for uid in assignee_ids:
                latest = task_get_latest_instance_for_user(form_id=int(form["id"]), user_id=int(uid))
                if not latest:
                    continue
                dl = latest.get("deadline_at")
                try:
                    latest_deadline = datetime.fromisoformat(str(dl))
                except Exception:
                    continue
                if latest_deadline > now:
                    continue
                next_deadline = _task_next_deadline(
                    latest_deadline,
                    str(form.get("repeat_type") or "daily"),
                    form.get("repeat_interval_days"),
                )
                task_create_instance(
                    form_id=int(form["id"]),
                    assigned_user_id=int(uid),
                    assigned_team_id=form.get("assigned_team_id"),
                    deadline_at=next_deadline.isoformat(),
                    cycle_key=_task_cycle_key(next_deadline),
                    status="pending",
                )
        except Exception:
            log_event(error_logger, "ERROR", "TASK_REPEAT_FAIL", "Failed to generate repeated task instance", exc_info=True)


def hash_password(password: str) -> str:
    # Use PBKDF2 to avoid passlib<->bcrypt backend compatibility issues.
    return pbkdf2_sha256.hash(password)


def verify_password(password: str, stored_hash: str) -> bool:
    if not stored_hash:
        return False
    if stored_hash.startswith("$2"):
        try:
            return pybcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8"))
        except Exception:
            return False
    try:
        return pbkdf2_sha256.verify(password, stored_hash)
    except Exception:
        return False


def _augment_readings(readings):
    # Backward-compatible wrapper for existing call sites.
    return augment_readings_for_view(readings)


def _filter_today_readings(readings):
    # Backward-compatible wrapper for existing call sites.
    return filter_dashboard_readings_scope(readings)


# -----------------------
# Startup
# -----------------------
import threading


def _env_flag(name: str, default: bool = False) -> bool:
    value = (os.getenv(name, "") or "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


@app.on_event("startup")
def startup():
    init_db()
    log_event(system_logger, "INFO", "SYSTEM_STARTUP", "Application startup initialized")
    if SESSION_SECRET_KEY == "CHANGE_ME_TO_A_RANDOM_LONG_SECRET":
        log_event(
            security_logger,
            "WARNING",
            "SECURITY_DEFAULT_SESSION_SECRET",
            "SESSION_SECRET_KEY is using the built-in fallback; set a strong secret in the environment",
        )
    if not SESSION_COOKIE_HTTPS_ONLY:
        log_event(
            security_logger,
            "WARNING",
            "SECURITY_INSECURE_SESSION_COOKIE",
            "Session cookies are not marked Secure; set SESSION_COOKIE_HTTPS_ONLY=1 when running behind HTTPS",
        )

    # Start server immediately. OCR warmup is opt-in to avoid hard crashes
    # on machines where native OCR dependencies are unstable.
    if _env_flag("ENABLE_OCR_WARMUP", default=False):
        def _warm():
            try:
                log_event(jobs_logger, "INFO", "OCR_WARMUP_START", "OCR warmup started")
                warmup_models()
                log_event(jobs_logger, "INFO", "OCR_WARMUP_DONE", "OCR warmup completed")
            except Exception as e:
                log_event(jobs_logger, "ERROR", "OCR_WARMUP_FAIL", "OCR warmup failed", error=str(e))

        threading.Thread(target=_warm, daemon=True).start()
    else:
        log_event(jobs_logger, "INFO", "OCR_WARMUP_SKIPPED", "OCR warmup skipped; set ENABLE_OCR_WARMUP=1 to enable")

    # create users...
    if not get_user_by_username("admin"):
        create_user("admin", hash_password("admin123"), "admin", None, force_password_change=True)
        log_event(auth_logger, "INFO", "USER_REGISTER_SUCCESS", "Bootstrap admin account created", username="admin", role="admin")

    for t in range(1, 7):
        uname = f"coadmin{t}"
        if not get_user_by_username(uname):
            create_user(uname, hash_password("coadmin123"), "coadmin", t, force_password_change=True)
            log_event(auth_logger, "INFO", "USER_REGISTER_SUCCESS", "Bootstrap coadmin account created", username=uname, role="coadmin", team=t)

    bootstrap_users = [("admin", "admin123")]
    bootstrap_users.extend((f"coadmin{t}", "coadmin123") for t in range(1, 7))
    for uname, default_password in bootstrap_users:
        existing = get_user_by_username(uname)
        if existing and verify_password(default_password, str(existing.get("password_hash") or "")):
            set_user_force_password_change(int(existing["id"]), True)

    log_event(system_logger, "INFO", "SYSTEM_READY", "Application startup completed", url="http://127.0.0.1:8000/login")
    if _env_flag("ENABLE_TASK_SCHEDULER", default=True):
        def _task_loop():
            log_event(jobs_logger, "INFO", "TASK_SCHEDULER_START", "Task scheduler started")
            while True:
                try:
                    _task_scheduler_cycle()
                except Exception:
                    log_event(error_logger, "ERROR", "TASK_SCHEDULER_FAIL", "Task scheduler cycle failed", exc_info=True)
                time.sleep(60)
        threading.Thread(target=_task_loop, daemon=True).start()

# -----------------------
# Auth
# -----------------------
@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return _auth_page(request)


@app.post("/login")
async def do_login(request: Request, username: str = Form(...), password: str = Form(...)):
    retry_after = _auth_rate_limit(request, "login", username.strip(), limit=8, window_seconds=300)
    if retry_after is not None:
        resp = _auth_page(request, error="Too many login attempts. Please try again in a few minutes.")
        resp.status_code = 429
        resp.headers["Retry-After"] = str(retry_after)
        return resp
    log_event(auth_logger, "INFO", "AUTH_LOGIN_ATTEMPT", "Login attempt", username=username.strip())
    u = get_user_by_username(username.strip())
    if not u or not verify_password(password, u["password_hash"]):
        log_event(auth_logger, "WARN", "AUTH_LOGIN_FAIL", "Login failed", username=username.strip())
        return _auth_page(request, error="Invalid credentials")

    request.session["uid"] = int(u["id"])
    role = u["role"]
    log_event(auth_logger, "INFO", "AUTH_LOGIN_SUCCESS", "Login successful", user_id=int(u["id"]), role=role)

    if _must_rotate_password(u):
        log_event(
            auth_logger,
            "INFO",
            "AUTH_PASSWORD_ROTATION_REQUIRED",
            "Login accepted with one-time password; password rotation required",
            user_id=int(u["id"]),
            role=role,
        )
        return RedirectResponse("/force-password-change", status_code=303)

    if role == "admin":
        return RedirectResponse("/admin", status_code=303)
    if role == "coadmin":
        return RedirectResponse(f"/coadmin/{u['team']}", status_code=303)
    return RedirectResponse("/", status_code=303)


@app.post("/register")
async def do_register(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
    referral_id: Optional[str] = Form(None),
):
    email_norm = (email or "").strip().lower()
    retry_after = _auth_rate_limit(request, "register", email_norm, limit=5, window_seconds=900)
    if retry_after is not None:
        resp = _auth_page(request, error="Too many registration attempts. Please wait and try again.")
        resp.status_code = 429
        resp.headers["Retry-After"] = str(retry_after)
        return resp
    if not email_norm or "@" not in email_norm:
        return _auth_page(request, error="Enter a valid email address.")
    if len(password or "") < 6:
        return _auth_page(request, error="Password must be at least 6 characters.")
    if password != confirm_password:
        return _auth_page(request, error="Password and confirm password do not match.")

    if get_user_by_email(email_norm):
        return _auth_page(request, error="Account already exists. Please sign in.")

    try:
        # Self-registered users start unassigned and must be added to a team from Settings.
        create_user(email_norm, hash_password(password), "user", None)
        created = get_user_by_username(email_norm)
        if created:
            update_user_identity(
                int(created["id"]),
                email=email_norm,
                display_name=email_norm.split("@")[0],
                auth_provider="password",
            )
        log_event(
            auth_logger,
            "INFO",
            "USER_REGISTER_SUCCESS",
            "User self-registration completed",
            email=email_norm,
            referral_id=(referral_id or "").strip() or None,
        )
        return RedirectResponse("/login?info=Account created. Please sign in.", status_code=303)
    except Exception:
        log_event(auth_logger, "ERROR", "USER_REGISTER_FAIL", "User self-registration failed", exc_info=True, email=email_norm)
        return _auth_page(request, error="Unable to create account right now.")


@app.post("/forgot-password")
async def forgot_password(
    request: Request,
    identifier: str = Form(...),
):
    ident = (identifier or "").strip()
    retry_after = _auth_rate_limit(request, "forgot_password", ident, limit=4, window_seconds=1800)
    if retry_after is not None:
        resp = _auth_page(request, error="Too many reset attempts. Please wait before trying again.")
        resp.status_code = 429
        resp.headers["Retry-After"] = str(retry_after)
        return resp
    user = _find_user_for_password_reset(ident)
    if not user:
        log_event(
            security_logger,
            "WARNING",
            "SECURITY_FORGOT_PASSWORD_UNKNOWN_ACCOUNT",
            "Password reset requested for unknown account",
            identifier=ident or None,
            ip=_client_ip(request),
        )
        return _auth_page(request, error="Account not found for password reset.")
    token = _create_password_reset_token(user)
    log_event(
        auth_logger,
        "INFO",
        "AUTH_PASSWORD_RESET_STARTED",
        "Password reset flow started",
        user_id=int(user["id"]),
        ip=_client_ip(request),
    )
    return RedirectResponse(f"/reset-password?token={urllib.parse.quote(token)}", status_code=303)


@app.get("/reset-password", response_class=HTMLResponse)
def reset_password_page(request: Request, token: str = ""):
    user = _validate_password_reset_token(token)
    if not user:
        return RedirectResponse("/login?error=Password reset link is invalid or expired.", status_code=303)
    return _password_reset_page(request, token)


@app.post("/reset-password")
async def reset_password_submit(
    request: Request,
    token: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
):
    user = _validate_password_reset_token(token)
    if not user:
        return RedirectResponse("/login?error=Password reset link is invalid or expired.", status_code=303)
    retry_after = _auth_rate_limit(request, "reset_password_submit", str(user.get("id") or "0"), limit=5, window_seconds=1800)
    if retry_after is not None:
        resp = _password_reset_page(request, token, error="Too many reset attempts. Please wait before trying again.")
        resp.status_code = 429
        resp.headers["Retry-After"] = str(retry_after)
        return resp
    if len(new_password or "") < 6:
        return _password_reset_page(request, token, error="Password must be at least 6 characters.")
    if new_password != confirm_password:
        return _password_reset_page(request, token, error="Password and confirm password do not match.")
    try:
        update_user_password(int(user["id"]), hash_password(new_password), force_password_change=False)
        log_event(
            auth_logger,
            "INFO",
            "AUTH_PASSWORD_RESET",
            "Password reset completed",
            user_id=int(user["id"]),
        )
        return RedirectResponse("/login?info=Password reset successful. Please sign in with your new password.", status_code=303)
    except Exception:
        log_event(
            auth_logger,
            "ERROR",
            "AUTH_PASSWORD_RESET_FAIL",
            "Password reset failed",
            exc_info=True,
            user_id=int(user["id"]),
        )
        return _password_reset_page(request, token, error="Unable to reset password right now.")


@app.get("/force-password-change", response_class=HTMLResponse)
def force_password_change_page(request: Request):
    u = current_user(request)
    if not u:
        return RedirectResponse("/login", status_code=303)
    if not _must_rotate_password(u):
        if u["role"] == "admin":
            return RedirectResponse("/admin", status_code=303)
        if u["role"] == "coadmin":
            return RedirectResponse(f"/coadmin/{u['team']}", status_code=303)
        return RedirectResponse("/", status_code=303)
    return _password_change_page(request, u)


@app.post("/force-password-change")
async def force_password_change_submit(
    request: Request,
    new_password: str = Form(...),
    confirm_password: str = Form(...),
):
    u = current_user(request)
    if not u:
        return RedirectResponse("/login", status_code=303)
    if not _must_rotate_password(u):
        return RedirectResponse("/login", status_code=303)
    if len(new_password or "") < 6:
        return _password_change_page(request, u, error="Password must be at least 6 characters.")
    if new_password != confirm_password:
        return _password_change_page(request, u, error="Password and confirm password do not match.")

    default_password = "admin123" if u["role"] == "admin" else "coadmin123"
    if new_password == default_password:
        return _password_change_page(request, u, error="Choose a new password different from the one-time password.")

    try:
        update_user_password(int(u["id"]), hash_password(new_password), force_password_change=False)
        log_event(
            auth_logger,
            "INFO",
            "AUTH_PASSWORD_ROTATED",
            "One-time password rotated successfully",
            user_id=int(u["id"]),
            role=u["role"],
        )
        request.session.clear()
        return RedirectResponse("/login?info=Password changed successfully. Please sign in again with your new password.", status_code=303)
    except Exception:
        log_event(
            auth_logger,
            "ERROR",
            "AUTH_PASSWORD_ROTATION_FAIL",
            "Failed to rotate one-time password",
            exc_info=True,
            user_id=int(u["id"]),
            role=u["role"],
        )
        return _password_change_page(request, u, error="Unable to change password right now.")


@app.get("/auth/google/start")
def auth_google_start(request: Request):
    retry_after = _auth_rate_limit(request, "google_auth_start", "", limit=10, window_seconds=300)
    if retry_after is not None:
        return RedirectResponse("/login?error=Too many authentication attempts. Please try again later.", status_code=303)
    client_id = (os.getenv("GOOGLE_CLIENT_ID", "") or "").strip()
    if not client_id:
        return RedirectResponse("/login?error=Google login is not configured.", status_code=303)
    state = uuid.uuid4().hex
    request.session["google_oauth_state"] = state
    params = {
        "client_id": client_id,
        "redirect_uri": _google_redirect_uri(request),
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "online",
        "include_granted_scopes": "true",
        "state": state,
        "prompt": "select_account",
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    return RedirectResponse(url, status_code=303)


@app.get("/auth/google/callback")
def auth_google_callback(request: Request, code: Optional[str] = None, state: Optional[str] = None):
    expected_state = request.session.get("google_oauth_state")
    if not code or not state or not expected_state or state != expected_state:
        return RedirectResponse("/login?error=Google authentication failed (state mismatch).", status_code=303)

    client_id = (os.getenv("GOOGLE_CLIENT_ID", "") or "").strip()
    client_secret = (os.getenv("GOOGLE_CLIENT_SECRET", "") or "").strip()
    if not client_id or not client_secret:
        return RedirectResponse("/login?error=Google login is not configured.", status_code=303)

    token_payload = urllib.parse.urlencode(
        {
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": _google_redirect_uri(request),
            "grant_type": "authorization_code",
        }
    ).encode("utf-8")

    try:
        token_req = urllib.request.Request(
            "https://oauth2.googleapis.com/token",
            data=token_payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(token_req, timeout=12) as resp:
            token_data = json.loads(resp.read().decode("utf-8"))
        access_token = token_data.get("access_token")
        if not access_token:
            return RedirectResponse("/login?error=Google authentication failed.", status_code=303)

        userinfo_req = urllib.request.Request(
            "https://openidconnect.googleapis.com/v1/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
            method="GET",
        )
        with urllib.request.urlopen(userinfo_req, timeout=12) as resp:
            info = json.loads(resp.read().decode("utf-8"))
    except Exception:
        log_event(auth_logger, "ERROR", "AUTH_GOOGLE_TOKEN_FAIL", "Google OAuth token/userinfo fetch failed", exc_info=True)
        return RedirectResponse("/login?error=Google authentication failed.", status_code=303)

    email = (info.get("email") or "").strip().lower()
    google_id = (info.get("sub") or "").strip()
    display_name = (info.get("name") or "").strip() or (email.split("@")[0] if email else "")
    if not email:
        return RedirectResponse("/login?error=Google account email is missing.", status_code=303)

    user = get_user_by_google_id(google_id) if google_id else None
    if not user:
        user = get_user_by_email(email)

    if not user:
        # Google self-registration starts as unassigned user.
        create_user(email, hash_password(uuid.uuid4().hex), "user", None)
        user = get_user_by_username(email)
        if user:
            update_user_identity(
                int(user["id"]),
                email=email,
                display_name=display_name,
                google_id=google_id or None,
                auth_provider="google",
            )
    else:
        update_user_identity(
            int(user["id"]),
            email=email,
            display_name=display_name or None,
            google_id=google_id or None,
            auth_provider="google",
        )

    if not user:
        return RedirectResponse("/login?error=Unable to complete Google login.", status_code=303)

    request.session["uid"] = int(user["id"])
    log_event(auth_logger, "INFO", "AUTH_GOOGLE_SUCCESS", "Google login successful", user_id=int(user["id"]), email=email)
    if user["role"] == "admin":
        return RedirectResponse("/admin", status_code=303)
    if user["role"] == "coadmin":
        return RedirectResponse(f"/coadmin/{user['team']}", status_code=303)
    return RedirectResponse("/", status_code=303)


@app.get("/logout")
def logout(request: Request):
    uid = None
    try:
        uid = request.session.get("uid")
    except Exception:
        uid = None
    log_event(auth_logger, "INFO", "AUTH_LOGOUT", "User logout", user_id=uid)
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


def _redirect_dashboard_with_message(user: Dict[str, Any], *, info: Optional[str] = None, err: Optional[str] = None):
    q = ""
    if info:
        q = f"?info={urllib.parse.quote_plus(info)}"
    elif err:
        q = f"?err={urllib.parse.quote_plus(err)}"
    if user["role"] == "admin":
        return RedirectResponse(f"/admin{q}", status_code=303)
    return RedirectResponse(f"/coadmin/{user['team']}{q}", status_code=303)


# -----------------------
# User page + Upload
# -----------------------
@app.get("/", response_class=HTMLResponse)
def user_page(request: Request):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)

    if u["role"] == "admin":
        return RedirectResponse("/admin", status_code=303)
    if u["role"] == "coadmin":
        return RedirectResponse(f"/coadmin/{u['team']}", status_code=303)

    return RedirectResponse("/tasks", status_code=303)


@app.post("/upload")
async def upload_meter_image(
    request: Request,
    label: str = Form(...),
    meter_type: str = Form(...),
    manual_value: Optional[str] = Form(None),
    fuel_economy: Optional[str] = Form(None),
    image: UploadFile = File(...),
    image2: Optional[UploadFile] = File(None),
):
    u = current_user(request)
    if not require_role(u, ["user"]):
        return RedirectResponse("/login", status_code=303)
    return RedirectResponse("/?err=direct_upload_disabled", status_code=303)

    if meter_type not in ["earthing", "temp", "voltage", "odometer", "fire_point"]:
        return templates.TemplateResponse(
            "user.html",
            {
                "request": request,
                "user": u,
                "readings": _augment_readings(_filter_today_readings(fetch_readings_by_user(int(u["id"])))),
                "error": "Invalid meter type",
            },
            status_code=400,
        )

    if not image.filename:
        return templates.TemplateResponse(
            "user.html",
            {
                "request": request,
                "user": u,
                "readings": _augment_readings(_filter_today_readings(fetch_readings_by_user(int(u["id"])))),
                "error": "Please select an image.",
            },
            status_code=400,
        )

    ext = os.path.splitext(image.filename.lower())[1]
    if ext not in [".jpg", ".jpeg", ".png", ".webp"]:
        return templates.TemplateResponse(
            "user.html",
            {
                "request": request,
                "user": u,
                "readings": _augment_readings(_filter_today_readings(fetch_readings_by_user(int(u["id"])))),
                "error": "Only image files are allowed.",
            },
            status_code=400,
        )

    filename = f"{uuid.uuid4().hex}{ext}"
    filepath = os.path.join(UPLOAD_DIR, filename)
    filename_2 = None
    filepath_2 = None

    if meter_type == "odometer":
        if image2 is None or not image2.filename:
            return templates.TemplateResponse(
                "user.html",
                {
                    "request": request,
                    "user": u,
                    "readings": _augment_readings(_filter_today_readings(fetch_readings_by_user(int(u["id"])))),
                    "error": "Please upload both odometer images.",
                },
                status_code=400,
            )
        ext2 = os.path.splitext(image2.filename.lower())[1]
        if ext2 not in [".jpg", ".jpeg", ".png", ".webp"]:
            return templates.TemplateResponse(
                "user.html",
                {
                    "request": request,
                    "user": u,
                    "readings": _augment_readings(_filter_today_readings(fetch_readings_by_user(int(u["id"])))),
                    "error": "Second odometer image must be JPG/PNG/WEBP.",
                },
                status_code=400,
            )
        filename_2 = f"{uuid.uuid4().hex}{ext2}"
        filepath_2 = os.path.join(UPLOAD_DIR, filename_2)

        fuel_economy = (fuel_economy or "").strip()
        try:
            if not fuel_economy or float(fuel_economy) <= 0:
                raise ValueError("invalid")
        except Exception:
            return templates.TemplateResponse(
                "user.html",
                {
                    "request": request,
                    "user": u,
                    "readings": _augment_readings(_filter_today_readings(fetch_readings_by_user(int(u["id"])))),
                    "error": "Fuel economy must be a positive number for odometer mode.",
                },
                status_code=400,
            )
    log_event(
        api_logger,
        "INFO",
        "UPLOAD_IMAGE_RECEIVED",
        "Meter image upload received",
        meter_type=meter_type,
        filename=filename,
        user_id=int(u["id"]),
    )

    # Save upload
    raw_image = await image.read()
    validation_error = _validate_upload_payload(raw_image, "photo", ext)
    if validation_error:
        return templates.TemplateResponse(
            "user.html",
            {
                "request": request,
                "user": u,
                "readings": _augment_readings(_filter_today_readings(fetch_readings_by_user(int(u["id"])))),
                "error": validation_error,
            },
            status_code=400,
        )
    with open(filepath, "wb") as f:
        f.write(raw_image)
    _warm_cached_preview(filepath)
    image_taken_at = _extract_image_taken_at(filepath)
    image_taken_at_2 = None
    if filepath_2 and image2 is not None:
        raw_image_2 = await image2.read()
        validation_error_2 = _validate_upload_payload(raw_image_2, "photo", ext2)
        if validation_error_2:
            return templates.TemplateResponse(
                "user.html",
                {
                    "request": request,
                    "user": u,
                    "readings": _augment_readings(_filter_today_readings(fetch_readings_by_user(int(u["id"])))),
                    "error": validation_error_2,
                },
                status_code=400,
            )
        with open(filepath_2, "wb") as f:
            f.write(raw_image_2)
        _warm_cached_preview(filepath_2)
        image_taken_at_2 = _extract_image_taken_at(filepath_2)

    ocr_result: Dict[str, Any] = {}
    numeric_value: Optional[str] = None
    if meter_type != "fire_point":
        # OCR in threadpool so request doesn't feel "stuck"
        debug_id = uuid.uuid4().hex
        try:
            ocr_result = await run_in_threadpool(run_ocr, filepath, debug_id, meter_type)
        except Exception as e:
            log_event(error_logger, "ERROR", "OCR_RUN_FAIL", "OCR processing failed", error=str(e), filename=filename)
            return templates.TemplateResponse(
                "user.html",
                {
                    "request": request,
                    "user": u,
                    "readings": _augment_readings(_filter_today_readings(fetch_readings_by_user(int(u["id"])))),
                    "error": f"OCR failed: {e}",
                },
                status_code=500,
            )
        if ocr_result.get("numeric"):
            numeric_value = ocr_result["numeric"].get("value")

    manual_value = (manual_value or "").strip() or None
    value_for_alert = manual_value or numeric_value

    odometer_start = None
    odometer_end = None
    distance_diff = None
    fuel_consumed = None
    fire_point_present: Optional[bool] = None
    fire_point_value: Optional[str] = None
    ocr_payload = ocr_result

    if meter_type == "odometer":
        if not filepath_2:
            return templates.TemplateResponse(
                "user.html",
                {
                    "request": request,
                    "user": u,
                    "readings": _augment_readings(_filter_today_readings(fetch_readings_by_user(int(u["id"])))),
                    "error": "Second odometer image missing.",
                },
                status_code=400,
            )
        debug_id_2 = uuid.uuid4().hex
        try:
            ocr_result_2 = await run_in_threadpool(run_ocr, filepath_2, debug_id_2, meter_type)
        except Exception as e:
            log_event(error_logger, "ERROR", "OCR_RUN_FAIL", "Second odometer OCR failed", error=str(e), filename=filename_2)
            return templates.TemplateResponse(
                "user.html",
                {
                    "request": request,
                    "user": u,
                    "readings": _augment_readings(_filter_today_readings(fetch_readings_by_user(int(u["id"])))),
                    "error": f"Odometer OCR failed for second image: {e}",
                },
                status_code=500,
            )

        odometer_start = numeric_value
        odometer_end = (ocr_result_2.get("numeric") or {}).get("value")
        try:
            if odometer_start is None or odometer_end is None:
                raise ValueError("Could not read odometer values from both images")
            distance_val = abs(float(odometer_end) - float(odometer_start))
            distance_diff = f"{distance_val:.2f}"
            fe = float(fuel_economy or "0")
            fuel_used = distance_val / fe
            fuel_consumed = f"{fuel_used:.2f}"
            value_for_alert = distance_diff
            numeric_value = distance_diff
            ocr_payload = {
                "odometer_first": ocr_result,
                "odometer_second": ocr_result_2,
                "odometer": {
                    "start": odometer_start,
                    "end": odometer_end,
                    "distance_diff": distance_diff,
                    "fuel_economy": fuel_economy,
                    "fuel_consumed": fuel_consumed,
                },
            }
        except Exception as e:
            return templates.TemplateResponse(
                "user.html",
                {
                    "request": request,
                    "user": u,
                    "readings": _augment_readings(_filter_today_readings(fetch_readings_by_user(int(u["id"])))),
                    "error": f"Unable to compute odometer distance/fuel: {e}",
                },
                status_code=400,
            )
    elif meter_type == "fire_point":
        try:
            fire_detection = None
            if openai_available():
                try:
                    openai_fire = await run_in_threadpool(
                        analyze_task_image_with_openai,
                        image_path=filepath,
                        task_title=label.strip() or "Fire Point Check",
                        task_description="Check whether fire fighting equipment is present in the image.",
                        local_result={},
                        mode_hint="present_absent",
                    )
                    if openai_fire.get("present") is not None:
                        fire_detection = {
                            "present": bool(openai_fire.get("present")),
                            "confidence": float(openai_fire.get("confidence") or 0.0),
                            "source": "openai",
                            "openai_result": openai_fire,
                        }
                except Exception as e:
                    fire_detection = {"openai_error": str(e)}
            if fire_detection is None or fire_detection.get("present") is None:
                local_detection = await run_in_threadpool(detect_task_objects, "fire_point", filepath)
                fire_detection = {**(fire_detection or {}), **local_detection}
            fire_point_present = bool(fire_detection.get("present"))
            fire_point_value = "Present" if fire_point_present else "Absent"
            numeric_value = None
            value_for_alert = fire_point_value
            ocr_payload = {"object_detection": fire_detection}
        except Exception as e:
            log_event(error_logger, "ERROR", "FIREPOINT_DETECT_FAIL", "Fire point detection failed", error=str(e), filename=filename)
            return templates.TemplateResponse(
                "user.html",
                {
                    "request": request,
                    "user": u,
                    "readings": _augment_readings(_filter_today_readings(fetch_readings_by_user(int(u["id"])))),
                    "error": f"Fire point detection failed: {e}",
                },
                status_code=500,
            )

    # Store reading
    rid = insert_reading(
        user_id=int(u["id"]),
        team=int(u["team"]),
        meter_type=meter_type,
        label=label.strip(),
        value=manual_value or fire_point_value or numeric_value,
        filename=filename,
        filename_2=filename_2,
        ocr_json=json.dumps(ocr_payload, default=str),
        manual_value=manual_value,
        odometer_start=odometer_start,
        odometer_end=odometer_end,
        distance_diff=distance_diff,
        fuel_economy=fuel_economy if meter_type == "odometer" else None,
        fuel_consumed=fuel_consumed,
        image_taken_at=image_taken_at,
        image_taken_at_2=image_taken_at_2,
    )

    # Alerts logic
    log_event(
        api_logger,
        "INFO",
        "ALERT_EVAL_START",
        "Evaluating reading against ideal range",
        meter_type=meter_type,
        numeric_value=value_for_alert,
    )
    is_alert, severity, msg = compare_against_ideal(meter_type, value_for_alert)
    log_event(
        api_logger,
        "INFO",
        "ALERT_EVAL_RESULT",
        "Alert evaluation completed",
        is_alert=is_alert,
        severity=severity,
        message_text=msg,
    )

    if meter_type != "odometer" and meter_type != "fire_point" and numeric_value and is_alert:
        # coadmin (team)
        create_alert(
            reading_id=rid,
            target_role="coadmin",
            target_team=int(u["team"]),
            message=f"Team {u['team']} - {label}: {msg}",
            severity=severity,
        )
        # admin
        create_alert(
            reading_id=rid,
            target_role="admin",
            target_team=None,
            message=f"Team {u['team']} - {label}: {msg}",
            severity=severity,
        )
        log_event(
            admin_logger,
            "WARN",
            "ALERT_CREATED",
            "Threshold alert created for coadmin and admin",
            team=int(u["team"]),
            label=label.strip(),
            severity=severity,
        )
    if meter_type == "fire_point" and fire_point_present is False:
        alert_text = f"Team {u['team']} - {label.strip()}: Fire fighting equipment absent."
        create_alert(
            reading_id=rid,
            target_role="coadmin",
            target_team=int(u["team"]),
            message=alert_text,
            severity="high",
        )
        create_alert(
            reading_id=rid,
            target_role="admin",
            target_team=None,
            message=alert_text,
            severity="high",
        )
        log_event(
            admin_logger,
            "WARN",
            "FIREPOINT_ALERT_CREATED",
            "Fire point absence alert created for coadmin and admin",
            team=int(u["team"]),
            label=label.strip(),
        )

    return RedirectResponse("/?uploaded=1", status_code=303)


@app.get("/success", response_class=HTMLResponse)
def success_page(request: Request):
    u = current_user(request)
    if not u:
        return RedirectResponse("/login", status_code=303)
    return templates.TemplateResponse("success.html", {"request": request, "user": u})


# -----------------------
# Tasks
# -----------------------
def _task_has_submission(item: Dict[str, Any]) -> bool:
    if str(item.get("status") or "").strip().lower() in {"submitted", "late", "completed"}:
        return True
    for key in ("response_value", "response_file_path", "response_file_path_2", "submitted_at"):
        value = item.get(key)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return True
    return False


def _task_status_bucket(item: Dict[str, Any], now: Optional[datetime] = None) -> str:
    status = str(item.get("status") or "").strip().lower()
    if status in {"submitted", "late", "completed"} or _task_has_submission(item):
        return "submitted"
    if status == "overdue":
        return "overdue"
    deadline_at = str(item.get("deadline_at") or "").strip()
    if deadline_at:
        try:
            deadline = datetime.fromisoformat(deadline_at)
            if (now or datetime.now()) > deadline:
                return "overdue"
        except Exception:
            pass
    return "pending"


@app.get("/tasks", response_class=HTMLResponse)
def tasks_page(
    request: Request,
    status: str = "pending",
    err: Optional[str] = None,
    ok: Optional[str] = None,
    workspace: Optional[str] = None,
):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)

    if u["role"] == "user":
        all_items = task_list_instances_for_user(user_id=int(u["id"]), status=None)
        forms = []
        owned_forms = []
        assignable_users = []
    else:
        team = _user_team_int(u)
        all_items = task_list_instances_for_scope(role=u["role"], team=team)
        all_items = [x for x in all_items if int(x.get("creator_user_id") or 0) == int(u["id"])]
        forms = task_list_forms_for_actor(role=u["role"], team=team, user_id=int(u["id"]))
        owned_forms = [f for f in forms if int(f.get("creator_user_id") or 0) == int(u["id"])]
        assignable_users = fetch_users_all() if u["role"] == "admin" else fetch_users_by_team(int(team or 0))
        assignable_users = [x for x in assignable_users if x.get("role") == "user"]
        for form in owned_forms:
            assigned_ids = [int(x) for x in _task_parse_json_list(form.get("assigned_user_ids_json")) if str(x).strip().isdigit()]
            form["assigned_user_ids"] = assigned_ids
            try:
                form_deadline = datetime.fromisoformat(str(form.get("deadline_at") or ""))
                form["deadline_date_value"] = form_deadline.strftime("%Y-%m-%d")
                form["deadline_time_value"] = form_deadline.strftime("%H:%M")
            except Exception:
                form["deadline_date_value"] = ""
                form["deadline_time_value"] = ""
        forms = owned_forms

    now = datetime.now()
    status_filter = status if status in {"submitted", "pending", "overdue"} else "pending"
    for item in all_items:
        item["status_bucket"] = _task_status_bucket(item, now)
    items = [x for x in all_items if x.get("status_bucket") == status_filter]
    now_iso = now.isoformat()
    summary = {
        "pending": len([x for x in all_items if x.get("status_bucket") == "pending"]),
        "submitted": len([x for x in all_items if x.get("status_bucket") == "submitted"]),
        "overdue": len([x for x in all_items if x.get("status_bucket") == "overdue"]),
        "total": len(all_items),
        "now_iso": now_iso,
    }
    workspace_mode = "assigned" if str(workspace or "").strip().lower() in {"assigned", "modify"} else "create"
    return templates.TemplateResponse(
        "tasks.html",
        {
            "request": request,
            "user": u,
            "instances": items,
            "forms": forms,
            "owned_forms": owned_forms,
            "assignable_users": assignable_users,
            "status_filter": status_filter,
            "summary": summary,
            "workspace_mode": workspace_mode,
            "error": err,
            "success": ok,
        },
    )


@app.post("/tasks/create")
async def tasks_create(
    request: Request,
    title: str = Form(...),
    custom_title: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    assigned_scope: str = Form(...),
    assigned_user_ids: Optional[List[str]] = Form(None),
    assigned_team_id: Optional[str] = Form(None),
    deadline_date: str = Form(...),
    deadline_time: str = Form(...),
    allowed_types: Optional[List[str]] = Form(None),
    question_type: str = Form("upload"),
    ideal_value: Optional[str] = Form(None),
    number_min: Optional[str] = Form(None),
    number_max: Optional[str] = Form(None),
    number_unit: Optional[str] = Form(None),
    image_upload_count: Optional[str] = Form(None),
    ai_enabled: Optional[str] = Form(None),
    repeat_enabled: Optional[str] = Form(None),
    repeat_type: Optional[str] = Form(None),
    repeat_interval_days: Optional[str] = Form(None),
    allow_resubmission: Optional[str] = Form(None),
):
    u = current_user(request)
    if not require_role(u, ["admin", "coadmin"]):
        return RedirectResponse("/login", status_code=303)

    if (title or "").strip().lower() == "custom":
        title = (custom_title or "").strip()
    if not (title or "").strip():
        return RedirectResponse("/tasks?err=invalid_task_title", status_code=303)

    scope = (assigned_scope or "").strip().lower()
    if scope not in {"users", "team"}:
        return RedirectResponse("/tasks?err=invalid_assignment_scope", status_code=303)

    deadline = _task_parse_deadline(deadline_date, deadline_time)
    if not deadline:
        return RedirectResponse("/tasks?err=invalid_deadline", status_code=303)
    allowed = [x for x in (allowed_types or []) if x in TASK_ALLOWED_TYPES]
    if not allowed:
        allowed = ["photo"]
    qtype = (question_type or "upload").strip().lower()
    if qtype not in {"upload", "number", "upload_number"}:
        qtype = "upload"
    if qtype in {"upload", "upload_number"} and not allowed:
        allowed = ["photo"]
    if qtype == "number":
        allowed = []
    try:
        requested_image_upload_count = int((image_upload_count or "1").strip())
    except Exception:
        requested_image_upload_count = 1
    requested_image_upload_count = 2 if requested_image_upload_count == 2 else 1
    if (title or "").strip().lower() == "odometer reading":
        requested_image_upload_count = 1

    qmin: Optional[float] = None
    qmax: Optional[float] = None
    if (number_min or "").strip():
        try:
            qmin = float((number_min or "").strip())
        except Exception:
            return RedirectResponse("/tasks?err=invalid_number_min", status_code=303)
    if (number_max or "").strip():
        try:
            qmax = float((number_max or "").strip())
        except Exception:
            return RedirectResponse("/tasks?err=invalid_number_max", status_code=303)
    if (ideal_value or "").strip():
        try:
            qmax = float((ideal_value or "").strip())
        except Exception:
            return RedirectResponse("/tasks?err=invalid_ideal_value", status_code=303)
    if qmin is not None and qmax is not None and qmin > qmax:
        return RedirectResponse("/tasks?err=invalid_number_range", status_code=303)

    repeat_on = bool(repeat_enabled)
    rep_type = (repeat_type or "daily").strip().lower()
    if repeat_on and rep_type not in TASK_REPEAT_TYPES:
        rep_type = "daily"
    rep_days = None
    if rep_type == "interval":
        try:
            rep_days = max(1, int((repeat_interval_days or "1").strip()))
        except Exception:
            rep_days = 1

    team_view = _user_team_int(u)
    assignees: List[int] = []
    assigned_team: Optional[int] = None

    if scope == "users":
        raw_tokens = [str(x).strip() for x in (assigned_user_ids or []) if str(x).strip()]
        for token in raw_tokens:
            try:
                assignees.append(int(token))
            except Exception:
                continue
        if u["role"] == "coadmin":
            allowed_ids = {int(x["id"]) for x in fetch_users_by_team(int(team_view or 0)) if x.get("role") == "user"}
            assignees = [uid for uid in assignees if uid in allowed_ids]
    else:
        if u["role"] == "admin":
            try:
                assigned_team = int((assigned_team_id or "").strip())
            except Exception:
                assigned_team = None
        else:
            assigned_team = int(team_view or 0)
        if assigned_team is not None:
            assignees = [int(x["id"]) for x in fetch_users_by_team(int(assigned_team)) if x.get("role") == "user"]

    if not assignees:
        return RedirectResponse("/tasks?err=no_valid_assignees", status_code=303)

    form_id = task_create_form(
        title=title,
        description=description,
        creator_user_id=int(u["id"]),
        creator_role=u["role"],
        assigned_scope=scope,
        assigned_user_ids=assignees if scope == "users" else None,
        assigned_team_id=assigned_team,
        deadline_at=deadline.isoformat(),
        allowed_types=allowed,
        ai_enabled=bool(ai_enabled),
        repeat_enabled=repeat_on,
        repeat_type=rep_type if repeat_on else None,
        repeat_interval_days=rep_days,
        allow_resubmission=False,
        question_type=qtype,
        number_min=qmin,
        number_max=qmax,
        number_unit=number_unit,
        image_upload_count=requested_image_upload_count,
        priority="medium",
        status="active",
    )
    for uid in assignees:
        user_obj = get_user_by_id(int(uid))
        team_id = _user_team_int(user_obj)
        iid = task_create_instance(
            form_id=form_id,
            assigned_user_id=int(uid),
            assigned_team_id=team_id,
            deadline_at=deadline.isoformat(),
            cycle_key=_task_cycle_key(deadline),
            status="pending",
        )
        task_log_activity(
            task_instance_id=iid,
            action="task_created",
            actor_user_id=int(u["id"]),
            actor_role=u["role"],
            meta={"form_id": form_id, "assigned_user_id": uid},
        )
    log_event(
        admin_logger,
        "INFO",
        "TASK_FORM_CREATED",
        "Task form created",
        creator_user_id=int(u["id"]),
        form_id=form_id,
        assignee_count=len(assignees),
    )
    return RedirectResponse("/tasks?workspace=modify", status_code=303)


@app.post("/tasks/forms/{form_id}/update")
async def tasks_update(
    request: Request,
    form_id: int,
    assigned_scope: str = Form(...),
    assigned_user_ids: Optional[List[str]] = Form(None),
    assigned_team_id: Optional[str] = Form(None),
    deadline_date: str = Form(...),
    deadline_time: str = Form(...),
    repeat_enabled: Optional[str] = Form(None),
    repeat_type: Optional[str] = Form(None),
    repeat_interval_days: Optional[str] = Form(None),
):
    u = current_user(request)
    if not require_role(u, ["admin", "coadmin"]):
        return RedirectResponse("/login", status_code=303)
    csrf_rejection = await _require_csrf(request)
    if csrf_rejection is not None:
        return csrf_rejection

    form = task_get_form(int(form_id))
    if not form:
        return RedirectResponse("/tasks?err=task_not_found", status_code=303)
    if int(form.get("creator_user_id") or 0) != int(u["id"]):
        return RedirectResponse("/tasks?err=task_not_found", status_code=303)

    team_view = _user_team_int(u)

    scope = (assigned_scope or "").strip().lower()
    if scope not in {"users", "team"}:
        return RedirectResponse("/tasks?err=invalid_assignment_scope", status_code=303)

    deadline = _task_parse_deadline(deadline_date, deadline_time)
    if not deadline:
        return RedirectResponse("/tasks?err=invalid_deadline", status_code=303)

    repeat_on = bool(repeat_enabled)
    rep_type = (repeat_type or "daily").strip().lower()
    if repeat_on and rep_type not in TASK_REPEAT_TYPES:
        return RedirectResponse("/tasks?err=invalid_repeat_type", status_code=303)
    rep_days = None
    if repeat_on and rep_type == "custom":
        try:
            rep_days = max(1, int((repeat_interval_days or "1").strip()))
        except Exception:
            return RedirectResponse("/tasks?err=invalid_repeat_interval", status_code=303)

    assignees: List[int] = []
    assigned_team: Optional[int] = None
    if scope == "users":
        raw_tokens = [str(x).strip() for x in (assigned_user_ids or []) if str(x).strip()]
        for token in raw_tokens:
            if token.isdigit():
                assignees.append(int(token))
        if u["role"] == "coadmin":
            allowed_ids = {int(x["id"]) for x in fetch_users_by_team(int(team_view or 0)) if x.get("role") == "user"}
            assignees = [uid for uid in assignees if uid in allowed_ids]
    else:
        if u["role"] == "admin":
            try:
                assigned_team = int((assigned_team_id or "").strip())
            except Exception:
                assigned_team = None
        else:
            assigned_team = int(team_view or 0)
        if assigned_team is not None:
            assignees = [int(x["id"]) for x in fetch_users_by_team(int(assigned_team)) if x.get("role") == "user"]

    assignees = list(dict.fromkeys([int(x) for x in assignees if int(x) > 0]))
    if not assignees:
        return RedirectResponse("/tasks?err=no_valid_assignees", status_code=303)

    deadline_iso = deadline.isoformat()
    cycle_key = _task_cycle_key(deadline)
    next_status = "overdue" if datetime.now() > deadline else "pending"

    task_update_form(
        form_id=int(form_id),
        assigned_scope=scope,
        assigned_user_ids=assignees if scope == "users" else None,
        assigned_team_id=assigned_team,
        deadline_at=deadline_iso,
        repeat_enabled=repeat_on,
        repeat_type=rep_type if repeat_on else None,
        repeat_interval_days=rep_days,
    )

    existing = task_list_instances_for_form(int(form_id))
    open_instances = [
        row for row in existing
        if not row.get("submission_id") and str(row.get("status") or "").lower() not in {"submitted", "late", "completed"}
    ]
    open_by_user = {int(row["assigned_user_id"]): row for row in open_instances if row.get("assigned_user_id") is not None}
    desired_set = set(assignees)

    for uid in assignees:
        user_obj = get_user_by_id(int(uid))
        instance_team_id = assigned_team if scope == "team" else _user_team_int(user_obj)
        current = open_by_user.get(int(uid))
        if current:
            task_update_instance_assignment(
                instance_id=int(current["id"]),
                assigned_user_id=int(uid),
                assigned_team_id=instance_team_id,
                deadline_at=deadline_iso,
                cycle_key=cycle_key,
                status=next_status,
            )
        else:
            iid = task_create_instance(
                form_id=int(form_id),
                assigned_user_id=int(uid),
                assigned_team_id=instance_team_id,
                deadline_at=deadline_iso,
                cycle_key=cycle_key,
                status=next_status,
            )
            task_log_activity(
                task_instance_id=iid,
                action="task_reassigned",
                actor_user_id=int(u["id"]),
                actor_role=u["role"],
                meta={"form_id": int(form_id), "assigned_user_id": int(uid)},
            )

    for row in open_instances:
        uid = int(row.get("assigned_user_id") or 0)
        if uid and uid not in desired_set:
            task_delete_instance(int(row["id"]))

    try:
        task_log_activity(
            task_instance_id=0,
            action="task_form_updated",
            actor_user_id=int(u["id"]),
            actor_role=u["role"],
            meta={
                "form_id": int(form_id),
                "assigned_scope": scope,
                "assigned_user_ids": assignees if scope == "users" else [],
                "assigned_team_id": assigned_team,
                "deadline_at": deadline_iso,
                "repeat_enabled": repeat_on,
                "repeat_type": rep_type if repeat_on else None,
                "repeat_interval_days": rep_days,
            },
        )
    except Exception:
        log_event(
            error_logger,
            "ERROR",
            "TASK_FORM_UPDATE_AUDIT_FAIL",
            "Task form update audit logging failed",
            exc_info=True,
            form_id=int(form_id),
            actor_user_id=int(u["id"]),
        )
    return RedirectResponse("/tasks?workspace=modify&ok=Task+modified+successfully", status_code=303)


@app.post("/tasks/forms/{form_id}/delete")
async def tasks_delete(
    request: Request,
    form_id: int,
):
    u = current_user(request)
    if not require_role(u, ["admin", "coadmin"]):
        return RedirectResponse("/login", status_code=303)
    csrf_rejection = await _require_csrf(request)
    if csrf_rejection is not None:
        return csrf_rejection

    form = task_get_form(int(form_id))
    if not form:
        return RedirectResponse("/tasks?err=task_not_found", status_code=303)
    if int(form.get("creator_user_id") or 0) != int(u["id"]):
        return RedirectResponse("/tasks?err=task_not_found", status_code=303)

    try:
        task_log_activity(
            task_instance_id=0,
            action="task_form_deleted",
            actor_user_id=int(u["id"]),
            actor_role=u["role"],
            meta={
                "form_id": int(form_id),
                "form_title": form.get("title"),
            },
        )
    except Exception:
        log_event(
            error_logger,
            "ERROR",
            "TASK_FORM_DELETE_AUDIT_FAIL",
            "Task form delete audit logging failed",
            exc_info=True,
            form_id=int(form_id),
            actor_user_id=int(u["id"]),
        )

    task_delete_form(int(form_id))
    return RedirectResponse("/tasks?workspace=modify&ok=Task+removed+successfully", status_code=303)


@app.post("/tasks/{instance_id}/submit")
async def tasks_submit(
    request: Request,
    instance_id: int,
    remarks: Optional[str] = Form(None),
    entered_number: Optional[str] = Form(None),
    response_file: Optional[UploadFile] = File(None),
    response_file_2: Optional[UploadFile] = File(None),
    response_file_start: Optional[UploadFile] = File(None),
    response_file_end: Optional[UploadFile] = File(None),
):
    u = current_user(request)
    if not require_role(u, ["user"]):
        return RedirectResponse("/login", status_code=303)

    item = task_get_instance(int(instance_id))
    if not item or int(item.get("assigned_user_id") or 0) != int(u["id"]):
        return RedirectResponse("/tasks?err=task_not_found", status_code=303)
    if (item.get("status") or "") == "overdue":
        return RedirectResponse("/tasks?status=overdue", status_code=303)

    old = task_get_submission(int(instance_id))
    if old:
        return RedirectResponse("/tasks?err=resubmission_not_allowed", status_code=303)

    qtype = (item.get("question_type") or "upload").strip().lower()
    allowed_types = [x for x in _task_parse_json_list(item.get("allowed_types_json")) if x in TASK_ALLOWED_TYPES]
    number_value: Optional[Any] = None
    if (entered_number or "").strip():
        try:
            number_value = float((entered_number or "").strip())
        except Exception:
            return RedirectResponse("/tasks?err=invalid_number", status_code=303)
    if qtype in {"number", "upload_number"} and number_value is not None:
        mn = item.get("number_min")
        mx = item.get("number_max")
        try:
            if mn is not None and number_value < float(mn):
                return RedirectResponse("/tasks?err=number_below_min", status_code=303)
            if mx is not None and number_value > float(mx):
                return RedirectResponse("/tasks?err=number_above_max", status_code=303)
        except Exception:
            pass
    if qtype == "number" and number_value is None:
        return RedirectResponse("/tasks?err=number_required", status_code=303)

    relative_url: Optional[str] = None
    relative_url_2: Optional[str] = None
    image_taken_at: Optional[str] = None
    image_taken_at_2: Optional[str] = None
    detected_type: Optional[str] = None
    full_path: Optional[str] = None
    full_path_2: Optional[str] = None
    file_size = 0
    avg_kmpl_val: Optional[float] = None
    distance_diff_val: Optional[float] = None
    fuel_consumed_val: Optional[float] = None
    task_text = f"{item.get('title') or ''} {item.get('description') or ''}".lower()
    question = None
    try:
        if item.get("form_id") is not None:
            question = task_get_question(int(item.get("form_id")))
    except Exception:
        question = None
    task_spec = _task_build_spec(item, question)
    task_kind = str(task_spec.get("task_kind") or "")
    image_upload_count = 1
    try:
        image_upload_count = 2 if int(item.get("image_upload_count") or 1) == 2 else 1
    except Exception:
        image_upload_count = 1
    is_odometer_task = task_kind == "odometer"
    is_earthing_task = task_kind == "earthing"
    is_fire_point_task = task_kind == "fire_point"
    is_timestamp_task = task_kind == "timestamp_value"
    is_unsupported_task = task_kind == "unsupported_notice"
    fire_point_present: Optional[bool] = None
    fire_point_value: Optional[str] = None

    if is_odometer_task:
        if not response_file_start or not response_file_start.filename or not response_file_end or not response_file_end.filename:
            return RedirectResponse("/tasks?err=odometer_images_required", status_code=303)
        try:
            configured_kmpl = item.get("number_max")
            if configured_kmpl is None:
                raise ValueError("missing")
            avg_kmpl_val = float(configured_kmpl)
            if avg_kmpl_val <= 0:
                raise ValueError("invalid")
        except Exception:
            return RedirectResponse("/tasks?err=invalid_average_kmpl_config", status_code=303)
        detected_type = "photo"
        ext1 = os.path.splitext(response_file_start.filename.lower())[1]
        ext2 = os.path.splitext(response_file_end.filename.lower())[1]
        if ext1 not in _task_allowed_ext("photo") or ext2 not in _task_allowed_ext("photo"):
            return RedirectResponse("/tasks?err=invalid_file_ext", status_code=303)
        raw1 = await response_file_start.read()
        raw2 = await response_file_end.read()
        max_mb = int((os.getenv("TASK_MAX_FILE_MB", "30") or "30").strip() or 30)
        file_size = len(raw1) + len(raw2)
        if file_size > (max_mb * 1024 * 1024 * 2):
            return RedirectResponse("/tasks?err=file_too_large", status_code=303)
        payload_error = _validate_upload_payload(raw1, "photo", ext1) or _validate_upload_payload(raw2, "photo", ext2)
        if payload_error:
            return _upload_error_redirect("/tasks", payload_error)
        now = datetime.now()
        subdir = os.path.join(UPLOAD_DIR, "tasks", now.strftime("%Y"), now.strftime("%m"))
        os.makedirs(subdir, exist_ok=True)
        filename1 = f"task_{instance_id}_start_{uuid.uuid4().hex}{ext1}"
        filename2 = f"task_{instance_id}_end_{uuid.uuid4().hex}{ext2}"
        full_path = os.path.join(subdir, filename1)
        full_path_2 = os.path.join(subdir, filename2)
        with open(full_path, "wb") as f:
            f.write(raw1)
        with open(full_path_2, "wb") as f:
            f.write(raw2)
        _warm_cached_preview(full_path)
        _warm_cached_preview(full_path_2)
        image_taken_at = _extract_image_taken_at(full_path)
        image_taken_at_2 = _extract_image_taken_at(full_path_2)
        relative_url = f"/uploads/tasks/{now.strftime('%Y')}/{now.strftime('%m')}/{filename1}"
        relative_url_2 = f"/uploads/tasks/{now.strftime('%Y')}/{now.strftime('%m')}/{filename2}"
    elif qtype in {"upload", "upload_number"}:
        if not response_file or not response_file.filename:
            return RedirectResponse("/tasks?err=file_required", status_code=303)
        if image_upload_count == 2 and (not response_file_2 or not response_file_2.filename):
            return RedirectResponse("/tasks?err=second_file_required", status_code=303)
        detected_type = _task_detect_file_type(response_file.filename)
        if not detected_type or detected_type not in allowed_types:
            return RedirectResponse("/tasks?err=invalid_file_type", status_code=303)
        ext = os.path.splitext(response_file.filename.lower())[1]
        if ext not in _task_allowed_ext(detected_type):
            return RedirectResponse("/tasks?err=invalid_file_ext", status_code=303)
        detected_type_2 = None
        ext_2 = ""
        if image_upload_count == 2 and response_file_2 and response_file_2.filename:
            detected_type_2 = _task_detect_file_type(response_file_2.filename)
            if not detected_type_2 or detected_type_2 != detected_type or detected_type_2 not in allowed_types:
                return RedirectResponse("/tasks?err=invalid_second_file_type", status_code=303)
            ext_2 = os.path.splitext(response_file_2.filename.lower())[1]
            if ext_2 not in _task_allowed_ext(detected_type_2):
                return RedirectResponse("/tasks?err=invalid_second_file_ext", status_code=303)
        max_mb = int((os.getenv("TASK_MAX_FILE_MB", "30") or "30").strip() or 30)
        raw = await response_file.read()
        raw_2 = b""
        if image_upload_count == 2 and response_file_2:
            raw_2 = await response_file_2.read()
        file_size = len(raw) + len(raw_2)
        if file_size > (max_mb * 1024 * 1024 * image_upload_count):
            return RedirectResponse("/tasks?err=file_too_large", status_code=303)
        payload_error = _validate_upload_payload(raw, detected_type, ext)
        if payload_error:
            return _upload_error_redirect("/tasks", payload_error)
        if image_upload_count == 2 and raw_2:
            payload_error = _validate_upload_payload(raw_2, detected_type_2 or detected_type, ext_2)
            if payload_error:
                return _upload_error_redirect("/tasks", payload_error)
        now = datetime.now()
        subdir = os.path.join(UPLOAD_DIR, "tasks", now.strftime("%Y"), now.strftime("%m"))
        os.makedirs(subdir, exist_ok=True)
        filename = f"task_{instance_id}_{uuid.uuid4().hex}{ext}"
        full_path = os.path.join(subdir, filename)
        with open(full_path, "wb") as f:
            f.write(raw)
        _warm_cached_preview(full_path)
        image_taken_at = _extract_image_taken_at(full_path)
        relative_url = f"/uploads/tasks/{now.strftime('%Y')}/{now.strftime('%m')}/{filename}"
        if image_upload_count == 2 and raw_2:
            filename_2 = f"task_{instance_id}_2_{uuid.uuid4().hex}{ext_2}"
            full_path_2 = os.path.join(subdir, filename_2)
            with open(full_path_2, "wb") as f:
                f.write(raw_2)
            _warm_cached_preview(full_path_2)
            image_taken_at_2 = _extract_image_taken_at(full_path_2)
            relative_url_2 = f"/uploads/tasks/{now.strftime('%Y')}/{now.strftime('%m')}/{filename_2}"
    else:
        # Keep DB compatibility where file_type may be constrained to pdf/photo/video.
        detected_type = "pdf"
        now = datetime.now()

    ai_requested = bool(item.get("ai_enabled"))
    ai_status = "not_requested"
    ai_result = None
    ai_payload: Optional[Dict[str, Any]] = None
    extracted_value: Optional[float] = None
    should_run_ai = bool(
        full_path
        and detected_type in {"photo", "video"}
        and ai_requested
        and not is_timestamp_task
        and not is_unsupported_task
        and not is_fire_point_task
        and not is_odometer_task
    )
    if is_timestamp_task:
        number_value = image_taken_at or task_spec.get("missing_value_message")
        ai_status = "completed"
        ai_result = f"Timestamp value used: {number_value}" if number_value else "Timestamp metadata not found"
        ai_payload = {
            "engine_used": "metadata",
            "validation_status": "validated" if image_taken_at else "no_value",
            "validation_reason": "" if image_taken_at else "No timestamp metadata found",
            "extracted_values": {"value": number_value} if number_value else {},
        }
    elif is_unsupported_task:
        number_value = task_spec.get("unsupported_message") or "currently not supported"
        ai_status = "completed"
        ai_result = str(number_value)
        ai_payload = {
            "engine_used": "rule_config",
            "validation_status": "validated",
            "validation_reason": "",
            "extracted_values": {"value": number_value},
        }
    if is_odometer_task and full_path and full_path_2:
        ai_status = "completed"
        try:
            o1 = await run_in_threadpool(run_ocr, full_path, uuid.uuid4().hex, "odometer")
            o2 = await run_in_threadpool(run_ocr, full_path_2, uuid.uuid4().hex, "odometer")
            start_val = await _task_openai_numeric_first(
                image_path=full_path,
                task_title=f"{item.get('title') or ''} start",
                task_description=str(item.get("description") or ""),
                local_result={"ocr": o1, "position": "start"},
            )
            end_val = await _task_openai_numeric_first(
                image_path=full_path_2,
                task_title=f"{item.get('title') or ''} end",
                task_description=str(item.get("description") or ""),
                local_result={"ocr": o2, "position": "end"},
            )
            if start_val is None:
                start_val = _task_numeric_from_ocr(o1)
            if end_val is None:
                end_val = _task_numeric_from_ocr(o2)
            if start_val is None:
                start_val = await _task_openai_numeric_fallback(
                    image_path=full_path,
                    task_title=f"{item.get('title') or ''} start",
                    task_description=str(item.get("description") or ""),
                    local_result={"ocr": o1, "position": "start"},
                )
            if end_val is None:
                end_val = await _task_openai_numeric_fallback(
                    image_path=full_path_2,
                    task_title=f"{item.get('title') or ''} end",
                    task_description=str(item.get("description") or ""),
                    local_result={"ocr": o2, "position": "end"},
                )
            if start_val is None or end_val is None:
                raise ValueError("Could not extract start/end odometer values")
            distance_diff_val = abs(end_val - start_val)
            fuel_consumed_val = distance_diff_val / float(avg_kmpl_val or 1.0)
            number_value = distance_diff_val
            ai_payload = {
                "engine_used": "openai_then_local",
                "extracted_values": {
                    "start": start_val,
                    "end": end_val,
                    "distance": distance_diff_val,
                    "fuel_consumed": fuel_consumed_val,
                },
                "validation_status": "validated",
                "validation_reason": "",
            }
            ai_result = (
                f"Odometer processed: start={start_val:.2f}, end={end_val:.2f}, "
                f"distance={distance_diff_val:.2f} km, avg_kmpl={avg_kmpl_val:.2f}, "
                f"fuel_consumed={fuel_consumed_val:.2f} L"
            )
            number_value = f"Diff {distance_diff_val:.2f}, Fuel {fuel_consumed_val:.2f} L"
        except Exception as e:
            ai_status = "failed"
            ai_result = f"Odometer processing failed: {e}"
            number_value = None
            ai_payload = {
                "engine_used": "openai_then_local",
                "validation_status": "failed",
                "validation_reason": str(e),
            }
    elif is_fire_point_task and full_path:
        try:
            fire_detection = None
            if openai_available():
                try:
                    openai_fire = await run_in_threadpool(
                        analyze_task_image_with_openai,
                        image_path=full_path,
                        task_title=str(item.get("title") or ""),
                        task_description=str(item.get("description") or ""),
                        local_result={},
                        task_spec=task_spec,
                        mode_hint="present_absent",
                    )
                    if openai_fire.get("present") is not None:
                        fire_detection = {
                            "openai_result": openai_fire,
                            "present": bool(openai_fire.get("present")),
                            "confidence": float(openai_fire.get("confidence") or 0.0),
                            "source": "openai",
                        }
                except Exception as e:
                    fire_detection = {"openai_error": str(e)}
            if fire_detection is None or fire_detection.get("present") is None:
                local_fire = await run_in_threadpool(detect_task_objects, "fire_point", full_path)
                fire_detection = {**(fire_detection or {}), **local_fire}
            fire_point_present = bool(fire_detection.get("present"))
            fire_point_value = "Present" if fire_point_present else "Absent"
            number_value = fire_point_value
            ai_status = "completed"
            ai_payload = fire_detection
            ai_result = (
                f"Fire-point detection ({fire_detection.get('source') or 'model'}): "
                f"{fire_point_value}"
            )
        except Exception as e:
            fire_point_present = False
            fire_point_value = "Absent"
            number_value = fire_point_value
            ai_status = "failed"
            ai_result = f"Fire-point detection failed: {e}"
    elif should_run_ai:
        ai_status = "queued"
        ai_payload = await _task_process_ai(item, full_path, detected_type)
        ai_status = ai_payload.get("status") or "failed"
        ai_result = ai_payload.get("summary")
        extracted_raw = ((ai_payload or {}).get("extracted_values") or {}).get("value")
        if extracted_raw is not None and extracted_raw != "":
            try:
                extracted_value = float(extracted_raw)
            except Exception:
                if number_value is None:
                    number_value = str(extracted_raw)
                extracted_value = None

    if number_value is None and extracted_value is not None:
        number_value = extracted_value

    semantic_alert_reason = _task_semantic_alert_reason(item, number_value, task_spec, ai_payload)
    missing_object_alert_reason = _task_missing_object_alert_reason(item, task_spec, ai_payload)
    submission_alert_reason = semantic_alert_reason or missing_object_alert_reason

    task_upsert_submission(
        task_instance_id=int(instance_id),
        user_id=int(u["id"]),
        file_path=relative_url or f"task://number/{instance_id}",
        file_type=detected_type or "number",
        file_size=file_size,
        remarks=remarks,
        submitted_value=number_value,
        file_path_2=relative_url_2,
        avg_kmpl=avg_kmpl_val,
        distance_diff=distance_diff_val,
        fuel_consumed=fuel_consumed_val,
        image_taken_at=image_taken_at,
        image_taken_at_2=image_taken_at_2,
        ai_requested=should_run_ai,
        ai_status=ai_status,
        ai_result_reference=ai_result,
    )
    try:
        task_upsert_ai_result(
            task_instance_id=int(instance_id),
            ai_engine_type=str((ai_payload or {}).get("engine_used") or ("local" if is_fire_point_task or is_odometer_task else "none")),
            processing_status=ai_status,
            extracted_text=str((ai_payload or {}).get("extracted_text") or ai_result or ""),
            extracted_values=(ai_payload or {}).get("extracted_values") or ({"value": number_value} if number_value is not None else {}),
            analysis_summary=ai_result,
            validation_status=str((ai_payload or {}).get("validation_status") or ("completed" if ai_status == "completed" else ai_status)),
            alert_triggered=bool((is_fire_point_task and fire_point_present is False) or submission_alert_reason),
            alert_reason=(
                f"Fire fighting equipment absent for task '{item.get('title')}'."
                if is_fire_point_task and fire_point_present is False
                else str((ai_payload or {}).get("validation_reason") or submission_alert_reason or "")
            ),
        )
    except Exception:
        pass
    try:
        deadline = datetime.fromisoformat(str(item.get("deadline_at")))
    except Exception:
        deadline = now
    status = "late" if now > deadline else "submitted"
    task_mark_instance_status(instance_id=int(instance_id), status=status, submitted_at=now.isoformat())

    if submission_alert_reason:
        try:
            _task_send_submission_alert(
                item,
                submission_alert_reason,
                severity="high",
            )
        except Exception:
            pass

    task_log_activity(
        task_instance_id=int(instance_id),
        action="task_submitted",
        actor_user_id=int(u["id"]),
        actor_role=u["role"],
        meta={"status": status, "file_type": detected_type, "ai_status": ai_status},
    )
    return RedirectResponse("/tasks", status_code=303)


@app.get("/api/tasks/my")
def api_tasks_my(request: Request, status: str = "all"):
    u = current_user(request)
    if not require_role(u, ["user"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return {"tasks": task_list_instances_for_user(user_id=int(u["id"]), status=status)}


@app.get("/api/tasks/assigned")
def api_tasks_assigned(request: Request):
    u = current_user(request)
    if not require_role(u, ["admin", "coadmin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return {"tasks": task_list_instances_for_scope(role=u["role"], team=_user_team_int(u))}


# -----------------------
# Coadmin pages
# -----------------------
@app.get("/coadmin/{team_id}", response_class=HTMLResponse)
def coadmin_page(request: Request, team_id: int):
    u = current_user(request)
    if not require_role(u, ["coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)

    viewer_team = _user_team_int(u)
    if u["role"] == "coadmin" and viewer_team != int(team_id):
        return HTMLResponse("Forbidden", status_code=403)

    readings = _augment_readings(_filter_today_readings(fetch_readings_by_team(int(team_id))))
    task_instances = task_list_instances_for_scope(role="coadmin", team=int(team_id))
    alerts = fetch_alerts_for_coadmin(int(team_id), unread_only=False)
    unread = count_unread_coadmin(int(team_id))
    latest_id = get_latest_reading_id_team(int(team_id))

    messages_team = int(team_id) if u["role"] == "admin" else viewer_team
    messages = fetch_messages_for_user(role=u["role"], user_id=int(u["id"]), team=messages_team)
    users_team = fetch_users_by_team(int(team_id))
    unassigned_users = fetch_users_without_team()
    context = build_coadmin_dashboard_context(
        user=u,
        team_id=team_id,
        readings=readings,
        task_instances=task_instances,
        alerts=alerts,
        unread_count=unread,
        latest_reading_id=latest_id,
        messages=messages,
        users_team=users_team,
    )
    context["unassigned_users"] = unassigned_users
    context["request"] = request
    return templates.TemplateResponse(
        "coadmin.html",
        context,
    )


# -----------------------
# Admin pages
# -----------------------
@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request):
    u = current_user(request)
    if not require_role(u, ["admin"]):
        return RedirectResponse("/login", status_code=303)

    readings = _augment_readings(_filter_today_readings(fetch_readings_all()))
    task_instances = task_list_instances_for_scope(role="admin", team=None)
    alerts = fetch_alerts_for_admin(unread_only=False)
    unread = count_unread_admin()
    latest_id = get_latest_reading_id_all()

    messages = fetch_messages_for_user(role=u["role"], user_id=int(u["id"]), team=None)
    all_users = fetch_users_all()
    unassigned_users = fetch_users_without_team()
    context = build_admin_dashboard_context(
        user=u,
        readings=readings,
        task_instances=task_instances,
        alerts=alerts,
        unread_count=unread,
        latest_reading_id=latest_id,
        messages=messages,
        all_users=all_users,
        teams=[1, 2, 3, 4, 5, 6],
    )
    context["unassigned_users"] = unassigned_users
    context["request"] = request
    return templates.TemplateResponse(
        "admin.html",
        context,
    )


@app.get("/uploads/{upload_path:path}")
def serve_upload(request: Request, upload_path: str):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        raise HTTPException(status_code=401, detail="Unauthorized")
    source_path = _resolve_upload_relative_path(upload_path)
    if source_path is None:
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(
        path=source_path,
        headers={"Cache-Control": "private, max-age=300"},
    )


@app.get("/api/uploads/preview")
def upload_preview(request: Request, src: str):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        raise HTTPException(status_code=401, detail="Unauthorized")

    source_path = _resolve_upload_url_path(src)
    if source_path is None:
        raise HTTPException(status_code=404, detail="Image not found")

    preview_path = _generate_cached_preview(source_path)
    target_path = preview_path or source_path
    media_type = "image/jpeg" if preview_path else None
    return FileResponse(
        path=target_path,
        media_type=media_type,
        headers={"Cache-Control": "private, max-age=86400"},
    )


@app.get("/teams", response_class=HTMLResponse)
def teams_page(request: Request):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)

    if u["role"] == "admin":
        teams = [1, 2, 3, 4, 5, 6]
        unread = count_unread_admin()
    else:
        team_id = int(_user_team_int(u) or 0)
        teams = [team_id] if team_id > 0 else []
        unread = count_unread_coadmin(team_id) if u["role"] == "coadmin" and team_id > 0 else 0

    return templates.TemplateResponse(
        "teams.html",
        {
            "request": request,
            "user": u,
            "teams": teams,
            "unread_count": unread,
        },
    )


@app.get("/users", response_class=HTMLResponse)
def users_page(request: Request):
    u = current_user(request)
    if not require_role(u, ["coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)

    if u["role"] == "admin":
        unread = count_unread_admin()
        users = fetch_users_all()
    else:
        team_id = int(_user_team_int(u) or 0)
        unread = count_unread_coadmin(team_id) if team_id > 0 else 0
        users = fetch_users_by_team(team_id) if team_id > 0 else []

    return templates.TemplateResponse(
        "users.html",
        {
            "request": request,
            "user": u,
            "users_list": users,
            "unread_count": unread,
        },
    )


@app.get("/alerts", response_class=HTMLResponse)
def alerts_page(request: Request):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)

    if u["role"] == "admin":
        alerts = fetch_alerts_for_admin(unread_only=False)
        unread = count_unread_admin()
    elif u["role"] == "coadmin":
        team_id = int(_user_team_int(u) or 0)
        alerts = fetch_alerts_for_coadmin(team_id, unread_only=False) if team_id > 0 else []
        unread = count_unread_coadmin(team_id) if team_id > 0 else 0
    else:
        alerts = []
        unread = 0

    return templates.TemplateResponse(
        "alerts.html",
        {
            "request": request,
            "user": u,
            "alerts": alerts,
            "unread_count": unread,
        },
    )


@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)

    if u["role"] == "admin":
        unread = count_unread_admin()
        team_members: List[Dict[str, Any]] = []
        for team_no in [1, 2, 3, 4, 5, 6]:
            for member in fetch_users_by_team(team_no):
                if member.get("role") == "user":
                    team_members.append(member)
    elif u["role"] == "coadmin":
        team_id = int(_user_team_int(u) or 0)
        unread = count_unread_coadmin(team_id) if team_id > 0 else 0
        team_members = [member for member in fetch_users_by_team(team_id) if member.get("role") == "user"] if team_id > 0 else []
    else:
        unread = 0
        team_members = []

    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "user": u,
            "unread_count": unread,
            "unassigned_users": fetch_users_without_team(),
            "team_members": team_members,
            "available_teams": [1, 2, 3, 4, 5, 6],
        },
    )


@app.post("/settings/username")
async def settings_update_username(
    request: Request,
    username: str = Form(...),
):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)

    new_username = (username or "").strip()
    if not new_username:
        return RedirectResponse("/settings?err=Username is required.", status_code=303)

    existing = get_user_by_username(new_username)
    if existing and int(existing["id"]) != int(u["id"]):
        return RedirectResponse("/settings?err=Username already exists.", status_code=303)

    try:
        update_user_username(int(u["id"]), new_username)
        log_event(
            auth_logger,
            "INFO",
            "ACCOUNT_USERNAME_UPDATED",
            "Username updated from settings",
            user_id=int(u["id"]),
        )
        return RedirectResponse("/settings?info=Username updated successfully.", status_code=303)
    except Exception:
        log_event(
            auth_logger,
            "ERROR",
            "ACCOUNT_USERNAME_UPDATE_FAIL",
            "Failed to update username from settings",
            exc_info=True,
            user_id=int(u["id"]),
        )
        return RedirectResponse("/settings?err=Unable to update username right now.", status_code=303)


@app.post("/settings/password")
async def settings_update_password(
    request: Request,
    new_password: str = Form(...),
    confirm_password: str = Form(...),
):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)

    if len(new_password or "") < 6:
        return RedirectResponse("/settings?err=Password must be at least 6 characters.", status_code=303)
    if new_password != confirm_password:
        return RedirectResponse("/settings?err=Password and confirm password do not match.", status_code=303)

    try:
        clear_force = u.get("role") in {"admin", "coadmin"}
        update_user_password(int(u["id"]), hash_password(new_password), force_password_change=False if clear_force else None)
        log_event(
            auth_logger,
            "INFO",
            "ACCOUNT_PASSWORD_UPDATED",
            "Password updated from settings",
            user_id=int(u["id"]),
        )
        return RedirectResponse("/settings?info=Password updated successfully.", status_code=303)
    except Exception:
        log_event(
            auth_logger,
            "ERROR",
            "ACCOUNT_PASSWORD_UPDATE_FAIL",
            "Failed to update password from settings",
            exc_info=True,
            user_id=int(u["id"]),
        )
        return RedirectResponse("/settings?err=Unable to update password right now.", status_code=303)


@app.post("/team-members/add")
async def team_members_add(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
    team_id: Optional[str] = Form(None),
):
    u = current_user(request)
    if not require_role(u, ["admin", "coadmin"]):
        return RedirectResponse("/login", status_code=303)

    uname = (username or "").strip()
    if not uname:
        return _redirect_dashboard_with_message(u, err="Username is required.")
    if len(password or "") < 6:
        return _redirect_dashboard_with_message(u, err="Password must be at least 6 characters.")
    if password != confirm_password:
        return _redirect_dashboard_with_message(u, err="Password and confirm password do not match.")
    if get_user_by_username(uname):
        return _redirect_dashboard_with_message(u, err="Username already exists.")

    if u["role"] == "admin":
        try:
            assigned_team = int((team_id or "").strip())
        except Exception:
            return _redirect_dashboard_with_message(u, err="Select a valid team.")
        if assigned_team not in {1, 2, 3, 4, 5, 6}:
            return _redirect_dashboard_with_message(u, err="Select a valid team.")
    else:
        assigned_team = int(_user_team_int(u) or 0)
        if assigned_team <= 0:
            return _redirect_dashboard_with_message(u, err="Your account has no valid team.")

    try:
        create_user(uname, hash_password(password), "user", assigned_team)
        created = get_user_by_username(uname)
        if created and "@" in uname:
            update_user_identity(
                int(created["id"]),
                email=uname.lower(),
                display_name=uname.split("@")[0],
                auth_provider="password_assigned",
            )
        log_event(
            admin_logger,
            "INFO",
            "TEAM_MEMBER_ADD",
            "Team member account created",
            by_user_id=int(u["id"]),
            username=uname,
            team=assigned_team,
        )
        return _redirect_dashboard_with_message(u, info=f"Member {uname} added to Team {assigned_team}.")
    except Exception:
        log_event(error_logger, "ERROR", "TEAM_MEMBER_ADD_FAIL", "Failed to add team member", exc_info=True, username=uname)
        return _redirect_dashboard_with_message(u, err="Unable to add member right now.")


@app.post("/team-members/assign")
async def team_members_assign(
    request: Request,
    user_id: str = Form(...),
    team_id: Optional[str] = Form(None),
):
    u = current_user(request)
    if not require_role(u, ["admin", "coadmin"]):
        return RedirectResponse("/login", status_code=303)

    try:
        uid = int((user_id or "").strip())
    except Exception:
        return _redirect_dashboard_with_message(u, err="Select a valid user.")

    target_user = get_user_by_id(uid)
    if not target_user or target_user.get("role") != "user":
        return _redirect_dashboard_with_message(u, err="Selected user is invalid.")
    if not _is_effectively_unassigned_user(target_user):
        return _redirect_dashboard_with_message(u, err="Selected user is already assigned to a team.")

    if u["role"] == "admin":
        try:
            assigned_team = int((team_id or "").strip())
        except Exception:
            return _redirect_dashboard_with_message(u, err="Select a valid team.")
        if assigned_team not in {1, 2, 3, 4, 5, 6}:
            return _redirect_dashboard_with_message(u, err="Select a valid team.")
    else:
        assigned_team = int(_user_team_int(u) or 0)
        if assigned_team <= 0:
            return _redirect_dashboard_with_message(u, err="Your account has no valid team.")

    try:
        update_user_team(uid, assigned_team)
        provider_now = str(target_user.get("auth_provider") or "")
        if provider_now in {"password", "google"}:
            update_user_identity(int(uid), auth_provider=f"{provider_now}_assigned")
        log_event(
            admin_logger,
            "INFO",
            "TEAM_MEMBER_ASSIGN",
            "Existing user assigned to team",
            by_user_id=int(u["id"]),
            assigned_user_id=uid,
            team=assigned_team,
        )
        return _redirect_dashboard_with_message(
            u,
            info=f"Member {target_user.get('username')} assigned to Team {assigned_team}.",
        )
    except Exception:
        log_event(
            error_logger,
            "ERROR",
            "TEAM_MEMBER_ASSIGN_FAIL",
            "Failed to assign existing user to team",
            exc_info=True,
            assigned_user_id=uid,
        )
        return _redirect_dashboard_with_message(u, err="Unable to assign member right now.")


@app.post("/team-members/remove")
async def team_members_remove(
    request: Request,
    user_id: str = Form(...),
):
    u = current_user(request)
    if not require_role(u, ["admin", "coadmin"]):
        return RedirectResponse("/login", status_code=303)

    try:
        uid = int((user_id or "").strip())
    except Exception:
        return _redirect_dashboard_with_message(u, err="Select a valid user.")

    target_user = get_user_by_id(uid)
    if not target_user or target_user.get("role") != "user":
        return _redirect_dashboard_with_message(u, err="Selected user is invalid.")

    target_team = _user_team_int(target_user)
    if target_team is None or target_team <= 0:
        return _redirect_dashboard_with_message(u, err="Selected user is not assigned to any team.")

    if u["role"] == "coadmin":
        actor_team = int(_user_team_int(u) or 0)
        if actor_team <= 0 or actor_team != int(target_team):
            return _redirect_dashboard_with_message(u, err="You can remove members only from your own team.")

    try:
        update_user_team(uid, None)
        provider_now = str(target_user.get("auth_provider") or "")
        if provider_now.endswith("_assigned"):
            update_user_identity(int(uid), auth_provider=provider_now.replace("_assigned", ""))
        display_name = (target_user.get("display_name") or "").strip() or target_user.get("username") or str(uid)
        log_event(
            admin_logger,
            "INFO",
            "TEAM_MEMBER_REMOVE",
            "User removed from team",
            by_user_id=int(u["id"]),
            removed_user_id=uid,
            team=target_team,
        )
        return _redirect_dashboard_with_message(
            u,
            info=f"Member {display_name} removed from Team {target_team}.",
        )
    except Exception:
        log_event(
            error_logger,
            "ERROR",
            "TEAM_MEMBER_REMOVE_FAIL",
            "Failed to remove team member",
            exc_info=True,
            removed_user_id=uid,
        )
        return _redirect_dashboard_with_message(u, err="Unable to remove member right now.")


# -----------------------
# CSV Downloads
# -----------------------
import csv

def _csv_rows(readings):
    header = [
        "created_at",
        "team",
        "username",
        "meter_type",
        "label",
        "value",
        "filename",
        "filename_2",
        "odometer_start",
        "odometer_end",
        "distance_diff",
        "fuel_economy",
        "fuel_consumed",
    ]
    yield header
    for r in readings:
        # r is dict-like
        yield [
            r.get("created_at", ""),
            r.get("team", ""),
            r.get("username", ""),
            r.get("meter_type", ""),
            r.get("label", ""),
            r.get("value", ""),
            r.get("filename", ""),
            r.get("filename_2", ""),
            r.get("odometer_start", ""),
            r.get("odometer_end", ""),
            r.get("distance_diff", ""),
            r.get("fuel_economy", ""),
            r.get("fuel_consumed", ""),
        ]


def _stream_csv(filename, rows):
    def gen():
        import io
        buf = io.StringIO()
        writer = csv.writer(buf)
        for row in rows:
            buf.seek(0)
            buf.truncate(0)
            writer.writerow(row)
            yield buf.getvalue()

    return StreamingResponse(
        gen(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/download/csv/my")
def download_my_csv(request: Request):
    u = current_user(request)
    if not require_role(u, ["user"]):
        return RedirectResponse("/login", status_code=303)
    readings = fetch_readings_by_user(int(u["id"]))
    return _stream_csv("my_readings.csv", _csv_rows(readings))


@app.get("/download/csv/team/{team_id}")
def download_team_csv(request: Request, team_id: int):
    u = current_user(request)
    if not require_role(u, ["coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)
    if u["role"] == "coadmin" and _user_team_int(u) != int(team_id):
        return HTMLResponse("Forbidden", status_code=403)
    readings = fetch_readings_by_team(int(team_id))
    return _stream_csv(f"team_{team_id}_readings.csv", _csv_rows(readings))


@app.get("/download/csv/admin")
def download_admin_csv(request: Request):
    u = current_user(request)
    if not require_role(u, ["admin"]):
        return RedirectResponse("/login", status_code=303)
    log_event(audit_logger, "INFO", "ADMIN_EXPORT_CSV", "Admin exported all readings CSV", user_id=int(u["id"]))
    readings = fetch_readings_all()
    return _stream_csv("all_readings.csv", _csv_rows(readings))


# -----------------------
# Alerts API (live refresh)
# -----------------------
@app.get("/api/alerts/admin")
def api_alerts_admin(request: Request):
    u = current_user(request)
    if not require_role(u, ["admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    alerts = fetch_alerts_for_admin(unread_only=False)
    unread = count_unread_admin()
    return {"unread_count": unread, "alerts": alerts}


@app.get("/api/readings/admin/latest")
def api_latest_reading_admin(request: Request):
    u = current_user(request)
    if not require_role(u, ["admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return {"latest_id": get_latest_reading_id_all()}


@app.get("/api/readings/coadmin/latest")
def api_latest_reading_coadmin(request: Request, team: int):
    u = current_user(request)
    if not require_role(u, ["coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if u["role"] == "coadmin" and _user_team_int(u) != int(team):
        return JSONResponse({"error": "forbidden"}, status_code=403)
    return {"latest_id": get_latest_reading_id_team(int(team))}


# ✅ your JS likely calls: /api/alerts/coadmin?team=1
@app.get("/api/alerts/coadmin")
def api_alerts_coadmin(request: Request, team: int):
    u = current_user(request)
    if not require_role(u, ["coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    if u["role"] == "coadmin" and _user_team_int(u) != int(team):
        return JSONResponse({"error": "forbidden"}, status_code=403)

    alerts = fetch_alerts_for_coadmin(int(team), unread_only=False)
    unread = count_unread_coadmin(int(team))
    return {"unread_count": unread, "alerts": alerts}


@app.post("/alerts/{alert_id}/read")
async def mark_read(request: Request, alert_id: int):
    u = current_user(request)
    if not require_role(u, ["coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)

    mark_alert_read(int(alert_id))

    if u["role"] == "admin":
        return RedirectResponse("/admin", status_code=303)
    return RedirectResponse(f"/coadmin/{u['team']}", status_code=303)


@app.post("/alerts/clear/admin")
async def clear_admin_alerts(request: Request):
    u = current_user(request)
    if not require_role(u, ["admin"]):
        return RedirectResponse("/login", status_code=303)
    clear_alerts_admin()
    log_event(audit_logger, "WARN", "ADMIN_CLEAR_ALERTS", "Admin cleared all alerts", user_id=int(u["id"]))
    return RedirectResponse("/admin", status_code=303)


@app.post("/alerts/clear/coadmin/{team_id}")
async def clear_coadmin_alerts(request: Request, team_id: int):
    u = current_user(request)
    if not require_role(u, ["coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)
    if u["role"] == "coadmin" and int(_user_team_int(u) or 0) != int(team_id):
        return HTMLResponse("Forbidden", status_code=403)
    clear_alerts_coadmin(int(team_id))
    log_event(
        audit_logger,
        "WARN",
        "COADMIN_CLEAR_ALERTS",
        "Coadmin/admin cleared coadmin alerts",
        user_id=int(u["id"]),
        team=int(team_id),
    )
    return RedirectResponse(f"/coadmin/{team_id}", status_code=303)


@app.post("/messages/send")
async def send_message(
    request: Request,
    target_role: str = Form(...),
    target_team: Optional[str] = Form(None),
    target_username: Optional[str] = Form(None),
    body: str = Form(...),
):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)

    target_role = target_role.strip().lower()
    if target_role not in ["user", "coadmin", "admin"]:
        return RedirectResponse("/", status_code=303)

    target_user_id = None
    if target_username:
        user_obj = get_user_by_username(target_username.strip())
        if user_obj:
            target_user_id = int(user_obj["id"])
            target_team = user_obj.get("team")

    # Role-based restrictions
    if u["role"] == "user" and target_role not in ["coadmin", "admin"]:
        return RedirectResponse("/", status_code=303)
    if u["role"] == "coadmin":
        coadmin_team = _user_team_int(u)
        if coadmin_team is None:
            return RedirectResponse("/login", status_code=303)
        if target_role == "user":
            # restrict to same team unless explicit user
            if target_team is None:
                target_team = int(coadmin_team)
            if int(target_team) != int(coadmin_team):
                return RedirectResponse(f"/coadmin/{u['team']}", status_code=303)
        elif target_role != "admin":
            return RedirectResponse(f"/coadmin/{u['team']}", status_code=303)

    team_val = None
    if target_team is not None and str(target_team).strip() != "":
        try:
            team_val = int(target_team)
        except Exception:
            team_val = None

    create_message(
        sender_user_id=int(u["id"]),
        sender_role=u["role"],
        sender_team=int(u["team"]) if u.get("team") is not None else None,
        target_role=target_role,
        target_team=team_val,
        target_user_id=target_user_id,
        body=body.strip(),
    )
    log_event(
        chat_logger,
        "INFO",
        "CHAT_LEGACY_MESSAGE_SENT",
        "Legacy role-based message sent",
        sender_user_id=int(u["id"]),
        target_role=target_role,
        target_team=team_val,
        target_user_id=target_user_id,
    )

    if u["role"] == "admin":
        return RedirectResponse("/admin", status_code=303)
    if u["role"] == "coadmin":
        return RedirectResponse(f"/coadmin/{u['team']}", status_code=303)
    return RedirectResponse("/", status_code=303)


@app.post("/messages/{message_id}/read")
async def mark_message_as_read(request: Request, message_id: int):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return RedirectResponse("/login", status_code=303)
    mark_message_read(int(message_id))
    if u["role"] == "admin":
        return RedirectResponse("/admin", status_code=303)
    if u["role"] == "coadmin":
        return RedirectResponse(f"/coadmin/{u['team']}", status_code=303)
    return RedirectResponse("/", status_code=303)


# -----------------------
# Chat V2 API (popup messenger)
# -----------------------
_typing_state: Dict[int, Dict[int, float]] = {}


def _chat_cleanup_typing(now_ts: float):
    for conv_id in list(_typing_state.keys()):
        members = _typing_state.get(conv_id, {})
        members = {uid: ts for uid, ts in members.items() if (now_ts - ts) <= 8.0}
        if members:
            _typing_state[conv_id] = members
        else:
            _typing_state.pop(conv_id, None)


def _chat_typing_users(conversation_id: int, self_user_id: int) -> List[int]:
    now_ts = time.time()
    _chat_cleanup_typing(now_ts)
    users = _typing_state.get(int(conversation_id), {})
    return [uid for uid in users.keys() if int(uid) != int(self_user_id)]


def _chat_user_can_pick_target(current_user: Dict[str, Any], target_user: Dict[str, Any]) -> bool:
    if int(current_user["id"]) == int(target_user["id"]):
        return False
    role = current_user["role"]
    if role == "admin":
        return True
    if role == "coadmin":
        return target_user["role"] == "admin" or int(target_user.get("team") or 0) == int(current_user.get("team") or 0)
    return (
        target_user["role"] == "admin"
        or (target_user["role"] == "coadmin" and int(target_user.get("team") or 0) == int(current_user.get("team") or 0))
    )


@app.get("/api/chat/bootstrap")
def api_chat_bootstrap(request: Request):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    picker = chat_list_users_for_picker(
        requester_id=int(u["id"]),
        role=u["role"],
        team=int(u["team"]) if u.get("team") is not None else None,
    )
    log_event(chat_logger, "DEBUG", "CHAT_BOOTSTRAP", "Chat bootstrap fetched", user_id=int(u["id"]), users=len(picker))
    return {
        "me": {
            "id": int(u["id"]),
            "username": u["username"],
            "display_name": (u.get("display_name") or "").strip() or u["username"],
            "role": u["role"],
            "team": u.get("team"),
        },
        "users": picker,
    }


@app.get("/api/chat/conversations")
def api_chat_list_conversations(request: Request, search: Optional[str] = "", limit: int = 50):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    conversations = chat_list_conversations(
        user_id=int(u["id"]),
        search=(search or "").strip(),
        limit=max(1, min(100, int(limit))),
    )
    for c in conversations:
        if c.get("conversation_type") == "direct":
            c["display_name"] = c.get("direct_peer_username") or "Direct Chat"
        else:
            c["display_name"] = c.get("title") or "Group Chat"
        c["typing_user_ids"] = _chat_typing_users(int(c["id"]), int(u["id"]))
    log_event(chat_logger, "DEBUG", "CHAT_LIST_CONVERSATIONS", "Chat conversations listed", user_id=int(u["id"]), count=len(conversations))
    return {"conversations": conversations}


@app.post("/api/chat/conversations")
async def api_chat_create_conversation(request: Request):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    ctype = (payload.get("type") or "direct").strip().lower()
    if ctype not in {"direct", "group"}:
        return JSONResponse({"error": "invalid conversation type"}, status_code=400)

    if ctype == "direct":
        target_id = int(payload.get("dm_user_id") or 0)
        target = get_user_by_id(target_id)
        if not target:
            return JSONResponse({"error": "target user not found"}, status_code=404)
        if not _chat_user_can_pick_target(u, target):
            return JSONResponse({"error": "forbidden target"}, status_code=403)
        if chat_is_blocked_between(int(u["id"]), target_id):
            return JSONResponse({"error": "chat blocked"}, status_code=403)
        cid = chat_get_or_create_direct(user_a=int(u["id"]), user_b=target_id, created_by=int(u["id"]))
        log_event(chat_logger, "INFO", "CHAT_CREATE_DIRECT", "Direct conversation created/fetched", user_id=int(u["id"]), target_user_id=target_id, conversation_id=cid)
        return {"conversation_id": cid}

    member_ids_raw = payload.get("member_ids") or []
    member_ids: List[int] = []
    for x in member_ids_raw:
        try:
            uid = int(x)
        except Exception:
            continue
        target = get_user_by_id(uid)
        if not target:
            continue
        if _chat_user_can_pick_target(u, target):
            member_ids.append(uid)
    if int(u["id"]) not in member_ids:
        member_ids.append(int(u["id"]))
    if len(set(member_ids)) < 2:
        return JSONResponse({"error": "group requires at least 2 users"}, status_code=400)
    title = (payload.get("title") or "New Group").strip()
    cid = chat_create_group(created_by=int(u["id"]), title=title, member_ids=member_ids)
    log_event(chat_logger, "INFO", "CHAT_CREATE_GROUP", "Group conversation created", user_id=int(u["id"]), conversation_id=cid, members=len(set(member_ids)))
    return {"conversation_id": cid}


@app.get("/api/chat/conversations/{conversation_id}/messages")
def api_chat_messages(request: Request, conversation_id: int, before_id: Optional[int] = None, limit: int = 40):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if not chat_is_member(conversation_id=int(conversation_id), user_id=int(u["id"])):
        return JSONResponse({"error": "forbidden"}, status_code=403)
    items = chat_fetch_messages(
        conversation_id=int(conversation_id),
        limit=max(1, min(100, int(limit))),
        before_id=int(before_id) if before_id else None,
    )
    reactions = chat_fetch_reactions([int(x["id"]) for x in items])
    for row in items:
        row["reactions"] = reactions.get(int(row["id"]), [])
    return {
        "messages": items,
        "members": chat_fetch_members(int(conversation_id)),
        "typing_user_ids": _chat_typing_users(int(conversation_id), int(u["id"])),
    }


@app.post("/api/chat/messages")
async def api_chat_send_message(request: Request):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    conversation_id = int(payload.get("conversation_id") or 0)
    body = (payload.get("body") or "").strip()
    message_type = (payload.get("message_type") or "text").strip().lower()
    reply_to = payload.get("reply_to_message_id")
    client_msg_id = (payload.get("client_msg_id") or "").strip() or None
    if message_type not in {"text", "system"}:
        return JSONResponse({"error": "invalid message type"}, status_code=400)
    if not body:
        return JSONResponse({"error": "message body required"}, status_code=400)
    if len(body) > 4000:
        return JSONResponse({"error": "message too long"}, status_code=400)
    if not chat_is_member(conversation_id=conversation_id, user_id=int(u["id"])):
        return JSONResponse({"error": "forbidden"}, status_code=403)

    members = chat_fetch_members(conversation_id)
    if len(members) == 2:
        peer = [m for m in members if int(m["user_id"]) != int(u["id"])]
        if peer and chat_is_blocked_between(int(u["id"]), int(peer[0]["user_id"])):
            return JSONResponse({"error": "chat blocked"}, status_code=403)

    mid = chat_create_message(
        conversation_id=conversation_id,
        sender_user_id=int(u["id"]),
        message_type=message_type,
        body=body,
        reply_to_message_id=int(reply_to) if reply_to else None,
        client_msg_id=client_msg_id,
    )
    msg = chat_get_message(mid)
    if not msg:
        return JSONResponse({"error": "failed to create message"}, status_code=500)
    msg["reactions"] = []
    log_event(chat_logger, "INFO", "CHAT_MESSAGE_SENT", "Chat message sent", user_id=int(u["id"]), conversation_id=conversation_id, message_id=mid)
    return {"message": msg}


@app.post("/api/chat/messages/{message_id}/read")
def api_chat_mark_read(request: Request, message_id: int):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    msg = chat_get_message(int(message_id))
    if not msg:
        return JSONResponse({"error": "message not found"}, status_code=404)
    if not chat_is_member(conversation_id=int(msg["conversation_id"]), user_id=int(u["id"])):
        return JSONResponse({"error": "forbidden"}, status_code=403)
    chat_mark_read(message_id=int(message_id), user_id=int(u["id"]))
    log_event(chat_logger, "DEBUG", "CHAT_MESSAGE_READ", "Message marked as read", user_id=int(u["id"]), message_id=int(message_id))
    return {"ok": True}


@app.post("/api/chat/messages/{message_id}/edit")
async def api_chat_edit_message(request: Request, message_id: int):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    msg = chat_get_message(int(message_id))
    if not msg:
        return JSONResponse({"error": "message not found"}, status_code=404)
    if int(msg["sender_user_id"]) != int(u["id"]):
        return JSONResponse({"error": "only sender can edit"}, status_code=403)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    body = (payload.get("body") or "").strip()
    if not body:
        return JSONResponse({"error": "message body required"}, status_code=400)
    chat_edit_message(message_id=int(message_id), body=body[:4000])
    log_event(chat_logger, "INFO", "CHAT_MESSAGE_EDIT", "Chat message edited", user_id=int(u["id"]), message_id=int(message_id))
    return {"ok": True}


@app.post("/api/chat/messages/{message_id}/delete")
def api_chat_delete_message(request: Request, message_id: int):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    msg = chat_get_message(int(message_id))
    if not msg:
        return JSONResponse({"error": "message not found"}, status_code=404)
    if int(msg["sender_user_id"]) != int(u["id"]) and u["role"] != "admin":
        return JSONResponse({"error": "forbidden"}, status_code=403)
    chat_soft_delete_message(int(message_id))
    log_event(chat_logger, "WARN", "CHAT_MESSAGE_DELETE", "Chat message soft deleted", user_id=int(u["id"]), message_id=int(message_id))
    return {"ok": True}


@app.post("/api/chat/messages/{message_id}/react")
async def api_chat_react_message(request: Request, message_id: int):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    msg = chat_get_message(int(message_id))
    if not msg:
        return JSONResponse({"error": "message not found"}, status_code=404)
    if not chat_is_member(conversation_id=int(msg["conversation_id"]), user_id=int(u["id"])):
        return JSONResponse({"error": "forbidden"}, status_code=403)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    emoji = (payload.get("emoji") or "").strip()
    if not emoji or len(emoji) > 16:
        return JSONResponse({"error": "invalid emoji"}, status_code=400)
    chat_toggle_reaction(message_id=int(message_id), user_id=int(u["id"]), emoji=emoji)
    reactions = chat_fetch_reactions([int(message_id)]).get(int(message_id), [])
    log_event(chat_logger, "DEBUG", "CHAT_REACTION_TOGGLE", "Chat reaction toggled", user_id=int(u["id"]), message_id=int(message_id), emoji=emoji)
    return {"reactions": reactions}


@app.post("/api/chat/conversations/{conversation_id}/typing")
async def api_chat_typing(request: Request, conversation_id: int):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if not chat_is_member(conversation_id=int(conversation_id), user_id=int(u["id"])):
        return JSONResponse({"error": "forbidden"}, status_code=403)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    is_typing = bool(payload.get("is_typing"))
    conv_id = int(conversation_id)
    _typing_state.setdefault(conv_id, {})
    if is_typing:
        _typing_state[conv_id][int(u["id"])] = time.time()
    else:
        _typing_state.get(conv_id, {}).pop(int(u["id"]), None)
    return {"typing_user_ids": _chat_typing_users(conv_id, int(u["id"]))}


@app.post("/api/chat/conversations/{conversation_id}/settings")
async def api_chat_settings(request: Request, conversation_id: int):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if not chat_is_member(conversation_id=int(conversation_id), user_id=int(u["id"])):
        return JSONResponse({"error": "forbidden"}, status_code=403)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    pinned = payload.get("pinned")
    mute_mode = (payload.get("mute") or "").strip().lower()
    muted_until = None
    if mute_mode == "8h":
        muted_until = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    elif mute_mode == "1w":
        muted_until = (datetime.utcnow() + timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    elif mute_mode == "always":
        muted_until = "9999-12-31 23:59:59"
    elif mute_mode == "off":
        muted_until = None
    chat_set_member_flags(
        conversation_id=int(conversation_id),
        user_id=int(u["id"]),
        pinned=int(bool(pinned)) if pinned is not None else None,
        muted_until=muted_until if mute_mode else None,
    )
    return {"ok": True}


@app.post("/api/chat/block")
async def api_chat_block_user(request: Request):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    blocked_user_id = int(payload.get("blocked_user_id") or 0)
    blocked_user = get_user_by_id(blocked_user_id)
    if not blocked_user:
        return JSONResponse({"error": "user not found"}, status_code=404)
    if blocked_user_id == int(u["id"]):
        return JSONResponse({"error": "cannot block self"}, status_code=400)
    chat_add_block(blocker_user_id=int(u["id"]), blocked_user_id=blocked_user_id)
    log_event(security_logger, "WARN", "SECURITY_BLOCK_USER", "User blocked another user in chat", blocker_user_id=int(u["id"]), blocked_user_id=blocked_user_id)
    return {"ok": True}


@app.post("/api/chat/report")
async def api_chat_report(request: Request):
    u = current_user(request)
    if not require_role(u, ["user", "coadmin", "admin"]):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    conversation_id = int(payload.get("conversation_id") or 0)
    message_id = payload.get("message_id")
    reason = (payload.get("reason") or "").strip()
    if not reason:
        return JSONResponse({"error": "reason required"}, status_code=400)
    if not chat_is_member(conversation_id=conversation_id, user_id=int(u["id"])):
        return JSONResponse({"error": "forbidden"}, status_code=403)
    rid = chat_create_report(
        reporter_user_id=int(u["id"]),
        conversation_id=conversation_id,
        message_id=int(message_id) if message_id else None,
        reason=reason,
    )
    log_event(audit_logger, "WARN", "CHAT_REPORT_CREATED", "Chat report created", reporter_user_id=int(u["id"]), report_id=rid, conversation_id=conversation_id)
    return {"report_id": rid}
