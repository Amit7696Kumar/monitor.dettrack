import contextvars
import gzip
import logging
import os
import re
import shutil
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


_REQUEST_CONTEXT: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar("request_context", default={})
_LOGGERS: Dict[str, logging.Logger] = {}
_CONFIGURED = False


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _get_tz():
    tz_name = (os.getenv("TZ", "Asia/Kolkata") or "Asia/Kolkata").strip()
    if ZoneInfo is None:
        return None
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo("Asia/Kolkata")


def _now_iso() -> str:
    tz = _get_tz()
    if tz is None:
        return datetime.now().astimezone().isoformat(timespec="milliseconds")
    return datetime.now(tz).isoformat(timespec="milliseconds")


def set_request_context(ctx: Dict[str, Any]):
    _REQUEST_CONTEXT.set(dict(ctx or {}))


def clear_request_context():
    _REQUEST_CONTEXT.set({})


def get_request_context() -> Dict[str, Any]:
    return dict(_REQUEST_CONTEXT.get() or {})


class RequestContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        ctx = get_request_context()
        for key, val in ctx.items():
            if not hasattr(record, key):
                setattr(record, key, val)
        return True


SENSITIVE_KEYS = {
    "password", "pass", "passwd", "otp", "token", "access_token", "refresh_token",
    "authorization", "api_key", "secret", "card", "cvv", "ssn", "pin"
}


def mask_email(value: str) -> str:
    if not value or "@" not in value:
        return value
    user, domain = value.split("@", 1)
    if not user:
        return f"***@{domain}"
    return f"{user[0]}***@{domain}"


def mask_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) < 4:
        return "***"
    return f"{digits[:2]}******{digits[-2:]}"


def mask_token(value: str) -> str:
    v = str(value or "")
    if len(v) <= 8:
        return "****"
    return f"{v[:4]}...{v[-4:]}"


def _mask_string(value: str) -> str:
    out = value
    out = re.sub(r"([A-Za-z0-9._%+-])[A-Za-z0-9._%+-]*@([A-Za-z0-9.-]+\.[A-Za-z]{2,})", r"\1***@\2", out)
    out = re.sub(r"\b(\d{2})\d{6,}(\d{2})\b", r"\1******\2", out)
    return out


def sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            lk = str(k).lower()
            if lk in SENSITIVE_KEYS:
                if isinstance(v, str) and "@" in v:
                    out[k] = mask_email(v)
                elif isinstance(v, str) and re.search(r"\d{8,}", v):
                    out[k] = mask_phone(v)
                else:
                    out[k] = mask_token(str(v))
            else:
                out[k] = sanitize(v)
        return out
    if isinstance(value, list):
        return [sanitize(x) for x in value]
    if isinstance(value, tuple):
        return tuple(sanitize(x) for x in value)
    if isinstance(value, str):
        return _mask_string(value)
    return value


class JsonFormatter(logging.Formatter):
    def __init__(self, service_name: str, include_stacktrace: bool):
        super().__init__()
        self.service_name = service_name
        self.include_stacktrace = include_stacktrace

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": _now_iso(),
            "level": record.levelname,
            "service": self.service_name,
            "module": getattr(record, "module_name", record.name.split(".")[-1]),
            "event": getattr(record, "event", "APP_EVENT"),
            "message": sanitize(record.getMessage()),
            "request_id": getattr(record, "request_id", None),
            "user_id": getattr(record, "user_id", None),
            "ip": getattr(record, "ip", None),
            "route": getattr(record, "route", None),
            "method": getattr(record, "method", None),
            "duration_ms": getattr(record, "duration_ms", None),
        }

        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            for k, v in sanitize(extra_fields).items():
                if k not in payload:
                    payload[k] = v

        if record.exc_info:
            exc_type = record.exc_info[0].__name__ if record.exc_info[0] else "Exception"
            err: Dict[str, Any] = {
                "error_type": exc_type,
                "error_message": str(record.exc_info[1]) if record.exc_info[1] else "",
            }
            if self.include_stacktrace:
                err["stacktrace"] = self.formatException(record.exc_info)
            payload["error"] = err

        clean = {k: v for k, v in payload.items() if v is not None}
        import json
        return json.dumps(clean, ensure_ascii=True)


class PrettyFormatter(logging.Formatter):
    def __init__(self, service_name: str, include_stacktrace: bool):
        super().__init__()
        self.service_name = service_name
        self.include_stacktrace = include_stacktrace

    def format(self, record: logging.LogRecord) -> str:
        ts = _now_iso()
        module_name = getattr(record, "module_name", record.name.split(".")[-1])
        event = getattr(record, "event", "APP_EVENT")
        req = getattr(record, "request_id", "-")
        route = getattr(record, "route", "-")
        method = getattr(record, "method", "-")
        msg = sanitize(record.getMessage())
        base = f"{ts} [{record.levelname}] {self.service_name} {module_name} {event} req={req} {method} {route} - {msg}"

        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict) and extra_fields:
            safe_fields = sanitize(extra_fields)
            base += f" | {safe_fields}"

        if record.exc_info and self.include_stacktrace:
            base += "\n" + self.formatException(record.exc_info)
        return base


def _gzip_rotator(source: str, dest: str):
    with open(source, "rb") as src, gzip.open(dest, "wb") as dst:
        shutil.copyfileobj(src, dst)
    os.remove(source)


def _cleanup_old_logs(log_dir: Path, retention_days: int):
    if retention_days <= 0:
        return
    cutoff = datetime.now() - timedelta(days=retention_days)
    for p in log_dir.glob("*.log*"):
        try:
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
            if mtime < cutoff:
                p.unlink(missing_ok=True)
        except Exception:
            continue


def setup_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    env = (os.getenv("ENV", os.getenv("APP_ENV", "dev")) or "dev").strip().lower()
    is_prod = env in {"prod", "production"}

    service_name = (os.getenv("SERVICE_NAME", "det-monitoring-application") or "det-monitoring-application").strip()
    log_level = (os.getenv("LOG_LEVEL", "info" if is_prod else "debug") or "info").strip().upper()
    log_dir = Path((os.getenv("LOG_DIR", "./logs") or "./logs").strip())
    log_dir.mkdir(parents=True, exist_ok=True)

    log_to_console = _env_bool("LOG_TO_CONSOLE", True)
    log_format = (os.getenv("LOG_FORMAT", "json" if is_prod else "pretty") or "json").strip().lower()
    retention_days = _env_int("LOG_RETENTION_DAYS", 14)
    max_size_mb = _env_int("LOG_MAX_SIZE_MB", 50)
    include_stacktrace = _env_bool("LOG_INCLUDE_STACKTRACE", not is_prod)

    _cleanup_old_logs(log_dir, retention_days)

    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level, logging.INFO))

    # Reduce noisy loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    formatter = JsonFormatter(service_name, include_stacktrace) if log_format == "json" else PrettyFormatter(service_name, include_stacktrace)

    modules = ["system", "api", "auth", "db", "admin", "jobs", "security", "audit", "chat", "errors"]

    for module_name in modules:
        logger_name = f"app.{module_name}"
        logger = logging.getLogger(logger_name)
        logger.setLevel(getattr(logging, log_level, logging.INFO))
        logger.propagate = False
        logger.handlers = []

        file_path = log_dir / f"{module_name}.log"
        file_handler = RotatingFileHandler(
            filename=str(file_path),
            maxBytes=max(1, max_size_mb) * 1024 * 1024,
            backupCount=max(10, retention_days * 10),
            encoding="utf-8",
        )
        file_handler.addFilter(RequestContextFilter())
        file_handler.setFormatter(formatter)
        file_handler.namer = lambda name: name + ".gz"
        file_handler.rotator = _gzip_rotator
        logger.addHandler(file_handler)

        if module_name != "errors":
            err_file = log_dir / "errors.log"
            err_handler = RotatingFileHandler(
                filename=str(err_file),
                maxBytes=max(1, max_size_mb) * 1024 * 1024,
                backupCount=max(10, retention_days * 10),
                encoding="utf-8",
            )
            err_handler.setLevel(logging.ERROR)
            err_handler.addFilter(RequestContextFilter())
            err_handler.setFormatter(formatter)
            err_handler.namer = lambda name: name + ".gz"
            err_handler.rotator = _gzip_rotator
            logger.addHandler(err_handler)

        if log_to_console:
            ch = logging.StreamHandler()
            ch.addFilter(RequestContextFilter())
            ch.setFormatter(formatter if log_format == "json" else PrettyFormatter(service_name, include_stacktrace))
            logger.addHandler(ch)

        _LOGGERS[module_name] = logger

    _CONFIGURED = True


def get_logger(module_name: str) -> logging.Logger:
    setup_logging()
    key = (module_name or "system").strip().lower()
    if key not in _LOGGERS:
        key = "system"
    return _LOGGERS[key]


def log_event(
    logger: logging.Logger,
    level: str,
    event: str,
    message: str,
    exc_info: Any = None,
    **fields: Any,
):
    lvl = getattr(logging, (level or "INFO").upper(), logging.INFO)
    module_name = logger.name.split(".")[-1] if logger.name else "system"
    logger.log(
        lvl,
        sanitize(message),
        exc_info=exc_info,
        extra={
            "event": event,
            "module_name": module_name,
            "extra_fields": sanitize(fields),
        },
    )
