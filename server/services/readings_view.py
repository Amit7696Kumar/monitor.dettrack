import json
import os
from datetime import datetime
from typing import Any, Dict, List


def augment_readings_for_view(readings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for r in readings:
        raw = r.get("ocr_json") if isinstance(r, dict) else None
        if not raw:
            r["debug_yolo"] = ""
            r["debug_crop"] = ""
            r["debug_prep"] = ""
            r["ocr_obj"] = None
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            r["debug_yolo"] = ""
            r["debug_crop"] = ""
            r["debug_prep"] = ""
            r["ocr_obj"] = None
            continue
        dbg = obj.get("debug_urls") or {}
        r["debug_yolo"] = dbg.get("yolo", "")
        r["debug_crop"] = dbg.get("crop", "")
        r["debug_prep"] = dbg.get("prep", "")
        r["ocr_obj"] = obj
    return readings


def filter_dashboard_readings_scope(readings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Hide synthetic task-alert reading rows from dashboard tables.
    # Task states are already shown through task_instances, and these system rows
    # otherwise create duplicate entries (e.g., OVERDUE TASK-* appearing twice).
    filtered: List[Dict[str, Any]] = []
    for r in readings:
        if not isinstance(r, dict):
            filtered.append(r)
            continue
        meter_type = str(r.get("meter_type") or "").strip().lower()
        filename = str(r.get("filename") or "").strip().lower()
        if meter_type == "task" and filename == "task://system":
            continue
        filtered.append(r)

    # Keep historical readings visible on dashboards.
    # Optional legacy behavior: set DASHBOARD_READINGS_SCOPE=today to show only today's data.
    scope = (os.getenv("DASHBOARD_READINGS_SCOPE", "all") or "").strip().lower()
    if scope != "today":
        return filtered

    today = datetime.now().strftime("%Y-%m-%d")
    out: List[Dict[str, Any]] = []
    for r in filtered:
        created = (r.get("created_at") if isinstance(r, dict) else None) or ""
        if str(created).startswith(today):
            out.append(r)
    return out
