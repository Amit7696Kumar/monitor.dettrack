from datetime import datetime
from typing import Any, Dict, List, Optional


def _dashboard_parse_dt(value: Any) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        return datetime.min
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt)
        except Exception:
            continue
    return datetime.min


def _combine_dashboard_rows(readings: List[Dict[str, Any]], task_instances: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for r in readings:
        item = dict(r)
        item["_row_kind"] = "reading"
        item["_sort_at"] = str(item.get("created_at") or "")
        rows.append(item)

    for t in task_instances:
        item = dict(t)
        item["_row_kind"] = "task"
        item["_sort_at"] = str(item.get("submitted_at") or item.get("deadline_at") or "")
        rows.append(item)

    rows.sort(key=lambda item: (_dashboard_parse_dt(item.get("_sort_at")), int(item.get("id") or 0)), reverse=True)
    return rows


def _row_result_alert(item: Dict[str, Any]) -> bool:
    row_kind = str(item.get("_row_kind") or "").strip().lower()
    if row_kind == "task":
        if bool(item.get("alert_triggered")):
            return True
        return bool(str(item.get("alert_reason") or "").strip())
    status_class = str(item.get("status_class") or "").strip().lower()
    return status_class in {"high", "alert", "danger"}


def _mark_result_alerts(items: List[Dict[str, Any]], row_kind: str) -> List[Dict[str, Any]]:
    marked: List[Dict[str, Any]] = []
    for raw in items:
        item = dict(raw)
        item.setdefault("_row_kind", row_kind)
        item["_result_alert"] = _row_result_alert(item)
        marked.append(item)
    return marked


def build_admin_dashboard_context(
    *,
    user: Dict[str, Any],
    readings: List[Dict[str, Any]],
    task_instances: List[Dict[str, Any]],
    alerts: List[Dict[str, Any]],
    unread_count: int,
    latest_reading_id: int,
    messages: List[Dict[str, Any]],
    all_users: List[Dict[str, Any]],
    teams: Optional[List[int]] = None,
) -> Dict[str, Any]:
    readings_view = _mark_result_alerts(readings, "reading")
    task_instances_view = _mark_result_alerts(task_instances, "task")
    return {
        "user": user,
        "readings": readings_view,
        "task_instances": task_instances_view,
        "alerts": alerts,
        "unread_count": unread_count,
        "teams": teams if teams is not None else [1, 2, 3, 4, 5, 6],
        "latest_reading_id": latest_reading_id,
        "messages": messages,
        "all_users": all_users,
        "dashboard_rows": _combine_dashboard_rows(readings_view, task_instances_view),
    }


def build_coadmin_dashboard_context(
    *,
    user: Dict[str, Any],
    team_id: int,
    readings: List[Dict[str, Any]],
    task_instances: List[Dict[str, Any]],
    alerts: List[Dict[str, Any]],
    unread_count: int,
    latest_reading_id: int,
    messages: List[Dict[str, Any]],
    users_team: List[Dict[str, Any]],
) -> Dict[str, Any]:
    readings_view = _mark_result_alerts(readings, "reading")
    task_instances_view = _mark_result_alerts(task_instances, "task")
    return {
        "user": user,
        "team_id": team_id,
        "readings": readings_view,
        "task_instances": task_instances_view,
        "alerts": alerts,
        "unread_count": unread_count,
        "latest_reading_id": latest_reading_id,
        "messages": messages,
        "users_team": users_team,
        "total_task_count": len(readings_view) + len(task_instances_view),
        "dashboard_rows": _combine_dashboard_rows(readings_view, task_instances_view),
    }
