from __future__ import annotations

import base64
import json
import mimetypes
import os
from pathlib import Path
from typing import Any, Dict, Optional

from openai import OpenAI


def openai_available() -> bool:
    return bool((os.getenv("OPENAI_API_KEY", "") or "").strip())


def _image_data_url(image_path: str) -> str:
    path = Path(image_path)
    mime, _ = mimetypes.guess_type(str(path))
    mime = mime or "image/jpeg"
    data = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{data}"


def _extract_json(content: str) -> Dict[str, Any]:
    raw = (content or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(raw[start : end + 1])
            except Exception:
                return {}
        return {}


def analyze_task_image_with_openai(
    *,
    image_path: str,
    task_title: str,
    task_description: str,
    local_result: Optional[Dict[str, Any]] = None,
    task_spec: Optional[Dict[str, Any]] = None,
    mode_hint: str = "auto",
) -> Dict[str, Any]:
    api_key = (os.getenv("OPENAI_API_KEY", "") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    client = OpenAI(api_key=api_key)
    model = (os.getenv("OPENAI_MODEL", "") or "gpt-4o-mini").strip()
    title = (task_title or "").strip()
    description = (task_description or "").strip()
    local_json = json.dumps(local_result or {}, default=str)
    task_spec_json = json.dumps(task_spec or {}, default=str)
    task_kind = str((task_spec or {}).get("task_kind") or "").strip().lower()
    extra_rules = ""
    if task_kind == "earthing":
        extra_rules = """
- This is an earthing meter task.
- Only return a value if the earthing meter device/display itself is clearly visible in the image.
- Do not use dates, stickers, handwritten notes, labels, calibration tags, or timestamps as the meter reading.
- If the earthing meter is not visible, set `present` to false, `readable` to false, and `value` to null.
""".strip()

    prompt = f"""
You are analyzing a task response image for an operations dashboard.

Task title: {title}
Task description: {description}
Mode hint: {mode_hint}
Task spec: {task_spec_json}
Local result: {local_json}

Rules:
- Treat the task title and task description as the user's query about this image.
- Use the task spec as the primary contract for what to look for and what output format is allowed.
- If task spec includes inspection points, use them as the checklist for deciding the result.
- If task spec includes allowed labels, return `value` as exactly one of those labels or null.
- Prefer what is visible in the image.
- Decide whether the image is actually relevant to the task before extracting anything.
- If this is a fire point / firefighting / extinguisher / bucket / hydrant check, decide whether firefighting equipment is present.
- If this is a numeric reading task, extract one best numeric reading only when the relevant meter/display/gauge is clearly visible and readable.
- If this is an area-maintenance task, mark the area bad when you see over hanging cables, messy layout, clutter, or poor cleanliness.
- If this is a fire-equipment serviceability task, mark it unserviceable unless the image clearly shows the extinguisher working/spraying.
- If this is a heater task, return ON only when a flame is visible and OFF when no flame is visible.
- If the image does not contain the required object, meter, display, or evidence for the task, return null values instead of guessing.
- Never extract a numeric value from unrelated background text, labels, timestamps, posters, or other non-task content.
- If this is a socket plug / proper connection / compliance check, return value as exactly "Correct" or "Incorrect".
- If the image is unclear, return low confidence and null values instead of guessing.
{extra_rules}
- Return JSON only.

Return exactly this object shape:
{{
  "relevant": true | false,
  "readable": true | false | null,
  "result_type": "numeric" | "present_absent" | "correct_incorrect" | "text",
  "value": string or null,
  "present": true | false | null,
  "confidence": number,
  "summary": string,
  "evidence": string
}}
""".strip()

    resp = client.chat.completions.create(
        model=model,
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": _image_data_url(image_path)}},
                ],
            }
        ],
    )

    content = ""
    try:
        content = resp.choices[0].message.content or ""
    except Exception:
        content = ""
    parsed = _extract_json(content)
    return {
        "relevant": bool(parsed.get("relevant")) if parsed.get("relevant") is not None else None,
        "readable": bool(parsed.get("readable")) if parsed.get("readable") is not None else None,
        "result_type": parsed.get("result_type") or "text",
        "value": parsed.get("value"),
        "present": parsed.get("present"),
        "confidence": float(parsed.get("confidence") or 0.0),
        "summary": str(parsed.get("summary") or "").strip(),
        "evidence": str(parsed.get("evidence") or "").strip(),
        "engine_used": "openai",
        "model": model,
        "raw": parsed,
    }
