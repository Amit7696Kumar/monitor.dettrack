from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from server.app import (
    BASE_DIR,
    UPLOAD_DIR,
    _extract_image_taken_at,
    _task_build_spec,
    _task_numeric_from_ocr,
    _task_openai_numeric_fallback,
    _task_openai_numeric_first,
    _task_process_ai,
)
from server.db import (
    DB_PATH,
    fetch_readings_all,
    init_db,
    task_get_instance,
    task_get_question,
    task_get_submission,
    task_upsert_ai_result,
    task_upsert_submission,
    update_reading_analysis,
)
from server.object_detection import detect_task_objects
from server.ocr_engine import run_ocr
from server.openai_ai import analyze_task_image_with_openai, openai_available


def _backup_db() -> Path:
    src = Path(DB_PATH)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = src.with_name(f"{src.name}.backfill_{stamp}.bak")
    shutil.copy2(src, dest)
    return dest


def _abs_upload_from_reading(filename: str) -> Optional[str]:
    name = (filename or "").strip()
    if not name or name.startswith("task://"):
        return None
    return str(Path(UPLOAD_DIR) / name)


def _abs_upload_from_submission(file_path: str) -> Optional[str]:
    path = (file_path or "").strip()
    if not path or path.startswith("task://"):
        return None
    if path.startswith("/uploads/"):
        return str(Path(BASE_DIR) / path.lstrip("/"))
    return str(Path(BASE_DIR) / path)


def _reprocess_reading(row: Dict[str, Any]) -> Dict[str, Any]:
    meter_type = str(row.get("meter_type") or "").strip().lower()
    reading_id = int(row["id"])
    filename = str(row.get("filename") or "")
    filename_2 = str(row.get("filename_2") or "") or None
    manual_value = (row.get("manual_value") or "").strip() or None
    full_path = _abs_upload_from_reading(filename)
    full_path_2 = _abs_upload_from_reading(filename_2 or "")

    if not full_path or not Path(full_path).exists():
        return {"updated": False, "skipped": True, "reason": "missing_primary_image", "reading_id": reading_id}

    image_taken_at = _extract_image_taken_at(full_path)
    image_taken_at_2 = _extract_image_taken_at(full_path_2) if full_path_2 and Path(full_path_2).exists() else None

    value = manual_value
    payload: Dict[str, Any] = {}
    odometer_start = None
    odometer_end = None
    distance_diff = None
    fuel_consumed = None
    fuel_economy = row.get("fuel_economy")

    if meter_type == "fire_point":
        fire_detection = None
        if openai_available():
            try:
                openai_fire = analyze_task_image_with_openai(
                    image_path=full_path,
                    task_title=str(row.get("label") or "Fire Point Check"),
                    task_description="Check whether fire fighting equipment is present in the image.",
                    local_result={},
                    task_spec={
                        "task_kind": "presence_check",
                        "mode_hint": "present_absent",
                        "expected_output_type": "present_absent",
                        "expected_object": "fire fighting equipment",
                        "allowed_labels": ["Present", "Absent"],
                        "return_null_if_missing": True,
                        "strict_relevance": True,
                    },
                    mode_hint="present_absent",
                )
                if openai_fire.get("present") is not None:
                    fire_detection = {
                        "present": bool(openai_fire.get("present")),
                        "confidence": float(openai_fire.get("confidence") or 0.0),
                        "source": "openai",
                        "openai_result": openai_fire,
                    }
            except Exception as exc:
                fire_detection = {"openai_error": str(exc)}
        if fire_detection is None or fire_detection.get("present") is None:
            local_detection = detect_task_objects("fire_point", full_path)
            fire_detection = {**(fire_detection or {}), **local_detection}
        value = "Present" if bool(fire_detection.get("present")) else "Absent"
        payload = {"object_detection": fire_detection}
    elif meter_type == "odometer":
        if not full_path_2 or not Path(full_path_2).exists():
            return {"updated": False, "skipped": True, "reason": "missing_second_odometer_image", "reading_id": reading_id}
        o1 = run_ocr(full_path, uuid.uuid4().hex, meter_type)
        o2 = run_ocr(full_path_2, uuid.uuid4().hex, meter_type)
        odometer_start = (o1.get("numeric") or {}).get("value")
        odometer_end = (o2.get("numeric") or {}).get("value")
        if odometer_start is None or odometer_end is None:
            return {"updated": False, "skipped": True, "reason": "odometer_value_missing", "reading_id": reading_id}
        distance_val = abs(float(odometer_end) - float(odometer_start))
        distance_diff = f"{distance_val:.2f}"
        value = manual_value or distance_diff
        if fuel_economy:
            try:
                fuel_used = distance_val / float(fuel_economy)
                fuel_consumed = f"{fuel_used:.2f}"
            except Exception:
                fuel_consumed = None
        payload = {
            "odometer_first": o1,
            "odometer_second": o2,
            "odometer": {
                "start": odometer_start,
                "end": odometer_end,
                "distance_diff": distance_diff,
                "fuel_economy": fuel_economy,
                "fuel_consumed": fuel_consumed,
            },
        }
    else:
        ocr = run_ocr(full_path, uuid.uuid4().hex, meter_type)
        numeric_value = (ocr.get("numeric") or {}).get("value")
        value = manual_value or numeric_value
        payload = ocr

    update_reading_analysis(
        reading_id=reading_id,
        value=value,
        ocr_json=json.dumps(payload, default=str),
        filename_2=filename_2,
        odometer_start=odometer_start,
        odometer_end=odometer_end,
        distance_diff=distance_diff,
        fuel_economy=fuel_economy,
        fuel_consumed=fuel_consumed,
        image_taken_at=image_taken_at,
        image_taken_at_2=image_taken_at_2,
    )
    return {"updated": True, "reading_id": reading_id, "value": value}


async def _reprocess_task_submission(instance_id: int) -> Dict[str, Any]:
    instance = task_get_instance(instance_id)
    submission = task_get_submission(instance_id)
    if not instance or not submission:
        return {"updated": False, "skipped": True, "reason": "missing_task_row", "instance_id": instance_id}
    if str(submission.get("file_type") or "").strip().lower() != "photo":
        return {"updated": False, "skipped": True, "reason": "non_photo_submission", "instance_id": instance_id}

    file_path = _abs_upload_from_submission(str(submission.get("file_path") or ""))
    file_path_2 = _abs_upload_from_submission(str(submission.get("file_path_2") or ""))
    if not file_path or not Path(file_path).exists():
        return {"updated": False, "skipped": True, "reason": "missing_submission_image", "instance_id": instance_id}

    image_taken_at = _extract_image_taken_at(file_path)
    image_taken_at_2 = _extract_image_taken_at(file_path_2) if file_path_2 and Path(file_path_2).exists() else None

    question = None
    try:
        if instance.get("form_id") is not None:
            question = task_get_question(int(instance.get("form_id")))
    except Exception:
        question = None
    task_spec = _task_build_spec(instance, question)
    task_kind = str(task_spec.get("task_kind") or "")
    is_odometer_task = task_kind == "odometer"
    is_fire_point_task = task_kind == "fire_point"
    is_timestamp_task = task_kind == "timestamp_value"
    is_unsupported_task = task_kind == "unsupported_notice"

    ai_payload: Dict[str, Any]
    ai_result: Optional[str]
    ai_status: str
    number_value: Any = submission.get("submitted_value")

    if is_timestamp_task:
        number_value = image_taken_at or task_spec.get("missing_value_message")
        ai_payload = {
            "engine_used": "metadata",
            "validation_status": "validated" if image_taken_at else "no_value",
            "validation_reason": "" if image_taken_at else "No timestamp metadata found",
            "extracted_values": {"value": number_value} if number_value else {},
        }
        ai_result = f"Timestamp value used: {number_value}" if number_value else "Timestamp metadata not found"
        ai_status = "completed"
    elif is_unsupported_task:
        number_value = task_spec.get("unsupported_message") or "This is not supported yet kindly see the image"
        ai_payload = {
            "engine_used": "rule_config",
            "validation_status": "validated",
            "validation_reason": "",
            "extracted_values": {"value": number_value},
        }
        ai_result = str(number_value)
        ai_status = "completed"
    elif is_odometer_task and file_path_2 and Path(file_path_2).exists():
        try:
            o1 = run_ocr(file_path, uuid.uuid4().hex, "odometer")
            o2 = run_ocr(file_path_2, uuid.uuid4().hex, "odometer")
            start_val = await _task_openai_numeric_first(
                image_path=file_path,
                task_title=f"{instance.get('title') or ''} start",
                task_description=str(instance.get("description") or ""),
                local_result={"ocr": o1, "position": "start"},
            )
            end_val = await _task_openai_numeric_first(
                image_path=file_path_2,
                task_title=f"{instance.get('title') or ''} end",
                task_description=str(instance.get("description") or ""),
                local_result={"ocr": o2, "position": "end"},
            )
            if start_val is None:
                start_val = _task_numeric_from_ocr(o1)
            if end_val is None:
                end_val = _task_numeric_from_ocr(o2)
            if start_val is None:
                start_val = await _task_openai_numeric_fallback(
                    image_path=file_path,
                    task_title=f"{instance.get('title') or ''} start",
                    task_description=str(instance.get("description") or ""),
                    local_result={"ocr": o1, "position": "start"},
                )
            if end_val is None:
                end_val = await _task_openai_numeric_fallback(
                    image_path=file_path_2,
                    task_title=f"{instance.get('title') or ''} end",
                    task_description=str(instance.get("description") or ""),
                    local_result={"ocr": o2, "position": "end"},
                )
            if start_val is None or end_val is None:
                raise ValueError("Could not extract start/end odometer values")
            avg_kmpl = submission.get("avg_kmpl") or instance.get("number_max")
            distance_diff = abs(float(end_val) - float(start_val))
            fuel_consumed = distance_diff / float(avg_kmpl or 1.0)
            number_value = f"Diff {distance_diff:.2f}, Fuel {fuel_consumed:.2f} L"
            ai_payload = {
                "engine_used": "openai_then_local",
                "validation_status": "validated",
                "validation_reason": "",
                "extracted_values": {
                    "start": start_val,
                    "end": end_val,
                    "distance": distance_diff,
                    "fuel_consumed": fuel_consumed,
                },
            }
            ai_result = (
                f"Odometer processed: start={start_val:.2f}, end={end_val:.2f}, "
                f"distance={distance_diff:.2f} km, avg_kmpl={float(avg_kmpl or 0):.2f}, "
                f"fuel_consumed={fuel_consumed:.2f} L"
            )
            ai_status = "completed"
        except Exception as exc:
            number_value = None
            ai_payload = {"engine_used": "openai_then_local", "validation_status": "failed", "validation_reason": str(exc)}
            ai_result = f"Odometer processing failed: {exc}"
            ai_status = "failed"
    elif is_fire_point_task:
        try:
            fire_detection = None
            if openai_available():
                try:
                    openai_fire = analyze_task_image_with_openai(
                    image_path=file_path,
                    task_title=str(instance.get("title") or ""),
                    task_description=str(instance.get("description") or ""),
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
                except Exception as exc:
                    fire_detection = {"openai_error": str(exc)}
            if fire_detection is None or fire_detection.get("present") is None:
                local_fire = detect_task_objects("fire_point", file_path)
                fire_detection = {**(fire_detection or {}), **local_fire}
            number_value = "Present" if bool(fire_detection.get("present")) else "Absent"
            ai_payload = {
                **fire_detection,
                "engine_used": str(fire_detection.get("source") or "local"),
                "validation_status": "validated",
                "validation_reason": "",
                "extracted_values": {"value": number_value},
            }
            ai_result = f"Fire-point detection ({fire_detection.get('source') or 'model'}): {number_value}"
            ai_status = "completed"
        except Exception as exc:
            number_value = None
            ai_payload = {"engine_used": "local", "validation_status": "failed", "validation_reason": str(exc)}
            ai_result = f"Fire-point detection failed: {exc}"
            ai_status = "failed"
    else:
        ai_payload = await _task_process_ai(instance, file_path, "photo")
        ai_status = str(ai_payload.get("status") or "failed")
        ai_result = ai_payload.get("summary")
        extracted_raw = ((ai_payload or {}).get("extracted_values") or {}).get("value")
        if extracted_raw is not None and extracted_raw != "":
            number_value = extracted_raw
        else:
            number_value = None

    task_upsert_submission(
        task_instance_id=instance_id,
        user_id=int(submission.get("user_id") or instance.get("assigned_user_id") or 0),
        file_path=str(submission.get("file_path") or ""),
        file_type=str(submission.get("file_type") or "photo"),
        file_size=int(submission.get("file_size") or 0),
        remarks=submission.get("remarks"),
        submitted_value=number_value,
        file_path_2=submission.get("file_path_2"),
        avg_kmpl=submission.get("avg_kmpl"),
        distance_diff=(ai_payload.get("extracted_values") or {}).get("distance") if is_odometer_task else submission.get("distance_diff"),
        fuel_consumed=(ai_payload.get("extracted_values") or {}).get("fuel_consumed") if is_odometer_task else submission.get("fuel_consumed"),
        image_taken_at=image_taken_at,
        image_taken_at_2=image_taken_at_2,
        ai_requested=bool(submission.get("ai_requested")),
        ai_status=ai_status,
        ai_result_reference=ai_result,
    )
    task_upsert_ai_result(
        task_instance_id=instance_id,
        ai_engine_type=str((ai_payload or {}).get("engine_used") or "backfill"),
        processing_status=ai_status,
        extracted_text=str((ai_payload or {}).get("extracted_text") or ai_result or ""),
        extracted_values=(ai_payload or {}).get("extracted_values") or ({"value": number_value} if number_value is not None else {}),
        analysis_summary=ai_result,
        validation_status=str((ai_payload or {}).get("validation_status") or ai_status),
        alert_triggered=False,
        alert_reason=str((ai_payload or {}).get("validation_reason") or ""),
    )
    return {"updated": True, "instance_id": instance_id, "value": number_value, "ai_status": ai_status}


def _task_submission_ids(limit: Optional[int]) -> list[int]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    sql = "SELECT task_instance_id FROM task_submissions WHERE file_type='photo' ORDER BY id DESC"
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    ids = [int(row[0]) for row in cur.fetchall()]
    conn.close()
    return ids


async def _run(limit: Optional[int], readings_only: bool, tasks_only: bool) -> None:
    init_db()
    backup_path = _backup_db()
    print(f"Database backup created: {backup_path}")

    reading_results = []
    task_results = []

    if not tasks_only:
        readings = [r for r in fetch_readings_all() if str(r.get("filename") or "").strip() and not str(r.get("filename") or "").startswith("task://")]
        if limit:
            readings = readings[: int(limit)]
        for row in readings:
            try:
                result = _reprocess_reading(row)
            except Exception as exc:
                result = {"updated": False, "skipped": True, "reason": str(exc), "reading_id": row.get("id")}
            reading_results.append(result)
            print(f"reading {row.get('id')}: {result}")

    if not readings_only:
        for instance_id in _task_submission_ids(limit):
            try:
                result = await _reprocess_task_submission(instance_id)
            except Exception as exc:
                result = {"updated": False, "skipped": True, "reason": str(exc), "instance_id": instance_id}
            task_results.append(result)
            print(f"task {instance_id}: {result}")

    print(
        json.dumps(
            {
                "readings_updated": sum(1 for r in reading_results if r.get("updated")),
                "readings_skipped": sum(1 for r in reading_results if r.get("skipped")),
                "tasks_updated": sum(1 for r in task_results if r.get("updated")),
                "tasks_skipped": sum(1 for r in task_results if r.get("skipped")),
            },
            indent=2,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Reprocess saved images and update database values using the latest AI/OCR flow.")
    parser.add_argument("--limit", type=int, default=None, help="Process only the most recent N readings/task submissions.")
    parser.add_argument("--readings-only", action="store_true", help="Backfill only regular readings.")
    parser.add_argument("--tasks-only", action="store_true", help="Backfill only task submissions.")
    args = parser.parse_args()
    asyncio.run(_run(args.limit, args.readings_only, args.tasks_only))


if __name__ == "__main__":
    main()
