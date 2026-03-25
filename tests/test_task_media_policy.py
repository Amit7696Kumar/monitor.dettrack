import unittest
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
import sys

from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.services.task_media_policy import MediaUploadEvidence, validate_task_media_uploads
from server.services.task_media_policy import extract_image_capture_datetime, format_capture_datetime


def _jpeg_with_exif(ts: datetime) -> bytes:
    image = Image.new("RGB", (8, 8), color="white")
    exif = Image.Exif()
    exif[36867] = ts.strftime("%Y:%m:%d %H:%M:%S")
    buf = BytesIO()
    image.save(buf, format="JPEG", exif=exif)
    return buf.getvalue()


def _jpeg_without_exif() -> bytes:
    image = Image.new("RGB", (8, 8), color="white")
    buf = BytesIO()
    image.save(buf, format="JPEG")
    return buf.getvalue()


def _png_with_text_timestamp(ts_text: str) -> bytes:
    image = Image.new("RGB", (8, 8), color="white")
    buf = BytesIO()
    image.save(buf, format="PNG", pnginfo=None)
    raw = buf.getvalue()
    payload = f"<x:xmpmeta><rdf:Description><exif:DateTimeOriginal>{ts_text}</exif:DateTimeOriginal></rdf:Description></x:xmpmeta>"
    return raw + payload.encode("utf-8")


class TaskMediaPolicyTests(unittest.TestCase):
    def test_non_odometer_camera_photo_accepted(self):
        result = validate_task_media_uploads(
            task_kind="voltage",
            allowed_types=["photo"],
            uploads=[
                MediaUploadEvidence(
                    field_name="response_file",
                    filename="capture.jpg",
                    media_type="photo",
                    media_source="camera",
                    raw_bytes=_jpeg_without_exif(),
                )
            ],
            now=datetime(2026, 3, 24, 10, 0, 0),
        )
        self.assertTrue(result["accepted"])

    def test_non_odometer_gallery_photo_rejected(self):
        result = validate_task_media_uploads(
            task_kind="voltage",
            allowed_types=["photo"],
            uploads=[
                MediaUploadEvidence(
                    field_name="response_file",
                    filename="gallery.jpg",
                    media_type="photo",
                    media_source="gallery",
                    raw_bytes=_jpeg_without_exif(),
                )
            ],
            now=datetime(2026, 3, 24, 10, 0, 0),
        )
        self.assertFalse(result["accepted"])
        self.assertEqual(result["violations"][0]["code"], "camera_only_required")

    def test_non_odometer_camera_video_accepted(self):
        result = validate_task_media_uploads(
            task_kind="voltage",
            allowed_types=["video"],
            uploads=[
                MediaUploadEvidence(
                    field_name="response_file",
                    filename="clip.mp4",
                    media_type="video",
                    media_source="camera",
                    raw_bytes=b"video-bytes",
                )
            ],
            now=datetime(2026, 3, 24, 10, 0, 0),
        )
        self.assertTrue(result["accepted"])

    def test_non_odometer_gallery_video_rejected(self):
        result = validate_task_media_uploads(
            task_kind="voltage",
            allowed_types=["video"],
            uploads=[
                MediaUploadEvidence(
                    field_name="response_file",
                    filename="clip.mp4",
                    media_type="video",
                    media_source="gallery",
                    raw_bytes=b"video-bytes",
                )
            ],
            now=datetime(2026, 3, 24, 10, 0, 0),
        )
        self.assertFalse(result["accepted"])
        self.assertEqual(result["violations"][0]["code"], "camera_only_required")

    def test_odometer_today_photo_accepted(self):
        now = datetime(2026, 3, 24, 10, 0, 0)
        result = validate_task_media_uploads(
            task_kind="odometer",
            allowed_types=["photo"],
            uploads=[
                MediaUploadEvidence(
                    field_name="response_file_start",
                    filename="odo.jpg",
                    media_type="photo",
                    media_source="gallery",
                    raw_bytes=_jpeg_with_exif(now),
                )
            ],
            now=now,
        )
        self.assertTrue(result["accepted"])

    def test_odometer_yesterday_photo_rejected(self):
        now = datetime(2026, 3, 24, 10, 0, 0)
        result = validate_task_media_uploads(
            task_kind="odometer",
            allowed_types=["photo"],
            uploads=[
                MediaUploadEvidence(
                    field_name="response_file_start",
                    filename="odo.jpg",
                    media_type="photo",
                    media_source="gallery",
                    raw_bytes=_jpeg_with_exif(now - timedelta(days=1)),
                )
            ],
            now=now,
        )
        self.assertFalse(result["accepted"])
        self.assertEqual(result["violations"][0]["code"], "odometer_metadata_date_mismatch")

    def test_odometer_missing_metadata_rejected(self):
        result = validate_task_media_uploads(
            task_kind="odometer",
            allowed_types=["photo"],
            uploads=[
                MediaUploadEvidence(
                    field_name="response_file_start",
                    filename="odo.jpg",
                    media_type="photo",
                    media_source="camera",
                    raw_bytes=_jpeg_without_exif(),
                )
            ],
            now=datetime(2026, 3, 24, 10, 0, 0),
        )
        self.assertFalse(result["accepted"])
        self.assertEqual(result["violations"][0]["code"], "odometer_metadata_missing")

    def test_odometer_future_date_rejected(self):
        now = datetime(2026, 3, 24, 10, 0, 0)
        result = validate_task_media_uploads(
            task_kind="odometer",
            allowed_types=["photo"],
            uploads=[
                MediaUploadEvidence(
                    field_name="response_file_start",
                    filename="odo.jpg",
                    media_type="photo",
                    media_source="camera",
                    raw_bytes=_jpeg_with_exif(now + timedelta(days=1)),
                )
            ],
            now=now,
        )
        self.assertFalse(result["accepted"])
        self.assertEqual(result["violations"][0]["code"], "odometer_metadata_date_mismatch")

    def test_odometer_png_xmp_today_photo_accepted(self):
        now = datetime(2026, 3, 24, 10, 0, 0)
        result = validate_task_media_uploads(
            task_kind="odometer",
            allowed_types=["photo"],
            uploads=[
                MediaUploadEvidence(
                    field_name="response_file_start",
                    filename="odo.png",
                    media_type="photo",
                    media_source="gallery",
                    raw_bytes=_png_with_text_timestamp("2026-03-24T09:15:00+05:30"),
                )
            ],
            now=now,
        )
        self.assertTrue(result["accepted"])

    def test_extract_image_capture_datetime_supports_iso_zulu(self):
        dt = extract_image_capture_datetime(_png_with_text_timestamp("2026-03-24T04:30:00Z"))
        self.assertIsNotNone(dt)
        self.assertEqual(format_capture_datetime(dt), "2026-03-24 04:30:00")

    def test_backend_bypass_unknown_source_rejected(self):
        result = validate_task_media_uploads(
            task_kind="voltage",
            allowed_types=["photo"],
            uploads=[
                MediaUploadEvidence(
                    field_name="response_file",
                    filename="manual.jpg",
                    media_type="photo",
                    media_source="upload",
                    raw_bytes=_jpeg_without_exif(),
                )
            ],
            now=datetime(2026, 3, 24, 10, 0, 0),
        )
        self.assertFalse(result["accepted"])
        self.assertEqual(result["violations"][0]["code"], "camera_only_required")


if __name__ == "__main__":
    unittest.main()
