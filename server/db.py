import os
import json
import sqlite3
import time
from typing import Any, Dict, List, Optional

from server.logging_utils import get_logger, log_event

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "meter.db")
DB_SLOW_QUERY_MS = int((os.getenv("DB_SLOW_QUERY_MS", "200") or "200").strip() or 200)
SCHEMA_USER_VERSION = 1
_db_logger = get_logger("db")
_db_connected_once = False


def _compact_sql(sql: str) -> str:
    return " ".join((sql or "").split())[:500]


class LoggingCursor(sqlite3.Cursor):
    def execute(self, sql, parameters=()):
        t0 = time.perf_counter()
        query_preview = _compact_sql(str(sql))
        try:
            out = super().execute(sql, parameters)
            duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            if duration_ms >= DB_SLOW_QUERY_MS:
                log_event(
                    _db_logger,
                    "WARN",
                    "DB_SLOW_QUERY",
                    "Slow database query detected",
                    duration_ms=duration_ms,
                    query=query_preview,
                    has_params=bool(parameters),
                )
            else:
                log_event(
                    _db_logger,
                    "DEBUG",
                    "DB_QUERY_OK",
                    "Database query executed",
                    duration_ms=duration_ms,
                    query=query_preview,
                )
            return out
        except Exception:
            duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            log_event(
                _db_logger,
                "ERROR",
                "DB_QUERY_FAIL",
                "Database query failed",
                exc_info=True,
                duration_ms=duration_ms,
                query=query_preview,
                has_params=bool(parameters),
            )
            raise

    def executemany(self, sql, seq_of_parameters):
        t0 = time.perf_counter()
        query_preview = _compact_sql(str(sql))
        try:
            out = super().executemany(sql, seq_of_parameters)
            duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            if duration_ms >= DB_SLOW_QUERY_MS:
                log_event(
                    _db_logger,
                    "WARN",
                    "DB_SLOW_QUERY",
                    "Slow database bulk query detected",
                    duration_ms=duration_ms,
                    query=query_preview,
                )
            else:
                log_event(
                    _db_logger,
                    "DEBUG",
                    "DB_QUERY_OK",
                    "Database bulk query executed",
                    duration_ms=duration_ms,
                    query=query_preview,
                )
            return out
        except Exception:
            duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            log_event(
                _db_logger,
                "ERROR",
                "DB_QUERY_FAIL",
                "Database bulk query failed",
                exc_info=True,
                duration_ms=duration_ms,
                query=query_preview,
            )
            raise


class LoggingConnection(sqlite3.Connection):
    def cursor(self, factory=None):
        return super().cursor(factory or LoggingCursor)


def _conn():
    global _db_connected_once
    try:
        conn = sqlite3.connect(DB_PATH, factory=LoggingConnection, timeout=30.0)
        conn.row_factory = sqlite3.Row
        # SQLite is shared by request handlers and background scheduler threads.
        # Use WAL plus a busy timeout so short write contention does not surface
        # as immediate "database is locked" failures to the user.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        if not _db_connected_once:
            log_event(_db_logger, "INFO", "DB_CONNECTION_OK", "SQLite connection established", db_path=DB_PATH)
            _db_connected_once = True
        return conn
    except Exception:
        log_event(_db_logger, "ERROR", "DB_CONNECTION_FAIL", "Failed to connect SQLite database", exc_info=True, db_path=DB_PATH)
        raise


def init_db():
    conn = _conn()
    cur = conn.cursor()
    cur.execute("PRAGMA user_version")
    schema_version_row = cur.fetchone()
    schema_version = int(schema_version_row[0] or 0) if schema_version_row else 0
    needs_legacy_migrations = schema_version < SCHEMA_USER_VERSION

    # Users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('user', 'coadmin', 'admin')),
        team INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    if needs_legacy_migrations:
        cur.execute("PRAGMA table_info(users)")
        user_cols = {row[1] for row in cur.fetchall()}
        if "email" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN email TEXT")
        if "display_name" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN display_name TEXT")
        if "google_id" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN google_id TEXT")
        if "auth_provider" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN auth_provider TEXT")
        if "force_password_change" not in user_cols:
            cur.execute("ALTER TABLE users ADD COLUMN force_password_change INTEGER NOT NULL DEFAULT 0")

    # Readings uploaded by users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS readings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        team INTEGER NOT NULL,
        meter_type TEXT NOT NULL,        -- 'earthing' or 'temp'
        label TEXT NOT NULL,             -- user label / site name etc
        value TEXT,                      -- numeric as text (to preserve '0.40')
        manual_value TEXT,               -- optional user-entered reading
        filename TEXT NOT NULL,
        ocr_json TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """)
    # Add missing columns for existing DBs
    if needs_legacy_migrations:
        cur.execute("PRAGMA table_info(readings)")
        cols = {row[1] for row in cur.fetchall()}
        if "manual_value" not in cols:
            cur.execute("ALTER TABLE readings ADD COLUMN manual_value TEXT")
        if "filename_2" not in cols:
            cur.execute("ALTER TABLE readings ADD COLUMN filename_2 TEXT")
        if "odometer_start" not in cols:
            cur.execute("ALTER TABLE readings ADD COLUMN odometer_start TEXT")
        if "odometer_end" not in cols:
            cur.execute("ALTER TABLE readings ADD COLUMN odometer_end TEXT")
        if "distance_diff" not in cols:
            cur.execute("ALTER TABLE readings ADD COLUMN distance_diff TEXT")
        if "fuel_economy" not in cols:
            cur.execute("ALTER TABLE readings ADD COLUMN fuel_economy TEXT")
        if "fuel_consumed" not in cols:
            cur.execute("ALTER TABLE readings ADD COLUMN fuel_consumed TEXT")
        if "image_taken_at" not in cols:
            cur.execute("ALTER TABLE readings ADD COLUMN image_taken_at TEXT")
        if "image_taken_at_2" not in cols:
            cur.execute("ALTER TABLE readings ADD COLUMN image_taken_at_2 TEXT")

    # Alerts for coadmin/admin
    cur.execute("""
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reading_id INTEGER NOT NULL,
        target_role TEXT NOT NULL CHECK(target_role IN ('coadmin', 'admin')),
        target_team INTEGER,              -- for coadmin alerts
        message TEXT NOT NULL,
        severity TEXT NOT NULL CHECK(severity IN ('low', 'high')),
        is_read INTEGER NOT NULL DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(reading_id) REFERENCES readings(id)
    )
    """)

    # Messages between roles/users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sender_user_id INTEGER,
        sender_role TEXT NOT NULL CHECK(sender_role IN ('user', 'coadmin', 'admin')),
        sender_team INTEGER,
        target_role TEXT NOT NULL CHECK(target_role IN ('user', 'coadmin', 'admin')),
        target_team INTEGER,
        target_user_id INTEGER,
        body TEXT NOT NULL,
        is_read INTEGER NOT NULL DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(sender_user_id) REFERENCES users(id),
        FOREIGN KEY(target_user_id) REFERENCES users(id)
    )
    """)

    # v2 chat conversations (popup messenger)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS chat_conversations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        conversation_type TEXT NOT NULL CHECK(conversation_type IN ('direct', 'group')),
        title TEXT,
        icon_url TEXT,
        created_by INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(created_by) REFERENCES users(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chat_members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        conversation_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        member_role TEXT NOT NULL CHECK(member_role IN ('member', 'admin')),
        pinned INTEGER NOT NULL DEFAULT 0,
        is_archived INTEGER NOT NULL DEFAULT 0,
        muted_until DATETIME,
        joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_read_message_id INTEGER,
        UNIQUE(conversation_id, user_id),
        FOREIGN KEY(conversation_id) REFERENCES chat_conversations(id),
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(last_read_message_id) REFERENCES chat_messages(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chat_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        conversation_id INTEGER NOT NULL,
        sender_user_id INTEGER NOT NULL,
        message_type TEXT NOT NULL CHECK(message_type IN ('text', 'system')),
        body TEXT NOT NULL,
        reply_to_message_id INTEGER,
        client_msg_id TEXT,
        is_edited INTEGER NOT NULL DEFAULT 0,
        is_deleted INTEGER NOT NULL DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        edited_at DATETIME,
        FOREIGN KEY(conversation_id) REFERENCES chat_conversations(id),
        FOREIGN KEY(sender_user_id) REFERENCES users(id),
        FOREIGN KEY(reply_to_message_id) REFERENCES chat_messages(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chat_receipts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        delivered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        read_at DATETIME,
        UNIQUE(message_id, user_id),
        FOREIGN KEY(message_id) REFERENCES chat_messages(id),
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chat_reactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        emoji TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(message_id, user_id, emoji),
        FOREIGN KEY(message_id) REFERENCES chat_messages(id),
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chat_blocks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        blocker_user_id INTEGER NOT NULL,
        blocked_user_id INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(blocker_user_id, blocked_user_id),
        FOREIGN KEY(blocker_user_id) REFERENCES users(id),
        FOREIGN KEY(blocked_user_id) REFERENCES users(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chat_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reporter_user_id INTEGER NOT NULL,
        conversation_id INTEGER NOT NULL,
        message_id INTEGER,
        reason TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open', 'reviewed', 'closed')),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(reporter_user_id) REFERENCES users(id),
        FOREIGN KEY(conversation_id) REFERENCES chat_conversations(id),
        FOREIGN KEY(message_id) REFERENCES chat_messages(id)
    )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_chat_messages_conv_created ON chat_messages(conversation_id, id DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chat_members_user ON chat_members(user_id, conversation_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chat_conversations_updated ON chat_conversations(updated_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chat_receipts_user ON chat_receipts(user_id, message_id)")

    # Task forms/templates
    cur.execute("""
    CREATE TABLE IF NOT EXISTS task_forms (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        description TEXT,
        creator_user_id INTEGER NOT NULL,
        creator_role TEXT NOT NULL CHECK(creator_role IN ('admin', 'coadmin')),
        assigned_scope TEXT NOT NULL CHECK(assigned_scope IN ('users', 'team')),
        assigned_user_ids_json TEXT,
        assigned_team_id INTEGER,
        deadline_at TEXT NOT NULL,
        allowed_types_json TEXT NOT NULL,
        ai_enabled INTEGER NOT NULL DEFAULT 0,
        repeat_enabled INTEGER NOT NULL DEFAULT 0,
        repeat_type TEXT,
        repeat_interval_days INTEGER,
        allow_resubmission INTEGER NOT NULL DEFAULT 0,
        image_upload_count INTEGER NOT NULL DEFAULT 1,
        priority TEXT NOT NULL DEFAULT 'medium',
        is_deleted INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('draft', 'active', 'expired', 'completed', 'overdue')),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(creator_user_id) REFERENCES users(id)
    )
    """)
    # One-question config (future extensible to multi-question)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS task_questions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        form_id INTEGER NOT NULL,
        question_text TEXT NOT NULL,
        question_type TEXT NOT NULL DEFAULT 'media_response',
        allowed_media_types_json TEXT NOT NULL,
        extraction_hints TEXT,
        threshold_rules_json TEXT,
        expected_field_type TEXT,
        ideal_min REAL,
        ideal_max REAL,
        unit TEXT,
        alert_condition TEXT,
        parsing_instructions TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(form_id) REFERENCES task_forms(id)
    )
    """)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_task_questions_form ON task_questions(form_id)")

    # Individual task assignment instances
    cur.execute("""
    CREATE TABLE IF NOT EXISTS task_instances (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        form_id INTEGER NOT NULL,
        assigned_user_id INTEGER NOT NULL,
        assigned_team_id INTEGER,
        deadline_at TEXT NOT NULL,
        cycle_key TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'submitted', 'late', 'overdue', 'completed')),
        submitted_at TEXT,
        is_alert_sent INTEGER NOT NULL DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(form_id) REFERENCES task_forms(id),
        FOREIGN KEY(assigned_user_id) REFERENCES users(id)
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_task_instances_user_status ON task_instances(assigned_user_id, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_task_instances_deadline ON task_instances(deadline_at)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_task_instances_cycle ON task_instances(form_id, assigned_user_id, cycle_key)")

    # File submissions by users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS task_submissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_instance_id INTEGER NOT NULL UNIQUE,
        user_id INTEGER NOT NULL,
        file_path TEXT NOT NULL,
        file_type TEXT NOT NULL CHECK(file_type IN ('pdf', 'photo', 'video', 'number')),
        file_size INTEGER,
        remarks TEXT,
        ai_requested INTEGER NOT NULL DEFAULT 0,
        ai_status TEXT NOT NULL DEFAULT 'not_requested',
        ai_result_reference TEXT,
        submitted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(task_instance_id) REFERENCES task_instances(id),
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """)
    # Legacy compatibility: older builds may already have task_submissions with
    # columns like task_id/submitted_by/evidence_path. Add bridge columns so new
    # APIs can run without destructive migration.
    if needs_legacy_migrations:
        cur.execute("PRAGMA table_info(task_submissions)")
        sub_cols = {row[1] for row in cur.fetchall()}
        if "task_instance_id" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN task_instance_id INTEGER")
        if "user_id" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN user_id INTEGER")
            if "submitted_by" in sub_cols:
                cur.execute("UPDATE task_submissions SET user_id=submitted_by WHERE user_id IS NULL")
        if "file_path" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN file_path TEXT")
            if "evidence_path" in sub_cols:
                cur.execute("UPDATE task_submissions SET file_path=evidence_path WHERE file_path IS NULL")
        if "file_type" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN file_type TEXT")
        if "file_size" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN file_size INTEGER")
        if "ai_result_reference" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN ai_result_reference TEXT")
            if "ai_result_summary" in sub_cols:
                cur.execute("UPDATE task_submissions SET ai_result_reference=ai_result_summary WHERE ai_result_reference IS NULL")
        if "ai_requested" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN ai_requested INTEGER NOT NULL DEFAULT 0")
        if "ai_status" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN ai_status TEXT NOT NULL DEFAULT 'not_requested'")
        if "submitted_value" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN submitted_value REAL")
        if "file_path_2" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN file_path_2 TEXT")
        if "avg_kmpl" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN avg_kmpl REAL")
        if "distance_diff" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN distance_diff REAL")
        if "fuel_consumed" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN fuel_consumed REAL")
        if "image_taken_at" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN image_taken_at TEXT")
        if "image_taken_at_2" not in sub_cols:
            cur.execute("ALTER TABLE task_submissions ADD COLUMN image_taken_at_2 TEXT")
    # Ensure upsert target exists for ON CONFLICT(task_instance_id)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_task_submissions_instance_unique ON task_submissions(task_instance_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_task_submissions_user ON task_submissions(user_id, submitted_at DESC)")

    # Notifications/audit trail for tasks
    cur.execute("""
    CREATE TABLE IF NOT EXISTS task_notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_instance_id INTEGER NOT NULL,
        recipient_role TEXT NOT NULL CHECK(recipient_role IN ('admin', 'coadmin')),
        recipient_user_id INTEGER,
        recipient_team INTEGER,
        alert_type TEXT NOT NULL,
        message TEXT NOT NULL,
        is_read INTEGER NOT NULL DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(task_instance_id) REFERENCES task_instances(id)
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_task_notifications_role ON task_notifications(recipient_role, recipient_team, is_read)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS task_activity_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_instance_id INTEGER,
        action TEXT NOT NULL,
        actor_user_id INTEGER,
        actor_role TEXT,
        meta_json TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(task_instance_id) REFERENCES task_instances(id)
    )
    """)
    if needs_legacy_migrations:
        cur.execute("PRAGMA table_info(task_activity_log)")
        act_cols = {row[1] for row in cur.fetchall()}
        if "task_instance_id" not in act_cols:
            cur.execute("ALTER TABLE task_activity_log ADD COLUMN task_instance_id INTEGER")
            if "task_id" in act_cols:
                cur.execute("UPDATE task_activity_log SET task_instance_id=task_id WHERE task_instance_id IS NULL")
        if "meta_json" not in act_cols:
            cur.execute("ALTER TABLE task_activity_log ADD COLUMN meta_json TEXT")
            if "details" in act_cols:
                cur.execute(
                    """
                    UPDATE task_activity_log
                    SET meta_json=CASE
                        WHEN details IS NULL OR details='' THEN NULL
                        ELSE json_object('legacy_details', details)
                    END
                    WHERE meta_json IS NULL
                    """
                )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_task_activity_instance ON task_activity_log(task_instance_id, created_at DESC)")

    # Backward compatible additive columns
    if needs_legacy_migrations:
        cur.execute("PRAGMA table_info(task_forms)")
        form_cols = {row[1] for row in cur.fetchall()}
        if "allow_resubmission" not in form_cols:
            cur.execute("ALTER TABLE task_forms ADD COLUMN allow_resubmission INTEGER NOT NULL DEFAULT 0")
        if "question_type" not in form_cols:
            cur.execute("ALTER TABLE task_forms ADD COLUMN question_type TEXT NOT NULL DEFAULT 'upload'")
        if "number_min" not in form_cols:
            cur.execute("ALTER TABLE task_forms ADD COLUMN number_min REAL")
        if "number_max" not in form_cols:
            cur.execute("ALTER TABLE task_forms ADD COLUMN number_max REAL")
        if "number_unit" not in form_cols:
            cur.execute("ALTER TABLE task_forms ADD COLUMN number_unit TEXT")
        if "is_required" not in form_cols:
            cur.execute("ALTER TABLE task_forms ADD COLUMN is_required INTEGER NOT NULL DEFAULT 1")
        if "ai_engine_type" not in form_cols:
            cur.execute("ALTER TABLE task_forms ADD COLUMN ai_engine_type TEXT NOT NULL DEFAULT 'auto'")
        if "extraction_hints" not in form_cols:
            cur.execute("ALTER TABLE task_forms ADD COLUMN extraction_hints TEXT")
        if "threshold_rules_json" not in form_cols:
            cur.execute("ALTER TABLE task_forms ADD COLUMN threshold_rules_json TEXT")
        if "image_upload_count" not in form_cols:
            cur.execute("ALTER TABLE task_forms ADD COLUMN image_upload_count INTEGER NOT NULL DEFAULT 1")
        if "is_deleted" not in form_cols:
            cur.execute("ALTER TABLE task_forms ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0")

    # AI result storage
    cur.execute("""
    CREATE TABLE IF NOT EXISTS task_ai_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_instance_id INTEGER NOT NULL,
        ai_engine_type TEXT NOT NULL,
        processing_status TEXT NOT NULL,
        extracted_text TEXT,
        extracted_values_json TEXT,
        analysis_summary TEXT,
        validation_status TEXT,
        alert_triggered INTEGER NOT NULL DEFAULT 0,
        alert_reason TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(task_instance_id) REFERENCES task_instances(id)
    )
    """)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_task_ai_results_instance ON task_ai_results(task_instance_id)")

    cur.execute(f"PRAGMA user_version={SCHEMA_USER_VERSION}")
    conn.commit()
    conn.close()


# -----------------------------
# Users
# -----------------------------
def create_user(
    username: str,
    password_hash: str,
    role: str,
    team: Optional[int],
    *,
    force_password_change: bool = False,
):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO users(username, password_hash, role, team, force_password_change) VALUES(?,?,?,?,?)",
        (username, password_hash, role, team, 1 if force_password_change else 0),
    )
    conn.commit()
    conn.close()


def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE username = ?", (username,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_users_by_usernames(usernames: List[str]) -> Dict[str, Dict[str, Any]]:
    cleaned = [str(name).strip() for name in (usernames or []) if str(name).strip()]
    if not cleaned:
        return {}
    placeholders = ",".join(["?"] * len(cleaned))
    conn = _conn()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM users WHERE username IN ({placeholders})", tuple(cleaned))
    rows = cur.fetchall()
    conn.close()
    return {str(row["username"]): dict(row) for row in rows}


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE lower(email) = lower(?) OR lower(username) = lower(?) LIMIT 1", (email, email))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_user_by_google_id(google_id: str) -> Optional[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE google_id = ? LIMIT 1", (google_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def update_user_identity(
    user_id: int,
    *,
    email: Optional[str] = None,
    display_name: Optional[str] = None,
    google_id: Optional[str] = None,
    auth_provider: Optional[str] = None,
):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE users
           SET email = COALESCE(?, email),
               display_name = COALESCE(?, display_name),
               google_id = COALESCE(?, google_id),
               auth_provider = COALESCE(?, auth_provider)
         WHERE id = ?
        """,
        (email, display_name, google_id, auth_provider, int(user_id)),
    )
    conn.commit()
    conn.close()


def update_user_username(user_id: int, username: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET username=? WHERE id=?", ((username or "").strip(), int(user_id)))
    conn.commit()
    conn.close()


def update_user_password(user_id: int, password_hash: str, force_password_change: Optional[bool] = None):
    conn = _conn()
    cur = conn.cursor()
    if force_password_change is None:
        cur.execute("UPDATE users SET password_hash=? WHERE id=?", (password_hash, int(user_id)))
    else:
        cur.execute(
            "UPDATE users SET password_hash=?, force_password_change=? WHERE id=?",
            (password_hash, 1 if force_password_change else 0, int(user_id)),
        )
    conn.commit()
    conn.close()


def set_user_force_password_change(user_id: int, required: bool):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE users SET force_password_change=? WHERE id=?",
        (1 if required else 0, int(user_id)),
    )
    conn.commit()
    conn.close()


def update_user_team(user_id: int, team: Optional[int]):
    conn = _conn()
    cur = conn.cursor()
    team_value = None if team is None else int(team)
    cur.execute("UPDATE users SET team=? WHERE id=?", (team_value, int(user_id)))
    conn.commit()
    conn.close()


def fetch_users_all() -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT id, username, role, team FROM users ORDER BY username ASC")
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def fetch_users_by_team(team: int) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, username, role, team, display_name, email
        FROM users
        WHERE team=?
        ORDER BY COALESCE(NULLIF(display_name, ''), username) ASC
        """,
        (team,),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def fetch_users_without_team() -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, username, role, team, display_name, email
        FROM users
        WHERE role='user'
          AND (
            team IS NULL
            OR CAST(team AS TEXT) = ''
            OR CAST(team AS TEXT) = '0'
            OR (
              CAST(team AS TEXT) = '1'
              AND (username LIKE '%@%' OR COALESCE(email, '') LIKE '%@%')
              AND COALESCE(auth_provider, '') IN ('password', 'google')
            )
          )
        ORDER BY username ASC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


# -----------------------------
# Readings
# -----------------------------
def insert_reading(
    *,
    user_id: int,
    team: int,
    meter_type: str,
    label: str,
    value: Optional[str],
    filename: str,
    ocr_json: str,
    manual_value: Optional[str] = None,
    filename_2: Optional[str] = None,
    odometer_start: Optional[str] = None,
    odometer_end: Optional[str] = None,
    distance_diff: Optional[str] = None,
    fuel_economy: Optional[str] = None,
    fuel_consumed: Optional[str] = None,
    image_taken_at: Optional[str] = None,
    image_taken_at_2: Optional[str] = None,
) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO readings(
            user_id, team, meter_type, label, value, manual_value, filename, filename_2, ocr_json,
            odometer_start, odometer_end, distance_diff, fuel_economy, fuel_consumed, image_taken_at, image_taken_at_2
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            user_id,
            team,
            meter_type,
            label,
            value,
            manual_value,
            filename,
            filename_2,
            ocr_json,
            odometer_start,
            odometer_end,
            distance_diff,
            fuel_economy,
            fuel_consumed,
            image_taken_at,
            image_taken_at_2,
        ),
    )
    rid = cur.lastrowid
    conn.commit()
    conn.close()
    return int(rid)


def update_reading_analysis(
    *,
    reading_id: int,
    value: Optional[str],
    ocr_json: str,
    filename_2: Optional[str] = None,
    odometer_start: Optional[str] = None,
    odometer_end: Optional[str] = None,
    distance_diff: Optional[str] = None,
    fuel_economy: Optional[str] = None,
    fuel_consumed: Optional[str] = None,
    image_taken_at: Optional[str] = None,
    image_taken_at_2: Optional[str] = None,
):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE readings
        SET value=?,
            ocr_json=?,
            filename_2=?,
            odometer_start=?,
            odometer_end=?,
            distance_diff=?,
            fuel_economy=?,
            fuel_consumed=?,
            image_taken_at=?,
            image_taken_at_2=?
        WHERE id=?
        """,
        (
            value,
            ocr_json,
            filename_2,
            odometer_start,
            odometer_end,
            distance_diff,
            fuel_economy,
            fuel_consumed,
            image_taken_at,
            image_taken_at_2,
            reading_id,
        ),
    )
    conn.commit()
    conn.close()


def fetch_readings_all() -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT r.*, COALESCE(NULLIF(u.display_name, ''), u.username) AS username
        FROM readings r
        JOIN users u ON u.id = r.user_id
        ORDER BY r.created_at DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def fetch_readings_by_team(team: int) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT r.*, COALESCE(NULLIF(u.display_name, ''), u.username) AS username
        FROM readings r
        JOIN users u ON u.id = r.user_id
        WHERE r.team = ?
        ORDER BY r.created_at DESC
    """, (team,))
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def fetch_readings_by_user(user_id: int) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT r.*, COALESCE(NULLIF(u.display_name, ''), u.username) AS username
        FROM readings r
        JOIN users u ON u.id = r.user_id
        WHERE r.user_id = ?
        ORDER BY r.created_at DESC
    """, (user_id,))
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


# -----------------------------
# Alerts
# -----------------------------
def create_alert(reading_id: int, target_role: str, target_team: Optional[int], message: str, severity: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO alerts(reading_id, target_role, target_team, message, severity)
        VALUES(?,?,?,?,?)
        """,
        (reading_id, target_role, target_team, message, severity),
    )
    conn.commit()
    conn.close()


def fetch_alerts_for_admin(unread_only: bool = False) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    q = """
        SELECT a.*, r.team, r.label, r.meter_type, r.value, r.filename, r.created_at AS reading_time
        FROM alerts a
        JOIN readings r ON r.id = a.reading_id
        WHERE a.target_role='admin'
        {extra}
        ORDER BY a.created_at DESC
    """
    extra = "AND a.is_read=0" if unread_only else ""
    cur.execute(q.format(extra=extra))
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def fetch_alerts_for_coadmin(team: int, unread_only: bool = False) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    q = """
        SELECT a.*, r.team, r.label, r.meter_type, r.value, r.filename, r.created_at AS reading_time
        FROM alerts a
        JOIN readings r ON r.id = a.reading_id
        WHERE a.target_role='coadmin' AND a.target_team=?
        {extra}
        ORDER BY a.created_at DESC
    """
    extra = "AND a.is_read=0" if unread_only else ""
    cur.execute(q.format(extra=extra), (team,))
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def mark_alert_read(alert_id: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE alerts SET is_read=1 WHERE id=?", (alert_id,))
    conn.commit()
    conn.close()


def clear_alerts_admin():
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM alerts WHERE target_role='admin'")
    conn.commit()
    conn.close()


def clear_alerts_coadmin(team: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM alerts WHERE target_role='coadmin' AND target_team=?", (team,))
    conn.commit()
    conn.close()


# -----------------------------
# Messages
# -----------------------------
def create_message(
    *,
    sender_user_id: Optional[int],
    sender_role: str,
    sender_team: Optional[int],
    target_role: str,
    target_team: Optional[int],
    target_user_id: Optional[int],
    body: str,
):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO messages(sender_user_id, sender_role, sender_team, target_role, target_team, target_user_id, body)
        VALUES(?,?,?,?,?,?,?)
        """,
        (sender_user_id, sender_role, sender_team, target_role, target_team, target_user_id, body),
    )
    conn.commit()
    conn.close()


def fetch_messages_for_user(*, role: str, user_id: int, team: Optional[int]) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    if role == "admin":
        cur.execute("""
            SELECT m.*, u.username AS sender_username
            FROM messages m
            LEFT JOIN users u ON u.id = m.sender_user_id
            WHERE m.target_role='admin'
            ORDER BY m.created_at DESC
        """)
    elif role == "coadmin":
        cur.execute("""
            SELECT m.*, u.username AS sender_username
            FROM messages m
            LEFT JOIN users u ON u.id = m.sender_user_id
            WHERE m.target_role='coadmin' AND (m.target_team=? OR m.target_user_id=?)
            ORDER BY m.created_at DESC
        """, (team, user_id))
    else:
        cur.execute("""
            SELECT m.*, u.username AS sender_username
            FROM messages m
            LEFT JOIN users u ON u.id = m.sender_user_id
            WHERE m.target_role='user' AND (m.target_user_id=? OR m.target_team=?)
            ORDER BY m.created_at DESC
        """, (user_id, team))
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def mark_message_read(message_id: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE messages SET is_read=1 WHERE id=?", (message_id,))
    conn.commit()
    conn.close()


def count_unread_admin() -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM alerts WHERE target_role='admin' AND is_read=0")
    row = cur.fetchone()
    conn.close()
    return int(row["c"])


def count_unread_coadmin(team: int) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM alerts WHERE target_role='coadmin' AND target_team=? AND is_read=0", (team,))
    row = cur.fetchone()
    conn.close()
    return int(row["c"])


def get_latest_reading_id_all() -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM readings ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    return int(row["id"]) if row else 0


def get_latest_reading_id_team(team: int) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM readings WHERE team=? ORDER BY id DESC LIMIT 1", (team,))
    row = cur.fetchone()
    conn.close()
    return int(row["id"]) if row else 0


# -----------------------------
# Chat V2 (popup messenger)
# -----------------------------
def chat_list_users_for_picker(*, requester_id: int, role: str, team: Optional[int]) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    if role == "admin":
        cur.execute(
            """
            SELECT id, username, COALESCE(NULLIF(display_name, ''), username) AS display_name, role, team
            FROM users
            WHERE id <> ?
            ORDER BY role ASC, team ASC, COALESCE(NULLIF(display_name, ''), username) ASC
            """,
            (requester_id,),
        )
    elif role == "coadmin":
        cur.execute(
            """
            SELECT id, username, COALESCE(NULLIF(display_name, ''), username) AS display_name, role, team
            FROM users
            WHERE id <> ?
              AND (role='admin' OR team=?)
            ORDER BY role ASC, team ASC, COALESCE(NULLIF(display_name, ''), username) ASC
            """,
            (requester_id, team),
        )
    else:
        cur.execute(
            """
            SELECT id, username, COALESCE(NULLIF(display_name, ''), username) AS display_name, role, team
            FROM users
            WHERE id <> ?
              AND (role='admin' OR (role='coadmin' AND team=?))
            ORDER BY role ASC, team ASC, COALESCE(NULLIF(display_name, ''), username) ASC
            """,
            (requester_id, team),
        )
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def chat_is_member(*, conversation_id: int, user_id: int) -> bool:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM chat_members WHERE conversation_id=? AND user_id=? LIMIT 1",
        (conversation_id, user_id),
    )
    row = cur.fetchone()
    conn.close()
    return bool(row)


def chat_is_blocked_between(user_a: int, user_b: int) -> bool:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM chat_blocks
        WHERE (blocker_user_id=? AND blocked_user_id=?)
           OR (blocker_user_id=? AND blocked_user_id=?)
        LIMIT 1
        """,
        (user_a, user_b, user_b, user_a),
    )
    row = cur.fetchone()
    conn.close()
    return bool(row)


def chat_get_or_create_direct(*, user_a: int, user_b: int, created_by: int) -> int:
    low, high = sorted([int(user_a), int(user_b)])
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.id
        FROM chat_conversations c
        JOIN chat_members m1 ON m1.conversation_id = c.id AND m1.user_id = ?
        JOIN chat_members m2 ON m2.conversation_id = c.id AND m2.user_id = ?
        WHERE c.conversation_type='direct'
          AND (
            SELECT COUNT(*)
            FROM chat_members m
            WHERE m.conversation_id = c.id
          ) = 2
        LIMIT 1
        """,
        (low, high),
    )
    row = cur.fetchone()
    if row:
        cid = int(row["id"])
        conn.close()
        return cid

    cur.execute(
        """
        INSERT INTO chat_conversations(conversation_type, title, created_by)
        VALUES('direct', NULL, ?)
        """,
        (created_by,),
    )
    cid = int(cur.lastrowid)
    cur.execute(
        "INSERT INTO chat_members(conversation_id, user_id, member_role) VALUES(?,?,'member')",
        (cid, low),
    )
    cur.execute(
        "INSERT INTO chat_members(conversation_id, user_id, member_role) VALUES(?,?,'member')",
        (cid, high),
    )
    conn.commit()
    conn.close()
    return cid


def chat_create_group(*, created_by: int, title: str, member_ids: List[int]) -> int:
    unique_members = sorted({int(x) for x in member_ids if int(x) > 0})
    if created_by not in unique_members:
        unique_members.insert(0, int(created_by))

    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO chat_conversations(conversation_type, title, created_by)
        VALUES('group', ?, ?)
        """,
        ((title or "New Group").strip()[:80], created_by),
    )
    cid = int(cur.lastrowid)
    for uid in unique_members:
        role = "admin" if uid == int(created_by) else "member"
        cur.execute(
            "INSERT INTO chat_members(conversation_id, user_id, member_role) VALUES(?,?,?)",
            (cid, uid, role),
        )
    conn.commit()
    conn.close()
    return cid


def chat_fetch_members(conversation_id: int) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT m.user_id, m.member_role, u.username, u.display_name, u.role, u.team
        FROM chat_members m
        JOIN users u ON u.id = m.user_id
        WHERE m.conversation_id=?
        ORDER BY COALESCE(NULLIF(u.display_name, ''), u.username) ASC
        """,
        (conversation_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def chat_list_conversations(*, user_id: int, search: str = "", limit: int = 50) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    q = """
        SELECT
            c.id,
            c.conversation_type,
            c.title,
            c.updated_at,
            c.created_at,
            m.member_role,
            m.pinned,
            m.muted_until,
            (
              SELECT dm.user_id
              FROM chat_members dm
              WHERE dm.conversation_id=c.id AND dm.user_id<>?
              ORDER BY dm.user_id ASC
              LIMIT 1
            ) AS direct_peer_user_id,
            (
              SELECT COALESCE(NULLIF(u.display_name, ''), u.username)
              FROM chat_members dm
              JOIN users u ON u.id=dm.user_id
              WHERE dm.conversation_id=c.id AND dm.user_id<>?
              ORDER BY dm.user_id ASC
              LIMIT 1
            ) AS direct_peer_username,
            (
              SELECT cm.body
              FROM chat_messages cm
              WHERE cm.conversation_id=c.id
              ORDER BY cm.id DESC
              LIMIT 1
            ) AS last_message_body,
            (
              SELECT cm.created_at
              FROM chat_messages cm
              WHERE cm.conversation_id=c.id
              ORDER BY cm.id DESC
              LIMIT 1
            ) AS last_message_at,
            (
              SELECT COUNT(*)
              FROM chat_messages msg
              LEFT JOIN chat_receipts r ON r.message_id=msg.id AND r.user_id=?
              WHERE msg.conversation_id=c.id
                AND msg.sender_user_id<>?
                AND (r.read_at IS NULL)
            ) AS unread_count
        FROM chat_conversations c
        JOIN chat_members m ON m.conversation_id=c.id AND m.user_id=?
        WHERE m.is_archived=0
    """
    params: List[Any] = [user_id, user_id, user_id, user_id, user_id]
    term = (search or "").strip()
    if term:
        q += """
          AND (
            lower(COALESCE(c.title, '')) LIKE ?
            OR lower(COALESCE((
              SELECT COALESCE(NULLIF(u.display_name, ''), u.username)
              FROM chat_members dm
              JOIN users u ON u.id=dm.user_id
              WHERE dm.conversation_id=c.id AND dm.user_id<>?
              LIMIT 1
            ), '')) LIKE ?
          )
        """
        like = f"%{term.lower()}%"
        params.extend([like, user_id, like])
    q += """
        ORDER BY m.pinned DESC, COALESCE(last_message_at, c.updated_at) DESC
        LIMIT ?
    """
    params.append(int(limit))
    cur.execute(q, tuple(params))
    rows = [dict(x) for x in cur.fetchall()]
    conn.close()
    return rows


def chat_fetch_messages(*, conversation_id: int, limit: int = 40, before_id: Optional[int] = None) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    if before_id:
        cur.execute(
            """
            SELECT m.*, COALESCE(NULLIF(u.display_name, ''), u.username) AS sender_username
            FROM chat_messages m
            JOIN users u ON u.id = m.sender_user_id
            WHERE m.conversation_id=? AND m.id < ?
            ORDER BY m.id DESC
            LIMIT ?
            """,
            (conversation_id, int(before_id), int(limit)),
        )
    else:
        cur.execute(
            """
            SELECT m.*, COALESCE(NULLIF(u.display_name, ''), u.username) AS sender_username
            FROM chat_messages m
            JOIN users u ON u.id = m.sender_user_id
            WHERE m.conversation_id=?
            ORDER BY m.id DESC
            LIMIT ?
            """,
            (conversation_id, int(limit)),
        )
    rows = [dict(x) for x in cur.fetchall()]
    rows.reverse()
    conn.close()
    return rows


def chat_fetch_reactions(message_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    if not message_ids:
        return {}
    conn = _conn()
    cur = conn.cursor()
    placeholders = ",".join(["?"] * len(message_ids))
    cur.execute(
        f"""
        SELECT r.message_id, r.user_id, r.emoji, COALESCE(NULLIF(u.display_name, ''), u.username) AS username
        FROM chat_reactions r
        JOIN users u ON u.id=r.user_id
        WHERE r.message_id IN ({placeholders})
        ORDER BY r.message_id ASC, r.emoji ASC, r.created_at ASC
        """,
        tuple(message_ids),
    )
    out: Dict[int, List[Dict[str, Any]]] = {}
    for row in cur.fetchall():
        d = dict(row)
        mid = int(d["message_id"])
        out.setdefault(mid, []).append(d)
    conn.close()
    return out


def chat_create_message(
    *,
    conversation_id: int,
    sender_user_id: int,
    message_type: str,
    body: str,
    reply_to_message_id: Optional[int],
    client_msg_id: Optional[str],
) -> int:
    conn = _conn()
    cur = conn.cursor()
    if client_msg_id:
        cur.execute(
            """
            SELECT id
            FROM chat_messages
            WHERE conversation_id=? AND sender_user_id=? AND client_msg_id=?
            LIMIT 1
            """,
            (conversation_id, sender_user_id, client_msg_id),
        )
        row = cur.fetchone()
        if row:
            conn.close()
            return int(row["id"])

    cur.execute(
        """
        INSERT INTO chat_messages(conversation_id, sender_user_id, message_type, body, reply_to_message_id, client_msg_id)
        VALUES(?,?,?,?,?,?)
        """,
        (
            conversation_id,
            sender_user_id,
            message_type,
            body,
            reply_to_message_id,
            client_msg_id,
        ),
    )
    mid = int(cur.lastrowid)
    cur.execute(
        """
        INSERT OR IGNORE INTO chat_receipts(message_id, user_id, delivered_at, read_at)
        VALUES(?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
        """,
        (mid, sender_user_id),
    )
    cur.execute(
        "UPDATE chat_conversations SET updated_at=CURRENT_TIMESTAMP WHERE id=?",
        (conversation_id,),
    )
    conn.commit()
    conn.close()
    return mid


def chat_get_message(message_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT m.*, COALESCE(NULLIF(u.display_name, ''), u.username) AS sender_username
        FROM chat_messages m
        JOIN users u ON u.id = m.sender_user_id
        WHERE m.id=?
        LIMIT 1
        """,
        (message_id,),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def chat_mark_read(*, message_id: int, user_id: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO chat_receipts(message_id, user_id, delivered_at, read_at)
        VALUES(?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
        ON CONFLICT(message_id, user_id) DO UPDATE SET
          delivered_at=COALESCE(chat_receipts.delivered_at, CURRENT_TIMESTAMP),
          read_at=CURRENT_TIMESTAMP
        """,
        (message_id, user_id),
    )
    cur.execute(
        """
        UPDATE chat_members
        SET last_read_message_id=?
        WHERE conversation_id=(SELECT conversation_id FROM chat_messages WHERE id=?)
          AND user_id=?
        """,
        (message_id, message_id, user_id),
    )
    conn.commit()
    conn.close()


def chat_toggle_reaction(*, message_id: int, user_id: int, emoji: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM chat_reactions WHERE message_id=? AND user_id=? AND emoji=?",
        (message_id, user_id, emoji),
    )
    row = cur.fetchone()
    if row:
        cur.execute("DELETE FROM chat_reactions WHERE id=?", (row["id"],))
    else:
        cur.execute(
            "INSERT INTO chat_reactions(message_id, user_id, emoji) VALUES(?,?,?)",
            (message_id, user_id, emoji),
        )
    conn.commit()
    conn.close()


def chat_edit_message(*, message_id: int, body: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE chat_messages
        SET body=?, is_edited=1, edited_at=CURRENT_TIMESTAMP
        WHERE id=?
        """,
        (body, message_id),
    )
    conn.commit()
    conn.close()


def chat_soft_delete_message(message_id: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE chat_messages
        SET is_deleted=1, body='This message was deleted', is_edited=0, edited_at=CURRENT_TIMESTAMP
        WHERE id=?
        """,
        (message_id,),
    )
    conn.commit()
    conn.close()


def chat_set_member_flags(
    *,
    conversation_id: int,
    user_id: int,
    pinned: Optional[int] = None,
    muted_until: Optional[str] = None,
):
    conn = _conn()
    cur = conn.cursor()
    if pinned is not None:
        cur.execute(
            "UPDATE chat_members SET pinned=? WHERE conversation_id=? AND user_id=?",
            (1 if int(pinned) else 0, conversation_id, user_id),
        )
    if muted_until is not None:
        cur.execute(
            "UPDATE chat_members SET muted_until=? WHERE conversation_id=? AND user_id=?",
            (muted_until, conversation_id, user_id),
        )
    conn.commit()
    conn.close()


def chat_add_block(*, blocker_user_id: int, blocked_user_id: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO chat_blocks(blocker_user_id, blocked_user_id)
        VALUES(?,?)
        """,
        (blocker_user_id, blocked_user_id),
    )
    conn.commit()
    conn.close()


def chat_create_report(
    *,
    reporter_user_id: int,
    conversation_id: int,
    message_id: Optional[int],
    reason: str,
) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO chat_reports(reporter_user_id, conversation_id, message_id, reason)
        VALUES(?,?,?,?)
        """,
        (reporter_user_id, conversation_id, message_id, (reason or "").strip()[:500]),
    )
    rid = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return rid


# -----------------------------
# Tasks
# -----------------------------
def task_create_form(
    *,
    title: str,
    description: Optional[str],
    creator_user_id: int,
    creator_role: str,
    assigned_scope: str,
    assigned_user_ids: Optional[List[int]],
    assigned_team_id: Optional[int],
    deadline_at: str,
    allowed_types: List[str],
    ai_enabled: bool,
    repeat_enabled: bool,
    repeat_type: Optional[str],
    repeat_interval_days: Optional[int],
    allow_resubmission: bool = False,
    question_type: str = "upload",
    number_min: Optional[float] = None,
    number_max: Optional[float] = None,
    number_unit: Optional[str] = None,
    is_required: bool = True,
    ai_engine_type: str = "auto",
    extraction_hints: Optional[str] = None,
    threshold_rules_json: Optional[str] = None,
    image_upload_count: int = 1,
    priority: str = "medium",
    status: str = "active",
) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO task_forms(
            title, description, creator_user_id, creator_role, assigned_scope,
            assigned_user_ids_json, assigned_team_id, deadline_at, allowed_types_json,
            ai_enabled, repeat_enabled, repeat_type, repeat_interval_days, allow_resubmission,
            question_type, number_min, number_max, number_unit, is_required,
            ai_engine_type, extraction_hints, threshold_rules_json, image_upload_count, priority, status
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            title.strip(),
            (description or "").strip() or None,
            creator_user_id,
            creator_role,
            assigned_scope,
            json.dumps(assigned_user_ids or []),
            assigned_team_id,
            deadline_at,
            json.dumps(allowed_types),
            1 if ai_enabled else 0,
            1 if repeat_enabled else 0,
            repeat_type,
            repeat_interval_days,
            1 if allow_resubmission else 0,
            question_type,
            number_min,
            number_max,
            (number_unit or "").strip() or None,
            1 if is_required else 0,
            (ai_engine_type or "auto").strip() or "auto",
            (extraction_hints or "").strip() or None,
            (threshold_rules_json or "").strip() or None,
            max(1, min(2, int(image_upload_count or 1))),
            priority,
            status,
        ),
    )
    fid = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return fid


def task_get_form(form_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT f.*, u.username AS creator_username
        FROM task_forms f
        LEFT JOIN users u ON u.id=f.creator_user_id
        WHERE f.id=?
        """,
        (form_id,),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def task_update_form(
    *,
    form_id: int,
    assigned_scope: str,
    assigned_user_ids: Optional[List[int]],
    assigned_team_id: Optional[int],
    deadline_at: str,
    repeat_enabled: bool,
    repeat_type: Optional[str],
    repeat_interval_days: Optional[int],
):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE task_forms
        SET assigned_scope=?,
            assigned_user_ids_json=?,
            assigned_team_id=?,
            deadline_at=?,
            repeat_enabled=?,
            repeat_type=?,
            repeat_interval_days=?,
            updated_at=CURRENT_TIMESTAMP
        WHERE id=?
        """,
        (
            assigned_scope,
            json.dumps(assigned_user_ids or []),
            assigned_team_id,
            deadline_at,
            1 if repeat_enabled else 0,
            repeat_type,
            repeat_interval_days,
            form_id,
        ),
    )
    conn.commit()
    conn.close()


def task_delete_form(form_id: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE task_forms
        SET is_deleted=1,
            repeat_enabled=0,
            updated_at=CURRENT_TIMESTAMP
        WHERE id=?
        """,
        (form_id,),
    )
    conn.commit()
    conn.close()


def task_list_instances_for_form(form_id: int) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT i.*, s.id AS submission_id
        FROM task_instances i
        LEFT JOIN task_submissions s ON s.task_instance_id=i.id
        WHERE i.form_id=?
        ORDER BY i.id DESC
        """,
        (form_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def task_update_instance_assignment(
    *,
    instance_id: int,
    assigned_user_id: int,
    assigned_team_id: Optional[int],
    deadline_at: str,
    cycle_key: str,
    status: str,
):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE task_instances
        SET assigned_user_id=?,
            assigned_team_id=?,
            deadline_at=?,
            cycle_key=?,
            status=?,
            updated_at=CURRENT_TIMESTAMP
        WHERE id=?
        """,
        (assigned_user_id, assigned_team_id, deadline_at, cycle_key, status, instance_id),
    )
    conn.commit()
    conn.close()


def task_delete_instance(instance_id: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM task_instances WHERE id=?", (instance_id,))
    conn.commit()
    conn.close()


def task_list_forms_for_actor(*, role: str, team: Optional[int], user_id: int) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    q = """
        SELECT f.*, u.username AS creator_username
        FROM task_forms f
        LEFT JOIN users u ON u.id=f.creator_user_id
        WHERE COALESCE(f.is_deleted, 0)=0
    """
    params: List[Any] = []
    if role in {"admin", "coadmin"}:
        q += " AND f.creator_user_id=? "
        params.append(user_id)
    elif role == "user":
        q += " AND 1=0 "
    q += " ORDER BY f.id DESC "
    cur.execute(q, tuple(params))
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def task_create_instance(
    *,
    form_id: int,
    assigned_user_id: int,
    assigned_team_id: Optional[int],
    deadline_at: str,
    cycle_key: str,
    status: str = "pending",
) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO task_instances(form_id, assigned_user_id, assigned_team_id, deadline_at, cycle_key, status)
        VALUES(?,?,?,?,?,?)
        """,
        (form_id, assigned_user_id, assigned_team_id, deadline_at, cycle_key, status),
    )
    if cur.lastrowid:
        iid = int(cur.lastrowid)
    else:
        cur.execute(
            """
            SELECT id FROM task_instances
            WHERE form_id=? AND assigned_user_id=? AND cycle_key=?
            """,
            (form_id, assigned_user_id, cycle_key),
        )
        row = cur.fetchone()
        iid = int(row["id"]) if row else 0
    conn.commit()
    conn.close()
    return iid


def task_get_instance(instance_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            i.*, f.title, f.description, f.allowed_types_json, f.ai_enabled, f.repeat_enabled, f.repeat_type,
            f.repeat_interval_days, f.allow_resubmission, f.question_type, f.number_min, f.number_max, f.number_unit,
            f.image_upload_count, f.priority, f.creator_user_id, f.creator_role,
            COALESCE(NULLIF(u.display_name, ''), u.username) AS assigned_username
        FROM task_instances i
        JOIN task_forms f ON f.id=i.form_id
        LEFT JOIN users u ON u.id=i.assigned_user_id
        WHERE i.id=?
        """,
        (instance_id,),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def task_list_instances_for_user(*, user_id: int, status: Optional[str] = None) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    q = """
        SELECT
            i.*, f.title, f.description, f.allowed_types_json, f.ai_enabled, f.allow_resubmission,
            f.question_type, f.number_min, f.number_max, f.number_unit, f.image_upload_count, f.priority,
            f.extraction_hints, f.threshold_rules_json, f.ai_engine_type,
            f.creator_role, f.creator_user_id,
            s.submitted_value AS response_value,
            s.file_path AS response_file_path,
            s.file_path_2 AS response_file_path_2,
            s.distance_diff AS response_distance_diff,
            s.fuel_consumed AS response_fuel_consumed,
            s.ai_result_reference AS ai_result_reference,
            s.image_taken_at AS response_image_taken_at,
            s.image_taken_at_2 AS response_image_taken_at_2
        FROM task_instances i
        JOIN task_forms f ON f.id=i.form_id
        LEFT JOIN task_submissions s ON s.task_instance_id=i.id
        WHERE i.assigned_user_id=?
    """
    params: List[Any] = [user_id]
    if status and status != "all":
        q += " AND i.status=? "
        params.append(status)
    q += " ORDER BY i.id DESC "
    cur.execute(q, tuple(params))
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def task_list_instances_for_scope(*, role: str, team: Optional[int]) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    q = """
        SELECT
            i.*, f.title, f.description, f.allowed_types_json, f.ai_enabled, f.repeat_enabled, f.repeat_type,
            f.repeat_interval_days, f.question_type, f.number_min, f.number_max, f.number_unit, f.image_upload_count,
            f.extraction_hints, f.threshold_rules_json, f.ai_engine_type,
            f.priority, f.creator_role, f.creator_user_id,
            COALESCE(NULLIF(u.display_name, ''), u.username) AS assigned_username,
            s.submitted_value AS response_value,
            s.file_path AS response_file_path,
            s.file_path_2 AS response_file_path_2,
            s.distance_diff AS response_distance_diff,
            s.fuel_consumed AS response_fuel_consumed,
            s.ai_result_reference AS ai_result_reference,
            s.image_taken_at AS response_image_taken_at,
            s.image_taken_at_2 AS response_image_taken_at_2
        FROM task_instances i
        JOIN task_forms f ON f.id=i.form_id
        LEFT JOIN users u ON u.id=i.assigned_user_id
        LEFT JOIN task_submissions s ON s.task_instance_id=i.id
        WHERE 1=1
    """
    params: List[Any] = []
    if role == "coadmin":
        q += " AND i.assigned_team_id=? "
        params.append(team)
    q += " ORDER BY i.id DESC "
    cur.execute(q, tuple(params))
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def task_mark_instance_status(*, instance_id: int, status: str, submitted_at: Optional[str] = None):
    conn = _conn()
    cur = conn.cursor()
    if submitted_at is None:
        cur.execute(
            "UPDATE task_instances SET status=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (status, instance_id),
        )
    else:
        cur.execute(
            """
            UPDATE task_instances
            SET status=?, submitted_at=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?
            """,
            (status, submitted_at, instance_id),
        )
    conn.commit()
    conn.close()


def task_get_submission(instance_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM task_submissions WHERE task_instance_id=?",
        (instance_id,),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def task_upsert_submission(
    *,
    task_instance_id: int,
    user_id: int,
    file_path: str,
    file_type: str,
    file_size: int,
    remarks: Optional[str],
    submitted_value: Optional[float] = None,
    file_path_2: Optional[str] = None,
    avg_kmpl: Optional[float] = None,
    distance_diff: Optional[float] = None,
    fuel_consumed: Optional[float] = None,
    image_taken_at: Optional[str] = None,
    image_taken_at_2: Optional[str] = None,
    ai_requested: bool,
    ai_status: str,
    ai_result_reference: Optional[str],
):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(task_submissions)")
    cols = {row[1] for row in cur.fetchall()}
    legacy_task_id = "task_id" in cols
    legacy_submitted_by = "submitted_by" in cols
    legacy_evidence_path = "evidence_path" in cols
    has_submitted_value = "submitted_value" in cols
    legacy_ai_summary = "ai_result_summary" in cols

    cur.execute("SELECT id FROM task_submissions WHERE task_instance_id=?", (task_instance_id,))
    existing = cur.fetchone()
    if existing:
        sets: List[str] = []
        params: List[Any] = []
        if "user_id" in cols:
            sets.append("user_id=?")
            params.append(user_id)
        if "file_path" in cols:
            sets.append("file_path=?")
            params.append(file_path)
        if "file_type" in cols:
            sets.append("file_type=?")
            params.append(file_type)
        if "file_path_2" in cols:
            sets.append("file_path_2=?")
            params.append(file_path_2)
        if "file_size" in cols:
            sets.append("file_size=?")
            params.append(file_size)
        sets.append("remarks=?")
        params.append((remarks or "").strip() or None)
        if "ai_requested" in cols:
            sets.append("ai_requested=?")
            params.append(1 if ai_requested else 0)
        if "ai_status" in cols:
            sets.append("ai_status=?")
            params.append(ai_status)
        if "ai_result_reference" in cols:
            sets.append("ai_result_reference=?")
            params.append(ai_result_reference)
        if legacy_task_id:
            sets.append("task_id=?")
            params.append(task_instance_id)
        if legacy_submitted_by:
            sets.append("submitted_by=?")
            params.append(user_id)
        if legacy_evidence_path:
            sets.append("evidence_path=?")
            params.append(file_path)
        if has_submitted_value:
            sets.append("submitted_value=?")
            params.append(submitted_value)
        if "avg_kmpl" in cols:
            sets.append("avg_kmpl=?")
            params.append(avg_kmpl)
        if "distance_diff" in cols:
            sets.append("distance_diff=?")
            params.append(distance_diff)
        if "fuel_consumed" in cols:
            sets.append("fuel_consumed=?")
            params.append(fuel_consumed)
        if "image_taken_at" in cols:
            sets.append("image_taken_at=?")
            params.append(image_taken_at)
        if "image_taken_at_2" in cols:
            sets.append("image_taken_at_2=?")
            params.append(image_taken_at_2)
        if legacy_ai_summary:
            sets.append("ai_result_summary=?")
            params.append(ai_result_reference)
        sets.append("submitted_at=CURRENT_TIMESTAMP")
        params.append(existing["id"])
        cur.execute(f"UPDATE task_submissions SET {', '.join(sets)} WHERE id=?", tuple(params))
    else:
        insert_cols = ["task_instance_id", "remarks"]
        values: List[Any] = [task_instance_id, (remarks or "").strip() or None]
        if "user_id" in cols:
            insert_cols.append("user_id")
            values.append(user_id)
        if "file_path" in cols:
            insert_cols.append("file_path")
            values.append(file_path)
        if "file_type" in cols:
            insert_cols.append("file_type")
            values.append(file_type)
        if "file_path_2" in cols:
            insert_cols.append("file_path_2")
            values.append(file_path_2)
        if "file_size" in cols:
            insert_cols.append("file_size")
            values.append(file_size)
        if "ai_requested" in cols:
            insert_cols.append("ai_requested")
            values.append(1 if ai_requested else 0)
        if "ai_status" in cols:
            insert_cols.append("ai_status")
            values.append(ai_status)
        if "ai_result_reference" in cols:
            insert_cols.append("ai_result_reference")
            values.append(ai_result_reference)
        if legacy_task_id:
            insert_cols.append("task_id")
            values.append(task_instance_id)
        if legacy_submitted_by:
            insert_cols.append("submitted_by")
            values.append(user_id)
        if legacy_evidence_path:
            insert_cols.append("evidence_path")
            values.append(file_path)
        if has_submitted_value:
            insert_cols.append("submitted_value")
            values.append(submitted_value)
        if "avg_kmpl" in cols:
            insert_cols.append("avg_kmpl")
            values.append(avg_kmpl)
        if "distance_diff" in cols:
            insert_cols.append("distance_diff")
            values.append(distance_diff)
        if "fuel_consumed" in cols:
            insert_cols.append("fuel_consumed")
            values.append(fuel_consumed)
        if "image_taken_at" in cols:
            insert_cols.append("image_taken_at")
            values.append(image_taken_at)
        if "image_taken_at_2" in cols:
            insert_cols.append("image_taken_at_2")
            values.append(image_taken_at_2)
        if legacy_ai_summary:
            insert_cols.append("ai_result_summary")
            values.append(ai_result_reference)
        placeholders = ",".join(["?"] * len(insert_cols))
        cur.execute(
            f"INSERT INTO task_submissions({', '.join(insert_cols)}) VALUES({placeholders})",
            tuple(values),
        )
    conn.commit()
    conn.close()


def task_list_due_for_overdue(now_iso: str) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT i.*, f.title, f.description, f.creator_role, f.creator_user_id
        FROM task_instances i
        JOIN task_forms f ON f.id=i.form_id
        WHERE i.status='pending' AND i.deadline_at < ?
        ORDER BY i.deadline_at ASC
        """,
        (now_iso,),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def task_mark_overdue_sent(instance_id: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE task_instances
        SET status='overdue', is_alert_sent=1, updated_at=CURRENT_TIMESTAMP
        WHERE id=?
        """,
        (instance_id,),
    )
    conn.commit()
    conn.close()


def task_list_repeat_forms() -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM task_forms
        WHERE status='active' AND repeat_enabled=1 AND COALESCE(is_deleted, 0)=0
        ORDER BY id DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]


def task_get_latest_instance_for_user(*, form_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM task_instances
        WHERE form_id=? AND assigned_user_id=?
        ORDER BY deadline_at DESC, id DESC
        LIMIT 1
        """,
        (form_id, user_id),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def task_create_notification(
    *,
    task_instance_id: int,
    recipient_role: str,
    recipient_user_id: Optional[int],
    recipient_team: Optional[int],
    alert_type: str,
    message: str,
) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO task_notifications(
            task_instance_id, recipient_role, recipient_user_id, recipient_team, alert_type, message
        )
        VALUES(?,?,?,?,?,?)
        """,
        (task_instance_id, recipient_role, recipient_user_id, recipient_team, alert_type, message),
    )
    nid = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return nid


def task_log_activity(
    *,
    task_instance_id: Optional[int],
    action: str,
    actor_user_id: Optional[int],
    actor_role: Optional[str],
    meta: Optional[Dict[str, Any]] = None,
):
    conn = None
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(task_activity_log)")
        cols = {row[1] for row in cur.fetchall()}
        insert_cols: List[str] = ["action"]
        values: List[Any] = [action]
        if "task_instance_id" in cols:
            insert_cols.append("task_instance_id")
            values.append(task_instance_id)
        if "task_id" in cols:
            insert_cols.append("task_id")
            # Legacy databases may still require task_id as NOT NULL even for
            # form-level events that do not target a concrete instance yet.
            values.append(int(task_instance_id) if task_instance_id is not None else 0)
        if "actor_user_id" in cols:
            insert_cols.append("actor_user_id")
            values.append(actor_user_id)
        if "actor_role" in cols:
            insert_cols.append("actor_role")
            values.append(actor_role)
        meta_json = json.dumps(meta or {}, default=str) if meta is not None else None
        if "meta_json" in cols:
            insert_cols.append("meta_json")
            values.append(meta_json)
        if "details" in cols:
            insert_cols.append("details")
            values.append(meta_json)
        placeholders = ",".join(["?"] * len(insert_cols))
        cur.execute(
            f"INSERT INTO task_activity_log({', '.join(insert_cols)}) VALUES({placeholders})",
            tuple(values),
        )
        conn.commit()
    except Exception:
        log_event(
            _db_logger,
            "ERROR",
            "TASK_ACTIVITY_LOG_FAIL",
            "Failed to persist task activity log",
            exc_info=True,
            action=action,
            task_instance_id=task_instance_id,
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def task_upsert_question(
    *,
    form_id: int,
    question_text: str,
    allowed_media_types: List[str],
    extraction_hints: Optional[str],
    threshold_rules_json: Optional[str],
    expected_field_type: Optional[str],
    ideal_min: Optional[float],
    ideal_max: Optional[float],
    unit: Optional[str],
    alert_condition: Optional[str],
    parsing_instructions: Optional[str],
):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO task_questions(
            form_id, question_text, question_type, allowed_media_types_json, extraction_hints, threshold_rules_json,
            expected_field_type, ideal_min, ideal_max, unit, alert_condition, parsing_instructions
        )
        VALUES(?,?, 'media_response', ?,?,?,?,?,?,?,?,?)
        ON CONFLICT(form_id) DO UPDATE SET
            question_text=excluded.question_text,
            allowed_media_types_json=excluded.allowed_media_types_json,
            extraction_hints=excluded.extraction_hints,
            threshold_rules_json=excluded.threshold_rules_json,
            expected_field_type=excluded.expected_field_type,
            ideal_min=excluded.ideal_min,
            ideal_max=excluded.ideal_max,
            unit=excluded.unit,
            alert_condition=excluded.alert_condition,
            parsing_instructions=excluded.parsing_instructions,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            form_id,
            question_text.strip(),
            json.dumps(allowed_media_types or []),
            (extraction_hints or "").strip() or None,
            (threshold_rules_json or "").strip() or None,
            (expected_field_type or "").strip() or None,
            ideal_min,
            ideal_max,
            (unit or "").strip() or None,
            (alert_condition or "").strip() or None,
            (parsing_instructions or "").strip() or None,
        ),
    )
    conn.commit()
    conn.close()


def task_get_question(form_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM task_questions WHERE form_id=?", (form_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def task_upsert_ai_result(
    *,
    task_instance_id: int,
    ai_engine_type: str,
    processing_status: str,
    extracted_text: Optional[str],
    extracted_values: Optional[Dict[str, Any]],
    analysis_summary: Optional[str],
    validation_status: Optional[str],
    alert_triggered: bool,
    alert_reason: Optional[str],
):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO task_ai_results(
            task_instance_id, ai_engine_type, processing_status, extracted_text, extracted_values_json,
            analysis_summary, validation_status, alert_triggered, alert_reason
        )
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(task_instance_id) DO UPDATE SET
            ai_engine_type=excluded.ai_engine_type,
            processing_status=excluded.processing_status,
            extracted_text=excluded.extracted_text,
            extracted_values_json=excluded.extracted_values_json,
            analysis_summary=excluded.analysis_summary,
            validation_status=excluded.validation_status,
            alert_triggered=excluded.alert_triggered,
            alert_reason=excluded.alert_reason,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            task_instance_id,
            (ai_engine_type or "auto").strip() or "auto",
            (processing_status or "").strip() or "failed",
            extracted_text,
            json.dumps(extracted_values or {}, default=str) if extracted_values is not None else None,
            analysis_summary,
            validation_status,
            1 if alert_triggered else 0,
            alert_reason,
        ),
    )
    conn.commit()
    conn.close()


def task_get_ai_result(task_instance_id: int) -> Optional[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM task_ai_results WHERE task_instance_id=?", (task_instance_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def task_report_rows(*, role: str, team: Optional[int]) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    q = """
        SELECT
            i.id AS task_instance_id,
            i.status AS task_status,
            i.deadline_at,
            i.submitted_at,
            i.assigned_team_id,
            f.id AS form_id,
            f.title AS form_title,
            f.creator_role,
            f.ai_enabled,
            f.ai_engine_type,
            q.question_text,
            q.unit,
            q.ideal_min,
            q.ideal_max,
            COALESCE(NULLIF(u.display_name, ''), u.username) AS assigned_username,
            s.file_path,
            s.file_type,
            s.remarks,
            ar.processing_status AS ai_processing_status,
            ar.extracted_text,
            ar.extracted_values_json,
            ar.analysis_summary,
            ar.validation_status,
            ar.alert_triggered,
            ar.alert_reason
        FROM task_instances i
        JOIN task_forms f ON f.id=i.form_id
        LEFT JOIN task_questions q ON q.form_id=f.id
        LEFT JOIN users u ON u.id=i.assigned_user_id
        LEFT JOIN task_submissions s ON s.task_instance_id=i.id
        LEFT JOIN task_ai_results ar ON ar.task_instance_id=i.id
        WHERE 1=1
    """
    params: List[Any] = []
    if role == "coadmin":
        q += " AND i.assigned_team_id=? "
        params.append(team)
    q += " ORDER BY i.deadline_at DESC, i.id DESC "
    cur.execute(q, tuple(params))
    rows = cur.fetchall()
    conn.close()
    return [dict(x) for x in rows]
