"""
Trigger: Event-driven DAG execution.

Provides `TriggerDef` for declarative trigger definitions,
`TriggerStore` for activation state persistence, and
`TriggerManager` for runtime coordination.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, model_validator

from lythonic.periodic import Interval
from lythonic.state import execute_sql, open_sqlite_db

_TRIGGER_ACTIVATIONS_DDL = """\
CREATE TABLE IF NOT EXISTS trigger_activations (
    name TEXT PRIMARY KEY,
    dag_nsref TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    status TEXT NOT NULL,
    last_run_at REAL,
    next_run_at REAL,
    last_run_id TEXT,
    created_at REAL NOT NULL,
    config_json TEXT
)"""

_TRIGGER_EVENTS_DDL = """\
CREATE TABLE IF NOT EXISTS trigger_events (
    event_id TEXT PRIMARY KEY,
    trigger_name TEXT NOT NULL,
    fired_at REAL NOT NULL,
    run_id TEXT,
    payload_json TEXT,
    status TEXT NOT NULL,
    FOREIGN KEY (trigger_name) REFERENCES trigger_activations(name)
)"""


class TriggerDef(BaseModel):
    """
    Declarative trigger definition. Registered in a `Namespace` via
    `register_trigger()`. Does not start anything -- purely metadata.
    """

    name: str
    dag_nsref: str
    trigger_type: str  # "poll" or "push"
    interval: Interval | None = None
    poll_fn: Callable[[], Any] | None = None

    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def _validate_trigger(self) -> TriggerDef:
        if self.trigger_type == "poll" and self.interval is None:
            raise ValueError(f"Trigger '{self.name}': poll triggers require an interval")
        return self


class TriggerStore:
    """SQLite-backed storage for trigger activations and events."""

    db_path: Path

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(cursor, _TRIGGER_ACTIVATIONS_DDL)
            execute_sql(cursor, _TRIGGER_EVENTS_DDL)
            conn.commit()

    def activate(self, trigger_def: TriggerDef) -> None:
        """Create or update an activation record from a trigger definition."""
        config: dict[str, Any] = {}
        if trigger_def.interval is not None:
            config["interval"] = str(trigger_def.interval)

        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "INSERT OR REPLACE INTO trigger_activations "
                "(name, dag_nsref, trigger_type, status, created_at, config_json) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    trigger_def.name,
                    trigger_def.dag_nsref,
                    trigger_def.trigger_type,
                    "active",
                    time.time(),
                    json.dumps(config),
                ),
            )
            conn.commit()

    def deactivate(self, name: str) -> None:
        """Set activation status to disabled."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "UPDATE trigger_activations SET status = ? WHERE name = ?",
                ("disabled", name),
            )
            conn.commit()

    def get_activation(self, name: str) -> dict[str, Any] | None:
        """Get activation record by name."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "SELECT * FROM trigger_activations WHERE name = ?",
                (name,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in cursor.description]
            return dict(zip(cols, row, strict=False))

    def get_active_poll_triggers(self) -> list[dict[str, Any]]:
        """Get all active poll trigger activations."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "SELECT * FROM trigger_activations WHERE trigger_type = ? AND status = ?",
                ("poll", "active"),
            )
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row, strict=False)) for row in cursor.fetchall()]

    def record_event(
        self,
        trigger_name: str,
        payload: dict[str, Any] | None = None,
        run_id: str | None = None,
        status: str = "pending",
    ) -> str:
        """Record a trigger event. Returns the event_id."""
        event_id = str(uuid.uuid4())
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "INSERT INTO trigger_events "
                "(event_id, trigger_name, fired_at, run_id, payload_json, status) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    event_id,
                    trigger_name,
                    time.time(),
                    run_id,
                    json.dumps(payload) if payload else None,
                    status,
                ),
            )
            conn.commit()
        return event_id

    def get_events(self, trigger_name: str) -> list[dict[str, Any]]:
        """Get all events for a trigger, ordered most recent first."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "SELECT * FROM trigger_events WHERE trigger_name = ? ORDER BY fired_at DESC",
                (trigger_name,),
            )
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row, strict=False)) for row in cursor.fetchall()]

    def update_last_run(self, name: str, run_id: str) -> None:
        """Update last run timestamp and run ID for an activation."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "UPDATE trigger_activations SET last_run_at = ?, last_run_id = ? WHERE name = ?",
                (time.time(), run_id, name),
            )
            conn.commit()
