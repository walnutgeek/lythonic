# pyright: reportImportCycles=false
"""
Trigger: Event-driven execution of namespace nodes.

Provides `TriggerStore` for activation state persistence and
`TriggerManager` for runtime coordination. Trigger definitions
live on `NsNodeConfig.triggers` as `TriggerConfig` instances.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

_log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from lythonic.compose.dag_provenance import DagProvenance, NullProvenance
    from lythonic.compose.dag_runner import DagRunResult
    from lythonic.compose.namespace import Namespace, TriggerConfig

from croniter import croniter

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

    def activate(self, trigger_config: TriggerConfig, dag_nsref: str) -> None:
        """Create or update an activation record from a trigger config."""
        config: dict[str, Any] = {}
        if trigger_config.schedule is not None:
            config["schedule"] = trigger_config.schedule
        if trigger_config.poll_fn is not None:
            config["poll_fn"] = str(trigger_config.poll_fn)

        now = time.time()
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "INSERT OR REPLACE INTO trigger_activations "
                "(name, dag_nsref, trigger_type, status, last_run_at, created_at, config_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    trigger_config.name,
                    dag_nsref,
                    trigger_config.type,
                    "active",
                    now,
                    now,
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


class TriggerManager:
    """
    Runtime coordinator for triggers. Reads trigger definitions from
    node configs in the namespace. `activate()` creates DB records,
    `fire()` runs triggers, `start()`/`stop()` runs a background poll loop.
    """

    namespace: Namespace
    store: TriggerStore
    provenance: DagProvenance | NullProvenance

    def __init__(
        self,
        namespace: Namespace,
        store: TriggerStore,
        provenance: DagProvenance | NullProvenance | None = None,
    ) -> None:
        from lythonic.compose.dag_provenance import NullProvenance

        self.namespace = namespace
        self.store = store
        self.provenance = provenance or NullProvenance()
        self._task: asyncio.Task[None] | None = None
        self._shutdown: asyncio.Event = asyncio.Event()

    def activate(self, name: str) -> None:
        """Activate a trigger by name (found in node configs)."""
        node, tc = self.namespace.get_trigger(name)
        self.store.activate(tc, dag_nsref=node.nsref)

    def deactivate(self, name: str) -> None:
        """Deactivate a trigger."""
        self.store.deactivate(name)

    async def fire(self, name: str, payload: dict[str, Any] | None = None) -> DagRunResult:
        """
        Fire a trigger: run the associated node with payload as inputs.
        Records the event in the store.
        """
        activation = self.store.get_activation(name)
        if activation is None or activation["status"] != "active":
            raise ValueError(
                f"Trigger '{name}' is not active (status: {activation['status'] if activation else 'not found'})"
            )

        dag_nsref = activation["dag_nsref"]
        dag_node = self.namespace.get(dag_nsref)

        # Resolve payload: explicit overrides config default
        _, tc = self.namespace.get_trigger(name)
        effective_payload = payload if payload is not None else (tc.payload or {})

        raw_result = await dag_node(**effective_payload)

        # Wrap non-DagRunResult callables (plain functions, not DAGs)
        from lythonic.compose.dag_runner import DagRunResult

        if isinstance(raw_result, DagRunResult):
            result = raw_result
        else:
            import uuid

            result = DagRunResult(
                run_id=str(uuid.uuid4()),
                status="completed",
                outputs={"result": raw_result},
            )

        self.store.record_event(
            trigger_name=name,
            payload=effective_payload,
            run_id=result.run_id,
            status=result.status,
        )
        self.store.update_last_run(name, result.run_id)
        return result

    def start(self) -> None:
        """Start the background asyncio task for polling active poll triggers."""
        if self._task is not None and not self._task.done():
            return
        self._shutdown = asyncio.Event()
        self._task = asyncio.create_task(self._poll_loop())

    def stop(self) -> None:
        """Signal the poll loop to stop and cancel the background task."""
        if self._task is not None and not self._task.done():
            self._shutdown.set()
            self._task.cancel()

    async def _poll_loop(self) -> None:
        """Background loop that checks active poll triggers on their intervals."""
        while not self._shutdown.is_set():
            try:
                active_polls = self.store.get_active_poll_triggers()
                now = time.time()

                for activation in active_polls:
                    config = json.loads(activation.get("config_json") or "{}")
                    schedule = config.get("schedule")
                    if not schedule:
                        continue

                    last_run = activation.get("last_run_at") or activation.get("created_at") or 0
                    next_fire = croniter(schedule, last_run).get_next(float)

                    if now < next_fire:
                        continue

                    poll_fn_gref = config.get("poll_fn")
                    payload: dict[str, Any] | None
                    if poll_fn_gref:
                        from lythonic import GlobalRef

                        fn = GlobalRef(poll_fn_gref).get_instance()
                        result: Any = fn()
                        if result is None:
                            continue
                        payload = dict(result) if isinstance(result, dict) else {"data": result}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
                    else:
                        payload = {}

                    try:
                        await self.fire(activation["name"], payload=payload)
                    except Exception:
                        _log.exception("Error firing poll trigger '%s'", activation["name"])

                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception:
                _log.exception("Error in poll loop")
                await asyncio.sleep(1)
