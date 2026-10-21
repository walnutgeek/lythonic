# pyright: reportImportCycles=false
"""
Trigger: Event-driven execution of namespace nodes.

Provides `TriggerStore` for activation state persistence and
`TriggerManager` for runtime coordination. Trigger definitions
live on `NsNodeConfig.triggers` as `TriggerConfig` instances.

Call `TriggerManager.activate_configured()` on every process start: it
activates the triggers the node configs define, but leaves those stored as
`disabled`, so a deliberate `deactivate()` survives restarts until an explicit
`activate()`. Scheduled fires missed while the process was down run once when
polling resumes: re-activating an active trigger keeps its `last_run_at`. A run
that overruns later scheduled fire times skips them and logs a warning.

Runs fired by the schedule record `initiated_by = SCHEDULER`; runs fired with
`TriggerManager.fire` record the caller's `run_initiator` (see
`lythonic.compose.dag_provenance`).

`activate` and `deactivate` record `run_initiator` as `changed_by`, with
`changed_at`, when it is set or when the call changes the trigger's status. So
`activate_configured()` at process start keeps the last deliberate change,
while an explicit `activate()` of a disabled trigger with no initiator records
`changed_by = NULL` rather than keeping the name of whoever disabled it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

_log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from lythonic.compose.dag_provenance import DagProvenance, NullProvenance
    from lythonic.compose.dag_runner import DagRunResult
    from lythonic.compose.namespace import Namespace, TriggerConfig

from croniter import croniter

from lythonic.compose.dag_provenance import (
    SCHEDULER,
    initiated_by,
    run_initiator,
    safe_json_dumps,
)
from lythonic.state import add_missing_column, execute_sql, open_sqlite_db

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
    config_json TEXT,
    changed_by TEXT,
    changed_at REAL
)"""


def _status(cursor: sqlite3.Cursor, name: str | None) -> str | None:
    execute_sql(cursor, "SELECT status FROM trigger_activations WHERE name = ?", (name,))
    row = cursor.fetchone()
    return row[0] if row else None


def _record_change(
    cursor: sqlite3.Cursor, name: str | None, status_before: str | None, now: float
) -> None:
    """
    Record `run_initiator` as `changed_by` when it names someone, or when the
    status actually changed. Re-activating an active trigger at process start
    names no one and leaves the last deliberate change in place; a status change
    by no one records `changed_by = NULL`, so an old name is never left on a
    state that person did not set.
    """
    initiator = run_initiator.get()
    if initiator is not None or _status(cursor, name) != status_before:
        execute_sql(
            cursor,
            "UPDATE trigger_activations SET changed_by = ?, changed_at = ? WHERE name = ?",
            (initiator, now, name),
        )


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


_MAX_SKIPPED_COUNT = 1000


def _croniter_for(schedule: str, base: float) -> croniter:
    # 6-field cron has seconds as first field
    return croniter(schedule, base, second_at_beginning=len(schedule.split()) == 6)


def _warn_skipped_fires(name: str, schedule: str, started_at: float, finished_at: float) -> None:
    """
    Warn about scheduled fire times that passed while a run was in progress. `last_run_at`
    is the completion time, so these are never fired.
    """
    it = _croniter_for(schedule, started_at)
    first_skipped = it.get_next(float)
    if first_skipped > finished_at:
        return
    # Counting runs on the event loop, so cap it for fine-grained schedules and long runs.
    skipped = 1
    while skipped < _MAX_SKIPPED_COUNT and it.get_next(float) <= finished_at:
        skipped += 1
    _log.warning(
        "Trigger '%s' run took %.1fs and overran %s scheduled fire(s) starting at %s; "
        "they will not run",
        name,
        finished_at - started_at,
        f"{skipped}+" if skipped == _MAX_SKIPPED_COUNT else skipped,
        datetime.fromtimestamp(first_skipped, tz=UTC).isoformat(timespec="seconds"),
    )


class TriggerStore:
    """SQLite-backed storage for trigger activations and events."""

    db_path: Path

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(cursor, _TRIGGER_ACTIVATIONS_DDL)
            add_missing_column(cursor, "trigger_activations", "changed_by", "TEXT")
            add_missing_column(cursor, "trigger_activations", "changed_at", "REAL")
            execute_sql(cursor, _TRIGGER_EVENTS_DDL)
            conn.commit()

    def activate(
        self, trigger_config: TriggerConfig, dag_nsref: str, *, keep_disabled: bool = False
    ) -> bool:
        """
        Create or update an activation record from a trigger config. Returns whether
        the trigger is now active: with `keep_disabled`, a trigger stored as `disabled`
        is left untouched, checked in the same transaction.

        Re-activating an active trigger (e.g. on every process start) keeps `last_run_at`,
        `last_run_id` and `created_at`, even if the config changed, so a scheduled fire
        missed while the process was down fires once when polling resumes. Activating a
        disabled trigger resets `last_run_at` to now, so a deliberate pause is not caught up.
        """
        config: dict[str, Any] = {}
        if trigger_config.schedule is not None:
            config["schedule"] = trigger_config.schedule
        if trigger_config.poll_fn is not None:
            config["poll_fn"] = str(trigger_config.poll_fn)

        now = time.time()
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            status_before = _status(cursor, trigger_config.name)
            if keep_disabled and status_before == "disabled":
                return False
            execute_sql(
                cursor,
                "INSERT INTO trigger_activations "
                "(name, dag_nsref, trigger_type, status, last_run_at, created_at, config_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET "
                "dag_nsref = excluded.dag_nsref, "
                "trigger_type = excluded.trigger_type, "
                "status = excluded.status, "
                "config_json = excluded.config_json, "
                "last_run_at = CASE WHEN trigger_activations.status = 'disabled' "
                "THEN excluded.last_run_at ELSE trigger_activations.last_run_at END",
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
            _record_change(cursor, trigger_config.name, status_before, now)
            conn.commit()
        return True

    def deactivate(self, name: str) -> None:
        """Set activation status to disabled."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            status_before = _status(cursor, name)
            execute_sql(
                cursor,
                "UPDATE trigger_activations SET status = ? WHERE name = ?",
                ("disabled", name),
            )
            _record_change(cursor, name, status_before, time.time())
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
                    safe_json_dumps(payload) if payload else None,
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


@dataclass(frozen=True)
class TriggerActivationResult:
    """Outcome of `TriggerManager.activate_configured`."""

    activated: list[str]
    left_disabled: list[str]


class TriggerManager:
    """
    Runtime coordinator for triggers. Reads trigger definitions from
    node configs in the namespace. `activate()` creates DB records,
    `fire()` runs triggers, `start()`/`stop()` runs a background poll loop.
    Call `activate_configured()` on every process start.
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
        """Activate a trigger by name (found in node configs), even if it is disabled."""
        node, tc = self.namespace.get_trigger(name)
        self.store.activate(tc, dag_nsref=str(node.nsref))

    def activate_configured(self) -> TriggerActivationResult:
        """
        Activate every trigger defined in node configs, except those stored as
        `disabled`: a deliberate `deactivate()` stays in force across restarts
        until an explicit `activate()`.
        """
        activated: list[str] = []
        left_disabled: list[str] = []
        for node, tc in self.namespace.triggers():
            assert tc.name is not None, "trigger name is set at registration"
            if self.store.activate(tc, dag_nsref=str(node.nsref), keep_disabled=True):
                activated.append(tc.name)
            else:
                left_disabled.append(tc.name)
        return TriggerActivationResult(activated=activated, left_disabled=left_disabled)

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
        dag = self.namespace.get_as_dag(dag_nsref)

        # Resolve payload: explicit overrides config default
        _, tc = self.namespace.get_trigger(name)
        effective_payload = payload if payload is not None else (tc.payload or {})

        from lythonic.compose.dag_runner import DagRunner

        # Run with provenance so dag_runs/node_executions are recorded
        runner = DagRunner(dag, provenance=self.provenance)
        source_inputs: dict[str, dict[str, Any]] = {}
        for src_node in dag.sources():
            node_args = src_node.ns_node.method.args
            if src_node.ns_node.expects_dag_context():
                node_args = node_args[1:]
            node_kwargs = {
                a.name: effective_payload[a.name] for a in node_args if a.name in effective_payload
            }
            if node_kwargs:
                source_inputs[src_node.label] = node_kwargs
        result: DagRunResult = await runner.run(source_inputs=source_inputs, dag_nsref=dag_nsref)

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
                    next_fire = _croniter_for(schedule, last_run).get_next(float)

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
                        payload = None

                    started_at = time.time()
                    try:
                        with initiated_by(SCHEDULER):
                            await self.fire(activation["name"], payload=payload)
                    except Exception:
                        _log.exception("Error firing poll trigger '%s'", activation["name"])
                    else:
                        _warn_skipped_fires(activation["name"], schedule, started_at, time.time())

                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception:
                _log.exception("Error in poll loop")
                await asyncio.sleep(1)
