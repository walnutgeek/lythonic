"""
DagProvenance: SQLite-backed storage for DAG run state and node execution traces.

Records the full lifecycle of each DAG run: creation, node-by-node execution
(inputs, outputs, timing, errors), and final status. Supports querying for
restart and replay scenarios.

## Pydantic Inspection Models

`DagRun`, `NodeExecution`, and `EdgeTraversal` are Pydantic models returned
by query methods like `get_run()` and `get_recent_runs()`. All timestamps
are timezone-aware UTC datetimes. `DagRun.nodes` is a dict keyed by
`node_label`. Each `NodeExecution` has an `edges` list and a `sub_dags`
dict (non-None for composite nodes only, keyed by expansion key like
`"chunks[0]"` or `"label[]"`). Convenience methods:

- `DagRun.latest_update()` — most recent timestamp across all nodes/edges
- `DagRun.nodes_changed_since(dt)` — nodes updated after a given datetime

## Parent Run Tracking

Sub-DAG runs link to their parent via `parent_run_id`. Use
`get_child_runs(parent_run_id)` to recursively retrieve all descendant runs
(children, grandchildren, etc.) using a recursive CTE.

## Batch Operations

`complete_node_with_edges()` and `fail_node_and_finish_run()` batch multiple
writes (node status + edge traversals, or node failure + run finish) into a
single `open_sqlite_db` cycle to reduce lock contention.

## Serialization Helpers

`json_default()` and `safe_json_dumps()` handle Pydantic models and other
non-JSON-serializable types when recording provenance data.

Private `_`-prefixed methods take a cursor and don't commit — they're building
blocks for batch operations. Public methods open the DB, batch writes, and
commit in one cycle.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from lythonic.state import execute_sql, open_sqlite_db

_log = logging.getLogger(__name__)


def json_default(obj: Any) -> Any:
    """JSON serialization fallback for Pydantic models and other types."""
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    return str(obj)


def safe_json_dumps(obj: Any) -> str:
    """JSON serialize with Pydantic support. Logs a warning on failure."""
    try:
        return json.dumps(obj, default=json_default)
    except Exception:
        _log.warning("Failed to serialize to JSON: %s", type(obj).__name__, exc_info=True)
        return json.dumps(str(obj))


_DAG_RUNS_DDL = """\
CREATE TABLE IF NOT EXISTS dag_runs (
    run_id TEXT PRIMARY KEY,
    dag_nsref TEXT NOT NULL,
    parent_run_id TEXT,
    status TEXT NOT NULL,
    started_at REAL NOT NULL,
    finished_at REAL,
    source_inputs_json TEXT,
    sink_outputs_json TEXT,
    FOREIGN KEY (parent_run_id) REFERENCES dag_runs(run_id)
)"""

_NODE_EXECUTIONS_DDL = """\
CREATE TABLE IF NOT EXISTS node_executions (
    run_id TEXT NOT NULL,
    node_label TEXT NOT NULL,
    status TEXT NOT NULL,
    is_source INTEGER DEFAULT 0,
    is_sink INTEGER DEFAULT 0,
    input_json TEXT,
    output_json TEXT,
    started_at REAL,
    finished_at REAL,
    error TEXT,
    PRIMARY KEY (run_id, node_label),
    FOREIGN KEY (run_id) REFERENCES dag_runs(run_id)
)"""

_EDGE_TRAVERSALS_DDL = """\
CREATE TABLE IF NOT EXISTS edge_traversals (
    run_id TEXT NOT NULL,
    upstream_label TEXT NOT NULL,
    downstream_label TEXT NOT NULL,
    traversed_at REAL NOT NULL,
    PRIMARY KEY (run_id, upstream_label, downstream_label),
    FOREIGN KEY (run_id) REFERENCES dag_runs(run_id)
)"""


def _ts(epoch: float | None) -> datetime | None:
    """Convert epoch float to timezone-aware UTC datetime."""
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC)


class EdgeTraversal(BaseModel):
    """Edge traversed during a DAG run. `downstream_label` identifies the target node."""

    downstream_label: str
    traversed_at: datetime


class IoPayload(BaseModel):
    """Input/output JSON payload. Loaded on demand via `load_io()`."""

    input_json: str | None = None
    output_json: str | None = None


class NodeExecution(BaseModel):
    """Execution record for a single node in a DAG run."""

    node_label: str
    status: str
    is_source: bool = False
    is_sink: bool = False
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    edges: list[EdgeTraversal] = []
    sub_dags: dict[str, DagRun] | None = None
    io: IoPayload | None = None


class DagRun(BaseModel):
    """A DAG execution record with nested node executions and edge traversals."""

    run_id: str
    dag_nsref: str
    parent_run_id: str | None = None
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    nodes: dict[str, NodeExecution] = {}
    io: IoPayload | None = None

    def latest_update(self) -> datetime:
        """The most recent timestamp across all nodes and traversals."""
        candidates: list[datetime] = [self.started_at]
        if self.finished_at:
            candidates.append(self.finished_at)
        for n in self.nodes.values():
            if n.started_at:
                candidates.append(n.started_at)
            if n.finished_at:
                candidates.append(n.finished_at)
            for e in n.edges:
                candidates.append(e.traversed_at)
        return max(candidates)

    def nodes_changed_since(self, dt: datetime) -> list[NodeExecution]:
        """Nodes whose started_at or finished_at is after `dt`."""
        result: list[NodeExecution] = []
        for n in self.nodes.values():
            if (n.started_at and n.started_at > dt) or (n.finished_at and n.finished_at > dt):
                result.append(n)
        return result


# Private cursor-level helpers (no commit, no open/close)


def _insert_run(
    cursor: sqlite3.Cursor,
    run_id: str,
    dag_nsref: str,
    source_inputs: dict[str, Any],
    parent_run_id: str | None = None,
) -> None:
    execute_sql(
        cursor,
        "INSERT INTO dag_runs (run_id, dag_nsref, parent_run_id, status, started_at, source_inputs_json) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (run_id, dag_nsref, parent_run_id, "running", time.time(), safe_json_dumps(source_inputs)),
    )


def _update_run_status(cursor: sqlite3.Cursor, run_id: str, status: str) -> None:
    execute_sql(
        cursor,
        "UPDATE dag_runs SET status = ? WHERE run_id = ?",
        (status, run_id),
    )


def _finish_run(
    cursor: sqlite3.Cursor, run_id: str, status: str, sink_outputs_json: str | None = None
) -> None:
    execute_sql(
        cursor,
        "UPDATE dag_runs SET status = ?, finished_at = ?, sink_outputs_json = ? WHERE run_id = ?",
        (status, time.time(), sink_outputs_json, run_id),
    )


def _insert_node_start(
    cursor: sqlite3.Cursor,
    run_id: str,
    node_label: str,
    input_json: str,
    is_source: bool = False,
    is_sink: bool = False,
) -> None:
    execute_sql(
        cursor,
        "INSERT OR REPLACE INTO node_executions "
        "(run_id, node_label, status, is_source, is_sink, input_json, started_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (run_id, node_label, "running", int(is_source), int(is_sink), input_json, time.time()),
    )


def _update_node_complete(
    cursor: sqlite3.Cursor, run_id: str, node_label: str, output_json: str
) -> None:
    execute_sql(
        cursor,
        "UPDATE node_executions SET status = ?, output_json = ?, finished_at = ? "
        "WHERE run_id = ? AND node_label = ?",
        ("completed", output_json, time.time(), run_id, node_label),
    )


def _update_node_failed(cursor: sqlite3.Cursor, run_id: str, node_label: str, error: str) -> None:
    execute_sql(
        cursor,
        "UPDATE node_executions SET status = ?, error = ?, finished_at = ? "
        "WHERE run_id = ? AND node_label = ?",
        ("failed", error, time.time(), run_id, node_label),
    )


def _insert_node_skipped(
    cursor: sqlite3.Cursor, run_id: str, node_label: str, output_json: str
) -> None:
    now = time.time()
    execute_sql(
        cursor,
        "INSERT OR REPLACE INTO node_executions "
        "(run_id, node_label, status, output_json, started_at, finished_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (run_id, node_label, "skipped", output_json, now, now),
    )


def _insert_edge_traversal(
    cursor: sqlite3.Cursor,
    run_id: str,
    upstream_label: str,
    downstream_label: str,
) -> None:
    execute_sql(
        cursor,
        "INSERT INTO edge_traversals "
        "(run_id, upstream_label, downstream_label, traversed_at) "
        "VALUES (?, ?, ?, ?)",
        (run_id, upstream_label, downstream_label, time.time()),
    )


class DagProvenance:
    """
    SQLite-backed storage for DAG run state and node execution traces.

    Public methods batch related writes into a single `open_sqlite_db` cycle.
    """

    db_path: Path

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(cursor, _DAG_RUNS_DDL)
            execute_sql(cursor, _NODE_EXECUTIONS_DDL)
            execute_sql(cursor, _EDGE_TRAVERSALS_DDL)
            conn.commit()

    def create_run(
        self,
        run_id: str,
        dag_nsref: str,
        source_inputs: dict[str, Any],
        parent_run_id: str | None = None,
    ) -> None:
        """Create a new run record with status 'running'."""
        with open_sqlite_db(self.db_path) as conn:
            _insert_run(conn.cursor(), run_id, dag_nsref, source_inputs, parent_run_id)
            conn.commit()

    def update_run_status(self, run_id: str, status: str) -> None:
        """Update the status of a run without setting finished_at."""
        with open_sqlite_db(self.db_path) as conn:
            _update_run_status(conn.cursor(), run_id, status)
            conn.commit()

    def finish_run(self, run_id: str, status: str, sink_outputs_json: str | None = None) -> None:
        """Set final status and finished_at timestamp."""
        with open_sqlite_db(self.db_path) as conn:
            _finish_run(conn.cursor(), run_id, status, sink_outputs_json)
            conn.commit()

    def record_node_start(
        self,
        run_id: str,
        node_label: str,
        input_json: str,
        is_source: bool = False,
        is_sink: bool = False,
    ) -> None:
        """Record that a node has started execution."""
        with open_sqlite_db(self.db_path) as conn:
            _insert_node_start(conn.cursor(), run_id, node_label, input_json, is_source, is_sink)
            conn.commit()

    def record_node_skipped(self, run_id: str, node_label: str, output_json: str) -> None:
        """Record a node as skipped with a copied output (used in replay)."""
        with open_sqlite_db(self.db_path) as conn:
            _insert_node_skipped(conn.cursor(), run_id, node_label, output_json)
            conn.commit()

    # Batch operations — multiple writes in one open/close cycle

    def complete_node_with_edges(
        self,
        run_id: str,
        node_label: str,
        output_json: str,
        edges: list[tuple[str, str]],
    ) -> None:
        """Record node completion and all outgoing edge traversals in one batch."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            _update_node_complete(cursor, run_id, node_label, output_json)
            for upstream, downstream in edges:
                _insert_edge_traversal(cursor, run_id, upstream, downstream)
            conn.commit()

    def fail_node_and_finish_run(
        self,
        run_id: str,
        node_label: str,
        error: str,
    ) -> None:
        """Record node failure and mark the run as failed in one batch."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            _update_node_failed(cursor, run_id, node_label, error)
            _finish_run(cursor, run_id, "failed")
            conn.commit()

    # Read operations

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Get a run record by ID, or None if not found."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "SELECT run_id, dag_nsref, parent_run_id, status, started_at, finished_at, "
                "source_inputs_json, sink_outputs_json FROM dag_runs WHERE run_id = ?",
                (run_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return {
                "run_id": row[0],
                "dag_nsref": row[1],
                "parent_run_id": row[2],
                "status": row[3],
                "started_at": row[4],
                "finished_at": row[5],
                "source_inputs_json": row[6],
                "sink_outputs_json": row[7],
            }

    def get_node_executions(self, run_id: str) -> list[dict[str, Any]]:
        """Get all node execution records for a run."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "SELECT run_id, node_label, status, is_source, is_sink, input_json, output_json, "
                "started_at, finished_at, error "
                "FROM node_executions WHERE run_id = ?",
                (run_id,),
            )
            return [
                {
                    "run_id": r[0],
                    "node_label": r[1],
                    "status": r[2],
                    "is_source": bool(r[3]),
                    "is_sink": bool(r[4]),
                    "input_json": r[5],
                    "output_json": r[6],
                    "started_at": r[7],
                    "finished_at": r[8],
                    "error": r[9],
                }
                for r in cursor.fetchall()
            ]

    def get_node_output(self, run_id: str, node_label: str) -> str | None:
        """Get the output JSON of a completed or skipped node, or None."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "SELECT output_json FROM node_executions "
                "WHERE run_id = ? AND node_label = ? AND status IN ('completed', 'skipped')",
                (run_id, node_label),
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def get_pending_nodes(self, run_id: str) -> list[str]:
        """Get labels of nodes that are not yet completed or skipped."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "SELECT node_label FROM node_executions "
                "WHERE run_id = ? AND status IN ('pending', 'running', 'failed')",
                (run_id,),
            )
            return [row[0] for row in cursor.fetchall()]

    def get_edge_traversals(self, run_id: str) -> list[dict[str, Any]]:
        """Get all edge traversals for a run."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "SELECT run_id, upstream_label, downstream_label, traversed_at "
                "FROM edge_traversals WHERE run_id = ? ORDER BY traversed_at",
                (run_id,),
            )
            return [
                {
                    "run_id": r[0],
                    "upstream_label": r[1],
                    "downstream_label": r[2],
                    "traversed_at": r[3],
                }
                for r in cursor.fetchall()
            ]

    # Inspection API

    def _load_dag_runs(
        self, cursor: sqlite3.Cursor, run_rows: list[tuple[Any, ...]]
    ) -> list[DagRun]:
        """
        Build DagRun models with nested nodes, edges, and sub-DAGs from run rows.

        JSON payload fields (`input_json`, `output_json`, `source_inputs_json`,
        `sink_outputs_json`) are NOT loaded here — `io` stays `None` on all
        returned models. Use `load_io()` to populate them on demand.
        """
        if not run_rows:
            return []

        run_ids = [r[0] for r in run_rows]
        placeholders = ",".join("?" * len(run_ids))

        # Load node executions (no JSON payload columns)
        execute_sql(
            cursor,
            f"SELECT run_id, node_label, status, is_source, is_sink, "
            f"started_at, finished_at, error "
            f"FROM node_executions WHERE run_id IN ({placeholders})",
            tuple(run_ids),
        )
        nodes_by_run: dict[str, dict[str, NodeExecution]] = {}
        for r in cursor.fetchall():
            node = NodeExecution(
                node_label=r[1],
                status=r[2],
                is_source=bool(r[3]),
                is_sink=bool(r[4]),
                started_at=_ts(r[5]),
                finished_at=_ts(r[6]),
                error=r[7],
            )
            nodes_by_run.setdefault(r[0], {})[r[1]] = node

        # Load edge traversals and attach to upstream nodes
        execute_sql(
            cursor,
            f"SELECT run_id, upstream_label, downstream_label, traversed_at "
            f"FROM edge_traversals WHERE run_id IN ({placeholders}) ORDER BY traversed_at",
            tuple(run_ids),
        )
        for r in cursor.fetchall():
            run_id, upstream, downstream, ts = r
            edge = EdgeTraversal(downstream_label=downstream, traversed_at=_ts(ts))  # pyright: ignore[reportArgumentType]
            run_nodes = nodes_by_run.get(run_id)
            if run_nodes and upstream in run_nodes:
                run_nodes[upstream].edges.append(edge)

        # Build DagRun models (no JSON payload columns)
        dag_runs: list[DagRun] = []
        runs_by_id: dict[str, DagRun] = {}
        for r in run_rows:
            dag_run = DagRun(
                run_id=r[0],
                dag_nsref=r[1],
                parent_run_id=r[2],
                status=r[3],
                started_at=_ts(r[4]),  # pyright: ignore[reportArgumentType]
                finished_at=_ts(r[5]),
                nodes=nodes_by_run.get(r[0], {}),
            )
            dag_runs.append(dag_run)
            runs_by_id[r[0]] = dag_run

        # Detect composite nodes from child runs across ALL loaded run_ids
        child_placeholders = ",".join("?" * len(run_ids))
        execute_sql(
            cursor,
            f"SELECT run_id, dag_nsref, parent_run_id, status, started_at, finished_at "
            f"FROM dag_runs WHERE parent_run_id IN ({child_placeholders})",
            tuple(run_ids),
        )
        child_rows = cursor.fetchall()
        if child_rows:
            child_runs = self._load_dag_runs(cursor, child_rows)
            for child in child_runs:
                parent_run = runs_by_id.get(child.parent_run_id or "")
                if parent_run is None:
                    continue
                # Extract expansion key from dag_nsref
                prefix = parent_run.dag_nsref + "/"
                if not child.dag_nsref.startswith(prefix):
                    continue
                expansion_key = child.dag_nsref[len(prefix) :]
                # Match key prefix to composite node label
                bracket_pos = expansion_key.find("[")
                node_label = expansion_key[:bracket_pos] if bracket_pos >= 0 else expansion_key
                node = parent_run.nodes.get(node_label)
                if node is not None:
                    if node.sub_dags is None:
                        node.sub_dags = {}
                    node.sub_dags[expansion_key] = child

        return dag_runs

    def _fetch_run_rows(
        self,
        cursor: sqlite3.Cursor,
        where: str,
        params: tuple[Any, ...],
    ) -> list[tuple[Any, ...]]:
        execute_sql(
            cursor,
            f"SELECT run_id, dag_nsref, parent_run_id, status, started_at, "
            f"finished_at FROM dag_runs {where}",
            params,
        )
        return cursor.fetchall()

    def inspect_run(self, run_id: str) -> DagRun | None:
        """Get a full DagRun model by ID, with nested nodes, edges, and sub-DAGs."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            rows = self._fetch_run_rows(cursor, "WHERE run_id = ?", (run_id,))
            runs = self._load_dag_runs(cursor, rows)
            return runs[0] if runs else None

    def load_io(
        self,
        dag_run: DagRun,
        node_labels: Sequence[str] | None = None,
    ) -> None:
        """
        Populate `io` on `dag_run` and its nodes in a single DB round-trip.

        `dag_run.io` gets `source_inputs_json` / `sink_outputs_json`.
        `node.io` gets `input_json` / `output_json` for each node, filtered
        by `node_labels` if provided (all nodes if `None`).

        Does NOT recurse into sub-DAGs.
        """
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()

            # Load run-level IO
            execute_sql(
                cursor,
                "SELECT source_inputs_json, sink_outputs_json FROM dag_runs WHERE run_id = ?",
                (dag_run.run_id,),
            )
            row = cursor.fetchone()
            if row:
                dag_run.io = IoPayload(input_json=row[0], output_json=row[1])

            # Load node-level IO
            labels = list(node_labels) if node_labels is not None else list(dag_run.nodes.keys())
            if labels:
                placeholders = ",".join("?" * len(labels))
                execute_sql(
                    cursor,
                    f"SELECT node_label, input_json, output_json FROM node_executions "
                    f"WHERE run_id = ? AND node_label IN ({placeholders})",
                    (dag_run.run_id, *labels),
                )
                for r in cursor.fetchall():
                    node = dag_run.nodes.get(r[0])
                    if node:
                        node.io = IoPayload(input_json=r[1], output_json=r[2])

    def get_recent_runs(self, limit: int = 20, status: str | None = None) -> list[DagRun]:
        """List runs with full node/edge data, ordered by started_at descending."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            if status:
                rows = self._fetch_run_rows(
                    cursor, "WHERE status = ? ORDER BY started_at DESC LIMIT ?", (status, limit)
                )
            else:
                rows = self._fetch_run_rows(cursor, "ORDER BY started_at DESC LIMIT ?", (limit,))
            return self._load_dag_runs(cursor, rows)

    def get_active_runs(self) -> list[DagRun]:
        """Get all currently running DAG executions."""
        return self.get_recent_runs(limit=100, status="running")

    def get_child_runs(self, parent_run_id: str) -> list[DagRun]:
        """Recursively get all descendant runs (children, grandchildren, etc.)."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "WITH RECURSIVE descendants AS ( "
                "  SELECT run_id, dag_nsref, parent_run_id, status, started_at, finished_at "
                "  FROM dag_runs WHERE parent_run_id = ? "
                "  UNION ALL "
                "  SELECT d.run_id, d.dag_nsref, d.parent_run_id, d.status, d.started_at, "
                "    d.finished_at "
                "  FROM dag_runs d JOIN descendants p ON d.parent_run_id = p.run_id "
                ") SELECT * FROM descendants ORDER BY started_at",
                (parent_run_id,),
            )
            rows = cursor.fetchall()
            return self._load_dag_runs(cursor, rows)


class NullProvenance:
    """
    No-op provenance -- discards writes, returns None/empty on reads.
    Used when `DagRunner` is created without explicit provenance.
    """

    def create_run(
        self,
        run_id: str,  # pyright: ignore[reportUnusedParameter]
        dag_nsref: str,  # pyright: ignore[reportUnusedParameter]
        source_inputs: dict[str, Any],  # pyright: ignore[reportUnusedParameter]
        parent_run_id: str | None = None,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        pass

    def update_run_status(self, run_id: str, status: str) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def finish_run(self, run_id: str, status: str, sink_outputs_json: str | None = None) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def record_node_start(
        self,
        run_id: str,  # pyright: ignore[reportUnusedParameter]
        node_label: str,  # pyright: ignore[reportUnusedParameter]
        input_json: str,  # pyright: ignore[reportUnusedParameter]
        is_source: bool = False,  # pyright: ignore[reportUnusedParameter]
        is_sink: bool = False,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        pass

    def record_node_skipped(self, run_id: str, node_label: str, output_json: str) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def complete_node_with_edges(
        self,
        run_id: str,  # pyright: ignore[reportUnusedParameter]
        node_label: str,  # pyright: ignore[reportUnusedParameter]
        output_json: str,  # pyright: ignore[reportUnusedParameter]
        edges: list[tuple[str, str]],  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        pass

    def fail_node_and_finish_run(self, run_id: str, node_label: str, error: str) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def get_run(self, run_id: str) -> dict[str, Any] | None:  # pyright: ignore[reportUnusedParameter]
        return None

    def get_node_executions(self, run_id: str) -> list[dict[str, Any]]:  # pyright: ignore[reportUnusedParameter]
        return []

    def get_node_output(self, run_id: str, node_label: str) -> str | None:  # pyright: ignore[reportUnusedParameter]
        return None

    def get_pending_nodes(self, run_id: str) -> list[str]:  # pyright: ignore[reportUnusedParameter]
        return []

    def get_edge_traversals(self, run_id: str) -> list[dict[str, Any]]:  # pyright: ignore[reportUnusedParameter]
        return []

    def inspect_run(self, run_id: str) -> DagRun | None:  # pyright: ignore[reportUnusedParameter]
        return None

    def get_recent_runs(self, limit: int = 20, status: str | None = None) -> list[DagRun]:  # pyright: ignore[reportUnusedParameter]
        return []

    def get_active_runs(self) -> list[DagRun]:
        return []

    def get_child_runs(self, parent_run_id: str) -> list[DagRun]:  # pyright: ignore[reportUnusedParameter]
        return []
