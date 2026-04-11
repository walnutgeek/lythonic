"""
DagProvenance: SQLite-backed storage for DAG run state and node execution traces.

Records the full lifecycle of each DAG run: creation, node-by-node execution
(inputs, outputs, timing, errors), and final status. Supports querying for
restart and replay scenarios.

Private `_`-prefixed methods take a cursor and don't commit — they're building
blocks for batch operations. Public methods open the DB, batch writes, and
commit in one cycle.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from lythonic.state import execute_sql, open_sqlite_db

_DAG_RUNS_DDL = """\
CREATE TABLE IF NOT EXISTS dag_runs (
    run_id TEXT PRIMARY KEY,
    dag_nsref TEXT NOT NULL,
    parent_run_id TEXT,
    status TEXT NOT NULL,
    started_at REAL NOT NULL,
    finished_at REAL,
    source_inputs_json TEXT,
    FOREIGN KEY (parent_run_id) REFERENCES dag_runs(run_id)
)"""

_NODE_EXECUTIONS_DDL = """\
CREATE TABLE IF NOT EXISTS node_executions (
    run_id TEXT NOT NULL,
    node_label TEXT NOT NULL,
    status TEXT NOT NULL,
    node_type TEXT,
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
    FOREIGN KEY (run_id) REFERENCES dag_runs(run_id)
)"""


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
        (run_id, dag_nsref, parent_run_id, "running", time.time(), json.dumps(source_inputs)),
    )


def _update_run_status(cursor: sqlite3.Cursor, run_id: str, status: str) -> None:
    execute_sql(
        cursor,
        "UPDATE dag_runs SET status = ? WHERE run_id = ?",
        (status, run_id),
    )


def _finish_run(cursor: sqlite3.Cursor, run_id: str, status: str) -> None:
    execute_sql(
        cursor,
        "UPDATE dag_runs SET status = ?, finished_at = ? WHERE run_id = ?",
        (status, time.time(), run_id),
    )


def _insert_node_start(
    cursor: sqlite3.Cursor,
    run_id: str,
    node_label: str,
    input_json: str,
    node_type: str | None = None,
) -> None:
    execute_sql(
        cursor,
        "INSERT OR REPLACE INTO node_executions "
        "(run_id, node_label, status, node_type, input_json, started_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (run_id, node_label, "running", node_type, input_json, time.time()),
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

    def finish_run(self, run_id: str, status: str) -> None:
        """Set final status and finished_at timestamp."""
        with open_sqlite_db(self.db_path) as conn:
            _finish_run(conn.cursor(), run_id, status)
            conn.commit()

    def record_node_start(
        self, run_id: str, node_label: str, input_json: str, node_type: str | None = None
    ) -> None:
        """Record that a node has started execution."""
        with open_sqlite_db(self.db_path) as conn:
            _insert_node_start(conn.cursor(), run_id, node_label, input_json, node_type)
            conn.commit()

    def record_node_complete(self, run_id: str, node_label: str, output_json: str) -> None:
        """Record that a node has completed successfully."""
        with open_sqlite_db(self.db_path) as conn:
            _update_node_complete(conn.cursor(), run_id, node_label, output_json)
            conn.commit()

    def record_node_failed(self, run_id: str, node_label: str, error: str) -> None:
        """Record that a node has failed."""
        with open_sqlite_db(self.db_path) as conn:
            _update_node_failed(conn.cursor(), run_id, node_label, error)
            conn.commit()

    def record_node_skipped(self, run_id: str, node_label: str, output_json: str) -> None:
        """Record a node as skipped with a copied output (used in replay)."""
        with open_sqlite_db(self.db_path) as conn:
            _insert_node_skipped(conn.cursor(), run_id, node_label, output_json)
            conn.commit()

    def record_edge_traversal(
        self, run_id: str, upstream_label: str, downstream_label: str
    ) -> None:
        """Record that an edge was traversed during execution."""
        with open_sqlite_db(self.db_path) as conn:
            _insert_edge_traversal(conn.cursor(), run_id, upstream_label, downstream_label)
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
                "source_inputs_json FROM dag_runs WHERE run_id = ?",
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
            }

    def get_node_executions(self, run_id: str) -> list[dict[str, Any]]:
        """Get all node execution records for a run."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "SELECT run_id, node_label, status, node_type, input_json, output_json, "
                "started_at, finished_at, error "
                "FROM node_executions WHERE run_id = ?",
                (run_id,),
            )
            return [
                {
                    "run_id": r[0],
                    "node_label": r[1],
                    "status": r[2],
                    "node_type": r[3],
                    "input_json": r[4],
                    "output_json": r[5],
                    "started_at": r[6],
                    "finished_at": r[7],
                    "error": r[8],
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

    def get_recent_runs(self, limit: int = 20, status: str | None = None) -> list[dict[str, Any]]:
        """List runs ordered by started_at descending. Optional status filter."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            if status:
                execute_sql(
                    cursor,
                    "SELECT run_id, dag_nsref, parent_run_id, status, started_at, finished_at "
                    "FROM dag_runs WHERE status = ? ORDER BY started_at DESC LIMIT ?",
                    (status, limit),
                )
            else:
                execute_sql(
                    cursor,
                    "SELECT run_id, dag_nsref, parent_run_id, status, started_at, finished_at "
                    "FROM dag_runs ORDER BY started_at DESC LIMIT ?",
                    (limit,),
                )
            return [
                {
                    "run_id": r[0],
                    "dag_nsref": r[1],
                    "parent_run_id": r[2],
                    "status": r[3],
                    "started_at": r[4],
                    "finished_at": r[5],
                }
                for r in cursor.fetchall()
            ]

    def get_active_runs(self) -> list[dict[str, Any]]:
        """Get all currently running DAG executions."""
        return self.get_recent_runs(limit=100, status="running")

    def get_run_detail(self, run_id: str) -> dict[str, Any] | None:
        """Get a run with its node executions, edge traversals, and child runs."""
        run = self.get_run(run_id)
        if run is None:
            return None
        return {
            "run": run,
            "nodes": self.get_node_executions(run_id),
            "edges": self.get_edge_traversals(run_id),
            "children": self.get_child_runs(run_id),
        }

    def get_child_runs(self, parent_run_id: str) -> list[dict[str, Any]]:
        """Get all sub-DAG runs spawned by a parent run."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "SELECT run_id, dag_nsref, parent_run_id, status, started_at, finished_at "
                "FROM dag_runs WHERE parent_run_id = ? ORDER BY started_at",
                (parent_run_id,),
            )
            return [
                {
                    "run_id": r[0],
                    "dag_nsref": r[1],
                    "parent_run_id": r[2],
                    "status": r[3],
                    "started_at": r[4],
                    "finished_at": r[5],
                }
                for r in cursor.fetchall()
            ]


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

    def finish_run(self, run_id: str, status: str) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def record_node_start(
        self,
        run_id: str,  # pyright: ignore[reportUnusedParameter]
        node_label: str,  # pyright: ignore[reportUnusedParameter]
        input_json: str,  # pyright: ignore[reportUnusedParameter]
        node_type: str | None = None,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        pass

    def record_node_complete(self, run_id: str, node_label: str, output_json: str) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def record_node_failed(self, run_id: str, node_label: str, error: str) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def record_node_skipped(self, run_id: str, node_label: str, output_json: str) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def record_edge_traversal(
        self,
        run_id: str,  # pyright: ignore[reportUnusedParameter]
        upstream_label: str,  # pyright: ignore[reportUnusedParameter]
        downstream_label: str,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
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

    def get_recent_runs(self, limit: int = 20, status: str | None = None) -> list[dict[str, Any]]:  # pyright: ignore[reportUnusedParameter]
        return []

    def get_active_runs(self) -> list[dict[str, Any]]:
        return []

    def get_run_detail(self, run_id: str) -> dict[str, Any] | None:  # pyright: ignore[reportUnusedParameter]
        return None

    def get_child_runs(self, parent_run_id: str) -> list[dict[str, Any]]:  # pyright: ignore[reportUnusedParameter]
        return []
