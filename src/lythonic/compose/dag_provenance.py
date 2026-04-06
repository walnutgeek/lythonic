"""
DagProvenance: SQLite-backed storage for DAG run state and node execution traces.

Records the full lifecycle of each DAG run: creation, node-by-node execution
(inputs, outputs, timing, errors), and final status. Supports querying for
restart and replay scenarios.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from lythonic.state import execute_sql, open_sqlite_db

_DAG_RUNS_DDL = """\
CREATE TABLE IF NOT EXISTS dag_runs (
    run_id TEXT PRIMARY KEY,
    dag_nsref TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at REAL NOT NULL,
    finished_at REAL,
    source_inputs_json TEXT
)"""

_NODE_EXECUTIONS_DDL = """\
CREATE TABLE IF NOT EXISTS node_executions (
    run_id TEXT NOT NULL,
    node_label TEXT NOT NULL,
    status TEXT NOT NULL,
    input_json TEXT,
    output_json TEXT,
    started_at REAL,
    finished_at REAL,
    error TEXT,
    PRIMARY KEY (run_id, node_label),
    FOREIGN KEY (run_id) REFERENCES dag_runs(run_id)
)"""


class DagProvenance:
    """SQLite-backed storage for DAG run state and node execution traces."""

    db_path: Path

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(cursor, _DAG_RUNS_DDL)
            execute_sql(cursor, _NODE_EXECUTIONS_DDL)
            conn.commit()

    def create_run(self, run_id: str, dag_nsref: str, source_inputs: dict[str, Any]) -> None:
        """Create a new run record with status 'running'."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "INSERT INTO dag_runs (run_id, dag_nsref, status, started_at, source_inputs_json) "
                "VALUES (?, ?, ?, ?, ?)",
                (run_id, dag_nsref, "running", time.time(), json.dumps(source_inputs)),
            )
            conn.commit()

    def update_run_status(self, run_id: str, status: str) -> None:
        """Update the status of a run without setting finished_at."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "UPDATE dag_runs SET status = ? WHERE run_id = ?",
                (status, run_id),
            )
            conn.commit()

    def finish_run(self, run_id: str, status: str) -> None:
        """Set final status and finished_at timestamp."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "UPDATE dag_runs SET status = ?, finished_at = ? WHERE run_id = ?",
                (status, time.time(), run_id),
            )
            conn.commit()

    def record_node_start(self, run_id: str, node_label: str, input_json: str) -> None:
        """Record that a node has started execution."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "INSERT OR REPLACE INTO node_executions "
                "(run_id, node_label, status, input_json, started_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (run_id, node_label, "running", input_json, time.time()),
            )
            conn.commit()

    def record_node_complete(self, run_id: str, node_label: str, output_json: str) -> None:
        """Record that a node has completed successfully."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "UPDATE node_executions SET status = ?, output_json = ?, finished_at = ? "
                "WHERE run_id = ? AND node_label = ?",
                ("completed", output_json, time.time(), run_id, node_label),
            )
            conn.commit()

    def record_node_failed(self, run_id: str, node_label: str, error: str) -> None:
        """Record that a node has failed."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "UPDATE node_executions SET status = ?, error = ?, finished_at = ? "
                "WHERE run_id = ? AND node_label = ?",
                ("failed", error, time.time(), run_id, node_label),
            )
            conn.commit()

    def record_node_skipped(self, run_id: str, node_label: str, output_json: str) -> None:
        """Record a node as skipped with a copied output (used in replay)."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            now = time.time()
            execute_sql(
                cursor,
                "INSERT OR REPLACE INTO node_executions "
                "(run_id, node_label, status, output_json, started_at, finished_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, node_label, "skipped", output_json, now, now),
            )
            conn.commit()

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Get a run record by ID, or None if not found."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "SELECT run_id, dag_nsref, status, started_at, finished_at, "
                "source_inputs_json FROM dag_runs WHERE run_id = ?",
                (run_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return {
                "run_id": row[0],
                "dag_nsref": row[1],
                "status": row[2],
                "started_at": row[3],
                "finished_at": row[4],
                "source_inputs_json": row[5],
            }

    def get_node_executions(self, run_id: str) -> list[dict[str, Any]]:
        """Get all node execution records for a run."""
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "SELECT run_id, node_label, status, input_json, output_json, "
                "started_at, finished_at, error "
                "FROM node_executions WHERE run_id = ?",
                (run_id,),
            )
            return [
                {
                    "run_id": r[0],
                    "node_label": r[1],
                    "status": r[2],
                    "input_json": r[3],
                    "output_json": r[4],
                    "started_at": r[5],
                    "finished_at": r[6],
                    "error": r[7],
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


class NullProvenance:
    """
    No-op provenance -- discards writes, returns None/empty on reads.
    Used when `DagRunner` is created without explicit provenance.
    """

    def create_run(self, run_id: str, dag_nsref: str, source_inputs: dict[str, Any]) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def update_run_status(self, run_id: str, status: str) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def finish_run(self, run_id: str, status: str) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def record_node_start(self, run_id: str, node_label: str, input_json: str) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def record_node_complete(self, run_id: str, node_label: str, output_json: str) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def record_node_failed(self, run_id: str, node_label: str, error: str) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def record_node_skipped(self, run_id: str, node_label: str, output_json: str) -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    def get_run(self, run_id: str) -> dict[str, Any] | None:  # pyright: ignore[reportUnusedParameter]
        return None

    def get_node_executions(self, run_id: str) -> list[dict[str, Any]]:  # pyright: ignore[reportUnusedParameter]
        return []

    def get_node_output(self, run_id: str, node_label: str) -> str | None:  # pyright: ignore[reportUnusedParameter]
        return None

    def get_pending_nodes(self, run_id: str) -> list[str]:  # pyright: ignore[reportUnusedParameter]
        return []
