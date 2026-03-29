# DAG Execution + Provenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an async DAG execution engine with SQLite-backed provenance, output-to-input wiring, DagContext injection, pause/restart, and selective replay.

**Architecture:** `dag_provenance.py` handles SQLite persistence for run state and node traces. `dag_runner.py` contains `DagRunner` which executes DAGs in topological order, wiring outputs to inputs by type. `namespace.py` is extended to allow registering a `Dag` as a callable.

**Tech Stack:** Python 3.11+, asyncio, SQLite, Pydantic, pytest, pytest-asyncio

**Spec:** `docs/superpowers/specs/2026-03-29-dag-execution-design.md`

---

### Task 1: DagProvenance — full module

**Files:**
- Create: `src/lythonic/compose/dag_provenance.py`
- Create: `tests/test_dag_runner.py` (initial tests)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_dag_runner.py`:

```python
from __future__ import annotations

import json
import tempfile
from pathlib import Path


def test_provenance_create_and_get_run():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "pipelines:daily", {"fetch": {"ticker": "AAPL"}})

        run = prov.get_run("run-1")
        assert run is not None
        assert run["run_id"] == "run-1"
        assert run["dag_nsref"] == "pipelines:daily"
        assert run["status"] == "running"
        assert run["started_at"] > 0
        assert json.loads(run["source_inputs_json"]) == {"fetch": {"ticker": "AAPL"}}


def test_provenance_update_and_finish_run():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.update_run_status("run-1", "paused")
        assert prov.get_run("run-1")["status"] == "paused"  # pyright: ignore

        prov.finish_run("run-1", "completed")
        run = prov.get_run("run-1")
        assert run is not None
        assert run["status"] == "completed"
        assert run["finished_at"] is not None


def test_provenance_node_lifecycle():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.record_node_start("run-1", "fetch", '{"ticker": "AAPL"}')
        execs = prov.get_node_executions("run-1")
        assert len(execs) == 1
        assert execs[0]["status"] == "running"

        prov.record_node_complete("run-1", "fetch", '{"price": 150.0}')
        execs = prov.get_node_executions("run-1")
        assert execs[0]["status"] == "completed"
        assert execs[0]["output_json"] == '{"price": 150.0}'
        assert execs[0]["finished_at"] is not None


def test_provenance_node_failed():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.record_node_start("run-1", "fetch", "{}")
        prov.record_node_failed("run-1", "fetch", "Connection timeout")

        execs = prov.get_node_executions("run-1")
        assert execs[0]["status"] == "failed"
        assert execs[0]["error"] == "Connection timeout"


def test_provenance_node_skipped():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.record_node_skipped("run-1", "fetch", '{"price": 100.0}')
        execs = prov.get_node_executions("run-1")
        assert execs[0]["status"] == "skipped"
        assert execs[0]["output_json"] == '{"price": 100.0}'


def test_provenance_get_node_output():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.record_node_start("run-1", "fetch", "{}")
        prov.record_node_complete("run-1", "fetch", '{"v": 1}')

        assert prov.get_node_output("run-1", "fetch") == '{"v": 1}'
        assert prov.get_node_output("run-1", "missing") is None


def test_provenance_get_pending_nodes():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.record_node_start("run-1", "fetch", "{}")
        prov.record_node_complete("run-1", "fetch", "{}")
        prov.record_node_start("run-1", "compute", "{}")
        prov.record_node_failed("run-1", "compute", "err")

        pending = prov.get_pending_nodes("run-1")
        assert "compute" in pending
        assert "fetch" not in pending


def test_provenance_get_missing_run():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        assert prov.get_run("nonexistent") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dag_runner.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

Create `src/lythonic/compose/dag_provenance.py`:

```python
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

    def create_run(
        self, run_id: str, dag_nsref: str, source_inputs: dict[str, Any]
    ) -> None:
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

    def record_node_start(
        self, run_id: str, node_label: str, input_json: str
    ) -> None:
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

    def record_node_complete(
        self, run_id: str, node_label: str, output_json: str
    ) -> None:
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

    def record_node_failed(
        self, run_id: str, node_label: str, error: str
    ) -> None:
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

    def record_node_skipped(
        self, run_id: str, node_label: str, output_json: str
    ) -> None:
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dag_runner.py -v`
Expected: All PASS

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/dag_provenance.py tests/test_dag_runner.py
git commit -m "feat(dag): add DagProvenance SQLite data layer"
```

---

### Task 2: DagRunner — linear execution with output-to-input wiring

**Files:**
- Create: `src/lythonic/compose/dag_runner.py`
- Modify: `tests/test_dag_runner.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_dag_runner.py`:

```python
import tests.test_dag_runner as this_module
from typing import Any


async def _async_source(ticker: str) -> float:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    return 100.0


async def _async_double(value: float) -> float:  # pyright: ignore[reportUnusedFunction]
    return value * 2


async def _async_format(value: float) -> str:  # pyright: ignore[reportUnusedFunction]
    return f"result={value}"


async def test_linear_dag_execution():
    """Three nodes in a line: source -> double -> format."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")
    ns.register(this_module._async_double, nsref="t:double")
    ns.register(this_module._async_format, nsref="t:format")

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        d = dag.node(ns.get("t:double"))
        f = dag.node(ns.get("t:format"))
        s >> d >> f

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")
        result = await runner.run(
            source_inputs={"source": {"ticker": "AAPL"}},
            dag_nsref="test:pipeline",
        )

        assert result.status == "completed"
        assert result.outputs["format"] == "result=200.0"
        assert result.failed_node is None


async def test_provenance_recorded_during_run():
    """Verify provenance DB is populated during execution."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")
    ns.register(this_module._async_double, nsref="t:double")

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        d = dag.node(ns.get("t:double"))
        s >> d

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "runs.db"
        runner = DagRunner(dag, db_path)
        result = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:pipe",
        )

        run = runner.provenance.get_run(result.run_id)
        assert run is not None
        assert run["status"] == "completed"

        execs = runner.provenance.get_node_executions(result.run_id)
        assert len(execs) == 2
        labels = {e["node_label"] for e in execs}
        assert labels == {"source", "double"}
        for e in execs:
            assert e["status"] == "completed"
            assert e["output_json"] is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dag_runner.py::test_linear_dag_execution -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

Create `src/lythonic/compose/dag_runner.py`:

```python
"""
DagRunner: Async execution engine for DAGs with provenance tracking.

Executes DAG nodes in topological order, wiring outputs to inputs by type.
Supports DagContext injection, pause/restart, and selective replay.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from lythonic.compose.dag_provenance import DagProvenance
from lythonic.compose.namespace import Dag, DagContext, DagNode


class DagPause(Exception):
    """Raise from a DAG node to signal the runner should pause."""


class DagRunResult(BaseModel):
    """Result of a DAG execution."""

    run_id: str
    status: str
    outputs: dict[str, Any]
    failed_node: str | None = None
    error: str | None = None


class DagRunner:
    """
    Async execution engine for DAGs with provenance tracking.

    Executes nodes in topological order, wires outputs to inputs by type
    (with fan-in list collection), injects `DagContext` when expected,
    and supports pause/restart/replay.
    """

    dag: Dag
    provenance: DagProvenance
    _pause_requested: bool

    def __init__(self, dag: Dag, db_path: Path) -> None:
        self.dag = dag
        self.provenance = DagProvenance(db_path)
        self._pause_requested = False

    def pause(self) -> None:
        """Request pause after the current node completes."""
        self._pause_requested = True

    async def run(
        self,
        source_inputs: dict[str, dict[str, Any]] | None = None,
        dag_nsref: str | None = None,
    ) -> DagRunResult:
        """Execute the DAG from scratch."""
        run_id = str(uuid.uuid4())
        nsref = dag_nsref or "unknown"
        self.provenance.create_run(run_id, nsref, source_inputs or {})
        self._pause_requested = False

        return await self._execute(
            run_id, nsref, source_inputs or {}, completed_outputs={}
        )

    async def _execute(
        self,
        run_id: str,
        dag_nsref: str,
        source_inputs: dict[str, dict[str, Any]],
        completed_outputs: dict[str, Any],
    ) -> DagRunResult:
        """Core execution loop shared by run, restart, and replay."""
        node_outputs: dict[str, Any] = dict(completed_outputs)
        order = self.dag.topological_order()
        sink_labels = {n.label for n in self.dag.sinks()}

        for dag_node in order:
            if dag_node.label in node_outputs:
                continue

            kwargs = self._wire_inputs(dag_node, node_outputs, source_inputs)
            self.provenance.record_node_start(
                run_id, dag_node.label, json.dumps(kwargs, default=str)
            )

            try:
                result = await self._call_node(dag_node, run_id, dag_nsref, kwargs)
                node_outputs[dag_node.label] = result
                self.provenance.record_node_complete(
                    run_id, dag_node.label, json.dumps(result, default=str)
                )
            except DagPause:
                self.provenance.update_run_status(run_id, "paused")
                return DagRunResult(
                    run_id=run_id,
                    status="paused",
                    outputs={l: node_outputs[l] for l in sink_labels if l in node_outputs},
                )
            except Exception as e:
                self.provenance.record_node_failed(run_id, dag_node.label, str(e))
                self.provenance.finish_run(run_id, "failed")
                return DagRunResult(
                    run_id=run_id,
                    status="failed",
                    outputs={l: node_outputs[l] for l in sink_labels if l in node_outputs},
                    failed_node=dag_node.label,
                    error=str(e),
                )

            if self._pause_requested:
                self.provenance.update_run_status(run_id, "paused")
                return DagRunResult(
                    run_id=run_id,
                    status="paused",
                    outputs={l: node_outputs[l] for l in sink_labels if l in node_outputs},
                )

        self.provenance.finish_run(run_id, "completed")
        return DagRunResult(
            run_id=run_id,
            status="completed",
            outputs={l: node_outputs[l] for l in sink_labels if l in node_outputs},
        )

    async def _call_node(
        self,
        dag_node: DagNode,
        run_id: str,
        dag_nsref: str,
        kwargs: dict[str, Any],
    ) -> Any:
        """Call a node's function, injecting DagContext if expected."""
        fn = dag_node.ns_node._decorated or dag_node.ns_node.method.o

        if dag_node.ns_node.expects_dag_context():
            ctx_type = dag_node.ns_node.dag_context_type() or DagContext
            ctx = ctx_type(
                dag_nsref=dag_nsref,
                node_label=dag_node.label,
                run_id=run_id,
            )
            if asyncio.iscoroutinefunction(fn):
                return await fn(ctx, **kwargs)
            return fn(ctx, **kwargs)

        if asyncio.iscoroutinefunction(fn):
            return await fn(**kwargs)
        return fn(**kwargs)

    def _wire_inputs(
        self,
        dag_node: DagNode,
        node_outputs: dict[str, Any],
        source_inputs: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Build kwargs for a node by wiring upstream outputs to parameters."""
        upstream_edges = [
            e for e in self.dag.edges if e.downstream == dag_node.label
        ]

        # Source nodes get inputs from source_inputs
        if not upstream_edges:
            return dict(source_inputs.get(dag_node.label, {}))

        args = dag_node.ns_node.method.args
        if dag_node.ns_node.expects_dag_context() and len(args) > 0:
            args = args[1:]

        kwargs: dict[str, Any] = {}
        for arg in args:
            matching_values: list[Any] = []
            for edge in upstream_edges:
                if edge.upstream in node_outputs:
                    upstream_node = self.dag.nodes[edge.upstream]
                    upstream_return = upstream_node.ns_node.method.return_annotation
                    if upstream_return is not None and upstream_return == arg.annotation:
                        matching_values.append(node_outputs[edge.upstream])

            if len(matching_values) == 1:
                kwargs[arg.name] = matching_values[0]
            elif len(matching_values) > 1:
                kwargs[arg.name] = matching_values

        return kwargs
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dag_runner.py -v`
Expected: All PASS

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/dag_runner.py tests/test_dag_runner.py
git commit -m "feat(dag): add DagRunner with linear execution and output wiring"
```

---

### Task 3: DagContext injection + fan-out/fan-in

**Files:**
- Modify: `tests/test_dag_runner.py`

- [ ] **Step 1: Write the tests**

Add to `tests/test_dag_runner.py`:

```python
from lythonic.compose.namespace import DagContext


async def _ctx_node(ctx: DagContext, value: float) -> str:  # pyright: ignore[reportUnusedFunction]
    return f"{ctx.node_label}:{ctx.run_id[:8]}:{value}"


async def test_dag_context_injection():
    """Node with DagContext first param receives injected context."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")
    ns.register(this_module._ctx_node, nsref="t:ctx_node")

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        c = dag.node(ns.get("t:ctx_node"))
        s >> c

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")
        result = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="test:ctx_pipe",
        )

        assert result.status == "completed"
        output = result.outputs["ctx_node"]
        assert output.startswith("ctx_node:")
        assert ":100.0" in output


async def _async_report(value: float) -> str:  # pyright: ignore[reportUnusedFunction]
    return f"report:{value}"


async def _async_archive(value: float) -> str:  # pyright: ignore[reportUnusedFunction]
    return f"archive:{value}"


async def _async_merge(values: list[str]) -> str:  # pyright: ignore[reportUnusedFunction]
    return "+".join(sorted(values))


async def test_fan_out_fan_in():
    """Fan-out from source, fan-in collecting list of values."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")
    ns.register(this_module._async_report, nsref="t:report")
    ns.register(this_module._async_archive, nsref="t:archive")
    ns.register(this_module._async_merge, nsref="t:merge")

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        r = dag.node(ns.get("t:report"))
        a = dag.node(ns.get("t:archive"))
        m = dag.node(ns.get("t:merge"))
        s >> r >> m
        s >> a >> m

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")
        result = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:fanio",
        )

        assert result.status == "completed"
        assert result.outputs["merge"] == "archive:100.0+report:100.0"
```

- [ ] **Step 2: Run tests**

Run: `uv run pytest tests/test_dag_runner.py::test_dag_context_injection tests/test_dag_runner.py::test_fan_out_fan_in -v`
Expected: PASS (implementation already supports these)

- [ ] **Step 3: Commit**

```bash
git add tests/test_dag_runner.py
git commit -m "test(dag): add DagContext injection and fan-out/fan-in tests"
```

---

### Task 4: Node failure + pause mechanisms

**Files:**
- Modify: `tests/test_dag_runner.py`

- [ ] **Step 1: Write the tests**

Add to `tests/test_dag_runner.py`:

```python
async def _async_fail(value: float) -> str:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    raise RuntimeError("intentional failure")


async def test_node_failure():
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")
    ns.register(this_module._async_fail, nsref="t:fail_node")

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        f = dag.node(ns.get("t:fail_node"))
        s >> f

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")
        result = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:fail_pipe",
        )

        assert result.status == "failed"
        assert result.failed_node == "fail_node"
        assert "intentional failure" in (result.error or "")

        run = runner.provenance.get_run(result.run_id)
        assert run is not None
        assert run["status"] == "failed"


async def _async_pause_node(value: float) -> str:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    from lythonic.compose.dag_runner import DagPause

    raise DagPause()


async def test_node_driven_pause():
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")
    ns.register(this_module._async_pause_node, nsref="t:pauser")
    ns.register(this_module._async_format, nsref="t:fmt")

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        p = dag.node(ns.get("t:pauser"))
        f = dag.node(ns.get("t:fmt"))
        s >> p >> f

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")
        result = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:pause_pipe",
        )

        assert result.status == "paused"
        assert result.failed_node is None
        # "fmt" should not have run
        assert "fmt" not in result.outputs


async def test_external_pause():
    import asyncio

    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    call_count = 0

    async def _slow_step1(ticker: str) -> float:  # pyright: ignore[reportUnusedParameter]
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return 1.0

    async def _slow_step2(value: float) -> str:  # pyright: ignore[reportUnusedParameter]
        nonlocal call_count
        call_count += 1
        return "done"

    ns = Namespace()
    ns.register(_slow_step1, nsref="t:step1")
    ns.register(_slow_step2, nsref="t:step2")

    with Dag() as dag:
        s1 = dag.node(ns.get("t:step1"))
        s2 = dag.node(ns.get("t:step2"))
        s1 >> s2

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")

        async def pause_after_delay():
            await asyncio.sleep(0.005)
            runner.pause()

        asyncio.create_task(pause_after_delay())
        result = await runner.run(
            source_inputs={"step1": {"ticker": "X"}},
            dag_nsref="t:ext_pause",
        )

        assert result.status == "paused"
        # step1 completed but step2 should not have run
        assert call_count == 1
```

- [ ] **Step 2: Run tests**

Run: `uv run pytest tests/test_dag_runner.py::test_node_failure tests/test_dag_runner.py::test_node_driven_pause tests/test_dag_runner.py::test_external_pause -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_dag_runner.py
git commit -m "test(dag): add failure, DagPause, and external pause tests"
```

---

### Task 5: Restart

**Files:**
- Modify: `src/lythonic/compose/dag_runner.py`
- Modify: `tests/test_dag_runner.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_dag_runner.py`:

```python
async def test_restart_paused_run():
    """Restart a paused run — resumes from the paused node."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    this_module._pause_once_count = 0  # pyright: ignore

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")
    ns.register(this_module._pause_once, nsref="t:maybe_pause")
    ns.register(this_module._async_format, nsref="t:fmt")

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        p = dag.node(ns.get("t:maybe_pause"))
        f = dag.node(ns.get("t:fmt"))
        s >> p >> f

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")

        # First run pauses
        result1 = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:restart_pipe",
        )
        assert result1.status == "paused"

        # Restart completes
        result2 = await runner.restart(result1.run_id)
        assert result2.status == "completed"
        assert result2.run_id == result1.run_id
        assert result2.outputs["fmt"] == "result=200.0"


async def test_restart_failed_run():
    """Restart a failed run — re-executes from the failed node."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    this_module._fail_once_count = 0  # pyright: ignore

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")
    ns.register(this_module._fail_once, nsref="t:maybe_fail")

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        f = dag.node(ns.get("t:maybe_fail"))
        s >> f

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")

        result1 = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:restart_fail",
        )
        assert result1.status == "failed"

        result2 = await runner.restart(result1.run_id)
        assert result2.status == "completed"
        assert result2.outputs["maybe_fail"] == "ok:100.0"
```

Add helper functions:

```python
_pause_once_count = 0


async def _pause_once(value: float) -> float:  # pyright: ignore[reportUnusedFunction]
    from lythonic.compose.dag_runner import DagPause

    this_module._pause_once_count += 1  # pyright: ignore
    if this_module._pause_once_count == 1:  # pyright: ignore
        raise DagPause()
    return value * 2


_fail_once_count = 0


async def _fail_once(value: float) -> str:  # pyright: ignore[reportUnusedFunction]
    this_module._fail_once_count += 1  # pyright: ignore
    if this_module._fail_once_count == 1:  # pyright: ignore
        raise RuntimeError("first attempt fails")
    return f"ok:{value}"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dag_runner.py::test_restart_paused_run -v`
Expected: FAIL with `AttributeError: 'DagRunner' object has no attribute 'restart'`

- [ ] **Step 3: Write minimal implementation**

Add to the `DagRunner` class in `src/lythonic/compose/dag_runner.py`:

```python
    async def restart(self, run_id: str) -> DagRunResult:
        """Restart a paused or failed run from where it left off."""
        run_info = self.provenance.get_run(run_id)
        if run_info is None:
            raise ValueError(f"Run '{run_id}' not found")
        if run_info["status"] not in ("paused", "failed"):
            raise ValueError(
                f"Run '{run_id}' has status '{run_info['status']}', "
                f"expected 'paused' or 'failed'"
            )

        # Load completed node outputs
        completed_outputs: dict[str, Any] = {}
        for node_exec in self.provenance.get_node_executions(run_id):
            if node_exec["status"] == "completed" and node_exec["output_json"]:
                completed_outputs[node_exec["node_label"]] = json.loads(
                    node_exec["output_json"]
                )

        self.provenance.update_run_status(run_id, "running")
        self._pause_requested = False

        source_inputs_json = run_info.get("source_inputs_json")
        source_inputs = json.loads(source_inputs_json) if source_inputs_json else {}

        return await self._execute(
            run_id, run_info["dag_nsref"], source_inputs, completed_outputs
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dag_runner.py -v`
Expected: All PASS

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/dag_runner.py tests/test_dag_runner.py
git commit -m "feat(dag): add DagRunner.restart() for paused and failed runs"
```

---

### Task 6: Replay

**Files:**
- Modify: `src/lythonic/compose/dag_runner.py`
- Modify: `tests/test_dag_runner.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_dag_runner.py`:

```python
_replay_source_count = 0


async def _replay_source(ticker: str) -> float:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    this_module._replay_source_count += 1  # pyright: ignore
    return float(this_module._replay_source_count) * 100  # pyright: ignore


async def test_replay_selective_reexecution():
    """Replay: rerun 'double' but keep 'source' output from previous run."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    this_module._replay_source_count = 0  # pyright: ignore

    ns = Namespace()
    ns.register(this_module._replay_source, nsref="t:source")
    ns.register(this_module._async_double, nsref="t:double")
    ns.register(this_module._async_format, nsref="t:fmt")

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        d = dag.node(ns.get("t:double"))
        f = dag.node(ns.get("t:fmt"))
        s >> d >> f

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")

        # First run
        result1 = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:replay",
        )
        assert result1.status == "completed"
        assert result1.outputs["fmt"] == "result=200.0"
        assert this_module._replay_source_count == 1  # pyright: ignore

        # Replay: rerun double and fmt, but keep source output
        result2 = await runner.replay(result1.run_id, rerun_nodes={"double", "fmt"})
        assert result2.status == "completed"
        # Source was NOT re-run (count stays 1), but its output (100.0) was reused
        assert this_module._replay_source_count == 1  # pyright: ignore
        assert result2.outputs["fmt"] == "result=200.0"
        assert result2.run_id != result1.run_id

        # Verify source node was recorded as skipped
        execs = runner.provenance.get_node_executions(result2.run_id)
        source_exec = [e for e in execs if e["node_label"] == "source"][0]
        assert source_exec["status"] == "skipped"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_dag_runner.py::test_replay_selective_reexecution -v`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

Add to the `DagRunner` class:

```python
    async def replay(
        self,
        run_id: str,
        rerun_nodes: set[str],
    ) -> DagRunResult:
        """
        Re-execute selected nodes using a previous run's outputs for
        the rest. Creates a new run.
        """
        old_run = self.provenance.get_run(run_id)
        if old_run is None:
            raise ValueError(f"Run '{run_id}' not found")

        # Load outputs from old run
        old_outputs: dict[str, Any] = {}
        for node_exec in self.provenance.get_node_executions(run_id):
            if node_exec["status"] in ("completed", "skipped") and node_exec["output_json"]:
                old_outputs[node_exec["node_label"]] = json.loads(
                    node_exec["output_json"]
                )

        # Create new run
        new_run_id = str(uuid.uuid4())
        source_inputs_json = old_run.get("source_inputs_json")
        source_inputs = json.loads(source_inputs_json) if source_inputs_json else {}
        self.provenance.create_run(new_run_id, old_run["dag_nsref"], source_inputs)
        self._pause_requested = False

        # Pre-populate outputs for nodes NOT being re-run
        completed_outputs: dict[str, Any] = {}
        for label, output in old_outputs.items():
            if label not in rerun_nodes:
                completed_outputs[label] = output
                self.provenance.record_node_skipped(
                    new_run_id, label, json.dumps(output, default=str)
                )

        return await self._execute(
            new_run_id, old_run["dag_nsref"], source_inputs, completed_outputs
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dag_runner.py -v`
Expected: All PASS

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/dag_runner.py tests/test_dag_runner.py
git commit -m "feat(dag): add DagRunner.replay() for selective re-execution"
```

---

### Task 7: Dag registration in Namespace

**Files:**
- Modify: `src/lythonic/compose/namespace.py`
- Modify: `tests/test_dag_runner.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_dag_runner.py`:

```python
async def test_dag_registered_in_namespace():
    """A Dag registered in Namespace becomes callable."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")
    ns.register(this_module._async_double, nsref="t:double")

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        d = dag.node(ns.get("t:double"))
        s >> d

    with tempfile.TemporaryDirectory() as tmp:
        dag.db_path = Path(tmp) / "runs.db"
        ns.register(dag, nsref="pipelines:my_pipe")

        node = ns.get("pipelines:my_pipe")
        result = await node(source={"ticker": "X"})

        assert result.run_id is not None  # pyright: ignore
        assert result.status == "completed"  # pyright: ignore
        assert result.outputs["double"] == 200.0  # pyright: ignore


def test_dag_registration_requires_db_path():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    dag = Dag()
    try:
        ns.register(dag, nsref="p:test")  # pyright: ignore
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "db_path" in str(e)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dag_runner.py::test_dag_registered_in_namespace -v`
Expected: FAIL

- [ ] **Step 3: Implement Dag.db_path and Namespace.register for Dag**

In `src/lythonic/compose/namespace.py`, add `db_path` attribute to `Dag.__init__`:

```python
    def __init__(self) -> None:
        self.nodes = {}
        self.edges = []
        self.db_path: Path | None = None
```

Add the `Path` import at the top:

```python
from pathlib import Path
```

Update `Namespace.register()` to handle `Dag` input. Change the signature and add Dag detection at the top of the method:

```python
    def register(
        self,
        c: Callable[..., Any] | str | Dag,
        nsref: str | None = None,
        decorate: Callable[[Callable[..., Any]], Callable[..., Any]] | None = None,
    ) -> NamespaceNode:
```

Add at the start of the method body, before the GlobalRef logic:

```python
        if isinstance(c, Dag):
            return self._register_dag(c, nsref)
```

Add a new method to `Namespace`:

```python
    def _register_dag(self, dag: Dag, nsref: str | None) -> NamespaceNode:
        """Register a Dag as a callable NamespaceNode."""
        if dag.db_path is None:
            raise ValueError("Dag.db_path must be set before registering in a Namespace")
        if nsref is None:
            raise ValueError("nsref is required when registering a Dag")

        from lythonic.compose.dag_runner import DagRunner

        runner = DagRunner(dag, dag.db_path)

        async def dag_wrapper(**kwargs: Any) -> Any:
            # Map kwargs to source node inputs: each source gets its matching kwargs
            source_labels = {n.label for n in dag.sources()}
            source_inputs: dict[str, dict[str, Any]] = {}
            for label in source_labels:
                node = dag.nodes[label]
                node_args = node.ns_node.method.args
                if node.ns_node.expects_dag_context():
                    node_args = node_args[1:]
                node_kwargs = {
                    a.name: kwargs[a.name] for a in node_args if a.name in kwargs
                }
                if node_kwargs:
                    source_inputs[label] = node_kwargs
            return await runner.run(source_inputs=source_inputs, dag_nsref=nsref)

        from lythonic.compose import Method

        method = Method(dag_wrapper)
        branch_parts, leaf_name = _parse_nsref(nsref)
        branch = self._get_or_create_branch(branch_parts)

        if leaf_name in branch._leaves:
            raise ValueError(f"Leaf '{leaf_name}' already exists in namespace")
        if leaf_name in branch._branches:
            raise ValueError(
                f"Cannot create leaf '{leaf_name}': a branch with that name already exists"
            )

        node = NamespaceNode(method=method, nsref=nsref, namespace=self)
        branch._leaves[leaf_name] = node
        return node
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dag_runner.py -v`
Expected: All PASS

- [ ] **Step 5: Run all tests**

Run: `uv run pytest tests/test_cached.py tests/test_namespace.py tests/test_dag_runner.py -v`
Expected: All PASS

- [ ] **Step 6: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_dag_runner.py
git commit -m "feat(dag): register Dag as callable in Namespace with DagRunner"
```

---

### Task 8: Docs and final verification

**Files:**
- Create: `docs/reference/compose-dag-provenance.md`
- Create: `docs/reference/compose-dag-runner.md`
- Modify: `docs/reference/compose-namespace.md`
- Modify: `mkdocs.yml`

- [ ] **Step 1: Create reference pages**

Create `docs/reference/compose-dag-provenance.md`:

```markdown
# lythonic.compose.dag_provenance

SQLite-backed storage for DAG run state and node execution traces.

::: lythonic.compose.dag_provenance
    options:
      show_root_heading: false
      members:
        - DagProvenance
```

Create `docs/reference/compose-dag-runner.md`:

```markdown
# lythonic.compose.dag_runner

Async execution engine for DAGs with provenance tracking.

::: lythonic.compose.dag_runner
    options:
      show_root_heading: false
      members:
        - DagRunner
        - DagRunResult
        - DagPause
```

- [ ] **Step 2: Update mkdocs.yml nav**

Add after `lythonic.compose.namespace`:

```yaml
      - lythonic.compose.dag_provenance: reference/compose-dag-provenance.md
      - lythonic.compose.dag_runner: reference/compose-dag-runner.md
```

- [ ] **Step 3: Update compose-namespace.md**

Add `Dag` `db_path` to the documented members if not already there. The existing reference page already lists `Dag`.

- [ ] **Step 4: Run full test suite**

Run: `make lint && make test`
Expected: All pass, zero lint errors

- [ ] **Step 5: Commit**

```bash
git add docs/reference/compose-dag-provenance.md docs/reference/compose-dag-runner.md mkdocs.yml
git commit -m "docs: add dag_provenance and dag_runner reference pages"
```
