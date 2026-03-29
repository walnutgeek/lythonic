# DAG Execution + Provenance Design (Spec B)

## Problem

Spec A built the Namespace registry and DAG definition layer (`Dag`,
`DagNode`, `DagContext`, etc.) but has no execution engine. DAGs can be
defined and validated but not run. There is no mechanism to execute nodes
in order, wire outputs to inputs, track execution state, or pause/restart/
replay runs.

## Solution

Two new modules:
- `dag_provenance.py` — SQLite-backed persistence for run state and node
  execution traces (inputs, outputs, timing, errors)
- `dag_runner.py` — async execution engine that runs DAGs in topological
  order with DagContext injection, type-based output-to-input wiring,
  pause/restart, and selective replay

## DagProvenance — SQLite Data Layer

### Schema

```sql
CREATE TABLE IF NOT EXISTS dag_runs (
    run_id TEXT PRIMARY KEY,
    dag_nsref TEXT NOT NULL,
    status TEXT NOT NULL,        -- "running", "paused", "completed", "failed"
    started_at REAL NOT NULL,
    finished_at REAL,
    source_inputs_json TEXT      -- JSON of the initial inputs
)

CREATE TABLE IF NOT EXISTS node_executions (
    run_id TEXT NOT NULL,
    node_label TEXT NOT NULL,
    status TEXT NOT NULL,        -- "pending", "running", "completed", "failed", "skipped"
    input_json TEXT,
    output_json TEXT,
    started_at REAL,
    finished_at REAL,
    error TEXT,
    PRIMARY KEY (run_id, node_label),
    FOREIGN KEY (run_id) REFERENCES dag_runs(run_id)
)
```

### API

```python
class DagProvenance:
    def __init__(self, db_path: Path) -> None: ...

    # Run lifecycle
    def create_run(self, run_id: str, dag_nsref: str, source_inputs: dict) -> None: ...
    def update_run_status(self, run_id: str, status: str) -> None: ...
    def finish_run(self, run_id: str, status: str) -> None: ...

    # Node recording
    def record_node_start(self, run_id: str, node_label: str, input_json: str) -> None: ...
    def record_node_complete(self, run_id: str, node_label: str, output_json: str) -> None: ...
    def record_node_failed(self, run_id: str, node_label: str, error: str) -> None: ...

    # Querying
    def get_run(self, run_id: str) -> dict | None: ...
    def get_node_executions(self, run_id: str) -> list[dict]: ...
    def get_node_output(self, run_id: str, node_label: str) -> str | None: ...
    def get_pending_nodes(self, run_id: str) -> list[str]: ...
```

Uses `open_sqlite_db` from `lythonic.state` for connection management. JSON
serialization via `json.dumps`/`json.loads` for simple types,
`model_dump_json`/`model_validate_json` for Pydantic models.

## DagRunner — Execution Engine

### Exceptions

```python
class DagPause(Exception):
    """Raise from a DAG node to signal the runner should pause."""
```

### Result Model

```python
class DagRunResult(BaseModel):
    run_id: str
    status: str              # "completed", "paused", "failed"
    outputs: dict[str, Any]  # sink node label -> output value
    failed_node: str | None
    error: str | None
```

### Runner API

```python
class DagRunner:
    dag: Dag
    provenance: DagProvenance

    def __init__(self, dag: Dag, db_path: Path) -> None: ...

    async def run(
        self,
        source_inputs: dict[str, dict[str, Any]] | None = None,
        dag_nsref: str | None = None,
    ) -> DagRunResult: ...

    async def restart(self, run_id: str) -> DagRunResult: ...

    async def replay(
        self,
        run_id: str,
        rerun_nodes: set[str],
    ) -> DagRunResult: ...

    def pause(self) -> None: ...
```

### Execution Flow — `run()`

1. Generate UUID `run_id`, create run record in provenance
2. Get `topological_order()` from the Dag
3. For each node in order:
   a. Build inputs: wire upstream outputs to downstream parameters
      (type-based, with fan-in list collection)
   b. Inject `DagContext` if the node expects it
   c. Record node start in provenance
   d. `await` the node's callable
   e. Record output in provenance
   f. If node raises `DagPause`: record node as "pending", set run
      status to "paused", return `DagRunResult`
   g. If `self._pause_requested` (external pause): save state, set run
      status to "paused", return
   h. If node raises other exception: record failure, set run status to
      "failed", return
4. After all nodes complete: set run status to "completed", return result
   with sink node outputs

### Execution Flow — `restart(run_id)`

1. Load run from provenance (must be "paused" or "failed")
2. Load completed node outputs from provenance
3. Get topological order, skip nodes already completed
4. Resume from the first pending/failed node, using saved outputs for
   upstream dependencies

### Execution Flow — `replay(run_id, rerun_nodes)`

1. Load previous run's provenance
2. Create a new run (new `run_id`)
3. For nodes NOT in `rerun_nodes`: copy outputs from previous run, mark
   as "skipped"
4. For nodes in `rerun_nodes`: execute normally, using fresh or copied
   upstream outputs as inputs

### Output-to-Input Wiring

For each downstream node, the runner builds its kwargs:

1. Get the downstream node's parameters (excluding DagContext if present)
2. For each parameter, find upstream nodes connected by edges whose output
   type is an exact match for the parameter's type annotation
3. **Single upstream match**: pass value directly as the kwarg
4. **Multiple upstream matches of same type** (fan-in): collect values into
   a `list[T]` — the downstream parameter must accept `list[T]`

Source nodes (no upstream edges) receive their inputs from `source_inputs`
dict, keyed by node label.

### DagContext Injection

When a node's first parameter is `DagContext` or a subclass (detected via
`NamespaceNode.expects_dag_context()`), the runner constructs and injects it:

```python
ctx_type = node.ns_node.dag_context_type() or DagContext
ctx = ctx_type(
    dag_nsref=dag_nsref,
    node_label=node.label,
    run_id=run_id,
)
result = await node_callable(ctx, **wired_inputs)
```

If the node expects a DagContext subclass, the runner instantiates that
subclass. Additional fields on the subclass must have defaults.

### Pause Mechanism

**Node-driven:** A node raises `DagPause`. The runner catches it, records
the node as "pending" (not completed), and sets the run to "paused".

```python
async def slow_fetch(ctx: DagContext, ticker: str) -> dict:
    data = await fetch_from_api(ticker)
    if rate_limited:
        raise DagPause()
    return data
```

**External:** Caller calls `runner.pause()` from another coroutine or
thread. Sets `self._pause_requested = True`. The runner checks this flag
after each node completes. The current node finishes, then the run pauses
before the next node.

## Dag Registration in Namespace

When a `Dag` is registered into a Namespace, it becomes a callable
`NamespaceNode`. The Dag carries its own `db_path` attribute for the
provenance database.

```python
dag.db_path = Path("runs.db")
ns.register(dag, nsref="pipelines:daily_refresh")

# Calling it runs the DAG
result = await ns.get("pipelines:daily_refresh")(ticker="AAPL")
```

`Namespace.register()` detects that `c` is a `Dag`:
- Requires `dag.db_path` is set (raises `ValueError` if not)
- Creates a `DagRunner(dag, dag.db_path)`
- Synthesizes a wrapper async function that maps kwargs to source node
  inputs and calls `runner.run()`
- The wrapper's inputs are the union of all source node parameters
  (excluding DagContext)
- The wrapper returns `DagRunResult`

## File Structure

**New file:** `src/lythonic/compose/dag_provenance.py`
- `DagProvenance`

**New file:** `src/lythonic/compose/dag_runner.py`
- `DagPause`
- `DagRunResult`
- `DagRunner`

**Modified:** `src/lythonic/compose/namespace.py`
- `Dag.db_path` attribute (optional, required for registration)
- `Namespace.register()` handles `Dag` input

**New tests:** `tests/test_dag_runner.py`

**Modified docs:** `docs/reference/` — add reference pages, update
`mkdocs.yml` nav

## Testing

Tests in `tests/test_dag_runner.py`:

1. Linear DAG execution — three nodes, outputs wired to inputs
2. DagContext injection — node receives context with correct run_id, label
3. Fan-out/fan-in wiring — single value fans out, multiple values
   collected into list
4. Node failure — runner records error, returns failed status
5. Node-driven pause via DagPause — run pauses, returns paused status
6. External pause — runner stops after current node
7. Restart paused run — resumes from pending nodes, uses saved outputs
8. Restart failed run — re-executes from the failed node
9. Replay with selective re-execution — rerun specific nodes, use cached
   outputs for others
10. Provenance recording — verify inputs/outputs/timing stored in DB
11. Provenance querying — get_run, get_node_executions, get_node_output
12. Dag registered in Namespace — callable, runs the DAG
13. Async node execution — verify await works correctly
