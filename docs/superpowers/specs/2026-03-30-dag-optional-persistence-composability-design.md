# DAG Optional Persistence + Composability Design

## Problem

1. `DagRunner` requires `db_path` and always creates a SQLite-backed
   `DagProvenance`. For simple fire-and-forget DAG execution, persistence
   is unnecessary overhead.

2. There is no way to reuse a DAG definition inside another DAG. If you have
   a reusable ETL pipeline, you must manually recreate its nodes in every
   parent DAG, risking label collisions.

## Solution

Two changes:

1. **NullProvenance** — a no-op implementation of the provenance interface
   that discards writes and returns empty/None on reads. `DagRunner` uses it
   when `db_path` is None.

2. **Dag.clone(prefix) + merge via >>** — clone a DAG with prefixed labels
   (`/` separator), then merge it into a parent DAG using the `>>` operator.
   Requires exactly one source and one sink on the sub-DAG.

## NullProvenance

```python
class NullProvenance:
    """No-op provenance — discards writes, returns None/empty on reads."""

    def create_run(self, run_id, dag_nsref, source_inputs): pass
    def update_run_status(self, run_id, status): pass
    def finish_run(self, run_id, status): pass
    def record_node_start(self, run_id, node_label, input_json): pass
    def record_node_complete(self, run_id, node_label, output_json): pass
    def record_node_failed(self, run_id, node_label, error): pass
    def record_node_skipped(self, run_id, node_label, output_json): pass
    def get_run(self, run_id): return None
    def get_node_executions(self, run_id): return []
    def get_node_output(self, run_id, node_label): return None
    def get_pending_nodes(self, run_id): return []
```

Lives in `dag_provenance.py` alongside `DagProvenance`.

### DagRunner Changes

- `DagRunner.__init__(self, dag, db_path=None)` — if `db_path` is None,
  `self.provenance = NullProvenance()`; otherwise
  `self.provenance = DagProvenance(db_path)`.
- `_execute()` unchanged — calls provenance methods unconditionally.
- `restart()` and `replay()` — `get_run()` returns None on NullProvenance,
  so they naturally raise `ValueError("Run 'xxx' not found")`.

### Dag.db_path Changes

`Dag.db_path` remains optional. When registering a Dag in Namespace without
`db_path`, the runner uses NullProvenance (fire-and-forget execution, no
restart/replay).

## Dag.clone(prefix)

```python
def clone(self, prefix: str) -> Dag:
    """
    Return a new Dag with all node labels prefixed with `prefix/`.
    Edges are remapped to the new labels. The original is unmodified.
    DagNodes in the clone share the same NamespaceNode references.
    """
```

- `prefix` must be non-empty (raises `ValueError` if empty)
- Separator is `/` (distinct from `.` for nsref branches and `:` for leaf)
- Example: `template.clone("etl")` turns `fetch`, `compute` into
  `etl/fetch`, `etl/compute`
- Edges are remapped: `DagEdge(upstream="fetch", downstream="compute")` →
  `DagEdge(upstream="etl/fetch", downstream="etl/compute")`
- Original Dag is unmodified
- Cloned DagNodes share the same `NamespaceNode` references (not deep-copied)

## Merge via >> Operator

`DagNode.__rshift__` is extended to accept a `Dag` as the right operand.

### Behavior

When `upstream_node >> sub_dag`:

1. Assert `sub_dag` is non-empty
2. Assert `sub_dag` has exactly one source and one sink
3. Copy all nodes from `sub_dag` into the parent Dag (reparent `dag`
   reference on each DagNode)
4. Copy all edges from `sub_dag` into the parent Dag
5. Add edge from `upstream_node` to the sub-dag's source
6. Return the sub-dag's sink DagNode (so `>>` chaining continues)

This is implemented as `Dag._merge_and_wire(upstream_node, sub_dag)`,
called from `DagNode.__rshift__`.

### Chaining

```python
setup >> sub >> report
```

- `setup >> sub` merges sub-dag into parent, wires `setup` → sub source,
  returns sub sink
- `sub_sink >> report` is normal `DagNode >> DagNode`

No `__rrshift__` needed — the first `>>` returns a DagNode, so the second
`>>` is already DagNode-to-DagNode.

### Usage Example

```python
# Define reusable template
ns.register(fetch_prices, nsref="market:fetch")
ns.register(compute_returns, nsref="analysis:compute")

with Dag() as template:
    f = template.node(ns.get("market:fetch"))
    c = template.node(ns.get("analysis:compute"))
    f >> c

# Use in parent DAG
ns.register(setup_fn, nsref="ops:setup")
ns.register(report_fn, nsref="ops:report")

sub = template.clone(prefix="etl")

with Dag() as parent:
    setup = parent.node(ns.get("ops:setup"))
    report = parent.node(ns.get("ops:report"))

    setup >> sub >> report
    # parent nodes: setup, etl/fetch, etl/compute, report
    # parent edges: setup >> etl/fetch, etl/fetch >> etl/compute,
    #               etl/compute >> report

# Nested composition
sub1 = template.clone(prefix="stage1")
sub2 = template.clone(prefix="stage2")

with Dag() as pipeline:
    s = pipeline.node(ns.get("ops:setup"))
    r = pipeline.node(ns.get("ops:report"))
    s >> sub1 >> sub2 >> r
```

## Validation and Error Cases

### clone()

- Empty `prefix` → `ValueError("prefix must be non-empty")`

### _merge_and_wire()

- Empty sub-dag → `ValueError("Cannot merge an empty DAG")`
- Multiple sources → `ValueError("Sub-DAG must have exactly one source, found N")`
- Multiple sinks → `ValueError("Sub-DAG must have exactly one sink, found N")`
- Label collision → `ValueError("Label 'etl/fetch' already exists in DAG")`

### DagRunner with NullProvenance

- `run()` works normally
- `restart()` → `ValueError("Run 'xxx' not found")`
- `replay()` → `ValueError("Run 'xxx' not found")`

## File Structure

**Modified:** `src/lythonic/compose/dag_provenance.py`
- Add `NullProvenance` class

**Modified:** `src/lythonic/compose/dag_runner.py`
- `DagRunner.__init__` `db_path` becomes optional (`Path | None = None`)

**Modified:** `src/lythonic/compose/namespace.py`
- `Dag.clone(prefix)` method
- `Dag._merge_and_wire(upstream_node, sub_dag)` method
- `DagNode.__rshift__` extended to accept `Dag`

**Modified:** `src/lythonic/compose/namespace.py` (Namespace.register)
- Allow Dag registration without `db_path` (uses NullProvenance)

**New tests:** Added to `tests/test_dag_runner.py` and `tests/test_namespace.py`

## Testing

1. DagRunner with `db_path=None` executes successfully
2. DagRunner with `db_path=None`, `restart()` raises ValueError
3. DagRunner with `db_path=None`, `replay()` raises ValueError
4. `Dag.clone()` produces prefixed labels with `/` separator
5. `Dag.clone()` remaps edges to new labels
6. `Dag.clone()` shares NamespaceNode references
7. `Dag.clone()` leaves original unmodified
8. `DagNode >> cloned_dag` merges and wires correctly
9. Chaining `setup >> sub >> report` produces correct topology
10. Merge with empty sub-dag raises ValueError
11. Merge with multi-source sub-dag raises ValueError
12. Merge with label collision raises ValueError
13. Clone with empty prefix raises ValueError
14. Nested composition: `setup >> sub1 >> sub2 >> report`
