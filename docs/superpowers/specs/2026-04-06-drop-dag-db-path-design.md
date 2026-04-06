# Drop `db_path` from Dag: Separate Graph from Execution

## Problem

`Dag` currently carries a `db_path: Path | None` attribute, and `DagRunner`
accepts `db_path` and internally constructs a `DagProvenance`. This couples
graph definition to execution concerns. `Dag` should be a pure graph structure.
`DagRunner` should receive a ready-made provenance object, not a file path.

## Design

### `Dag` changes

Remove the `db_path: Path | None` attribute and its initialization from
`Dag.__init__()`. `Dag` becomes a pure graph with no execution-related state.

### `DagRunner` changes

Replace `db_path: Path | None = None` with
`provenance: DagProvenance | NullProvenance | None = None` in `__init__`:

```python
def __init__(self, dag: Dag, provenance: DagProvenance | NullProvenance | None = None) -> None:
    self.dag = dag
    self.provenance = provenance or NullProvenance()
```

The caller constructs the provenance object. `DagRunner` just uses it.

### `MapNode` changes

Drop `provenance_override: type | Path | None` from `MapNode` entirely.
Drop the `provenance_override` parameter from `Dag.map()`.

In `_execute_map_node`, sub-runners inherit the parent runner's provenance:

```python
sub_runner = DagRunner(map_node.sub_dag, provenance=self.provenance)
```

The same provenance instance tracks the entire run hierarchy. Sub-dag node
labels are already namespaced (e.g. `dag/chunks[0]/process`) so there is
no collision.

### `_register_dag` in `namespace.py`

The wrapper `DagRunner` uses `NullProvenance()` directly:

```python
runner = DagRunner(dag, provenance=NullProvenance())
```

`Namespace.register()` signature is unchanged. The namespace module has
minimal knowledge of provenance.

### Config layer (`namespace_config.py`)

Remove lines 157-158 (`dag.db_path = dag_db`). The config layer no longer
sets provenance on the DAG. If provenance-tracked execution is needed,
the call site constructs a `DagRunner` with the appropriate provenance.

### Testing

Existing tests that pass `db_path` to `DagRunner` change to
`provenance=DagProvenance(path)`. Tests using `MapNode` with
`provenance_override` drop that parameter. No new tests needed.

## Files Changed

| File | Change |
|------|--------|
| `src/lythonic/compose/namespace.py` | Remove `db_path` from `Dag`, remove `provenance_override` from `MapNode` and `Dag.map()`, update `_register_dag` to use `NullProvenance()` |
| `src/lythonic/compose/dag_runner.py` | `DagRunner.__init__` takes `provenance` instead of `db_path`, `_execute_map_node` passes `self.provenance` to sub-runners |
| `src/lythonic/compose/namespace_config.py` | Remove `dag.db_path = dag_db` |
| `tests/test_dag_runner.py` | Update `DagRunner` construction to pass `provenance=DagProvenance(path)` |
