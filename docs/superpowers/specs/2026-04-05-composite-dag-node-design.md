# CompositeDagNode + CallNode

Extract a `CompositeDagNode` base class from `MapNode`, add `CallNode` for
running a sub-DAG once inline, and refactor the runner to share execution
logic between map and call.

## `CompositeDagNode` base class

`CompositeDagNode` subclasses `DagNode` and holds:

- `sub_dag: Dag`

Constructor validates the sub-DAG has exactly one source and one sink.
Creates a placeholder `NamespaceNode` (logic moved from `MapNode.__init__`).

`MapNode` and `CallNode` both subclass `CompositeDagNode` with no extra
fields -- they are type markers the runner uses to decide execution strategy.

## `CallNode` and `Dag.node()` overload

`CallNode` subclasses `CompositeDagNode` with no extra fields.

`Dag.node()` gains a third source type: `Dag`. When `source` is a `Dag`,
it creates a `CallNode`. `label` is required (raises `ValueError` if `None`).
Validation (one source, one sink) happens in `CompositeDagNode.__init__`.

`Dag.map()` stays as-is but `MapNode.__init__` delegates to
`CompositeDagNode.__init__`.

## Runner execution

Refactor `_execute_map_node` into shared + specific parts:

- `_get_upstream_output(composite_node, node_outputs)` -- shared helper, finds
  single upstream edge, returns its output.
- `_run_sub_dag(composite_node, element, key, dag_nsref)` -- shared helper,
  creates `DagRunner(sub_dag, provenance=self.provenance)`, builds
  `source_inputs` from element, runs, returns sink output.
- `_execute_map_node` -- calls `_get_upstream_output`, iterates collection,
  calls `_run_sub_dag` per element via `asyncio.gather`.
- `_execute_call_node` -- calls `_get_upstream_output`, calls `_run_sub_dag`
  once with upstream output directly.

In `_execute()`, check `MapNode` then `CallNode` then normal node.

## Test Coverage

- CompositeDagNode validation via `Dag.node(sub_dag)` (one source/sink).
- CallNode creation: `dag.node(sub_dag, label="enrich")` creates `CallNode`,
  label required.
- CallNode execution: end-to-end linear DAG with sub-DAG step.
- CallNode error propagation.
- Existing MapNode tests still pass.
- Word count example still works.

## Files Changed

- `src/lythonic/compose/namespace.py` -- `CompositeDagNode`, `CallNode`,
  refactor `MapNode`, update `Dag.node()`.
- `src/lythonic/compose/dag_runner.py` -- refactor `_execute_map_node`,
  add `_get_upstream_output`, `_run_sub_dag`, `_execute_call_node`.
- `tests/test_namespace.py` -- CallNode creation and validation tests.
- `tests/test_dag_runner.py` -- CallNode execution tests.
