# Dag.map() — Sub-DAG Mapping Over Collections

Run a sub-DAG on each element of a collection (list or dict) produced by an
upstream node, with concurrent execution and structured provenance.

## API: `Dag.map()` and `MapNode`

`Dag.map(sub_dag: Dag, label: str) -> MapNode` creates a `MapNode` in the
parent DAG. `label` is required (no auto-derive).

`MapNode` subclasses `DagNode` and adds:

- `sub_dag: Dag` -- the sub-DAG to run per element.
- `provenance_override: type | Path | None = None` -- optional provenance for
  sub-DAG runners. `None` means `NullProvenance` (sub-iterations don't inherit
  parent provenance by default). A `Path` means `DagProvenance` with that path.

`MapNode` wires into the parent DAG with `>>` like any other node. The sub-DAG
must have exactly one source and one sink.

## Runner Execution

When `DagRunner._execute()` encounters a `MapNode`:

1. Gets the upstream output (a `list` or `dict`).
2. For a `list`: iterates with integer indices. For a `dict`: iterates with
   string keys.
3. For each element, creates a `DagRunner` for the sub-DAG with provenance
   from `provenance_override` (default `NullProvenance`), and `source_inputs`
   mapping the sub-DAG's single source to the element.
4. Runs all iterations concurrently via `asyncio.gather`.
5. Collects sink outputs: `list[Y]` if input was `list`, `dict[K, Y]` if input
   was `dict`.
6. Records one `node_executions` entry for the `MapNode` in the parent
   provenance with the aggregate output.
7. Per-iteration node executions use labels like `fragments[0]/tokenize` and
   go into the sub-DAG's own provenance (if not null).

## Label Scheme

The `MapNode` label (e.g., `fragments`) is the node identity in the parent
DAG's edges and provenance.

Sub-DAG iteration labels: `{map_label}[{key}]/{sub_node_label}`:

- List: `fragments[0]/tokenize`, `fragments[1]/count`
- Dict: `fragments[us]/tokenize`, `fragments[eu]/count`

The `dag_nsref` for sub-DAG runners:
`{parent_dag_nsref}/{map_label}[{key}]`.

## Validation and Error Handling

- `Dag.map()` validates the sub-DAG has exactly one source and one sink
  (`ValueError` otherwise).
- `label` is required -- `ValueError` if empty or `None`.
- Duplicate label check -- same as `dag.node()`.
- Runtime: `TypeError` if upstream output is neither `list` nor `dict`.
- If any iteration fails during `asyncio.gather`, the `MapNode` is marked
  failed in parent provenance. The first exception propagates
  (`return_exceptions=False`).

## Files Changed

- `src/lythonic/compose/namespace.py` -- `MapNode` class, `Dag.map()` method.
- `src/lythonic/compose/dag_runner.py` -- `MapNode` detection in `_execute()`,
  sub-DAG runner creation, `asyncio.gather`, result collection.
- `tests/test_namespace.py` -- `MapNode` creation, validation tests.
- `tests/test_dag_runner.py` -- end-to-end: map over list, map over dict,
  provenance, provenance override, error propagation.
