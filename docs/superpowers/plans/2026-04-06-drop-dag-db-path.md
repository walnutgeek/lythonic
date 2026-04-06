# Drop db_path from Dag Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `db_path` from `Dag` and `DagRunner`, and `provenance_override` from `MapNode`, so that `Dag` is a pure graph and provenance is owned by the caller.

**Architecture:** `DagRunner.__init__` takes a `provenance` instance instead of a `db_path`. `Dag` drops `db_path`. `MapNode` drops `provenance_override` — sub-runners inherit parent provenance. Config layer stops setting `dag.db_path`.

**Tech Stack:** Python, pydantic, SQLite (via DagProvenance)

**Spec:** `docs/superpowers/specs/2026-04-06-drop-dag-db-path-design.md`

---

### Task 1: Change `DagRunner` to accept `provenance` instead of `db_path`

**Files:**
- Modify: `src/lythonic/compose/dag_runner.py:64-69`

- [ ] **Step 1: Modify `DagRunner.__init__` signature**

In `src/lythonic/compose/dag_runner.py`, replace the `__init__` method (lines 64-70):

Old:
```python
    def __init__(self, dag: Dag, db_path: Path | None = None) -> None:
        self.dag = dag
        if db_path is not None:
            self.provenance = DagProvenance(db_path)
        else:
            self.provenance = NullProvenance()
        self._pause_requested = False
```

New:
```python
    def __init__(self, dag: Dag, provenance: DagProvenance | NullProvenance | None = None) -> None:
        self.dag = dag
        self.provenance = provenance or NullProvenance()
        self._pause_requested = False
```

- [ ] **Step 2: Remove unused `Path` import if no longer needed**

Check if `Path` is still used in `dag_runner.py`. It is used on line 218 (`isinstance(map_node.provenance_override, Path)`) — that gets removed in Task 3. For now, leave the import.

- [ ] **Step 3: Run lint**

Run: `make lint`
Expected: passes (tests will break but lint should be clean)

- [ ] **Step 4: Commit**

```bash
git add src/lythonic/compose/dag_runner.py
git commit -m "refactor: DagRunner accepts provenance instance instead of db_path"
```

---

### Task 2: Remove `db_path` from `Dag` and update `_register_dag` and `Dag.__call__`

**Files:**
- Modify: `src/lythonic/compose/namespace.py:537-552,401-403,746`

- [ ] **Step 1: Remove `db_path` from `Dag`**

In `src/lythonic/compose/namespace.py`, remove the `db_path` class attribute (line 546) and the `self.db_path = None` line from `__init__` (line 552).

Old `Dag` class (lines 537-552):
```python
class Dag:
    """
    Directed acyclic graph of `DagNode`s with type-based validation.
    Use `node()` to add nodes and `>>` to declare edges.
    """

    nodes: dict[str, DagNode]
    edges: list[DagEdge]
    namespace: Namespace
    db_path: Path | None = None

    def __init__(self) -> None:
        self.nodes = {}
        self.edges = []
        self.namespace = Namespace()
        self.db_path = None
```

New:
```python
class Dag:
    """
    Directed acyclic graph of `DagNode`s with type-based validation.
    Use `node()` to add nodes and `>>` to declare edges.
    """

    nodes: dict[str, DagNode]
    edges: list[DagEdge]
    namespace: Namespace

    def __init__(self) -> None:
        self.nodes = {}
        self.edges = []
        self.namespace = Namespace()
```

- [ ] **Step 2: Update `_register_dag` to use `NullProvenance()`**

In `src/lythonic/compose/namespace.py`, update the `_register_dag` method. Replace line 401-403:

Old:
```python
        from lythonic.compose.dag_runner import DagRunner  # pyright: ignore[reportImportCycles]

        runner = DagRunner(dag, dag.db_path)
```

New:
```python
        from lythonic.compose.dag_runner import DagRunner  # pyright: ignore[reportImportCycles]
        from lythonic.compose.dag_provenance import NullProvenance  # pyright: ignore[reportImportCycles]

        runner = DagRunner(dag, provenance=NullProvenance())
```

- [ ] **Step 3: Update `Dag.__call__` to not pass `db_path=None`**

In `src/lythonic/compose/namespace.py`, update line 746:

Old:
```python
        runner = DagRunner(self, db_path=None)
```

New:
```python
        runner = DagRunner(self)
```

- [ ] **Step 4: Run lint**

Run: `make lint`
Expected: passes

- [ ] **Step 5: Commit**

```bash
git add src/lythonic/compose/namespace.py
git commit -m "refactor: remove db_path from Dag, use NullProvenance in _register_dag"
```

---

### Task 3: Remove `provenance_override` from `MapNode` and `Dag.map()`, update `_execute_map_node`

**Files:**
- Modify: `src/lythonic/compose/namespace.py:509-534,587-619`
- Modify: `src/lythonic/compose/dag_runner.py:218-224`

- [ ] **Step 1: Remove `provenance_override` from `MapNode`**

In `src/lythonic/compose/namespace.py`, update the `MapNode` class (lines 509-534):

Old:
```python
class MapNode(DagNode):
    """
    A DAG node that runs a sub-DAG on each element of a collection.
    Created via `Dag.map()`. The sub-DAG must have exactly one source
    and one sink.
    """

    sub_dag: Dag
    provenance_override: type | Path | None

    def __init__(
        self,
        sub_dag: Dag,
        label: str,
        dag: Dag,
        provenance_override: type | Path | None = None,
    ) -> None:
        # MapNode has no real callable — create a placeholder NamespaceNode.
        placeholder = NamespaceNode(
            method=Method(lambda: None),
            nsref=f"__map__:{label}",
            namespace=dag.namespace,
        )
        super().__init__(ns_node=placeholder, label=label, dag=dag)
        self.sub_dag = sub_dag
        self.provenance_override = provenance_override
```

New:
```python
class MapNode(DagNode):
    """
    A DAG node that runs a sub-DAG on each element of a collection.
    Created via `Dag.map()`. The sub-DAG must have exactly one source
    and one sink.
    """

    sub_dag: Dag

    def __init__(
        self,
        sub_dag: Dag,
        label: str,
        dag: Dag,
    ) -> None:
        # MapNode has no real callable — create a placeholder NamespaceNode.
        placeholder = NamespaceNode(
            method=Method(lambda: None),
            nsref=f"__map__:{label}",
            namespace=dag.namespace,
        )
        super().__init__(ns_node=placeholder, label=label, dag=dag)
        self.sub_dag = sub_dag
```

- [ ] **Step 2: Remove `provenance_override` from `Dag.map()`**

In `src/lythonic/compose/namespace.py`, update the `map` method (lines 587-622):

Old:
```python
    def map(
        self,
        sub_dag: Dag,
        label: str,
        provenance_override: type | Path | None = None,
    ) -> MapNode:
        """
        Create a `MapNode` that runs `sub_dag` on each element of an
        upstream collection. The sub-DAG must have exactly one source
        and one sink. `label` is required.
        """
        if not label:
            raise ValueError("label is required for map()")

        sources = sub_dag.sources()
        sinks = sub_dag.sinks()
        if len(sources) != 1:
            raise ValueError(
                f"Sub-DAG must have exactly one source for map(), found {len(sources)}"
            )
        if len(sinks) != 1:
            raise ValueError(f"Sub-DAG must have exactly one sink for map(), found {len(sinks)}")

        if label in self.nodes:
            raise ValueError(
                f"Label '{label}' already exists in DAG. Use a unique label for map nodes."
            )

        map_node = MapNode(
            sub_dag=sub_dag,
            label=label,
            dag=self,
            provenance_override=provenance_override,
        )
        self.nodes[label] = map_node
        return map_node
```

New:
```python
    def map(
        self,
        sub_dag: Dag,
        label: str,
    ) -> MapNode:
        """
        Create a `MapNode` that runs `sub_dag` on each element of an
        upstream collection. The sub-DAG must have exactly one source
        and one sink. `label` is required.
        """
        if not label:
            raise ValueError("label is required for map()")

        sources = sub_dag.sources()
        sinks = sub_dag.sinks()
        if len(sources) != 1:
            raise ValueError(
                f"Sub-DAG must have exactly one source for map(), found {len(sources)}"
            )
        if len(sinks) != 1:
            raise ValueError(f"Sub-DAG must have exactly one sink for map(), found {len(sinks)}")

        if label in self.nodes:
            raise ValueError(
                f"Label '{label}' already exists in DAG. Use a unique label for map nodes."
            )

        map_node = MapNode(
            sub_dag=sub_dag,
            label=label,
            dag=self,
        )
        self.nodes[label] = map_node
        return map_node
```

- [ ] **Step 3: Update `_execute_map_node` to inherit parent provenance**

In `src/lythonic/compose/dag_runner.py`, replace lines 218-224:

Old:
```python
        if isinstance(map_node.provenance_override, Path):
            sub_db_path: Path | None = map_node.provenance_override
        else:
            sub_db_path = None

        async def run_iteration(key: str, element: Any) -> Any:
            sub_runner = DagRunner(map_node.sub_dag, sub_db_path)
```

New:
```python
        async def run_iteration(key: str, element: Any) -> Any:
            sub_runner = DagRunner(map_node.sub_dag, provenance=self.provenance)
```

- [ ] **Step 4: Remove unused `Path` import from `dag_runner.py`**

In `src/lythonic/compose/dag_runner.py`, remove `from pathlib import Path` (line 15) since `Path` is no longer used anywhere in the file.

- [ ] **Step 5: Check if `Path` import can be removed from `namespace.py`**

Check if `Path` is still used in `namespace.py`. It was used for `provenance_override: type | Path | None` and `db_path: Path | None`. If no other usage remains, remove `from pathlib import Path` (line 67).

- [ ] **Step 6: Run lint**

Run: `make lint`
Expected: passes

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/compose/namespace.py src/lythonic/compose/dag_runner.py
git commit -m "refactor: remove provenance_override from MapNode, sub-runners inherit parent provenance"
```

---

### Task 4: Remove `dag.db_path` from config layer

**Files:**
- Modify: `src/lythonic/compose/namespace_config.py:157-158`

- [ ] **Step 1: Remove `dag.db_path = dag_db` from `load_namespace`**

In `src/lythonic/compose/namespace_config.py`, delete lines 157-158:

```python
        if dag_db is not None:
            dag.db_path = dag_db
```

The `dag_db` variable remains in scope (line 108) since it may be useful for future provenance setup at the call site, but it is no longer set on the Dag. (If `dag_db` becomes unused after this change and lint warns, remove line 108 as well.)

- [ ] **Step 2: Run lint**

Run: `make lint`
Expected: passes. If lint warns about unused `dag_db`, also remove line 108 (`dag_db = ...`).

- [ ] **Step 3: Commit**

```bash
git add src/lythonic/compose/namespace_config.py
git commit -m "refactor: remove dag.db_path assignment from config layer"
```

---

### Task 5: Update all tests

**Files:**
- Modify: `tests/test_dag_runner.py` (many lines)
- Modify: `tests/test_word_count.py:6`

- [ ] **Step 1: Add `DagProvenance` import to test file**

In `tests/test_dag_runner.py`, the `DagProvenance` import is already used in some tests via local imports. Add a top-level import so all tests can use it:

At the top of the file (after line 7 `from pathlib import Path`), add:

```python
from lythonic.compose.dag_provenance import DagProvenance
```

- [ ] **Step 2: Update all `DagRunner(dag, Path(tmp) / "runs.db")` calls**

Replace every occurrence of `DagRunner(dag, Path(tmp) / "runs.db")` and `DagRunner(parent, Path(tmp) / "runs.db")` with `DagRunner(dag, provenance=DagProvenance(Path(tmp) / "runs.db"))` (or `parent` instead of `dag`).

Lines to update (each one follows the same pattern):

- Line 210: `DagRunner(dag, Path(tmp) / "runs.db")` → `DagRunner(dag, provenance=DagProvenance(Path(tmp) / "runs.db"))`
- Line 237: `runner = DagRunner(dag, db_path)` → `runner = DagRunner(dag, provenance=DagProvenance(db_path))`
- Line 271: same pattern as 210
- Line 303: same pattern as 210
- Line 331: same pattern as 210
- Line 368: same pattern as 210
- Line 423: same pattern as 210
- Line 482: same pattern as 210
- Line 515: same pattern as 210
- Line 555: same pattern as 210
- Line 719: `DagRunner(parent, Path(tmp) / "runs.db")` → `DagRunner(parent, provenance=DagProvenance(Path(tmp) / "runs.db"))`
- Line 759: same pattern as 719
- Line 789: same pattern as 719
- Line 817: `DagRunner(parent, Path(tmp) / "runs.db")` → `DagRunner(parent, provenance=DagProvenance(Path(tmp) / "runs.db"))`
- Line 846: same pattern as 817

- [ ] **Step 3: Update the `dag.db_path` test (lines 585-602)**

The test `test_dag_registered_with_db_path` (around line 580) sets `dag.db_path`. Update it to register the dag without db_path (it now always uses NullProvenance when registered):

Replace lines 593-594:
```python
        dag.db_path = Path(tmp) / "runs.db"
        ns.register(dag, nsref="pipelines:my_pipe")
```

With:
```python
        ns.register(dag, nsref="pipelines:my_pipe")
```

The `with tempfile.TemporaryDirectory() as tmp:` block is no longer needed since there's no db_path. Simplify the test to not use a temp dir, matching the pattern in `test_dag_registered_without_db_path`.

- [ ] **Step 4: Update test docstrings**

Update the docstring for `test_dag_registered_without_db_path` (line 606) since the concept of "without db_path" no longer applies — all registered DAGs use NullProvenance. Consider merging this test with the one above it since they now test the same thing, or simply update the docstring to reflect the new behavior.

- [ ] **Step 5: Remove or update `test_map_provenance_override` (lines 769-796)**

This test verifies `provenance_override` on `MapNode`, which no longer exists. Delete the entire test function `test_map_provenance_override` (lines 769-796).

- [ ] **Step 6: Update `tests/test_word_count.py`**

In `tests/test_word_count.py`, line 6:

Old:
```python
    drr = await DagRunner(wc.main_dag, None).run()
```

New:
```python
    drr = await DagRunner(wc.main_dag).run()
```

- [ ] **Step 7: Run full test suite**

Run: `make test`
Expected: all tests pass

- [ ] **Step 8: Run lint**

Run: `make lint`
Expected: zero warnings/errors

- [ ] **Step 9: Commit**

```bash
git add tests/test_dag_runner.py tests/test_word_count.py
git commit -m "test: update all tests for provenance-based DagRunner API"
```
