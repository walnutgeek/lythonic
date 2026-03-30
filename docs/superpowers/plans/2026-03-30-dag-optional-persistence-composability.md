# DAG Optional Persistence + Composability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make DAG persistence optional (NullProvenance for fire-and-forget execution) and add DAG composability via `clone(prefix)` and `>>` merge.

**Architecture:** Add `NullProvenance` to `dag_provenance.py` as a no-op drop-in. Make `DagRunner.__init__` accept `db_path=None`. Add `Dag.clone()` and `Dag._merge_and_wire()` to `namespace.py`. Extend `DagNode.__rshift__` to accept `Dag` operand.

**Tech Stack:** Python 3.11+, asyncio, SQLite, Pydantic, pytest, pytest-asyncio

**Spec:** `docs/superpowers/specs/2026-03-30-dag-optional-persistence-composability-design.md`

---

### Task 1: NullProvenance + optional db_path on DagRunner

**Files:**
- Modify: `src/lythonic/compose/dag_provenance.py`
- Modify: `src/lythonic/compose/dag_runner.py`
- Modify: `tests/test_dag_runner.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_dag_runner.py`:

```python
async def test_runner_without_persistence():
    """DagRunner with db_path=None executes successfully."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")
    ns.register(this_module._async_double, nsref="t:double")

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        d = dag.node(ns.get("t:double"))
        s >> d

    runner = DagRunner(dag)
    result = await runner.run(
        source_inputs={"source": {"ticker": "X"}},
        dag_nsref="t:no_persist",
    )
    assert result.status == "completed"
    assert result.outputs["double"] == 200.0


async def test_runner_without_persistence_restart_raises():
    """restart() with NullProvenance raises ValueError (run not found)."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")

    with Dag() as dag:
        dag.node(ns.get("t:source"))

    runner = DagRunner(dag)
    result = await runner.run(
        source_inputs={"source": {"ticker": "X"}},
    )

    try:
        await runner.restart(result.run_id)
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "not found" in str(e)


async def test_runner_without_persistence_replay_raises():
    """replay() with NullProvenance raises ValueError (run not found)."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")

    with Dag() as dag:
        dag.node(ns.get("t:source"))

    runner = DagRunner(dag)
    result = await runner.run(
        source_inputs={"source": {"ticker": "X"}},
    )

    try:
        await runner.replay(result.run_id, rerun_nodes={"source"})
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "not found" in str(e)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dag_runner.py::test_runner_without_persistence -v`
Expected: FAIL — `DagRunner.__init__` requires `db_path` positional arg

- [ ] **Step 3: Write implementation**

Add `NullProvenance` to `src/lythonic/compose/dag_provenance.py`, after the `DagProvenance` class:

```python
class NullProvenance:
    """
    No-op provenance — discards writes, returns None/empty on reads.
    Used when `DagRunner` is created without a `db_path`.
    """

    def create_run(self, run_id: str, dag_nsref: str, source_inputs: dict[str, Any]) -> None:
        pass

    def update_run_status(self, run_id: str, status: str) -> None:
        pass

    def finish_run(self, run_id: str, status: str) -> None:
        pass

    def record_node_start(self, run_id: str, node_label: str, input_json: str) -> None:
        pass

    def record_node_complete(self, run_id: str, node_label: str, output_json: str) -> None:
        pass

    def record_node_failed(self, run_id: str, node_label: str, error: str) -> None:
        pass

    def record_node_skipped(self, run_id: str, node_label: str, output_json: str) -> None:
        pass

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return None

    def get_node_executions(self, run_id: str) -> list[dict[str, Any]]:
        return []

    def get_node_output(self, run_id: str, node_label: str) -> str | None:
        return None

    def get_pending_nodes(self, run_id: str) -> list[str]:
        return []
```

Update `DagRunner.__init__` in `src/lythonic/compose/dag_runner.py`:

Change from:
```python
    def __init__(self, dag: Dag, db_path: Path) -> None:
        self.dag = dag
        self.provenance = DagProvenance(db_path)
        self._pause_requested = False
```

To:
```python
    def __init__(self, dag: Dag, db_path: Path | None = None) -> None:
        self.dag = dag
        if db_path is not None:
            self.provenance: DagProvenance | NullProvenance = DagProvenance(db_path)
        else:
            self.provenance = NullProvenance()
        self._pause_requested = False
```

Add `NullProvenance` to the import:
```python
from lythonic.compose.dag_provenance import DagProvenance, NullProvenance
```

Update the type annotation on the class:
```python
    provenance: DagProvenance | NullProvenance
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dag_runner.py -v`
Expected: All PASS

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/dag_provenance.py src/lythonic/compose/dag_runner.py tests/test_dag_runner.py
git commit -m "feat(dag): add NullProvenance, make DagRunner.db_path optional"
```

---

### Task 2: Dag.clone(prefix)

**Files:**
- Modify: `src/lythonic/compose/namespace.py`
- Modify: `tests/test_namespace.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_namespace.py`:

```python
def test_dag_clone_prefixes_labels():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")
    ns.register(this_module._another_fn, nsref="a:compute")

    with Dag() as template:
        f = template.node(ns.get("a:fetch"))
        c = template.node(ns.get("a:compute"))
        f >> c

    clone = template.clone("etl")
    assert "etl/fetch" in clone.nodes
    assert "etl/compute" in clone.nodes
    assert len(clone.nodes) == 2


def test_dag_clone_remaps_edges():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")
    ns.register(this_module._another_fn, nsref="a:compute")

    with Dag() as template:
        f = template.node(ns.get("a:fetch"))
        c = template.node(ns.get("a:compute"))
        f >> c

    clone = template.clone("etl")
    assert len(clone.edges) == 1
    assert clone.edges[0].upstream == "etl/fetch"
    assert clone.edges[0].downstream == "etl/compute"


def test_dag_clone_shares_ns_node():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")

    with Dag() as template:
        template.node(ns.get("a:fetch"))

    clone = template.clone("sub")
    assert clone.nodes["sub/fetch"].ns_node is template.nodes["fetch"].ns_node


def test_dag_clone_leaves_original_unmodified():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")
    ns.register(this_module._another_fn, nsref="a:compute")

    with Dag() as template:
        f = template.node(ns.get("a:fetch"))
        c = template.node(ns.get("a:compute"))
        f >> c

    original_labels = set(template.nodes.keys())
    original_edges = list(template.edges)

    template.clone("etl")

    assert set(template.nodes.keys()) == original_labels
    assert template.edges == original_edges


def test_dag_clone_empty_prefix_raises():
    from lythonic.compose.namespace import Dag

    dag = Dag()
    try:
        dag.clone("")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "non-empty" in str(e)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py::test_dag_clone_prefixes_labels -v`
Expected: FAIL — `Dag` has no `clone` method

- [ ] **Step 3: Write implementation**

Add to the `Dag` class in `src/lythonic/compose/namespace.py`, after the `__init__` method:

```python
    def clone(self, prefix: str) -> Dag:
        """
        Return a new Dag with all node labels prefixed with `prefix/`.
        Edges are remapped to the new labels. The original is unmodified.
        DagNodes in the clone share the same `NamespaceNode` references.

        >>> from lythonic.compose.namespace import Dag, Namespace, NamespaceNode
        >>> from lythonic.compose import Method
        >>> ns = Namespace()
        >>> def f() -> int: return 1
        >>> def g(x: int) -> int: return x
        >>> ns.register(f, nsref='a:f')
        NamespaceNode(nsref='a:f')
        >>> ns.register(g, nsref='a:g')
        NamespaceNode(nsref='a:g')
        >>> d = Dag()
        >>> _ = d.node(ns.get('a:f'))
        >>> _ = d.node(ns.get('a:g'))
        >>> _ = d.nodes['f'] >> d.nodes['g']
        >>> c = d.clone('sub')
        >>> sorted(c.nodes.keys())
        ['sub/f', 'sub/g']

        """
        if not prefix:
            raise ValueError("prefix must be non-empty")

        new_dag = Dag()
        for old_label, old_node in self.nodes.items():
            new_label = f"{prefix}/{old_label}"
            dag_node = DagNode(ns_node=old_node.ns_node, label=new_label, dag=new_dag)
            new_dag.nodes[new_label] = dag_node

        for edge in self.edges:
            new_dag.edges.append(
                DagEdge(
                    upstream=f"{prefix}/{edge.upstream}",
                    downstream=f"{prefix}/{edge.downstream}",
                )
            )

        return new_dag
```

Also add a `__repr__` to `NamespaceNode` so the doctest output for `ns.register()` is predictable:

```python
    def __repr__(self) -> str:
        return f"NamespaceNode(nsref={self.nsref!r})"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace.py -v`
Expected: All PASS

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace.py
git commit -m "feat(dag): add Dag.clone(prefix) for composable DAG templates"
```

---

### Task 3: DagNode >> Dag merge and wire

**Files:**
- Modify: `src/lythonic/compose/namespace.py`
- Modify: `tests/test_namespace.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_namespace.py`:

```python
def test_rshift_dag_merges_and_wires():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:setup")
    ns.register(this_module._sample_fn, nsref="a:fetch")
    ns.register(this_module._another_fn, nsref="a:compute")
    ns.register(this_module._another_fn, nsref="a:report")

    with Dag() as template:
        f = template.node(ns.get("a:fetch"))
        c = template.node(ns.get("a:compute"))
        f >> c

    sub = template.clone("etl")

    with Dag() as parent:
        setup = parent.node(ns.get("a:setup"))
        report = parent.node(ns.get("a:report"))
        setup >> sub >> report

    assert "setup" in parent.nodes
    assert "etl/fetch" in parent.nodes
    assert "etl/compute" in parent.nodes
    assert "report" in parent.nodes
    assert len(parent.nodes) == 4

    edge_pairs = [(e.upstream, e.downstream) for e in parent.edges]
    assert ("setup", "etl/fetch") in edge_pairs
    assert ("etl/fetch", "etl/compute") in edge_pairs
    assert ("etl/compute", "report") in edge_pairs


def test_rshift_dag_chained_composition():
    """setup >> sub1 >> sub2 >> report"""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:setup")
    ns.register(this_module._sample_fn, nsref="a:step")
    ns.register(this_module._another_fn, nsref="a:report")

    with Dag() as template:
        template.node(ns.get("a:step"))

    sub1 = template.clone("stage1")
    sub2 = template.clone("stage2")

    with Dag() as parent:
        setup = parent.node(ns.get("a:setup"))
        report = parent.node(ns.get("a:report"))
        setup >> sub1 >> sub2 >> report

    assert len(parent.nodes) == 4
    labels = sorted(parent.nodes.keys())
    assert labels == ["report", "setup", "stage1/step", "stage2/step"]

    edge_pairs = [(e.upstream, e.downstream) for e in parent.edges]
    assert ("setup", "stage1/step") in edge_pairs
    assert ("stage1/step", "stage2/step") in edge_pairs
    assert ("stage2/step", "report") in edge_pairs


def test_rshift_dag_empty_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:setup")

    parent = Dag()
    setup = parent.node(ns.get("a:setup"))

    empty_dag = Dag()
    try:
        setup >> empty_dag
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "empty" in str(e).lower()


def test_rshift_dag_multi_source_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:setup")
    ns.register(this_module._sample_fn, nsref="a:s1")
    ns.register(this_module._another_fn, nsref="a:s2")

    multi = Dag()
    multi.node(ns.get("a:s1"))
    multi.node(ns.get("a:s2"))

    parent = Dag()
    setup = parent.node(ns.get("a:setup"))

    try:
        setup >> multi
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "one source" in str(e).lower()


def test_rshift_dag_multi_sink_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:setup")
    ns.register(this_module._sample_fn, nsref="a:src")
    ns.register(this_module._another_fn, nsref="a:sink1")
    ns.register(this_module._another_fn, nsref="a:sink2")

    multi = Dag()
    src = multi.node(ns.get("a:src"))
    s1 = multi.node(ns.get("a:sink1"))
    s2 = multi.node(ns.get("a:sink2"))
    src >> s1
    src >> s2

    parent = Dag()
    setup = parent.node(ns.get("a:setup"))

    try:
        setup >> multi
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "one sink" in str(e).lower()


def test_rshift_dag_label_collision_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")

    sub = Dag()
    sub.node(ns.get("a:fetch"))

    parent = Dag()
    parent.node(ns.get("a:fetch"))

    try:
        parent.nodes["fetch"] >> sub
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "already exists" in str(e)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py::test_rshift_dag_merges_and_wires -v`
Expected: FAIL

- [ ] **Step 3: Write implementation**

Update `DagNode.__rshift__` in `src/lythonic/compose/namespace.py`:

```python
    def __rshift__(self, other: DagNode | Dag) -> DagNode:
        """
        Register edge from self to other. If other is a Dag, merge it
        into the parent and wire the boundary edges. Returns the
        downstream DagNode for chaining.
        """
        if isinstance(other, Dag):
            return self.dag._merge_and_wire(self, other)
        self.dag.add_edge(self, other)
        return other
```

Add `_merge_and_wire` method to the `Dag` class:

```python
    def _merge_and_wire(self, upstream_node: DagNode, sub_dag: Dag) -> DagNode:
        """
        Merge a sub-DAG into this DAG and wire boundary edges.
        The sub-DAG must have exactly one source and one sink.
        Returns the sink node (for `>>` chaining).
        """
        if not sub_dag.nodes:
            raise ValueError("Cannot merge an empty DAG")

        sources = sub_dag.sources()
        sinks = sub_dag.sinks()
        if len(sources) != 1:
            raise ValueError(
                f"Sub-DAG must have exactly one source, found {len(sources)}"
            )
        if len(sinks) != 1:
            raise ValueError(
                f"Sub-DAG must have exactly one sink, found {len(sinks)}"
            )

        # Check for label collisions
        for label in sub_dag.nodes:
            if label in self.nodes:
                raise ValueError(f"Label '{label}' already exists in DAG")

        # Copy nodes, reparenting to this DAG
        for label, node in sub_dag.nodes.items():
            reparented = DagNode(ns_node=node.ns_node, label=label, dag=self)
            self.nodes[label] = reparented

        # Copy edges
        for edge in sub_dag.edges:
            self.edges.append(DagEdge(upstream=edge.upstream, downstream=edge.downstream))

        # Wire upstream to sub-dag source
        source_label = sources[0].label
        self.add_edge(upstream_node, self.nodes[source_label])

        # Return sub-dag sink (reparented) for chaining
        sink_label = sinks[0].label
        return self.nodes[sink_label]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace.py -v`
Expected: All PASS

- [ ] **Step 5: Run all tests**

Run: `uv run pytest tests/test_dag_runner.py tests/test_namespace.py tests/test_cached.py -v`
Expected: All PASS

- [ ] **Step 6: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace.py
git commit -m "feat(dag): add DagNode >> Dag merge for composable DAG templates"
```

---

### Task 4: Update Namespace Dag registration for optional db_path

**Files:**
- Modify: `src/lythonic/compose/namespace.py`
- Modify: `tests/test_dag_runner.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_dag_runner.py`:

```python
async def test_dag_registered_without_db_path():
    """Dag with no db_path registered in Namespace uses NullProvenance."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")
    ns.register(this_module._async_double, nsref="t:double")

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        d = dag.node(ns.get("t:double"))
        s >> d

    # No db_path set
    ns.register(dag, nsref="pipelines:no_persist")

    node = ns.get("pipelines:no_persist")
    result = await node(source={"ticker": "X"})
    assert result.run_id is not None  # pyright: ignore
    assert result.status == "completed"  # pyright: ignore
    assert result.outputs["double"] == 200.0  # pyright: ignore
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_dag_runner.py::test_dag_registered_without_db_path -v`
Expected: FAIL — current `_register_dag` raises `ValueError("Dag.db_path must be set")`

- [ ] **Step 3: Update _register_dag**

In `src/lythonic/compose/namespace.py`, update `_register_dag` to allow `db_path=None`:

Change from:
```python
    def _register_dag(self, dag: Dag, nsref: str | None) -> NamespaceNode:
        """Register a Dag as a callable NamespaceNode."""
        if dag.db_path is None:
            raise ValueError("Dag.db_path must be set before registering in a Namespace")
```

To:
```python
    def _register_dag(self, dag: Dag, nsref: str | None) -> NamespaceNode:
        """
        Register a Dag as a callable NamespaceNode. If `dag.db_path` is
        None, the runner uses NullProvenance (no persistence, no
        restart/replay).
        """
```

And update the `DagRunner` creation line from:
```python
        runner = DagRunner(dag, dag.db_path)
```

To:
```python
        runner = DagRunner(dag, dag.db_path)  # db_path=None uses NullProvenance
```

(No code change needed here since `DagRunner.__init__` now accepts `None` from Task 1.)

Just remove the `db_path is None` check at the top.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_dag_runner.py -v`
Expected: All PASS

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_dag_runner.py
git commit -m "feat(dag): allow Dag registration without db_path (uses NullProvenance)"
```

---

### Task 5: Final verification + docs

**Files:**
- Modify: `docs/reference/compose-dag-provenance.md`

- [ ] **Step 1: Update docs reference**

Update `docs/reference/compose-dag-provenance.md` to include `NullProvenance`:

```markdown
# lythonic.compose.dag_provenance

SQLite-backed storage for DAG run state and node execution traces.

::: lythonic.compose.dag_provenance
    options:
      show_root_heading: false
      members:
        - DagProvenance
        - NullProvenance
```

- [ ] **Step 2: Run full test suite**

Run: `make lint && make test`
Expected: All pass, zero lint errors

- [ ] **Step 3: Commit**

```bash
git add docs/reference/compose-dag-provenance.md
git commit -m "docs: add NullProvenance to reference page"
```
