# Dag-Namespace Refactoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stabilize the Dag-owns-Namespace pattern with proper collision handling, update DagNode references on registration, and remove the clone/merge composition mechanism.

**Architecture:** Dags own a `Namespace` and auto-register callables passed to `dag.node()`. When a Dag is registered into a parent namespace via `_register_dag`, its nodes are copied with skip-if-identical / raise-if-conflict semantics, and all `DagNode.ns_node` references are updated to point to the parent. `clone()`, `_merge_and_wire()`, and `DagNode >> Dag` are removed.

**Tech Stack:** Python 3.11+, Pydantic, pytest

---

## File Structure

| File | Responsibility |
|---|---|
| `src/lythonic/compose/namespace.py` | `Dag.__init__`, `Dag.node()`, `Namespace._register_dag()`, `DagNode.__rshift__`, remove `Dag.clone()` and `Dag._merge_and_wire()` |
| `tests/test_namespace.py` | Delete clone/merge tests, add auto-registration and node-copying tests |

---

### Task 1: Remove clone, merge, and DagNode >> Dag

Remove the composition mechanisms that `for_each` will replace. This is done first so subsequent tasks build on a clean codebase.

**Files:**
- Modify: `src/lythonic/compose/namespace.py:461-551`
- Modify: `tests/test_namespace.py:541-773`

- [ ] **Step 1: Simplify `DagNode.__rshift__` to only accept `DagNode`**

In `src/lythonic/compose/namespace.py`, replace the current `__rshift__` method (lines 461-470):

```python
    def __rshift__(self, other: DagNode) -> DagNode:
        """Register edge from self to other. Returns downstream for chaining."""
        self.dag.add_edge(self, other)
        return other
```

- [ ] **Step 2: Delete `Dag.clone()` and `Dag._merge_and_wire()`**

In `src/lythonic/compose/namespace.py`, delete the `clone` method (lines 490-513) and `_merge_and_wire` method (lines 515-551) entirely.

- [ ] **Step 3: Delete clone/merge tests**

In `tests/test_namespace.py`, delete everything from the comment `# Task 2 (plan): Dag.clone(prefix)` (line 541) through `test_rshift_dag_label_collision_raises` (line 773, the line with `assert "already exists" in str(e)`). This removes these tests:

- `test_dag_clone_prefixes_labels`
- `test_dag_clone_remaps_edges`
- `test_dag_clone_shares_ns_node`
- `test_dag_clone_leaves_original_unmodified`
- `test_dag_clone_empty_prefix_raises`
- `test_rshift_dag_merges_and_wires`
- `test_rshift_dag_chained_composition`
- `test_rshift_dag_empty_raises`
- `test_rshift_dag_multi_source_raises`
- `test_rshift_dag_multi_sink_raises`
- `test_rshift_dag_label_collision_raises`

Also delete the two section comments: `# Task 2 (plan): Dag.clone(prefix)` and `# Task 3 (plan): DagNode >> Dag merge and wire`.

- [ ] **Step 4: Run full test suite and lint**

Run: `make lint && make test`
Expected: All pass. Some test count reduction (removed ~11 tests).

- [ ] **Step 5: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace.py
git commit -m "refactor: remove Dag.clone(), _merge_and_wire(), and DagNode >> Dag"
```

---

### Task 2: Fix `_register_dag` — collision handling and reference update

The user's uncommitted changes already have the basic node-copying logic. This task fixes it per the spec: replace `assert` with `ValueError`, add skip-if-identical/raise-if-conflict semantics, and update `DagNode.ns_node` references.

**Files:**
- Modify: `src/lythonic/compose/namespace.py:336-394`
- Test: `tests/test_namespace.py`

- [ ] **Step 1: Write failing tests for the new `_register_dag` behavior**

Add to `tests/test_namespace.py` after the existing DAG tests (after the context manager tests, before the `# Tags` comment):

```python
# Dag-namespace registration


def test_register_dag_copies_nodes_to_parent():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    dag = Dag()
    dag.node(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]
    dag.node(this_module._another_fn)  # pyright: ignore[reportPrivateUsage]

    ns.register(dag, nsref="pipelines:my_dag")

    # Nodes from the DAG should now be in the parent namespace
    sample_node = ns.get("tests.test_namespace:_sample_fn")
    assert sample_node is not None
    another_node = ns.get("tests.test_namespace:_another_fn")
    assert another_node is not None


def test_register_dag_updates_dagnode_references():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    dag = Dag()
    dag.node(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]

    ns.register(dag, nsref="pipelines:my_dag")

    # DagNode.ns_node should now point to the parent namespace's copy
    dag_node = dag.nodes["_sample_fn"]
    assert dag_node.ns_node.namespace is ns


def test_register_dag_skip_identical():
    """If parent already has the same callable, skip without error."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]

    # Should not raise — same callable
    ns.register(dag, nsref="pipelines:my_dag")

    # DagNode should now reference the parent's node
    dag_node = dag.nodes["_sample_fn"]
    assert dag_node.ns_node.namespace is ns


def test_register_dag_conflict_raises():
    """If parent has same nsref but different callable, raise ValueError."""
    from lythonic.compose import Method
    from lythonic.compose.namespace import Dag, Namespace, NamespaceNode

    ns = Namespace()
    # Register _sample_fn under the nsref that _another_fn will use
    ns.register(
        this_module._sample_fn,  # pyright: ignore[reportPrivateUsage]
        nsref="tests.test_namespace:_another_fn",
    )

    dag = Dag()
    dag.node(this_module._another_fn)  # pyright: ignore[reportPrivateUsage]

    try:
        ns.register(dag, nsref="pipelines:my_dag")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "conflict" in str(e).lower() or "different" in str(e).lower()


def test_register_dag_double_registration_raises():
    """Registering a DAG to the same namespace twice raises ValueError."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    dag = Dag()
    dag.node(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]

    ns.register(dag, nsref="pipelines:first")

    try:
        ns.register(dag, nsref="pipelines:second")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "already registered" in str(e).lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py::test_register_dag_copies_nodes_to_parent tests/test_namespace.py::test_register_dag_updates_dagnode_references tests/test_namespace.py::test_register_dag_skip_identical tests/test_namespace.py::test_register_dag_conflict_raises tests/test_namespace.py::test_register_dag_double_registration_raises -v`
Expected: Some FAIL — collision handling and reference update not yet correct.

- [ ] **Step 3: Implement the fixed `_register_dag`**

Replace the `_register_dag` method in `src/lythonic/compose/namespace.py` (lines 336-394) with:

```python
    def _register_dag(
        self, dag: Dag, nsref: str | None, tags: frozenset[str] | set[str] | list[str] | None = None
    ) -> NamespaceNode:
        """
        Register a Dag as a callable NamespaceNode. Copies the Dag's
        internal namespace nodes into this namespace (skip if identical
        callable, raise on conflict). Updates DagNode references to
        point to this namespace.
        """
        if nsref is None:
            raise ValueError("nsref is required when registering a Dag")

        if dag.namespace is self:
            raise ValueError(f"Dag '{nsref}' is already registered with this namespace")

        # Copy nodes from DAG's namespace to parent, with collision handling
        for ns_node in dag.namespace._all_leaves():
            branch_parts, leaf_name = _parse_nsref(ns_node.nsref)
            branch = self._get_or_create_branch(branch_parts)

            if leaf_name in branch._leaves:
                existing = branch._leaves[leaf_name]
                if str(existing.method.gref) == str(ns_node.method.gref):
                    continue  # Same callable, skip
                raise ValueError(
                    f"Namespace conflict for '{ns_node.nsref}': "
                    f"existing callable {existing.method.gref} differs from {ns_node.method.gref}"
                )

            node = NamespaceNode(
                method=ns_node.method,
                nsref=ns_node.nsref,
                namespace=self,
                decorated=ns_node._decorated,
                tags=ns_node.tags,
            )
            branch._leaves[leaf_name] = node

        # Update DagNode references to point to parent namespace copies
        for dag_node in dag.nodes.values():
            dag_node.ns_node = self.get(dag_node.ns_node.nsref)

        dag.namespace = self

        from lythonic.compose.dag_runner import DagRunner  # pyright: ignore[reportImportCycles]

        runner = DagRunner(dag, dag.db_path)

        async def dag_wrapper(**kwargs: Any) -> Any:
            source_labels = {n.label for n in dag.sources()}
            source_inputs: dict[str, dict[str, Any]] = {}
            for label in source_labels:
                # If a kwarg key matches a source label and its value is a dict,
                # use it directly as that node's inputs.
                if label in kwargs and isinstance(kwargs[label], dict):
                    source_inputs[label] = kwargs[label]
                    continue
                node = dag.nodes[label]
                node_args = node.ns_node.method.args
                if node.ns_node.expects_dag_context():
                    node_args = node_args[1:]
                node_kwargs = {a.name: kwargs[a.name] for a in node_args if a.name in kwargs}
                if node_kwargs:
                    source_inputs[label] = node_kwargs
            return await runner.run(source_inputs=source_inputs, dag_nsref=nsref)

        method = Method(dag_wrapper)
        branch_parts, leaf_name = _parse_nsref(nsref)
        branch = self._get_or_create_branch(branch_parts)

        if leaf_name in branch._leaves:
            raise ValueError(f"Leaf '{leaf_name}' already exists in namespace")
        if leaf_name in branch._branches:
            raise ValueError(
                f"Cannot create leaf '{leaf_name}': a branch with that name already exists"
            )

        node = NamespaceNode(method=method, nsref=nsref, namespace=self, tags=tags)
        branch._leaves[leaf_name] = node
        return node
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace.py -k "register_dag" -v`
Expected: All PASS.

- [ ] **Step 5: Run full test suite and lint**

Run: `make lint && make test`
Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace.py
git commit -m "feat: add collision handling and reference update to _register_dag"
```

---

### Task 3: Test Dag.node() auto-registration

Add tests that verify the `Dag.node(callable)` auto-registration behavior already implemented in the user's uncommitted changes.

**Files:**
- Test: `tests/test_namespace.py`

- [ ] **Step 1: Write tests for auto-registration**

Add to `tests/test_namespace.py` in the "Dag-namespace registration" section (after the tests added in Task 2):

```python
def test_dag_node_auto_registers_callable():
    """dag.node(callable) auto-registers the callable in dag.namespace."""
    from lythonic.compose.namespace import Dag

    dag = Dag()
    dag.node(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]

    # Should be in the DAG's internal namespace
    node = dag.namespace.get("tests.test_namespace:_sample_fn")
    assert node is not None
    assert node(ticker="AAPL") == {"ticker": "AAPL", "limit": 10}


def test_dag_node_reuses_namespace_node():
    """Calling dag.node(same_fn) twice reuses the same NamespaceNode."""
    from lythonic.compose.namespace import Dag

    dag = Dag()
    d1 = dag.node(this_module._sample_fn, label="first")  # pyright: ignore[reportPrivateUsage]
    d2 = dag.node(this_module._sample_fn, label="second")  # pyright: ignore[reportPrivateUsage]

    assert d1.ns_node is d2.ns_node
    assert len(dag.nodes) == 2


def test_dag_node_with_namespace_node_skips_registration():
    """Passing a NamespaceNode directly does not re-register."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    registered = ns.register(this_module._sample_fn, nsref="market:fetch")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    d = dag.node(registered)

    assert d.ns_node is registered
    # DAG's own namespace should remain empty
    assert dag.namespace._all_leaves() == []
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace.py::test_dag_node_auto_registers_callable tests/test_namespace.py::test_dag_node_reuses_namespace_node tests/test_namespace.py::test_dag_node_with_namespace_node_skips_registration -v`
Expected: All PASS (the implementation already exists in the uncommitted changes).

- [ ] **Step 3: Run full test suite and lint**

Run: `make lint && make test`
Expected: All pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_namespace.py
git commit -m "test: add Dag.node() auto-registration tests"
```

---

### Task 4: Verify existing DAG runner tests still pass

This is a verification task. The refactoring should not break existing DAG execution, fan-in/fan-out, provenance, or pause/restart.

**Files:**
- Verify: `tests/test_dag_runner.py`

- [ ] **Step 1: Run all DAG runner tests**

Run: `uv run pytest tests/test_dag_runner.py -v`
Expected: All PASS (approximately 20 tests).

- [ ] **Step 2: Run full test suite one final time**

Run: `make lint && make test`
Expected: All pass, zero lint warnings.

- [ ] **Step 3: Commit any remaining unstaged changes**

If there are any linter auto-fixes or formatting changes:

```bash
git add -A && git status
```

If changes exist:
```bash
git commit -m "style: apply linter fixes after dag-namespace refactoring"
```
