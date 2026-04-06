# CompositeDagNode + CallNode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract `CompositeDagNode` base class from `MapNode`, add `CallNode` for running a sub-DAG once inline, refactor the runner to share execution logic, and overload `Dag.node()` to accept `Dag`.

**Architecture:** `CompositeDagNode` holds `sub_dag` and validates one source/one sink. `MapNode` and `CallNode` are thin subclasses used as type markers by the runner. `Dag.node()` detects `Dag` input and creates `CallNode`. Runner extracts shared helpers (`_get_upstream_output`, `_run_sub_dag`) from the existing `_execute_map_node`.

**Tech Stack:** Python 3.11+, asyncio, pytest

---

## File Structure

| File | Responsibility |
|---|---|
| `src/lythonic/compose/namespace.py` | `CompositeDagNode`, `CallNode`, refactor `MapNode`, update `Dag.node()` |
| `src/lythonic/compose/dag_runner.py` | Refactor runner: shared helpers, `_execute_call_node`, update `_execute()` |
| `tests/test_namespace.py` | CallNode creation and validation tests |
| `tests/test_dag_runner.py` | CallNode execution tests |

---

### Task 1: Extract CompositeDagNode and add CallNode

**Files:**
- Modify: `src/lythonic/compose/namespace.py`
- Test: `tests/test_namespace.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_namespace.py` after the `# Dag.map()` section, before `# Dag.__call__`:

```python
# CallNode


def test_dag_node_with_sub_dag_creates_call_node():
    from lythonic.compose.namespace import CallNode, Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="t:process")  # pyright: ignore[reportPrivateUsage]

    sub_dag = Dag()
    sub_dag.node(ns.get("t:process"))

    parent = Dag()
    c = parent.node(sub_dag, label="enrich")

    assert isinstance(c, CallNode)
    assert c.label == "enrich"
    assert c.sub_dag is sub_dag
    assert "enrich" in parent.nodes


def test_dag_node_with_sub_dag_requires_label():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="t:process")  # pyright: ignore[reportPrivateUsage]

    sub_dag = Dag()
    sub_dag.node(ns.get("t:process"))

    parent = Dag()
    try:
        parent.node(sub_dag)
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "label" in str(e).lower()


def test_dag_node_with_sub_dag_multi_source_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="t:a")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="t:b")  # pyright: ignore[reportPrivateUsage]

    sub_dag = Dag()
    sub_dag.node(ns.get("t:a"))
    sub_dag.node(ns.get("t:b"))

    parent = Dag()
    try:
        parent.node(sub_dag, label="bad")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "one source" in str(e).lower()


def test_dag_node_with_sub_dag_multi_sink_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="t:src")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="t:sink1")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="t:sink2")  # pyright: ignore[reportPrivateUsage]

    sub_dag = Dag()
    s = sub_dag.node(ns.get("t:src"))
    s1 = sub_dag.node(ns.get("t:sink1"))
    s2 = sub_dag.node(ns.get("t:sink2"))
    s >> s1  # pyright: ignore[reportUnusedExpression]
    s >> s2  # pyright: ignore[reportUnusedExpression]

    parent = Dag()
    try:
        parent.node(sub_dag, label="bad")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "one sink" in str(e).lower()


def test_call_node_wires_with_rshift():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="t:process")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="t:sink")  # pyright: ignore[reportPrivateUsage]

    sub_dag = Dag()
    sub_dag.node(ns.get("t:process"))

    parent = Dag()
    s = parent.node(ns.get("t:source"))
    c = parent.node(sub_dag, label="enrich")
    k = parent.node(ns.get("t:sink"), label="sink")
    s >> c >> k  # pyright: ignore[reportUnusedExpression]

    assert len(parent.edges) == 2
    edge_pairs = [(e.upstream, e.downstream) for e in parent.edges]
    assert ("source", "enrich") in edge_pairs
    assert ("enrich", "sink") in edge_pairs
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py -k "call_node or dag_node_with_sub_dag" -v`
Expected: FAIL — `CallNode` and `CompositeDagNode` don't exist yet.

- [ ] **Step 3: Implement CompositeDagNode, refactor MapNode, add CallNode, update Dag.node()**

In `src/lythonic/compose/namespace.py`, replace the `MapNode` class (lines 508-530) with:

```python
class CompositeDagNode(DagNode):
    """
    Base class for DAG nodes that contain a sub-DAG. Validates the
    sub-DAG has exactly one source and one sink.
    """

    sub_dag: Dag

    def __init__(self, sub_dag: Dag, label: str, dag: Dag) -> None:
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

        placeholder = NamespaceNode(
            method=Method(lambda: None),
            nsref=f"__composite__:{label}",
            namespace=dag.namespace,
        )
        super().__init__(ns_node=placeholder, label=label, dag=dag)
        self.sub_dag = sub_dag


class MapNode(CompositeDagNode):
    """Runs a sub-DAG on each element of a collection."""

    pass


class CallNode(CompositeDagNode):
    """Runs a sub-DAG once as a single step."""

    pass
```

Update `Dag.node()` signature from `source: NamespaceNode | Callable[..., Any]` to `source: NamespaceNode | Callable[..., Any] | Dag`. Add a `Dag` branch at the top of the method:

```python
    def node(
        self,
        source: NamespaceNode | Callable[..., Any] | Dag,
        label: str | None = None,
    ) -> DagNode:
        """
        Create a unique `DagNode` in this graph. If `source` is a `Dag`,
        creates a `CallNode` (label required). If `label` is `None`,
        derived from the source's nsref leaf name. Raises `ValueError`
        if the label already exists.
        """
        if isinstance(source, Dag):
            if label is None:
                raise ValueError("label is required when passing a Dag to node()")
            if label in self.nodes:
                raise ValueError(
                    f"Label '{label}' already exists in DAG. "
                    f"Use an explicit label for duplicate callables."
                )
            call_node = CallNode(sub_dag=source, label=label, dag=self)
            self.nodes[label] = call_node
            return call_node

        if isinstance(source, NamespaceNode):
            ns_node = source
        else:
            gref = GlobalRef(source)
            try:
                ns_node = self.namespace.get(str(gref))
            except KeyError:
                ns_node = self.namespace.register(source)

        if label is None:
            _, leaf = _parse_nsref(ns_node.nsref)
            label = leaf

        if label in self.nodes:
            raise ValueError(
                f"Label '{label}' already exists in DAG. "
                f"Use an explicit label for duplicate callables."
            )

        dag_node = DagNode(ns_node=ns_node, label=label, dag=self)
        self.nodes[label] = dag_node
        return dag_node
```

Update `Dag.map()` to remove the source/sink validation (now in `CompositeDagNode.__init__`):

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

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace.py -k "call_node or dag_node_with_sub_dag" -v`
Expected: All PASS.

- [ ] **Step 5: Run full test suite and lint**

Run: `make lint && make test`
Expected: All pass (existing MapNode tests should still work).

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace.py
git commit -m "feat: extract CompositeDagNode base, add CallNode, overload Dag.node() for Dag"
```

---

### Task 2: Refactor runner and add CallNode execution

**Files:**
- Modify: `src/lythonic/compose/dag_runner.py`
- Test: `tests/test_dag_runner.py`

- [ ] **Step 1: Write failing tests for CallNode execution**

Add module-level test helpers and tests to `tests/test_dag_runner.py`. Add these helpers near the other `_async_*` helpers at the top of the file:

```python
async def _async_double_str(text: str) -> str:  # pyright: ignore[reportUnusedFunction]
    return text + text


async def _async_make_text() -> str:  # pyright: ignore[reportUnusedFunction]
    return "hello"


async def test_call_node_execution():
    """CallNode runs a sub-DAG once with upstream output."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_split, nsref="t:split")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_upper, nsref="t:upper")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_join, nsref="t:join")  # pyright: ignore[reportPrivateUsage]

    # Sub-DAG: upper a single string
    sub = Dag()
    sub.node(ns.get("t:upper"))

    # Parent: split -> call(upper) -> join
    # split returns list[str], but call passes the whole thing to upper
    # Let's use a simpler pipeline: source -> call(sub_dag) -> sink
    ns.register(this_module._async_double_str, nsref="t:double")  # pyright: ignore[reportPrivateUsage]

    sub2 = Dag()
    sub2.node(ns.get("t:double"))

    ns.register(this_module._async_make_text, nsref="t:make_text")  # pyright: ignore[reportPrivateUsage]

    parent = Dag()
    s = parent.node(ns.get("t:make_text"))
    c = parent.node(sub2, label="doubler")
    s >> c  # pyright: ignore[reportUnusedExpression]

    runner = DagRunner(parent)
    result = await runner.run(dag_nsref="t:call_test")

    assert result.status == "completed"
    assert result.outputs["doubler"] == "hellohello"


async def test_call_node_in_chain():
    """CallNode works in a multi-step chain: source -> call -> sink."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_upper, nsref="t:upper")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_double_str, nsref="t:double")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_make_text, nsref="t:make_text")  # pyright: ignore[reportPrivateUsage]

    # Sub-DAG: upper
    sub = Dag()
    sub.node(ns.get("t:upper"))

    # Parent: make_text -> call(upper) -> double
    parent = Dag()
    s = parent.node(ns.get("t:make_text"))
    c = parent.node(sub, label="shout")
    d = parent.node(ns.get("t:double"))
    s >> c >> d  # pyright: ignore[reportUnusedExpression]

    runner = DagRunner(parent)
    result = await runner.run(dag_nsref="t:chain_test")

    assert result.status == "completed"
    assert result.outputs["_async_double_str"] == "HELLOHELLO"


async def test_call_node_error_propagation():
    """If call sub-DAG fails, parent reports failure."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_fail_str, nsref="t:fail")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_make_text, nsref="t:make_text")  # pyright: ignore[reportPrivateUsage]

    sub = Dag()
    sub.node(ns.get("t:fail"))

    parent = Dag()
    s = parent.node(ns.get("t:make_text"))
    c = parent.node(sub, label="broken")
    s >> c  # pyright: ignore[reportUnusedExpression]

    runner = DagRunner(parent)
    result = await runner.run(dag_nsref="t:fail_call")

    assert result.status == "failed"
    assert result.failed_node == "broken"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dag_runner.py::test_call_node_execution tests/test_dag_runner.py::test_call_node_in_chain tests/test_dag_runner.py::test_call_node_error_propagation -v`
Expected: FAIL — runner doesn't handle `CallNode` yet.

- [ ] **Step 3: Refactor runner with shared helpers and add CallNode handling**

In `src/lythonic/compose/dag_runner.py`, update the import to include `CallNode` and `CompositeDagNode`:

```python
from lythonic.compose.namespace import CallNode, CompositeDagNode, Dag, DagContext, DagNode, MapNode
```

Add shared helper methods to `DagRunner` (after `_execute_map_node`):

```python
    def _get_upstream_output(
        self,
        composite_node: CompositeDagNode,
        node_outputs: dict[str, Any],
    ) -> Any:
        """Get the single upstream output for a composite node."""
        upstream_edges = [e for e in self.dag.edges if e.downstream == composite_node.label]
        if len(upstream_edges) != 1:
            raise ValueError(
                f"CompositeDagNode '{composite_node.label}' must have exactly one upstream edge, "
                f"found {len(upstream_edges)}"
            )
        return node_outputs[upstream_edges[0].upstream]

    async def _run_sub_dag(
        self,
        composite_node: CompositeDagNode,
        element: Any,
        key: str,
        dag_nsref: str,
    ) -> Any:
        """Run a sub-DAG once with a single element as input."""
        sub_sources = composite_node.sub_dag.sources()
        sub_sinks = composite_node.sub_dag.sinks()
        source_label = sub_sources[0].label
        sink_label = sub_sinks[0].label

        sub_runner = DagRunner(composite_node.sub_dag, provenance=self.provenance)
        iter_nsref = f"{dag_nsref}/{composite_node.label}[{key}]"
        sub_result = await sub_runner.run(
            source_inputs={
                source_label: _single_element_inputs(
                    composite_node.sub_dag, source_label, element
                )
            },
            dag_nsref=iter_nsref,
        )
        if sub_result.status != "completed":
            raise RuntimeError(f"Sub-DAG [{key}] failed: {sub_result.error}")
        return sub_result.outputs[sink_label]
```

Refactor `_execute_map_node` to use the shared helpers:

```python
    async def _execute_map_node(
        self,
        map_node: MapNode,
        dag_nsref: str,
        node_outputs: dict[str, Any],
    ) -> Any:
        """Execute a MapNode by running sub-DAG on each collection element."""
        collection = self._get_upstream_output(map_node, node_outputs)

        if isinstance(collection, list):
            items: list[tuple[str, Any]] = [
                (str(i), v) for i, v in enumerate(cast(list[Any], collection))
            ]
            is_dict = False
        elif isinstance(collection, dict):
            items = [(str(k), v) for k, v in cast(dict[str, Any], collection).items()]
            is_dict = True
        else:
            raise TypeError(
                f"MapNode '{map_node.label}' received {type(collection).__name__}, "
                f"expected list or dict"
            )

        results = await asyncio.gather(
            *(self._run_sub_dag(map_node, v, k, dag_nsref) for k, v in items)
        )

        if is_dict:
            return {items[i][0]: results[i] for i in range(len(items))}
        return list(results)
```

Add `_execute_call_node`:

```python
    async def _execute_call_node(
        self,
        call_node: CallNode,
        dag_nsref: str,
        node_outputs: dict[str, Any],
    ) -> Any:
        """Execute a CallNode by running sub-DAG once with upstream output."""
        upstream = self._get_upstream_output(call_node, node_outputs)
        return await self._run_sub_dag(call_node, upstream, "call", dag_nsref)
```

Update the `_execute()` loop. Change the `isinstance(dag_node, MapNode)` block to handle both composite types:

```python
            if isinstance(dag_node, CompositeDagNode):
                self.provenance.record_node_start(
                    run_id, dag_node.label, json.dumps({"composite": type(dag_node).__name__}, default=str)
                )
                try:
                    if isinstance(dag_node, MapNode):
                        result = await self._execute_map_node(
                            dag_node, dag_nsref, node_outputs
                        )
                    elif isinstance(dag_node, CallNode):
                        result = await self._execute_call_node(
                            dag_node, dag_nsref, node_outputs
                        )
                    else:
                        raise ValueError(f"Unknown composite node type: {type(dag_node)}")
                    node_outputs[dag_node.label] = result
                    self.provenance.record_node_complete(
                        run_id, dag_node.label, json.dumps(result, default=str)
                    )
                except Exception as e:
                    self.provenance.record_node_failed(run_id, dag_node.label, str(e))
                    self.provenance.finish_run(run_id, "failed")
                    return DagRunResult(
                        run_id=run_id,
                        status="failed",
                        outputs={lb: node_outputs[lb] for lb in sink_labels if lb in node_outputs},
                        failed_node=dag_node.label,
                        error=str(e),
                    )

                if self._pause_requested:
                    self.provenance.update_run_status(run_id, "paused")
                    return DagRunResult(
                        run_id=run_id,
                        status="paused",
                        outputs={lb: node_outputs[lb] for lb in sink_labels if lb in node_outputs},
                    )
                continue
```

Also update `_wire_inputs` to handle `CompositeDagNode` instead of just `MapNode`:

Change the MapNode-specific wiring (lines 361-370) from:

```python
        map_edges = [
            e
            for e in upstream_edges
            if isinstance(self.dag.nodes[e.upstream], MapNode) and e.upstream in node_outputs
        ]
```

to:

```python
        composite_edges = [
            e
            for e in upstream_edges
            if isinstance(self.dag.nodes[e.upstream], CompositeDagNode) and e.upstream in node_outputs
        ]
        if composite_edges and args:
            for edge, arg in zip(composite_edges, args, strict=False):
                kwargs[arg.name] = node_outputs[edge.upstream]
            return kwargs
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dag_runner.py -k "call_node" -v`
Expected: All PASS.

- [ ] **Step 5: Run full test suite and lint**

Run: `make lint && make test`
Expected: All pass (existing map tests, word count, etc. unchanged).

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/dag_runner.py tests/test_dag_runner.py
git commit -m "feat: add CallNode execution and refactor runner with shared composite helpers"
```
