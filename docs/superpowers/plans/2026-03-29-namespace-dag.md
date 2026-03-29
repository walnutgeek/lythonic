# Namespace + DAG Definition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract Namespace into a standalone module with callable metadata via Method, hierarchical registry, and DAG composition support.

**Architecture:** New `compose/namespace.py` contains Namespace (registry), NamespaceNode (Method wrapper), DagContext (injectable context model), DagNode/DagEdge/Dag (graph builder). cached.py is refactored to use the new Namespace. Path scheme uses GlobalRef syntax `"branch.sub:leaf"`.

**Tech Stack:** Python 3.11+, Pydantic, pytest

**Spec:** `docs/superpowers/specs/2026-03-29-namespace-dag-design.md`

**Deferred to Spec B:** Registering a `Dag` into a `Namespace` as a callable
(requires synthesizing a Method from DAG source inputs and sink outputs, plus
the execution engine). `Namespace.register()` currently only accepts callables
and GlobalRef strings.

---

### Task 1: Scaffold namespace.py with DagContext, _parse_nsref, and NamespaceNode

**Files:**
- Create: `src/lythonic/compose/namespace.py`
- Create: `tests/test_namespace.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_namespace.py`:

```python
from __future__ import annotations

from typing import Any

import tests.test_namespace as this_module


def test_parse_nsref_with_branch_and_leaf():
    from lythonic.compose.namespace import _parse_nsref

    assert _parse_nsref("market:fetch_prices") == (["market"], "fetch_prices")
    assert _parse_nsref("market.data:fetch_prices") == (["market", "data"], "fetch_prices")
    assert _parse_nsref("fetch_prices") == ([], "fetch_prices")
    assert _parse_nsref(":fetch_prices") == ([], "fetch_prices")


def test_dag_context_fields():
    from lythonic.compose.namespace import DagContext

    ctx = DagContext(dag_nsref="pipelines:daily", node_label="fetch", run_id="abc123")
    assert ctx.dag_nsref == "pipelines:daily"
    assert ctx.node_label == "fetch"
    assert ctx.run_id == "abc123"


def _sample_fn(ticker: str, limit: int = 10) -> dict[str, Any]:
    """Fetch some data."""
    return {"ticker": ticker, "limit": limit}


def _ctx_fn(ctx: "DagContext", value: float) -> float:  # pyright: ignore[reportUnusedParameter]
    """A DAG-aware function."""
    return value * 2


def test_namespace_node_callable():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(this_module._sample_fn)
    ns = Namespace()
    node = NamespaceNode(method=method, nsref="test:sample_fn", namespace=ns)

    result = node(ticker="AAPL")
    assert result == {"ticker": "AAPL", "limit": 10}


def test_namespace_node_metadata():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(this_module._sample_fn)
    ns = Namespace()
    node = NamespaceNode(method=method, nsref="test:sample_fn", namespace=ns)

    assert node.nsref == "test:sample_fn"
    assert len(node.method.args) == 2
    assert node.method.args[0].name == "ticker"
    assert node.method.doc == "Fetch some data."


def test_namespace_node_decorated():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(this_module._sample_fn)
    ns = Namespace()
    node = NamespaceNode(
        method=method,
        nsref="test:sample_fn",
        namespace=ns,
        decorated=lambda **kw: {"decorated": True},
    )

    result = node(ticker="AAPL")
    assert result == {"decorated": True}
    # Metadata still reflects the original
    assert node.method.args[0].name == "ticker"


def test_namespace_node_expects_dag_context():
    from lythonic.compose import Method
    from lythonic.compose.namespace import DagContext, Namespace, NamespaceNode

    ns = Namespace()

    plain_node = NamespaceNode(method=Method(this_module._sample_fn), nsref="t:a", namespace=ns)
    assert not plain_node.expects_dag_context()
    assert plain_node.dag_context_type() is None

    ctx_node = NamespaceNode(method=Method(this_module._ctx_fn), nsref="t:b", namespace=ns)
    assert ctx_node.expects_dag_context()
    assert ctx_node.dag_context_type() is DagContext
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

Create `src/lythonic/compose/namespace.py`:

```python
"""
Namespace: Hierarchical registry of callables with metadata and DAG composition.

Provides `Namespace` for registering callables wrapped in `NamespaceNode`
(which wraps `Method`), using GlobalRef-style paths (`"branch.sub:leaf"`).
Includes `Dag` for composing callables into directed acyclic graphs.

## Path Scheme

- `.` separates namespace levels (branches)
- `:` separates the leaf callable from its namespace
- Example: `"market.data:fetch_prices"` -> branch `market.data`, leaf `fetch_prices`

## Usage

```python
from lythonic.compose.namespace import Namespace

ns = Namespace()
ns.register(fetch_prices, nsref="market:fetch_prices")
node = ns.get("market:fetch_prices")
result = node(ticker="AAPL")
```
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel

from lythonic.compose import Method


class DagContext(BaseModel):
    """
    Base context injected into DAG-participating callables.
    Subclass to add domain-specific fields.
    """

    dag_nsref: str
    node_label: str
    run_id: str


def _parse_nsref(nsref: str) -> tuple[list[str], str]:
    """
    Parse nsref into (branch_parts, leaf_name).

    `"market.data:fetch_prices"` -> `(["market", "data"], "fetch_prices")`
    `"market:fetch_prices"` -> `(["market"], "fetch_prices")`
    `"fetch_prices"` -> `([], "fetch_prices")`
    """
    if ":" in nsref:
        branch_path, leaf_name = nsref.rsplit(":", 1)
        branch_parts = branch_path.split(".") if branch_path else []
    else:
        branch_parts = []
        leaf_name = nsref
    return branch_parts, leaf_name


class NamespaceNode:
    """
    Wraps a `Method` with namespace identity. Callable — delegates to the
    decorated callable if present, otherwise to `method.o`.
    """

    method: Method
    nsref: str
    namespace: Namespace
    _decorated: Callable[..., Any] | None

    def __init__(
        self,
        method: Method,
        nsref: str,
        namespace: Namespace,
        decorated: Callable[..., Any] | None = None,
    ) -> None:
        self.method = method
        self.nsref = nsref
        self.namespace = namespace
        self._decorated = decorated

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._decorated is not None:
            return self._decorated(*args, **kwargs)
        return self.method(*args, **kwargs)

    def expects_dag_context(self) -> bool:
        """True if first parameter is `DagContext` or a subclass."""
        if not self.method.args:
            return False
        ann = self.method.args[0].annotation
        return ann is not None and isinstance(ann, type) and issubclass(ann, DagContext)

    def dag_context_type(self) -> type[DagContext] | None:
        """Return the `DagContext` subclass expected, or `None`."""
        if self.expects_dag_context():
            return self.method.args[0].annotation  # pyright: ignore
        return None


class Namespace:
    """
    Hierarchical registry of callables wrapped in `NamespaceNode`.
    Uses GlobalRef-style paths: `"namespace.sub:callable_name"`.
    """

    _branches: dict[str, Namespace]
    _leaves: dict[str, NamespaceNode]

    def __init__(self) -> None:
        self._branches = {}
        self._leaves = {}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace.py -v`
Expected: PASS

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No errors (some warnings about unused functions are OK if temporary)

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace.py
git commit -m "feat(namespace): add DagContext, NamespaceNode, and _parse_nsref"
```

---

### Task 2: Namespace — register and get

**Files:**
- Modify: `src/lythonic/compose/namespace.py`
- Modify: `tests/test_namespace.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_namespace.py`:

```python
def test_register_by_callable():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    node = ns.register(this_module._sample_fn, nsref="test:sample_fn")
    assert node.nsref == "test:sample_fn"
    assert node(ticker="X") == {"ticker": "X", "limit": 10}


def test_register_by_string_gref():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    node = ns.register("tests.test_namespace:_sample_fn", nsref="test:sample")
    assert node(ticker="Y") == {"ticker": "Y", "limit": 10}


def test_register_derives_nsref():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    node = ns.register(this_module._sample_fn)
    assert node.nsref == "tests.test_namespace:_sample_fn"


def test_get_retrieves_node():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market:fetch")
    node = ns.get("market:fetch")
    assert node.nsref == "market:fetch"
    assert node(ticker="Z") == {"ticker": "Z", "limit": 10}


def test_get_nested_branch():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market.data:fetch")
    node = ns.get("market.data:fetch")
    assert node.nsref == "market.data:fetch"


def test_get_root_level():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="fetch")
    node = ns.get("fetch")
    assert node.nsref == "fetch"


def test_get_missing_raises_key_error():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    try:
        ns.get("nonexistent:thing")
        raise AssertionError("Expected KeyError")
    except KeyError:
        pass
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py::test_register_by_callable tests/test_namespace.py::test_get_retrieves_node -v`
Expected: FAIL with `AttributeError: 'Namespace' object has no attribute 'register'`

- [ ] **Step 3: Write minimal implementation**

Add to the `Namespace` class in `src/lythonic/compose/namespace.py`:

```python
    def _get_or_create_branch(self, parts: list[str]) -> Namespace:
        """Navigate to a branch, creating intermediate branches as needed."""
        current = self
        for part in parts:
            if part in current._leaves:
                raise ValueError(
                    f"Cannot create branch '{part}': a leaf with that name already exists"
                )
            if part not in current._branches:
                current._branches[part] = Namespace()
            current = current._branches[part]
        return current

    def _get_branch(self, parts: list[str]) -> Namespace:
        """Navigate to an existing branch. Raises `KeyError` if not found."""
        current = self
        for part in parts:
            if part not in current._branches:
                raise KeyError(f"Branch '{part}' not found")
            current = current._branches[part]
        return current

    def register(
        self,
        c: Callable[..., Any] | str,
        nsref: str | None = None,
        decorate: Callable[[Callable[..., Any]], Callable[..., Any]] | None = None,
    ) -> NamespaceNode:
        """
        Register a callable. If `nsref` is `None`, derive from the callable's
        module and name. If `decorate` is provided, wrap the callable for
        invocation (metadata is still extracted from the original).
        """
        from lythonic import GlobalRef

        if isinstance(c, str):
            gref = GlobalRef(c)
        elif isinstance(c, GlobalRef):
            gref = GlobalRef(c)
        else:
            gref = GlobalRef(c)

        if nsref is None:
            nsref = f"{gref.module}:{gref.name}"

        method = Method(gref)
        decorated = decorate(method.o) if decorate else None

        branch_parts, leaf_name = _parse_nsref(nsref)
        branch = self._get_or_create_branch(branch_parts)

        if leaf_name in branch._leaves:
            raise ValueError(f"Leaf '{leaf_name}' already exists in namespace")
        if leaf_name in branch._branches:
            raise ValueError(
                f"Cannot create leaf '{leaf_name}': a branch with that name already exists"
            )

        node = NamespaceNode(method=method, nsref=nsref, namespace=self, decorated=decorated)
        branch._leaves[leaf_name] = node
        return node

    def get(self, nsref: str) -> NamespaceNode:
        """Retrieve a node by nsref. Raises `KeyError` if not found."""
        branch_parts, leaf_name = _parse_nsref(nsref)
        branch = self._get_branch(branch_parts)
        if leaf_name not in branch._leaves:
            raise KeyError(f"Leaf '{leaf_name}' not found")
        return branch._leaves[leaf_name]
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
git commit -m "feat(namespace): add Namespace.register() and get()"
```

---

### Task 3: Namespace — register_all, __getattr__, decorate, error cases

**Files:**
- Modify: `src/lythonic/compose/namespace.py`
- Modify: `tests/test_namespace.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_namespace.py`:

```python
def _another_fn(code: str) -> str:  # pyright: ignore[reportUnusedParameter]
    return code.upper()


def test_register_all():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    nodes = ns.register_all(this_module._sample_fn, this_module._another_fn)
    assert len(nodes) == 2
    assert ns.get("tests.test_namespace:_sample_fn") is not None
    assert ns.get("tests.test_namespace:_another_fn") is not None


def test_register_with_decorate():
    from lythonic.compose.namespace import Namespace

    def uppercase_decorator(fn: Any) -> Any:  # pyright: ignore[reportUnusedParameter]
        def wrapper(**kwargs: Any) -> dict[str, Any]:
            return {"decorated": True}
        return wrapper

    ns = Namespace()
    node = ns.register(this_module._sample_fn, nsref="test:sample", decorate=uppercase_decorator)
    result = node(ticker="X")
    assert result == {"decorated": True}
    # Metadata still from original
    assert node.method.args[0].name == "ticker"


def test_getattr_leaf():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market:fetch")
    node = ns.market.fetch  # pyright: ignore
    assert node.nsref == "market:fetch"
    assert node(ticker="A") == {"ticker": "A", "limit": 10}


def test_getattr_nested():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market.data:fetch")
    node = ns.market.data.fetch  # pyright: ignore
    assert node.nsref == "market.data:fetch"


def test_getattr_root_level():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="_my_download")
    node = ns._my_download  # pyright: ignore
    assert node.nsref == "_my_download"


def test_getattr_missing_raises():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    try:
        ns.nonexistent  # pyright: ignore
        raise AssertionError("Expected AttributeError")
    except AttributeError:
        pass


def test_register_duplicate_leaf_raises():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="test:fetch")
    try:
        ns.register(this_module._another_fn, nsref="test:fetch")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "already exists" in str(e)


def test_register_leaf_on_branch_raises():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market.data:fetch")
    # "market" is now a branch, can't register a leaf with that name at root
    try:
        ns.register(this_module._another_fn, nsref="market")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "branch" in str(e).lower() or "already exists" in str(e)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py::test_register_all tests/test_namespace.py::test_getattr_leaf -v`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

Add to the `Namespace` class in `src/lythonic/compose/namespace.py`:

```python
    def register_all(
        self,
        *cc: Callable[..., Any],
        decorate: Callable[[Callable[..., Any]], Callable[..., Any]] | None = None,
    ) -> list[NamespaceNode]:
        """Bulk register callables using derived paths."""
        return [self.register(c, decorate=decorate) for c in cc]

    def __getattr__(self, name: str) -> Any:
        # Avoid recursion for internal attributes
        if name in ("_branches", "_leaves"):
            raise AttributeError(name)
        if name in self._branches:
            return self._branches[name]
        if name in self._leaves:
            return self._leaves[name]
        raise AttributeError(f"'{name}' not found in namespace")
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
git commit -m "feat(namespace): add register_all, __getattr__, and error handling"
```

---

### Task 4: DagEdge, DagNode, and >> operator

**Files:**
- Modify: `src/lythonic/compose/namespace.py`
- Modify: `tests/test_namespace.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_namespace.py`:

```python
def test_dag_edge_fields():
    from lythonic.compose.namespace import DagEdge

    edge = DagEdge(upstream="fetch", downstream="compute")
    assert edge.upstream == "fetch"
    assert edge.downstream == "compute"


def test_dag_node_rshift_creates_edge():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="test:fetch")
    ns.register(this_module._another_fn, nsref="test:compute")

    dag = Dag()
    f = dag.node(ns.get("test:fetch"))
    c = dag.node(ns.get("test:compute"))

    result = f >> c
    assert result is c
    assert len(dag.edges) == 1
    assert dag.edges[0].upstream == "fetch"
    assert dag.edges[0].downstream == "compute"


def test_dag_node_rshift_chaining():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:step1")
    ns.register(this_module._another_fn, nsref="a:step2")
    ns.register(this_module._sample_fn, nsref="a:step3")

    dag = Dag()
    s1 = dag.node(ns.get("a:step1"))
    s2 = dag.node(ns.get("a:step2"))
    s3 = dag.node(ns.get("a:step3"))

    s1 >> s2 >> s3

    assert len(dag.edges) == 2
    assert dag.edges[0].upstream == "step1"
    assert dag.edges[0].downstream == "step2"
    assert dag.edges[1].upstream == "step2"
    assert dag.edges[1].downstream == "step3"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py::test_dag_edge_fields tests/test_namespace.py::test_dag_node_rshift_creates_edge -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write minimal implementation**

Add to `src/lythonic/compose/namespace.py` after the `Namespace` class:

```python
class DagEdge(BaseModel):
    """Edge between two `DagNode`s in a `Dag`."""

    upstream: str
    downstream: str


class DagNode:
    """
    Unique node in a `Dag`, wrapping a `NamespaceNode`.
    Same callable can appear multiple times with different labels.
    """

    ns_node: NamespaceNode
    label: str
    dag: Dag

    def __init__(self, ns_node: NamespaceNode, label: str, dag: Dag) -> None:
        self.ns_node = ns_node
        self.label = label
        self.dag = dag

    def __rshift__(self, other: DagNode) -> DagNode:
        """Register edge from self to other, return other for chaining."""
        self.dag.add_edge(self, other)
        return other


class Dag:
    """
    Directed acyclic graph of `DagNode`s with type-based validation.
    Use `node()` to add nodes and `>>` to declare edges.
    """

    nodes: dict[str, DagNode]
    edges: list[DagEdge]

    def __init__(self) -> None:
        self.nodes = {}
        self.edges = []

    def node(
        self,
        source: NamespaceNode | Callable[..., Any],
        label: str | None = None,
    ) -> DagNode:
        """
        Create a unique `DagNode` in this graph. If `label` is `None`,
        derived from the source's nsref leaf name. Raises `ValueError`
        if the label already exists.
        """
        if isinstance(source, NamespaceNode):
            ns_node = source
        else:
            method = Method(source)
            from lythonic import GlobalRef

            gref = GlobalRef(source)
            ns_node = NamespaceNode(
                method=method, nsref=f"{gref.module}:{gref.name}", namespace=Namespace()
            )

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

    def add_edge(self, upstream: DagNode, downstream: DagNode) -> DagEdge:
        """Register a directed edge between two nodes."""
        edge = DagEdge(upstream=upstream.label, downstream=downstream.label)
        self.edges.append(edge)
        return edge
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
git commit -m "feat(namespace): add DagEdge, DagNode with >> operator, and Dag basics"
```

---

### Task 5: Dag — fan-out/fan-in, duplicate labels, callable source

**Files:**
- Modify: `tests/test_namespace.py`

- [ ] **Step 1: Write the tests**

Add to `tests/test_namespace.py`:

```python
def test_dag_fan_out_fan_in():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")
    ns.register(this_module._another_fn, nsref="a:report")
    ns.register(this_module._sample_fn, nsref="a:archive")
    ns.register(this_module._another_fn, nsref="a:summarize")

    dag = Dag()
    f = dag.node(ns.get("a:fetch"))
    r = dag.node(ns.get("a:report"))
    a = dag.node(ns.get("a:archive"))
    s = dag.node(ns.get("a:summarize"))

    f >> r >> s    # linear
    f >> a >> s    # fan-out from f, fan-in to s

    assert len(dag.edges) == 4
    upstream_of_s = [e.upstream for e in dag.edges if e.downstream == "summarize"]
    assert sorted(upstream_of_s) == ["archive", "report"]
    downstream_of_f = [e.downstream for e in dag.edges if e.upstream == "fetch"]
    assert sorted(downstream_of_f) == ["archive", "report"]


def test_dag_same_callable_different_labels():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market:fetch")

    dag = Dag()
    f1 = dag.node(ns.get("market:fetch"), label="fetch_us")
    f2 = dag.node(ns.get("market:fetch"), label="fetch_eu")

    assert f1.label == "fetch_us"
    assert f2.label == "fetch_eu"
    assert len(dag.nodes) == 2
    # Both wrap the same NamespaceNode
    assert f1.ns_node is f2.ns_node


def test_dag_duplicate_auto_label_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market:fetch")

    dag = Dag()
    dag.node(ns.get("market:fetch"))
    try:
        dag.node(ns.get("market:fetch"))
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "already exists" in str(e)


def test_dag_node_from_callable():
    from lythonic.compose.namespace import Dag

    dag = Dag()
    n = dag.node(this_module._sample_fn)
    assert n.label == "_sample_fn"
    assert n.ns_node.method.doc == "Fetch some data."
```

- [ ] **Step 2: Run tests**

Run: `uv run pytest tests/test_namespace.py::test_dag_fan_out_fan_in tests/test_namespace.py::test_dag_same_callable_different_labels tests/test_namespace.py::test_dag_duplicate_auto_label_raises tests/test_namespace.py::test_dag_node_from_callable -v`
Expected: All PASS (implementation already supports these)

- [ ] **Step 3: Commit**

```bash
git add tests/test_namespace.py
git commit -m "test(namespace): add DAG fan-out/fan-in, duplicate label, and callable source tests"
```

---

### Task 6: Dag — validate (cycle detection + type compatibility)

**Files:**
- Modify: `src/lythonic/compose/namespace.py`
- Modify: `tests/test_namespace.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_namespace.py`:

```python
def test_dag_validate_passes_for_valid_dag():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:step1")
    ns.register(this_module._another_fn, nsref="a:step2")

    dag = Dag()
    s1 = dag.node(ns.get("a:step1"))
    s2 = dag.node(ns.get("a:step2"))
    s1 >> s2
    dag.validate()


def test_dag_validate_detects_cycle():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:n1")
    ns.register(this_module._another_fn, nsref="a:n2")
    ns.register(this_module._sample_fn, nsref="a:n3")

    dag = Dag()
    n1 = dag.node(ns.get("a:n1"))
    n2 = dag.node(ns.get("a:n2"))
    n3 = dag.node(ns.get("a:n3"))

    n1 >> n2 >> n3
    # Create a cycle
    dag.add_edge(n3, n1)

    try:
        dag.validate()
        raise AssertionError("Expected ValueError for cycle")
    except ValueError as e:
        assert "cycle" in str(e).lower()


def _typed_source() -> float:
    return 1.0


def _typed_sink(value: float) -> str:  # pyright: ignore[reportUnusedParameter]
    return "ok"


def _incompatible_sink(data: list[int]) -> str:  # pyright: ignore[reportUnusedParameter]
    return "ok"


def test_dag_validate_type_compatible():
    """Type-compatible edges should pass validation."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._typed_source, nsref="t:source")
    ns.register(this_module._typed_sink, nsref="t:sink")

    dag = Dag()
    src = dag.node(ns.get("t:source"))
    snk = dag.node(ns.get("t:sink"))
    src >> snk
    dag.validate()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py::test_dag_validate_detects_cycle -v`
Expected: FAIL with `AttributeError: 'Dag' object has no attribute 'validate'`

- [ ] **Step 3: Write minimal implementation**

Add to the `Dag` class in `src/lythonic/compose/namespace.py`:

```python
    def validate(self) -> None:
        """Check acyclicity and type compatibility between connected nodes."""
        self._check_acyclicity()
        self._check_type_compatibility()

    def _check_acyclicity(self) -> None:
        """Kahn's algorithm for topological sort — raises if cycle detected."""
        in_degree: dict[str, int] = {label: 0 for label in self.nodes}
        adj: dict[str, list[str]] = {label: [] for label in self.nodes}
        for edge in self.edges:
            adj[edge.upstream].append(edge.downstream)
            in_degree[edge.downstream] += 1

        queue = [label for label, deg in in_degree.items() if deg == 0]
        visited = 0
        while queue:
            current = queue.pop(0)
            visited += 1
            for neighbor in adj[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if visited != len(self.nodes):
            raise ValueError("DAG contains a cycle")

    def _check_type_compatibility(self) -> None:
        """Warn-level check: upstream return type should match a downstream input type."""
        import inspect as _inspect

        for edge in self.edges:
            upstream_node = self.nodes[edge.upstream]
            downstream_node = self.nodes[edge.downstream]

            upstream_return = upstream_node.ns_node.method.return_annotation
            if upstream_return is None or upstream_return is _inspect.Parameter.empty:
                continue

            downstream_args = downstream_node.ns_node.method.args
            if downstream_node.ns_node.expects_dag_context() and len(downstream_args) > 0:
                downstream_args = downstream_args[1:]

            if not downstream_args:
                continue

            downstream_types = {
                arg.annotation for arg in downstream_args if arg.annotation is not None
            }
            if downstream_types and upstream_return not in downstream_types:
                import logging

                logging.getLogger(__name__).warning(
                    "Type mismatch on edge %s -> %s: upstream returns %s, "
                    "downstream accepts %s",
                    edge.upstream,
                    edge.downstream,
                    upstream_return,
                    downstream_types,
                )
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
git commit -m "feat(namespace): add Dag.validate() with cycle detection and type checking"
```

---

### Task 7: Dag — introspection and context manager

**Files:**
- Modify: `src/lythonic/compose/namespace.py`
- Modify: `tests/test_namespace.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_namespace.py`:

```python
def test_dag_topological_order():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")
    ns.register(this_module._another_fn, nsref="a:compute")
    ns.register(this_module._sample_fn, nsref="a:report")

    dag = Dag()
    f = dag.node(ns.get("a:fetch"))
    c = dag.node(ns.get("a:compute"))
    r = dag.node(ns.get("a:report"))
    f >> c >> r

    order = dag.topological_order()
    labels = [n.label for n in order]
    assert labels.index("fetch") < labels.index("compute")
    assert labels.index("compute") < labels.index("report")


def test_dag_sources_and_sinks():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")
    ns.register(this_module._another_fn, nsref="a:compute")
    ns.register(this_module._sample_fn, nsref="a:report")

    dag = Dag()
    f = dag.node(ns.get("a:fetch"))
    c = dag.node(ns.get("a:compute"))
    r = dag.node(ns.get("a:report"))
    f >> c >> r

    source_labels = sorted([n.label for n in dag.sources()])
    sink_labels = sorted([n.label for n in dag.sinks()])
    assert source_labels == ["fetch"]
    assert sink_labels == ["report"]


def test_dag_sources_sinks_fan_out():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")
    ns.register(this_module._another_fn, nsref="a:report")
    ns.register(this_module._sample_fn, nsref="a:archive")

    dag = Dag()
    f = dag.node(ns.get("a:fetch"))
    r = dag.node(ns.get("a:report"))
    a = dag.node(ns.get("a:archive"))
    f >> r
    f >> a

    assert len(dag.sources()) == 1
    sink_labels = sorted([n.label for n in dag.sinks()])
    assert sink_labels == ["archive", "report"]


def test_dag_context_manager_validates():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:step1")
    ns.register(this_module._another_fn, nsref="a:step2")

    with Dag() as dag:
        s1 = dag.node(ns.get("a:step1"))
        s2 = dag.node(ns.get("a:step2"))
        s1 >> s2
    # No error — validation passed


def test_dag_context_manager_catches_cycle():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:n1")
    ns.register(this_module._another_fn, nsref="a:n2")

    try:
        with Dag() as dag:
            n1 = dag.node(ns.get("a:n1"))
            n2 = dag.node(ns.get("a:n2"))
            n1 >> n2
            dag.add_edge(n2, n1)
        raise AssertionError("Expected ValueError for cycle")
    except ValueError as e:
        assert "cycle" in str(e).lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py::test_dag_topological_order tests/test_namespace.py::test_dag_context_manager_validates -v`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

Add to the `Dag` class in `src/lythonic/compose/namespace.py`:

```python
    def topological_order(self) -> list[DagNode]:
        """Return nodes in topological order. Stable sort by label for determinism."""
        in_degree: dict[str, int] = {label: 0 for label in self.nodes}
        adj: dict[str, list[str]] = {label: [] for label in self.nodes}
        for edge in self.edges:
            adj[edge.upstream].append(edge.downstream)
            in_degree[edge.downstream] += 1

        queue = sorted([label for label, deg in in_degree.items() if deg == 0])
        result: list[DagNode] = []
        while queue:
            current = queue.pop(0)
            result.append(self.nodes[current])
            for neighbor in sorted(adj[current]):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return result

    def sources(self) -> list[DagNode]:
        """Return nodes with no upstream edges."""
        has_upstream = {edge.downstream for edge in self.edges}
        return [self.nodes[label] for label in self.nodes if label not in has_upstream]

    def sinks(self) -> list[DagNode]:
        """Return nodes with no downstream edges."""
        has_downstream = {edge.upstream for edge in self.edges}
        return [self.nodes[label] for label in self.nodes if label not in has_downstream]

    def __enter__(self) -> Dag:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            self.validate()
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
git commit -m "feat(namespace): add Dag introspection and context manager"
```

---

### Task 8: Refactor cached.py to use new Namespace

**Files:**
- Modify: `src/lythonic/compose/cached.py`
- Test: `tests/test_cached.py` (must all pass unchanged)

- [ ] **Step 1: Run existing cached tests to confirm baseline**

Run: `uv run pytest tests/test_cached.py -v`
Expected: All PASS

- [ ] **Step 2: Refactor cached.py**

In `src/lythonic/compose/cached.py`, make these changes:

**Remove** the old `Namespace` class (lines 176-190 in current file):

```python
# DELETE this entire class:
class Namespace:
    """..."""
    def install(self, path: str, func: Callable[..., Any]) -> None:
        ...
```

**Add import** near the top, after the existing imports:

```python
from lythonic.compose.namespace import Namespace
```

**Add helper function** after `table_name_from_path`:

```python
def _namespace_path_to_nsref(path: str) -> str:
    """
    Convert old dot-separated namespace path to nsref format.
    `"market.fetch_prices"` -> `"market:fetch_prices"`
    `"fetch_prices"` -> `"fetch_prices"`
    """
    parts = path.rsplit(".", 1)
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0]}:{parts[1]}"
```

**Update `CacheRegistry._register_rule`** — replace the wrapper installation at the end of the method. Change from:

```python
        if gref.is_async():
            wrapper = _build_async_wrapper(
                method, tbl_name, self.db_path, min_ttl_s, max_ttl_s, namespace_path
            )
        else:
            wrapper = _build_sync_wrapper(
                method, tbl_name, self.db_path, min_ttl_s, max_ttl_s, namespace_path
            )

        self.cached.install(namespace_path, wrapper)
```

To:

```python
        nsref = _namespace_path_to_nsref(namespace_path)

        if gref.is_async():
            wrapper = _build_async_wrapper(
                method, tbl_name, self.db_path, min_ttl_s, max_ttl_s, namespace_path
            )
        else:
            wrapper = _build_sync_wrapper(
                method, tbl_name, self.db_path, min_ttl_s, max_ttl_s, namespace_path
            )

        node = self.cached.register(gref, nsref=nsref)
        node._decorated = wrapper
```

- [ ] **Step 3: Run cached tests to verify no regressions**

Run: `uv run pytest tests/test_cached.py -v`
Expected: All PASS

- [ ] **Step 4: Run all tests**

Run: `uv run pytest tests/test_cached.py tests/test_namespace.py -v`
Expected: All PASS

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/cached.py
git commit -m "refactor(cached): use new Namespace from compose.namespace"
```

---

### Task 9: Docs and final verification

**Files:**
- Create: `docs/reference/compose-namespace.md`
- Modify: `mkdocs.yml`
- Modify: `docs/reference/compose-cached.md`

- [ ] **Step 1: Create reference page**

Create `docs/reference/compose-namespace.md`:

```markdown
# lythonic.compose.namespace

Hierarchical registry of callables with metadata and DAG composition.

::: lythonic.compose.namespace
    options:
      show_root_heading: false
      members:
        - Namespace
        - NamespaceNode
        - DagContext
        - Dag
        - DagNode
        - DagEdge
```

- [ ] **Step 2: Update mkdocs.yml nav**

Add the new reference page to the nav, after `lythonic.compose`:

```yaml
      - lythonic.compose.namespace: reference/compose-namespace.md
```

- [ ] **Step 3: Update compose-cached.md**

Remove `Namespace` from the members list in `docs/reference/compose-cached.md` since it's now in the namespace module:

```markdown
# lythonic.compose.cached

YAML-configured caching layer for download methods.

::: lythonic.compose.cached
    options:
      show_root_heading: false
      members:
        - CacheRegistry
        - CacheConfig
        - CacheRule
        - CacheRefreshPushback
        - CacheRefreshSuppressed
        - generate_cache_table_ddl
        - table_name_from_path
```

- [ ] **Step 4: Run full test suite**

Run: `make lint && make test`
Expected: All pass, zero lint errors

- [ ] **Step 5: Commit**

```bash
git add docs/reference/compose-namespace.md docs/reference/compose-cached.md mkdocs.yml
git commit -m "docs: add compose.namespace reference page, update cached reference"
```
