# Namespace + DAG Definition Design (Spec A)

## Problem

The `Namespace` class in `lythonic.compose.cached` is a minimal 15-line class
that only supports `install()` via dynamic `setattr`. It carries no metadata
about callables, has no introspection, and is tightly coupled to the caching
use case. The project needs a general-purpose hierarchical registry of callables
with rich metadata and the ability to compose callables into DAGs.

## Solution

Extract and redesign `Namespace` as a standalone module in `lythonic.compose`.
The new Namespace is a hierarchical registry of callables wrapped in
`NamespaceNode` (which wraps `Method`), with a DAG builder for composing
callables into directed acyclic graphs.

This is Spec A — covering the registry, metadata, and declarative DAG building.
Spec B (separate) will cover DAG execution, runtime state tracking, and
provenance.

## Path Scheme

Uses GlobalRef syntax: `"namespace.sub:callable_name"`.

- `.` separates namespace levels (branches)
- `:` separates the leaf callable from its namespace
- Example: `"market.data:fetch_prices"` → branch `market.data`, leaf
  `fetch_prices`

When `nsref` is not provided during registration, derived from the callable's
`__module__` and `__qualname__`.

## Core Classes

### Namespace

The top-level registry. Holds a tree of `NamespaceNode` leaf entries and
intermediate branch containers.

```python
class Namespace:
    def register(
        self,
        c: Callable | GlobalRef | str,
        nsref: str | None = None,
        decorate: Callable[[Callable], Callable] | None = None,
    ) -> NamespaceNode:
        """
        Register a callable. If `nsref` is None, derive path from the
        callable's module and name. If `decorate` is provided, wrap the
        callable before creating the Method.
        """

    def register_all(
        self,
        *cc: Callable,
        decorate: Callable[[Callable], Callable] | None = None,
    ) -> list[NamespaceNode]:
        """Bulk register callables using derived paths."""

    def get(self, nsref: str) -> NamespaceNode:
        """Retrieve a node by nsref. Raises KeyError if not found."""

    def __getattr__(self, name: str) -> Any:
        """
        Attribute access for interactive use. Returns a NamespaceNode
        (leaf) or a branch proxy (intermediate namespace). Linter-unfriendly
        by nature; use get() for production code.
        """
```

**Tree rules:**
- Branches are internal containers (keyed by namespace segments before `:`)
- Leaves are `NamespaceNode` instances (keyed by the name after `:`)
- Registering a leaf where a leaf already exists raises `ValueError`
- Registering a leaf where a branch exists at that exact name raises
  `ValueError`

### NamespaceNode

Wraps a `Method` and adds namespace identity plus DAG-building capability.

```python
class NamespaceNode:
    method: Method
    nsref: str
    namespace: Namespace

    def __call__(self, **kwargs: Any) -> Any:
        """Delegate to the wrapped method."""

    def expects_dag_context(self) -> bool:
        """True if first parameter is DagContext or a subclass."""

    def dag_context_type(self) -> type[DagContext] | None:
        """Return the DagContext subclass expected, or None."""
```

`NamespaceNode` is callable, delegating to `method.o`. All Method metadata
is accessible: `node.method.args`, `node.method.doc`,
`node.method.return_annotation`.

## DAG Builder

### DagContext

Base context model for DAG-participating callables.

```python
class DagContext(BaseModel):
    dag_nsref: str     # nsref of the Dag being executed
    node_label: str    # current node's label within the Dag
    run_id: str        # unique identifier for this execution
```

**Convention:** If a callable's first parameter is typed as `DagContext` or a
subclass, the DAG runtime (Spec B) injects it automatically. Callables without
it work normally.

```python
# DAG-aware
def compute(ctx: DagContext, prices: list[float]) -> float:
    return sum(prices) / len(prices)

# Plain — also works in DAG
def add_tax(amount: float) -> float:
    return amount * 1.1
```

For Spec A: we define `DagContext` and the detection helpers
(`expects_dag_context()`, `dag_context_type()`). Actual injection is Spec B.

### DagNode

Unique node within a DAG. Same callable can appear multiple times with
different labels.

```python
class DagNode:
    ns_node: NamespaceNode
    label: str           # unique within the Dag
    dag: Dag             # back-reference

    def __rshift__(self, other: DagNode) -> DagNode:
        """Register edge from self to other, return other for chaining."""
        self.dag.add_edge(self, other)
        return other
```

**Auto-label:** When `label` is None in `dag.node()`, derived from the
node's nsref leaf name (e.g., `"fetch_prices"`). If that label already exists
(same callable used twice), raises `ValueError` — explicit label required.

### DagEdge

```python
class DagEdge(BaseModel):
    upstream: str      # DagNode label
    downstream: str    # DagNode label
    # Future (Spec B): retries, on_error, timeout, etc.
```

### Dag

```python
class Dag:
    nodes: dict[str, DagNode]    # label -> DagNode
    edges: list[DagEdge]

    def node(
        self,
        source: NamespaceNode | Callable | str,
        label: str | None = None,
    ) -> DagNode:
        """
        Create a unique DagNode in this graph. `source` can be:
        - A `NamespaceNode` (from `ns.get()`)
        - A callable (wrapped in a temporary NamespaceNode/Method)
        - A string nsref (looked up from the Dag's associated Namespace)
        Callables not in a Namespace create anonymous nodes.
        """

    def add_edge(self, upstream: DagNode, downstream: DagNode) -> DagEdge:
        """Register a directed edge."""

    def validate(self) -> None:
        """Check acyclicity and type compatibility between connected nodes."""

    def topological_order(self) -> list[DagNode]: ...
    def sources(self) -> list[DagNode]: ...
    def sinks(self) -> list[DagNode]: ...

    # Context manager — validates on exit
    def __enter__(self) -> Dag: ...
    def __exit__(self, *exc: Any) -> None: ...
```

**Type-based validation:** `validate()` checks that for each edge, at least
one output type of the upstream node matches an input type of the downstream
node. This is a compile-time check — actual value routing is Spec B.

**Registerable:** A `Dag` can be registered in a Namespace via
`ns.register(dag, nsref="pipelines:daily_refresh")`. Its synthesized `Method`
has inputs from source nodes and outputs from sink nodes.

### Usage Example

```python
ns = Namespace()
ns.register(fetch_prices, nsref="market:fetch_prices")
ns.register(compute_returns, nsref="analysis:compute_returns")
ns.register(generate_report, nsref="reports:generate")
ns.register(archive_data, nsref="storage:archive")
ns.register(summarize, nsref="reports:summarize")

with Dag() as dag:
    f = dag.node(ns.get("market:fetch_prices"))
    c = dag.node(ns.get("analysis:compute_returns"))
    r = dag.node(ns.get("reports:generate"))
    a = dag.node(ns.get("storage:archive"))
    s = dag.node(ns.get("reports:summarize"))

    f >> c >> r >> s    # linear chain
    c >> a >> s          # fan-out from c, fan-in to s

    # Same callable, different positions
    f_us = dag.node(ns.get("market:fetch_prices"), label="fetch_us")
    f_eu = dag.node(ns.get("market:fetch_prices"), label="fetch_eu")
    m = dag.node(ns.get("analysis:compute_returns"), label="merge")
    f_us >> m
    f_eu >> m

ns.register(dag, nsref="pipelines:daily_refresh")
```

## Refactoring cached.py

The current `Namespace` in `cached.py` is replaced by the new one.

**Changes:**
- Remove old `Namespace` class from `cached.py`
- Import `Namespace`, `NamespaceNode` from `lythonic.compose.namespace`
- `CacheRegistry.cached` becomes the new `Namespace` type
- Old `Namespace.install(path, wrapper)` calls become
  `ns.register(gref, nsref=namespace_path, decorate=cache_wrapper_fn)`
- The `decorate` parameter receives a factory that produces the cache wrapper
  (sync or async) for the given callable
- `NamespaceNode.__call__` invokes the decorated callable, so existing
  `registry.cached.market.fetch_prices(ticker="AAPL")` still works

## File Structure

**New file:** `src/lythonic/compose/namespace.py`
- `Namespace`
- `NamespaceNode`
- `DagNode`
- `DagEdge`
- `Dag`
- `DagContext`

**Modified:** `src/lythonic/compose/cached.py`
- Remove old `Namespace` class
- Import from `lythonic.compose.namespace`
- Refactor `CacheRegistry` to use `register()` with `decorate`

**New tests:** `tests/test_namespace.py`

**Modified docs:** `docs/reference/` — add `compose-namespace.md` reference
page, update `mkdocs.yml` nav

## Testing

Tests in `tests/test_namespace.py`:

1. Register callable by GlobalRef, by function, by string
2. Register with explicit nsref
3. Register with decorate
4. `register_all()` bulk registration
5. `get()` retrieval and attribute access
6. Leaf-on-leaf registration raises `ValueError`
7. Metadata access via `node.method.args`, `.doc`, `.return_annotation`
8. `DagNode` creation and `>>` operator
9. Linear DAG chaining
10. Fan-out / fan-in patterns
11. Same callable with different labels
12. Duplicate auto-label raises `ValueError`
13. `Dag` context manager runs `validate()`
14. Cycle detection in `validate()`
15. Type compatibility check in `validate()`
16. `DagContext` detection on callables
17. `Dag` registered into Namespace
18. Existing `test_cached.py` tests still pass after refactor
