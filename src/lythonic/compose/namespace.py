# pyright: reportImportCycles=false
"""
Namespace: Hierarchical registry of callables with metadata and DAG composition.

Provides `Namespace` for registering callables wrapped in `NamespaceNode`
(which wraps `Method`), using GlobalRef-style paths (`"branch.sub:leaf"`).
Includes `DagContext` as base context for DAG-participating callables.

## Path Scheme

- `.` separates namespace levels (branches)
- `:` separates the leaf callable from its namespace
- Example: `"market.data:fetch_prices"` -> branch `market.data`, leaf `fetch_prices`

## Usage

Each `Dag` owns a `Namespace`. Passing a callable to `dag.node()` auto-registers
it in the DAG's namespace (derived from the callable's module and name):

```python
from lythonic.compose.namespace import Dag

def fetch(url: str) -> dict:
    return {"source": url, "raw": [1, 2, 3]}

def transform(data: dict) -> dict:
    return {"source": data["source"], "values": [v * 2 for v in data["raw"]]}

dag = Dag()
dag.node(fetch) >> dag.node(transform)
```

You can also register callables in a standalone `Namespace` and look them up
by path or dot-access (e.g. `ns.get("market:fetch")` or `ns.market.fetch`).

## Tags

Nodes can be tagged at registration for later querying:

```python
ns.register(fetch_prices, nsref="market:fetch", tags={"slow", "market-data"})
ns.register(compute_stats, nsref="market:stats", tags={"fast", "market-data"})

ns.query("market-data")              # both nodes
ns.query("slow & market-data")       # only fetch
ns.query("~slow")                    # only stats
ns.query("slow | fast")              # both nodes
```

Tag expressions support `&` (AND), `|` (OR), `~` (NOT) with standard
precedence (`~` > `&` > `|`). Tags must not contain spaces or operator
characters.

## Map

`Dag.map(sub_dag, label)` creates a `MapNode` that runs a sub-DAG on each
element of an upstream `list` or `dict`, concurrently via `asyncio.gather`:

```python
sub_dag = Dag()
sub_dag.node(tokenize) >> sub_dag.node(count)

parent = Dag()
parent.node(split_text) >> parent.map(sub_dag, label="chunks") >> parent.node(reduce)
```

The sub-DAG must have exactly one source and one sink.

## Composable DAGs

`dag.node(sub_dag, label="enrich")` creates a `CallNode` that runs a sub-DAG
once as a single step, passing the upstream output to the sub-DAG's source:

```python
enrich_dag = Dag()
enrich_dag.node(lookup) >> enrich_dag.node(annotate)

parent = Dag()
parent.node(fetch) >> parent.node(enrich_dag, label="enrich") >> parent.node(save)
```

## Callable DAGs

A `Dag` is directly callable. `await dag(**kwargs)` creates a `DagRunner`
with `NullProvenance` and returns a `DagRunResult`. Kwargs are matched to
source node parameters by name:

```python
result = await dag(url="https://example.com")
print(result.status)   # "completed"
print(result.outputs)  # sink node outputs
```

## DAG Factories

Use `@dag_factory` to mark a function that builds a DAG. When registered
in a namespace, the factory is called and the resulting DAG is registered
with a `__` suffix:

```python
from lythonic.compose.namespace import dag_factory, Dag

@dag_factory
def my_pipeline() -> Dag:
    dag = Dag()
    dag.node(step1) >> dag.node(step2)
    return dag

ns.register(my_pipeline)  # registers DAG under "module:my_pipeline__"
```
"""

from __future__ import annotations

import inspect as _inspect
import logging
import typing
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from lythonic.compose.dag_runner import DagRunResult  # pyright: ignore[reportImportCycles]

from pydantic import BaseModel

from lythonic import GlobalRef, GRef
from lythonic.compose import Method
from lythonic.compose._inline import inline as inline

_F = TypeVar("_F", bound=Callable[..., Any])


def dag_factory(fn: _F) -> _F:
    """
    Mark a no-arg function as a DAG factory. When registered in a
    namespace, the factory is called and the returned DAG is registered
    with a `__` suffix appended to the name. Also recognized by
    `Dag.node()` and `Dag.map()` which call the factory and auto-derive
    the label from the function name.
    """
    fn._is_dag_factory = True  # pyright: ignore[reportFunctionMemberAccess]
    return fn


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

    >>> _parse_nsref('market.data:fetch_prices')
    (['market', 'data'], 'fetch_prices')
    >>> _parse_nsref('market:fetch_prices')
    (['market'], 'fetch_prices')
    >>> _parse_nsref('fetch_prices')
    ([], 'fetch_prices')
    >>> _parse_nsref(':fetch_prices')
    ([], 'fetch_prices')

    """
    if ":" in nsref:
        branch_path, leaf_name = nsref.rsplit(":", 1)
        branch_parts = branch_path.split(".") if branch_path else []
    else:
        branch_parts = []
        leaf_name = nsref
    return branch_parts, leaf_name


def _resolve_first_param_type(func: Callable[..., Any]) -> type | None:
    """
    Resolve the type annotation of the first parameter, handling
    string annotations from `from __future__ import annotations`.
    """
    # Provide DagContext in localns so string forward references resolve.
    try:
        hints = typing.get_type_hints(func, localns={"DagContext": DagContext})
    except Exception:
        return None
    if not hints:
        return None
    # get_type_hints returns an ordered dict in Python 3.7+;
    # grab the first non-return key
    import inspect

    sig = inspect.signature(func)
    params = list(sig.parameters)
    if not params:
        return None
    first_name = params[0]
    ann = hints.get(first_name)
    if ann is not None and isinstance(ann, type):
        return ann
    return None


_INVALID_TAG_CHARS = frozenset("&|~")


def _validate_tags(tags: frozenset[str] | set[str] | list[str] | None) -> frozenset[str]:
    """
    Validate and normalize tags to `frozenset[str]`. Rejects `str` input
    (common mistake) and tags containing operator characters or whitespace.
    """
    if tags is None:
        return frozenset()
    if isinstance(tags, str):
        raise TypeError("tags must be a collection of strings, not a str")
    result = frozenset(tags)
    for tag in result:
        if any(c in _INVALID_TAG_CHARS for c in tag):
            raise ValueError(f"Tag {tag!r} contains invalid characters (&, |, or ~)")
        if any(c.isspace() for c in tag):
            raise ValueError(f"Tag {tag!r} contains whitespace")
    return result


def _parse_tag_expr(expr: str) -> list[str]:
    """
    Tokenize a tag query expression into a list of tokens. Tokens are
    tag names, `&`, `|`, or `~`. Raises `ValueError` for empty or
    whitespace-only expressions.
    """
    tokens: list[str] = []
    for part in expr.split():
        i = 0
        while i < len(part):
            if part[i] in ("&", "|", "~"):
                tokens.append(part[i])
                i += 1
            else:
                j = i
                while j < len(part) and part[j] not in ("&", "|", "~"):
                    j += 1
                tokens.append(part[i:j])
                i = j
    if not tokens:
        raise ValueError("Tag query expression is empty")
    return tokens


def _eval_tag_expr(tokens: list[str], tags: frozenset[str]) -> bool:
    """
    Evaluate a tokenized tag expression against a set of tags.
    Precedence: ~ (NOT, highest) > & (AND) > | (OR, lowest).
    No parentheses grouping.

    Grammar:
        or_expr  = and_expr ("|" and_expr)*
        and_expr = not_expr ("&" not_expr)*
        not_expr = "~" not_expr | tag
    """
    pos = 0

    def _or_expr() -> bool:
        nonlocal pos
        result = _and_expr()
        while pos < len(tokens) and tokens[pos] == "|":
            pos += 1
            right = _and_expr()
            result = result or right
        return result

    def _and_expr() -> bool:
        nonlocal pos
        result = _not_expr()
        while pos < len(tokens) and tokens[pos] == "&":
            pos += 1
            right = _not_expr()
            result = result and right
        return result

    def _not_expr() -> bool:
        nonlocal pos
        if pos < len(tokens) and tokens[pos] == "~":
            pos += 1
            return not _not_expr()
        if pos >= len(tokens) or tokens[pos] in ("&", "|"):
            raise ValueError(f"Expected tag name at position {pos}")
        tag = tokens[pos]
        pos += 1
        return tag in tags

    result = _or_expr()
    if pos < len(tokens):
        raise ValueError(f"Unexpected token {tokens[pos]!r} at position {pos}")
    return result


class NsNodeConfig(BaseModel):
    """
    Serializable configuration for a namespace node. The `type` field
    acts as a discriminator for Pydantic deserialization — subclasses
    set a literal `type` value so `model_validate` picks the right class.
    Use `GRef` for the `gref` field (serializes as string automatically).
    """

    type: str = "auto"
    nsref: str
    gref: GRef | None = None
    tags: list[str] | None = None


class NsCacheConfig(NsNodeConfig):
    """Cache-wrapped callable configuration."""

    type: str = "cache"  # pyright: ignore[reportIncompatibleVariableOverride]
    min_ttl: float
    max_ttl: float


class NamespaceNode:
    """
    Wraps a `Method` with namespace identity. Callable -- delegates to the
    decorated callable if present, otherwise to `method.o`.
    """

    method: Method
    nsref: str
    namespace: Namespace
    config: NsNodeConfig
    _decorated: Callable[..., Any] | None

    def __init__(
        self,
        method: Method,
        nsref: str,
        namespace: Namespace,
        decorated: Callable[..., Any] | None = None,
        tags: frozenset[str] | set[str] | list[str] | None = None,
        config: NsNodeConfig | None = None,
    ) -> None:
        self.method = method
        self.nsref = nsref
        self.namespace = namespace
        self._decorated = decorated
        self.metadata: dict[str, Any] = {}
        self.tags: frozenset[str] = _validate_tags(tags)
        self.config = config or NsNodeConfig(
            nsref=nsref,
            gref=method.gref if method.gref else None,
            tags=sorted(self.tags) if self.tags else None,
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._decorated is not None:
            return self._decorated(*args, **kwargs)
        return self.method(*args, **kwargs)

    def expects_dag_context(self) -> bool:
        """True if first parameter is `DagContext` or a subclass."""
        return self.dag_context_type() is not None

    def dag_context_type(self) -> type[DagContext] | None:
        """Return the `DagContext` subclass expected, or `None`."""
        resolved = _resolve_first_param_type(self.method.o)
        if resolved is not None and issubclass(resolved, DagContext):
            return resolved
        return None

    def __repr__(self) -> str:  # pyright: ignore[reportImplicitOverride]
        return f"NamespaceNode(nsref={self.nsref!r})"


class Namespace:
    """
    Flat registry of callables wrapped in `NamespaceNode`.
    Uses GlobalRef-style paths as keys: `"module.sub:callable_name"`.
    """

    _nodes: dict[str, NamespaceNode]

    def __init__(self) -> None:
        self._nodes = {}
        self._triggers: dict[str, Any] = {}

    def register(
        self,
        c: Callable[..., Any] | str | Dag,
        nsref: str | None = None,
        decorate: Callable[[Callable[..., Any]], Callable[..., Any]] | None = None,
        tags: frozenset[str] | set[str] | list[str] | None = None,
    ) -> NamespaceNode:
        """
        Register a callable. If `nsref` is `None`, derive from the callable's
        module and name. If the callable is marked with `@dag_factory`,
        call it and register the returned DAG with a `__` suffix. If
        `decorate` is provided, wrap the callable for invocation (metadata
        is still extracted from the original). Optional `tags` are stored
        on the node for querying via `query()`.
        """
        if isinstance(c, Dag):
            return self._register_dag(c, nsref, tags=tags)

        # Handle @dag_factory decorated callables
        if callable(c) and getattr(c, "_is_dag_factory", False):
            gref = GlobalRef(c)
            factory_nsref = nsref or f"{gref.module}:{gref.name}__"
            dag = c()
            if not isinstance(dag, Dag):
                raise TypeError(f"@dag_factory {gref} must return a Dag, got {type(dag).__name__}")
            return self._register_dag(dag, factory_nsref, tags=tags)

        gref = GlobalRef(c)

        if nsref is None:
            nsref = f"{gref.module}:{gref.name}"

        method = Method(gref)
        decorated = decorate(method.o) if decorate else None

        if nsref in self._nodes:
            raise ValueError(f"'{nsref}' already exists in namespace")

        node = NamespaceNode(
            method=method, nsref=nsref, namespace=self, decorated=decorated, tags=tags
        )
        self._nodes[nsref] = node
        return node

    def _register_dag(
        self, dag: Dag, nsref: str | None, tags: frozenset[str] | set[str] | list[str] | None = None
    ) -> NamespaceNode:
        """
        Register a Dag as a callable NamespaceNode. Sets the DAG's
        `parent_namespace` to this namespace for runtime resolution.
        The DAG keeps its own namespace; callables are not copied.
        """
        if nsref is None:
            raise ValueError("nsref is required when registering a Dag")

        if dag.parent_namespace is self:
            raise ValueError(f"Dag '{nsref}' is already registered with this namespace")

        dag.parent_namespace = self

        from lythonic.compose.dag_runner import DagRunner  # pyright: ignore[reportImportCycles]

        runner = DagRunner(dag)

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

        if nsref in self._nodes:
            raise ValueError(f"'{nsref}' already exists in namespace")

        node = NamespaceNode(method=method, nsref=nsref, namespace=self, tags=tags)
        self._nodes[nsref] = node
        return node

    def get(self, nsref: str) -> NamespaceNode:
        """Retrieve a node by nsref. Raises `KeyError` if not found."""
        if nsref not in self._nodes:
            raise KeyError(f"'{nsref}' not found in namespace")
        return self._nodes[nsref]

    def register_all(
        self,
        *cc: Callable[..., Any],
        decorate: Callable[[Callable[..., Any]], Callable[..., Any]] | None = None,
        tags: frozenset[str] | set[str] | list[str] | None = None,
    ) -> list[NamespaceNode]:
        """Bulk register callables using derived paths."""
        return [self.register(c, decorate=decorate, tags=tags) for c in cc]

    def _all_leaves(self) -> list[NamespaceNode]:
        """Return all registered nodes."""
        return list(self._nodes.values())

    def query(self, expr: str) -> list[NamespaceNode]:
        """
        Return all nodes matching a tag expression. Supports `&` (AND),
        `|` (OR), `~` (NOT) with standard precedence (`~` > `&` > `|`).
        """
        tokens = _parse_tag_expr(expr)
        return [node for node in self._all_leaves() if _eval_tag_expr(tokens, node.tags)]

    def register_trigger(
        self,
        name: str,
        dag_nsref: str,
        trigger_type: str,
        schedule: str | None = None,
        poll_fn: Callable[[], Any] | None = None,
    ) -> Any:
        """Register a trigger definition. Purely declarative."""
        from lythonic.compose.trigger import TriggerDef

        if name in self._triggers:
            raise ValueError(f"Trigger '{name}' already registered")

        td = TriggerDef(
            name=name,
            dag_nsref=dag_nsref,
            trigger_type=trigger_type,
            schedule=schedule,
            poll_fn=poll_fn,
        )
        self._triggers[name] = td
        return td

    def get_trigger(self, name: str) -> Any:
        """Retrieve a trigger definition by name. Raises `KeyError` if not found."""
        if name not in self._triggers:
            raise KeyError(f"Trigger '{name}' not found")
        return self._triggers[name]

    def __getattr__(self, name: str) -> Any:
        # Avoid recursion for internal attributes.
        if name in ("_nodes", "_triggers"):
            raise AttributeError(name)
        # Look up by name as a leaf suffix (for simple nsrefs like "t:fetch" -> "fetch")
        for nsref, node in self._nodes.items():
            _, leaf = _parse_nsref(nsref)
            if leaf == name:
                return node
        raise AttributeError(f"'{name}' not found in namespace")


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
        """Register edge from self to other. Returns downstream for chaining."""
        self.dag.add_edge(self, other)
        return other


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
            raise ValueError(f"Sub-DAG must have exactly one source, found {len(sources)}")
        if len(sinks) != 1:
            raise ValueError(f"Sub-DAG must have exactly one sink, found {len(sinks)}")

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
        self.parent_namespace: Namespace | None = None

    def resolve(self, nsref: str) -> NamespaceNode:
        """
        Resolve a callable by nsref. Tries `parent_namespace` first
        (if set), then falls back to the DAG's own namespace.
        """
        if self.parent_namespace is not None:
            try:
                return self.parent_namespace.get(nsref)
            except KeyError:
                pass
        return self.namespace.get(nsref)

    def node(
        self,
        source: NamespaceNode | Callable[..., Any] | Dag,
        label: str | None = None,
    ) -> DagNode:
        """
        Create a unique `DagNode` in this graph. If `label` is `None`,
        derived from the source's nsref leaf name. Raises `ValueError`
        if the label already exists.

        When `source` is a `Dag` or `@dag_factory`, creates a `CallNode`
        that runs the sub-DAG as a single step. Label is auto-derived
        from the factory function name if not provided.
        """
        # Expand @dag_factory to a Dag
        if callable(source) and getattr(source, "_is_dag_factory", False):
            if label is None:
                label = str(getattr(source, "__name__", ""))
            factory_result = source()  # pyright: ignore[reportCallIssue]
            if not isinstance(factory_result, Dag):
                raise TypeError(
                    f"@dag_factory must return a Dag, got {type(factory_result).__name__}"
                )
            source = factory_result

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

    def map(
        self,
        sub_dag: Dag | Callable[..., Any],
        label: str | None = None,
    ) -> MapNode:
        """
        Create a `MapNode` that runs `sub_dag` on each element of an
        upstream collection. The sub-DAG must have exactly one source
        and one sink. Accepts a `@dag_factory` function, in which case
        the label is auto-derived from the function name.
        """
        # Expand @dag_factory to a Dag
        if callable(sub_dag) and getattr(sub_dag, "_is_dag_factory", False):
            if label is None:
                label = str(getattr(sub_dag, "__name__", ""))
            factory_result = sub_dag()  # pyright: ignore[reportCallIssue]
            if not isinstance(factory_result, Dag):
                raise TypeError(
                    f"@dag_factory must return a Dag, got {type(factory_result).__name__}"
                )
            sub_dag = factory_result

        if not label:
            raise ValueError("label is required for map()")

        if label in self.nodes:
            raise ValueError(
                f"Label '{label}' already exists in DAG. Use a unique label for map nodes."
            )

        map_node = MapNode(
            sub_dag=sub_dag,  # pyright: ignore[reportArgumentType]
            label=label,
            dag=self,
        )
        self.nodes[label] = map_node
        return map_node

    def add_edge(self, upstream: DagNode, downstream: DagNode) -> DagEdge:
        """Register a directed edge between two nodes."""
        edge = DagEdge(upstream=upstream.label, downstream=downstream.label)
        self.edges.append(edge)
        return edge

    def validate(self) -> None:
        """Check acyclicity and type compatibility between connected nodes."""
        self._check_acyclicity()
        self._check_type_compatibility()

    def _check_acyclicity(self) -> None:
        """Kahn's algorithm for topological sort -- raises if cycle detected."""
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
            # Also accept list[X] params when upstream returns X (fan-in)
            fan_in_types: set[str] = set()
            for dt in downstream_types:
                if isinstance(dt, str) and dt.startswith("list[") and dt.endswith("]"):
                    fan_in_types.add(dt[5:-1])
            upstream_str = str(upstream_return)
            if (
                downstream_types
                and upstream_return not in downstream_types
                and upstream_str not in fan_in_types
            ):
                logging.getLogger(__name__).warning(
                    "Type mismatch on edge %s -> %s: upstream returns %s, downstream accepts %s",
                    edge.upstream,
                    edge.downstream,
                    upstream_return,
                    downstream_types,
                )

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

    async def __call__(self, **kwargs: Any) -> DagRunResult:
        """
        Run the DAG directly with DagRunner's default - NullProvenance. Kwargs are matched
        to source node parameters by name.
        """
        from lythonic.compose.dag_runner import (  # pyright: ignore[reportImportCycles]
            DagRunner,
        )

        source_inputs: dict[str, dict[str, Any]] = {}
        for node in self.sources():
            if node.label in kwargs and isinstance(kwargs[node.label], dict):
                source_inputs[node.label] = kwargs[node.label]
                continue
            node_args = node.ns_node.method.args
            if node.ns_node.expects_dag_context():
                node_args = node_args[1:]
            node_kwargs = {a.name: kwargs[a.name] for a in node_args if a.name in kwargs}
            if node_kwargs:
                source_inputs[node.label] = node_kwargs

        runner = DagRunner(self)
        return await runner.run(source_inputs=source_inputs)

    def __enter__(self) -> Dag:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any
    ) -> None:
        if exc_type is None:
            self.validate()
