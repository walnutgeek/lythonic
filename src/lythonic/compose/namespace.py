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

```python
from lythonic.compose.namespace import Namespace

ns = Namespace()
ns.register(fetch_prices, nsref="market:fetch_prices")
node = ns.get("market:fetch_prices")
result = node(ticker="AAPL")
```
"""

from __future__ import annotations

import inspect as _inspect
import logging
import typing
from collections.abc import Callable
from pathlib import Path
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
            result = _and_expr() or result
        return result

    def _and_expr() -> bool:
        nonlocal pos
        result = _not_expr()
        while pos < len(tokens) and tokens[pos] == "&":
            pos += 1
            result = _not_expr() and result
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


class NamespaceNode:
    """
    Wraps a `Method` with namespace identity. Callable -- delegates to the
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
        tags: frozenset[str] | set[str] | list[str] | None = None,
    ) -> None:
        self.method = method
        self.nsref = nsref
        self.namespace = namespace
        self._decorated = decorated
        self.metadata: dict[str, Any] = {}
        self.tags: frozenset[str] = _validate_tags(tags)

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
    Hierarchical registry of callables wrapped in `NamespaceNode`.
    Uses GlobalRef-style paths: `"namespace.sub:callable_name"`.
    """

    _branches: dict[str, Namespace]
    _leaves: dict[str, NamespaceNode]

    def __init__(self) -> None:
        self._branches = {}
        self._leaves = {}

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
        c: Callable[..., Any] | str | Dag,
        nsref: str | None = None,
        decorate: Callable[[Callable[..., Any]], Callable[..., Any]] | None = None,
        tags: frozenset[str] | set[str] | list[str] | None = None,
    ) -> NamespaceNode:
        """
        Register a callable. If `nsref` is `None`, derive from the callable's
        module and name. If `decorate` is provided, wrap the callable for
        invocation (metadata is still extracted from the original).
        """
        if isinstance(c, Dag):
            return self._register_dag(c, nsref, tags=tags)

        from lythonic import GlobalRef

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

        node = NamespaceNode(
            method=method, nsref=nsref, namespace=self, decorated=decorated, tags=tags
        )
        branch._leaves[leaf_name] = node
        return node

    def _register_dag(
        self, dag: Dag, nsref: str | None, tags: frozenset[str] | set[str] | list[str] | None = None
    ) -> NamespaceNode:
        """
        Register a Dag as a callable NamespaceNode. If `dag.db_path` is
        None, the runner uses NullProvenance (no persistence, no
        restart/replay).
        """
        if nsref is None:
            raise ValueError("nsref is required when registering a Dag")

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

    def get(self, nsref: str) -> NamespaceNode:
        """Retrieve a node by nsref. Raises `KeyError` if not found."""
        branch_parts, leaf_name = _parse_nsref(nsref)
        branch = self._get_branch(branch_parts)
        if leaf_name not in branch._leaves:
            raise KeyError(f"Leaf '{leaf_name}' not found")
        return branch._leaves[leaf_name]

    def register_all(
        self,
        *cc: Callable[..., Any],
        decorate: Callable[[Callable[..., Any]], Callable[..., Any]] | None = None,
        tags: frozenset[str] | set[str] | list[str] | None = None,
    ) -> list[NamespaceNode]:
        """Bulk register callables using derived paths."""
        return [self.register(c, decorate=decorate, tags=tags) for c in cc]

    def _all_leaves(self) -> list[NamespaceNode]:
        """Recursively collect all leaf nodes from this namespace and all branches."""
        leaves: list[NamespaceNode] = list(self._leaves.values())
        for branch in self._branches.values():
            leaves.extend(branch._all_leaves())
        return leaves

    def query(self, expr: str) -> list[NamespaceNode]:
        """
        Return all nodes matching a tag expression. Supports `&` (AND),
        `|` (OR), `~` (NOT) with standard precedence (`~` > `&` > `|`).
        """
        tokens = _parse_tag_expr(expr)
        return [node for node in self._all_leaves() if _eval_tag_expr(tokens, node.tags)]

    def __getattr__(self, name: str) -> Any:
        # Avoid recursion for internal attributes.
        if name in ("_branches", "_leaves"):
            raise AttributeError(name)
        if name in self._branches:
            return self._branches[name]
        if name in self._leaves:
            return self._leaves[name]
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

    def __rshift__(self, other: DagNode | Dag) -> DagNode:
        """
        Register edge from self to other. If other is a Dag, merge it
        into the parent and wire the boundary edges. Returns the
        downstream DagNode for chaining.
        """
        if isinstance(other, Dag):
            return self.dag._merge_and_wire(self, other)  # pyright: ignore[reportPrivateUsage]
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
        self.db_path: Path | None = None

    def clone(self, prefix: str) -> Dag:
        """
        Return a new Dag with all node labels prefixed with `prefix/`.
        Edges are remapped to the new labels. The original is unmodified.
        DagNodes in the clone share the same `NamespaceNode` references.
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
            raise ValueError(f"Sub-DAG must have exactly one source, found {len(sources)}")
        if len(sinks) != 1:
            raise ValueError(f"Sub-DAG must have exactly one sink, found {len(sinks)}")

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

    def __enter__(self) -> Dag:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any
    ) -> None:
        if exc_type is None:
            self.validate()
