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

import typing
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


def _parse_nsref(nsref: str) -> tuple[list[str], str]:  # pyright: ignore[reportUnusedFunction]
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
        return self.dag_context_type() is not None

    def dag_context_type(self) -> type[DagContext] | None:
        """Return the `DagContext` subclass expected, or `None`."""
        resolved = _resolve_first_param_type(self.method.o)
        if resolved is not None and issubclass(resolved, DagContext):
            return resolved
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
