# Namespace Mounting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `register_cached_callable()` with declarative namespace mounting — load config, then `ns.mount(storage)` to activate caching and provenance.

**Architecture:** `Namespace` gains `mount(storage)` which iterates nodes with `NsCacheConfig` and wraps them in-place. `_register_dag` creates runners at call time using namespace provenance. Two marker decorators (`@mountable`, `@mount_required`) enable introspection.

**Tech Stack:** Python 3.11+, Pydantic, SQLite, asyncio

---

### Task 1: Add `@mountable` and `@mount_required` decorators

**Files:**
- Modify: `src/lythonic/compose/namespace.py` (near `dag_factory`, line ~178)
- Test: `tests/test_cached.py` (append new test)

- [ ] **Step 1: Write the test**

Append to `tests/test_cached.py`:

```python
def test_mountable_and_mount_required_are_transparent_markers():
    from lythonic.compose.namespace import mount_required, mountable

    @mountable
    def optional_fn(x: int) -> int:
        return x + 1

    @mount_required
    def required_fn(x: int) -> int:
        return x + 2

    assert optional_fn(1) == 2
    assert required_fn(1) == 3
    assert getattr(optional_fn, "_is_mountable", False) is True
    assert getattr(required_fn, "_is_mount_required", False) is True
    assert getattr(optional_fn, "_is_mount_required", False) is False
    assert getattr(required_fn, "_is_mountable", False) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cached.py::test_mountable_and_mount_required_are_transparent_markers -v`
Expected: FAIL with `ImportError` (symbols don't exist yet)

- [ ] **Step 3: Implement decorators**

In `src/lythonic/compose/namespace.py`, after the `dag_factory` function (line ~187), add:

```python
def mountable(fn: _F) -> _F:
    """Mark a callable as benefiting from a mounted namespace."""
    fn._is_mountable = True  # pyright: ignore[reportFunctionMemberAccess]
    return fn


def mount_required(fn: _F) -> _F:
    """Mark a callable as requiring a mounted namespace."""
    fn._is_mount_required = True  # pyright: ignore[reportFunctionMemberAccess]
    return fn
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_cached.py::test_mountable_and_mount_required_are_transparent_markers -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_cached.py
git commit -m "feat: add @mountable and @mount_required marker decorators"
```

---

### Task 2: Add mount state and introspection properties to Namespace

**Files:**
- Modify: `src/lythonic/compose/namespace.py` — `Namespace.__init__` and new properties
- Test: `tests/test_cached.py` (append new test)

**Depends on:** Task 1

- [ ] **Step 1: Write the test**

Append to `tests/test_cached.py`:

```python
def test_mount_awareness_properties():
    from lythonic.compose.namespace import (
        Namespace,
        NsCacheConfig,
        TriggerConfig,
        mount_required,
        mountable,
    )

    # Pure namespace: no mount needed
    ns_pure = Namespace()
    ns_pure.register(lambda: "ok", nsref="t:pure")
    assert ns_pure.is_mounted is False
    assert ns_pure.requires_mount is False
    assert ns_pure.has_mountable is False

    # Namespace with NsCacheConfig: requires mount
    ns_cache = Namespace()
    cache_cfg = NsCacheConfig(nsref="t:cached", gref=None, min_ttl=1.0, max_ttl=2.0)
    ns_cache.register(lambda ticker="X": {"p": 1}, nsref="t:cached", config=cache_cfg)
    assert ns_cache.requires_mount is True
    assert ns_cache.has_mountable is True

    # Namespace with triggers: requires mount
    ns_trig = Namespace()
    trig_cfg = TriggerConfig(name="my_trigger", type="poll", schedule="* * * * *")
    from lythonic.compose.namespace import NsNodeConfig

    node_cfg = NsNodeConfig(nsref="t:triggered", triggers=[trig_cfg])
    ns_trig.register(lambda: None, nsref="t:triggered", config=node_cfg)
    assert ns_trig.requires_mount is True

    # Namespace with @mount_required callable
    @mount_required
    def needs_mount() -> str:
        return "hi"

    ns_req = Namespace()
    ns_req.register(needs_mount, nsref="t:needs")
    assert ns_req.requires_mount is True
    assert ns_req.has_mountable is True

    # Namespace with @mountable callable (not required, but mountable)
    @mountable
    def optional_mount() -> str:
        return "hi"

    ns_opt = Namespace()
    ns_opt.register(optional_mount, nsref="t:opt")
    assert ns_opt.requires_mount is False
    assert ns_opt.has_mountable is True

    # Namespace with a DAG: mountable (benefits from provenance)
    from lythonic.compose.namespace import Dag

    dag = Dag()
    dag.node(lambda: "x", label="src")
    ns_dag = Namespace()
    ns_dag.register(dag, nsref="t:my_dag")
    assert ns_dag.requires_mount is False
    assert ns_dag.has_mountable is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cached.py::test_mount_awareness_properties -v`
Expected: FAIL with `AttributeError: 'Namespace' object has no attribute 'is_mounted'`

- [ ] **Step 3: Implement mount state and properties**

In `src/lythonic/compose/namespace.py`, modify `Namespace.__init__` (line ~449):

```python
def __init__(self) -> None:
    self._nodes: dict[str, NamespaceNode] = {}
    self._storage: StorageConfig | None = None
    self._provenance: DagProvenance | NullProvenance | None = None
```

Add the `TYPE_CHECKING` imports at the top of the file (inside the existing `if TYPE_CHECKING:` block, line ~166):

```python
if TYPE_CHECKING:
    from lythonic.compose.dag_runner import DagRunResult  # pyright: ignore[reportImportCycles]
    from lythonic.compose.dag_provenance import DagProvenance, NullProvenance  # pyright: ignore[reportImportCycles]
    from lythonic.compose.engine import StorageConfig  # pyright: ignore[reportImportCycles]
```

Add these properties to `Namespace` after `is_mounted`, before `register()`:

```python
@property
def is_mounted(self) -> bool:
    """True if mount() has been called."""
    return self._storage is not None

@property
def requires_mount(self) -> bool:
    """True if any node has cache config, triggers, or @mount_required."""
    for node in self._nodes.values():
        if isinstance(node.config, NsCacheConfig):
            return True
        if node.config.triggers:
            return True
        if getattr(node.method.o, "_is_mount_required", False):
            return True
    return False

@property
def has_mountable(self) -> bool:
    """True if any node would alter behavior when mounted."""
    if self.requires_mount:
        return True
    for node in self._nodes.values():
        if node.dag is not None:
            return True
        if getattr(node.method.o, "_is_mountable", False):
            return True
    return False
```

Also update `__getattr__` to avoid recursion on the new attributes (line ~622). Change:

```python
if name == "_nodes":
    raise AttributeError(name)
```

to:

```python
if name.startswith("_"):
    raise AttributeError(name)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_cached.py::test_mount_awareness_properties -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_cached.py
git commit -m "feat: add mount state and introspection properties to Namespace"
```

---

### Task 3: Add `_mount_cached_node` to `cached.py` and remove `register_cached_callable`

**Files:**
- Modify: `src/lythonic/compose/cached.py`

- [ ] **Step 1: Replace `register_cached_callable` with `mount_cached_node`**

Replace the `register_cached_callable` function (lines 409-469) with:

```python
def mount_cached_node(node: NamespaceNode, db_path: Path) -> None:
    """
    Activate caching on a node that has `NsCacheConfig`. Builds the
    sync/async wrapper, creates DDL, and sets `node._decorated`.
    Called by `Namespace.mount()`.
    """
    from lythonic.compose.namespace import NsCacheConfig

    config = node.config
    if not isinstance(config, NsCacheConfig):
        raise TypeError(f"Expected NsCacheConfig, got {type(config).__name__}")

    method = node.method
    method.validate_simple_type_args()

    nsref = node.nsref
    tbl_name = table_name_from_path(nsref.replace(":", "__").replace(".", "__"))

    db_path.parent.mkdir(parents=True, exist_ok=True)

    ddl = generate_cache_table_ddl(tbl_name, method)
    with open_sqlite_db(db_path) as conn:
        cursor = conn.cursor()
        execute_sql(cursor, ddl)
        execute_sql(
            cursor,
            "CREATE TABLE IF NOT EXISTS _pushback "
            "(namespace_prefix TEXT NOT NULL, suppressed_until REAL NOT NULL)",
        )
        conn.commit()

    min_ttl_s = config.min_ttl * DAYS_TO_SECONDS
    max_ttl_s = config.max_ttl * DAYS_TO_SECONDS
    namespace_path = nsref.replace(":", ".")

    gref = node.method.gref
    if gref is not None and gref.is_async():
        wrapper = _build_async_wrapper(
            method, tbl_name, db_path, min_ttl_s, max_ttl_s, namespace_path
        )
    else:
        wrapper = _build_sync_wrapper(
            method, tbl_name, db_path, min_ttl_s, max_ttl_s, namespace_path
        )

    node._decorated = wrapper
```

- [ ] **Step 2: Update module docstring**

Replace the module docstring (lines 1-61) with:

```python
"""
Cached: SQLite-backed caching layer for namespace callables.

Wraps sync or async methods that return `dict` or Pydantic `BaseModel` with
SQLite-backed caching. Each cached method gets its own table with typed
parameter columns (derived from the method signature) and a composite
primary key.

## Usage

Declare cached callables in namespace config using `NsCacheConfig`, then
call `ns.mount(storage)` to activate caching:

```python
from lythonic.compose.namespace import Namespace, NsCacheConfig
from lythonic.compose.engine import StorageConfig

ns = Namespace()
cfg = NsCacheConfig(
    nsref="market:fetch_prices",
    gref="myapp.downloads:fetch_prices",
    min_ttl=0.5, max_ttl=2.0,
)
ns.register("myapp.downloads:fetch_prices", nsref="market:fetch_prices", config=cfg)
ns.mount(StorageConfig(cache_db=Path("cache.db")))

result = ns.market.fetch_prices(ticker="AAPL")
```

## TTL Behavior

- **age < `min_ttl`**: return cached value (fresh)
- **`min_ttl` <= age < `max_ttl`**: probabilistic refresh — probability increases
  linearly from 0 to 1. On refresh failure, returns stale value.
- **age >= `max_ttl`** or cache miss: call original method. On failure, raises.

## Validation

All method parameters must have types registered as `simple_type` in
`KNOWN_TYPES` (primitives, date, datetime, Path). Validated at mount time
via `Method.validate_simple_type_args()`.

## Pushback

When a cached method raises `CacheRefreshPushback(days, namespace_prefix)`, all
probabilistic refreshes matching the scope are suppressed for the given duration.
If `namespace_prefix` is omitted, only the raising method is suppressed.

- During the probabilistic window with active pushback: returns stale data.
- Past `max_ttl` with active pushback: raises `CacheRefreshSuppressed`.
- Cache miss: always calls the method regardless of pushback.
"""
```

- [ ] **Step 3: Update imports — remove `GlobalRef` from non-TYPE_CHECKING imports**

The `GlobalRef` import (line 77) is only used by the now-removed `register_cached_callable`. Remove it from the regular imports. Keep `Namespace` import since `mount_cached_node` uses `NamespaceNode` from TYPE_CHECKING.

Remove this line:
```python
from lythonic import GlobalRef
```

- [ ] **Step 4: Run lint**

Run: `make lint`
Expected: PASS (0 errors)

- [ ] **Step 5: Commit**

```bash
git add src/lythonic/compose/cached.py
git commit -m "feat: replace register_cached_callable with mount_cached_node"
```

---

### Task 4: Add `Namespace.mount()` method

**Files:**
- Modify: `src/lythonic/compose/namespace.py`
- Test: `tests/test_cached.py` (append new test)

**Depends on:** Tasks 2 and 3

- [ ] **Step 1: Write the test**

Append to `tests/test_cached.py`:

```python
def test_namespace_mount_wraps_cached_nodes():
    """mount() activates caching for NsCacheConfig nodes."""
    from lythonic.compose.engine import StorageConfig
    from lythonic.compose.namespace import Namespace, NsCacheConfig

    import tests.test_cached as mod

    mod._fake_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        cache_db = Path(tmp) / "cache.db"

        ns = Namespace()
        cfg = NsCacheConfig(
            nsref="market:fetch",
            gref="tests.test_cached:_fake_fetch",
            min_ttl=1.0,
            max_ttl=2.0,
        )
        ns.register("tests.test_cached:_fake_fetch", nsref="market:fetch", config=cfg)

        assert ns.is_mounted is False
        ns.mount(StorageConfig(cache_db=cache_db))
        assert ns.is_mounted is True

        result = ns.get("market:fetch")(ticker="AAPL")
        assert result == {"price": 100.0, "ticker": "AAPL"}
        assert mod._fake_fetch_count == 1

        # Second call served from cache
        result2 = ns.get("market:fetch")(ticker="AAPL")
        assert result2 == {"price": 100.0, "ticker": "AAPL"}
        assert mod._fake_fetch_count == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cached.py::test_namespace_mount_wraps_cached_nodes -v`
Expected: FAIL — `Namespace` has no `mount` method

- [ ] **Step 3: Implement `mount()` method**

In `src/lythonic/compose/namespace.py`, add this method to `Namespace` after the `has_mountable` property:

```python
def mount(self, storage: StorageConfig) -> None:
    """Activate persistence features for all declared nodes."""
    from lythonic.compose.engine import StorageConfig as _SC  # pyright: ignore[reportUnusedImport]

    self._storage = storage

    if storage.dag_db is not None:
        from lythonic.compose.dag_provenance import DagProvenance

        self._provenance = DagProvenance(storage.dag_db)

    if storage.cache_db is not None:
        from lythonic.compose.cached import mount_cached_node

        for node in self._nodes.values():
            if isinstance(node.config, NsCacheConfig):
                mount_cached_node(node, storage.cache_db)
```

Note: The `StorageConfig` type annotation in the signature uses a string or the TYPE_CHECKING import already added in Task 2. Since the file uses `from __future__ import annotations`, the string annotation resolves at runtime via the lazy import inside the method body. The actual parameter type hint `StorageConfig` resolves from the `TYPE_CHECKING` import.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_cached.py::test_namespace_mount_wraps_cached_nodes -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_cached.py
git commit -m "feat: add Namespace.mount() method"
```

---

### Task 5: Add discriminated config deserialization to `from_dict`

**Files:**
- Modify: `src/lythonic/compose/namespace.py` — `from_dict` classmethod
- Test: `tests/test_cached.py` (append new test)

**Depends on:** Task 4

- [ ] **Step 1: Write the test**

Append to `tests/test_cached.py`:

```python
def test_from_dict_deserializes_cache_config():
    """from_dict creates NsCacheConfig for type='cache' entries."""
    from lythonic.compose.namespace import Namespace, NsCacheConfig

    entries = [
        {
            "type": "cache",
            "nsref": "market:fetch",
            "gref": "tests.test_cached:_fake_fetch",
            "min_ttl": 0.5,
            "max_ttl": 2.0,
        },
        {
            "nsref": "t:plain",
            "gref": "tests.test_cached:_ns_test_ok",
        },
    ]

    ns = Namespace.from_dict(entries)

    cache_node = ns.get("market:fetch")
    assert isinstance(cache_node.config, NsCacheConfig)
    assert cache_node.config.min_ttl == 0.5
    assert cache_node.config.max_ttl == 2.0

    plain_node = ns.get("t:plain")
    assert not isinstance(plain_node.config, NsCacheConfig)
    assert plain_node.config.type == "auto"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cached.py::test_from_dict_deserializes_cache_config -v`
Expected: FAIL — `from_dict` creates `NsNodeConfig` for everything, so `isinstance(cache_node.config, NsCacheConfig)` is `False`

- [ ] **Step 3: Add config type registry and update `from_dict`**

In `src/lythonic/compose/namespace.py`, after `NsCacheConfig` (line ~378), add:

```python
_CONFIG_TYPES: dict[str, type[NsNodeConfig]] = {
    "auto": NsNodeConfig,
    "cache": NsCacheConfig,
}
```

Replace the `from_dict` classmethod (lines 608-620) with:

```python
@classmethod
def from_dict(cls, entries: list[dict[str, Any]]) -> Namespace:
    """Build a Namespace from a list of serialized node configs."""
    ns = cls()
    for entry in entries:
        config_cls = _CONFIG_TYPES.get(entry.get("type", "auto"), NsNodeConfig)
        config = config_cls.model_validate(entry)
        if config.gref is not None:
            gref = GlobalRef(config.gref)
            instance = gref.get_instance()
            ns.register(instance, nsref=config.nsref, config=config)
        else:
            ns.register(lambda: None, nsref=config.nsref, config=config)
    return ns
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_cached.py::test_from_dict_deserializes_cache_config -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_cached.py
git commit -m "feat: discriminated config deserialization in from_dict"
```

---

### Task 6: Update `_register_dag` for mount-aware provenance

**Files:**
- Modify: `src/lythonic/compose/namespace.py` — `_register_dag` method
- Test: `tests/test_cached.py` (append new test)

**Depends on:** Task 4

- [ ] **Step 1: Write the test**

Append to `tests/test_cached.py`:

```python
async def test_dag_uses_provenance_when_mounted():
    """A mounted namespace provides provenance to DAG runners."""
    from lythonic.compose.engine import StorageConfig
    from lythonic.compose.namespace import Dag, Namespace

    async def step_a(x: int) -> int:
        return x + 1

    async def step_b(data: int) -> int:
        return data * 2

    dag = Dag()
    dag.node(step_a) >> dag.node(step_b)

    ns = Namespace()
    ns.register(dag, nsref="t:my_dag")

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        dag_db = Path(tmp) / "dags.db"
        ns.mount(StorageConfig(dag_db=dag_db))

        result = await ns.get("t:my_dag")(x=5)
        assert result.status == "completed"
        assert result.outputs["step_b"] == 12

        # Verify provenance was recorded (DB should have dag_runs)
        from lythonic.state import execute_sql, open_sqlite_db

        with open_sqlite_db(dag_db) as conn:
            cursor = conn.cursor()
            execute_sql(cursor, "SELECT COUNT(*) FROM dag_runs")
            count = cursor.fetchone()[0]
            assert count >= 1


async def test_dag_works_without_mount():
    """An unmounted namespace DAG uses NullProvenance (no DB)."""
    from lythonic.compose.namespace import Dag, Namespace

    async def add_one(x: int) -> int:
        return x + 1

    dag = Dag()
    dag.node(add_one)

    ns = Namespace()
    ns.register(dag, nsref="t:simple_dag")

    result = await ns.get("t:simple_dag")(x=10)
    assert result.status == "completed"
    assert result.outputs["add_one"] == 11
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cached.py::test_dag_uses_provenance_when_mounted tests/test_cached.py::test_dag_works_without_mount -v`
Expected: `test_dag_uses_provenance_when_mounted` FAIL (no provenance recorded because `_register_dag` creates a fixed runner at registration time)

- [ ] **Step 3: Update `_register_dag` to use namespace provenance at call time**

In `src/lythonic/compose/namespace.py`, replace `_register_dag` method (lines 502-553). The key change: instead of creating one `DagRunner(dag)` at registration, create a runner per call using `ns_ref._provenance`:

```python
def _register_dag(
    self,
    dag: Dag,
    nsref: str | None,
    tags: frozenset[str] | set[str] | list[str] | None = None,
    config: NsNodeConfig | None = None,
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

    ns_ref = self

    async def dag_wrapper(**kwargs: Any) -> Any:
        from lythonic.compose.dag_runner import DagRunner  # pyright: ignore[reportImportCycles]

        runner = DagRunner(dag, provenance=ns_ref._provenance)
        source_labels = {n.label for n in dag.sources()}
        source_inputs: dict[str, dict[str, Any]] = {}
        for label in source_labels:
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

    node = NamespaceNode(
        method=method, nsref=nsref, namespace=self, tags=tags, config=config, dag=dag
    )
    self._nodes[nsref] = node
    return node
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cached.py::test_dag_uses_provenance_when_mounted tests/test_cached.py::test_dag_works_without_mount -v`
Expected: PASS

- [ ] **Step 5: Run full test suite to check for regressions**

Run: `make test`
Expected: All tests pass. The `DagRunner` import moved inside the closure but the behavior is identical for existing callers.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_cached.py
git commit -m "feat: _register_dag uses namespace provenance at call time"
```

---

### Task 7: Migrate `test_cached.py` to declarative mount flow

**Files:**
- Modify: `tests/test_cached.py` — replace all `register_cached_callable` usage

**Depends on:** Tasks 4 and 5

- [ ] **Step 1: Create a helper function**

Add this helper near the top of `tests/test_cached.py` (after the imports):

```python
def _mount_cached(
    ns: Any, gref: str, min_ttl: float, max_ttl: float, db_path: Path, nsref: str
) -> None:
    """Register a callable with NsCacheConfig and mount the namespace."""
    from lythonic.compose.engine import StorageConfig
    from lythonic.compose.namespace import NsCacheConfig

    cfg = NsCacheConfig(nsref=nsref, gref=gref, min_ttl=min_ttl, max_ttl=max_ttl)
    ns.register(gref, nsref=nsref, config=cfg)
    if not ns.is_mounted:
        ns.mount(StorageConfig(cache_db=db_path))
```

- [ ] **Step 2: Replace all `register_cached_callable` calls**

In every test function that uses `register_cached_callable`, replace:

```python
from lythonic.compose.cached import register_cached_callable
...
register_cached_callable(ns, GREF, MIN_TTL, MAX_TTL, db_path, nsref=NSREF)
```

with:

```python
_mount_cached(ns, GREF, MIN_TTL, MAX_TTL, db_path, NSREF)
```

The affected test functions are:
- `test_sync_wrapper_miss_fetches_and_caches`
- `test_sync_wrapper_expired_refetches`
- `test_async_wrapper_miss_fetches_and_caches`
- `test_pydantic_return_type_cached`
- `test_probabilistic_refresh_between_ttls`
- `test_default_namespace_path_uses_function_name`
- `test_pushback_table_created_on_register` (rename to `test_pushback_table_created_on_mount`)
- `test_pushback_suppresses_probabilistic_refresh`
- `test_async_pushback_suppresses_probabilistic_refresh`
- `test_pushback_recorded_on_exception`
- `test_past_max_ttl_with_pushback_raises_suppressed`
- `test_cache_miss_ignores_pushback`
- `test_default_scope_uses_method_namespace_path`
- `test_require_cache_context_passes_through_wrapper`

For `test_default_scope_uses_method_namespace_path` which registers two cached callables in the same namespace, use the helper twice — the second call's `ns.is_mounted` check will skip re-mounting:

```python
_mount_cached(ns, "tests.test_cached:_rate_limited_fetch", 1.0, 3.0, db_path, "api:rate_limited")
_mount_cached(ns, "tests.test_cached:_pushback_fetch", 1.0, 3.0, db_path, "api:other")
```

Note: the `_mount_cached` helper calls `ns.mount()` on first use. When a second cached callable is registered after mounting, it won't be wrapped. To handle this, change the helper to register first, then check if already mounted — if already mounted, wrap individually:

```python
def _mount_cached(
    ns: Any, gref: str, min_ttl: float, max_ttl: float, db_path: Path, nsref: str
) -> None:
    """Register a callable with NsCacheConfig and mount/wrap."""
    from lythonic.compose.cached import mount_cached_node
    from lythonic.compose.engine import StorageConfig
    from lythonic.compose.namespace import NsCacheConfig

    cfg = NsCacheConfig(nsref=nsref, gref=gref, min_ttl=min_ttl, max_ttl=max_ttl)
    ns.register(gref, nsref=nsref, config=cfg)
    if not ns.is_mounted:
        ns.mount(StorageConfig(cache_db=db_path))
    else:
        mount_cached_node(ns.get(nsref), db_path)
```

- [ ] **Step 3: Remove all `from lythonic.compose.cached import register_cached_callable` lines**

Search and delete every import of `register_cached_callable` in this file.

- [ ] **Step 4: Run full test suite**

Run: `uv run pytest tests/test_cached.py -v`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add tests/test_cached.py
git commit -m "refactor: migrate test_cached.py to declarative mount flow"
```

---

### Task 8: Migrate `test_dag_runner.py` cache test

**Files:**
- Modify: `tests/test_dag_runner.py`

**Depends on:** Task 4

- [ ] **Step 1: Update `test_runtime_resolution_attached_dag_succeeds_with_cache`**

Replace the test body (lines 1337-1375) with:

```python
async def test_runtime_resolution_attached_dag_succeeds_with_cache():
    """An attached DAG resolves to the cached version and succeeds."""
    from lythonic.compose.cached import mount_cached_node
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.engine import StorageConfig
    from lythonic.compose.namespace import Dag, Namespace, NsCacheConfig

    ns = Namespace()

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        db_path = Path(tmp) / "cache.db"

        # Register guarded function with NsCacheConfig
        cfg = NsCacheConfig(
            nsref="tests.test_dag_runner:_guarded_fetch",
            gref="tests.test_dag_runner:_guarded_fetch",
            min_ttl=1.0,
            max_ttl=2.0,
        )
        ns.register(
            "tests.test_dag_runner:_guarded_fetch",
            config=cfg,
        )
        ns.mount(StorageConfig(cache_db=db_path))

        # Build a DAG using the namespace nodes
        dag = Dag()
        f = dag.node(this_module._guarded_fetch)  # pyright: ignore[reportPrivateUsage]
        p = dag.node(this_module._format_price)  # pyright: ignore[reportPrivateUsage]
        f >> p  # pyright: ignore[reportUnusedExpression]

        # Register DAG in namespace (sets parent_namespace for runtime resolution)
        ns.register(dag, nsref="pipelines:prices")

        # Run via the registered DAG — runner resolves via parent_namespace
        runner = DagRunner(dag)
        result = await runner.run(
            source_inputs={"_guarded_fetch": {"ticker": "AAPL"}},
            dag_nsref="test:attached",
        )
        assert result.status == "completed"
        print(result.outputs)
        assert result.outputs["_format_price"] == "AAPL=100.0"
```

- [ ] **Step 2: Run the test**

Run: `uv run pytest tests/test_dag_runner.py::test_runtime_resolution_attached_dag_succeeds_with_cache -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_dag_runner.py
git commit -m "refactor: migrate dag_runner cache test to mount flow"
```

---

### Task 9: Update `_build_namespace` in `lyth.py`

**Files:**
- Modify: `src/lythonic/compose/lyth.py`

**Depends on:** Tasks 4 and 5

- [ ] **Step 1: Simplify `_build_namespace`**

Replace `_build_namespace` (lines 90-99) with:

```python
def _build_namespace(engine_config: EngineConfig) -> Any:
    """Build a live Namespace from EngineConfig: load declaratively, then mount."""
    from lythonic.compose.namespace import Namespace

    ns = Namespace.from_dict(
        [e.model_dump(exclude_none=True) for e in engine_config.namespace]
    )
    ns.mount(engine_config.storage)
    return ns
```

Also remove the now-unused `GlobalRef` import at line 26:
```python
from lythonic import GlobalRef
```

- [ ] **Step 2: Run lint and full test suite**

Run: `make lint && make test`
Expected: All pass. The `start` command in lyth.py creates its own `DagProvenance` and `TriggerManager` separately, which still works — `mount()` sets up namespace-level provenance, and `TriggerManager` can override per-trigger-fire.

- [ ] **Step 3: Commit**

```bash
git add src/lythonic/compose/lyth.py
git commit -m "refactor: simplify _build_namespace to use from_dict + mount"
```

---

### Task 10: Update documentation

**Files:**
- Modify: `docs/tutorials/compose-pipeline.md`
- Modify: `docs/reference/compose-cached.md`
- Modify: `src/lythonic/compose/cached.py` (module docstring already updated in Task 3)

- [ ] **Step 1: Update tutorial cache section**

Replace lines 222-263 of `docs/tutorials/compose-pipeline.md` (the "Add Caching" section) with:

```markdown
## Add Caching

For expensive callables (API calls, slow computations), declare them with
`NsCacheConfig` and mount the namespace. This adds SQLite-backed caching with
configurable TTL.

Declare a cache-wrapped callable using `NsCacheConfig` and mount the namespace
with a `StorageConfig`:

```python
from pathlib import Path
from lythonic.compose.namespace import Namespace, NsCacheConfig
from lythonic.compose.engine import StorageConfig

ns = Namespace()
cfg = NsCacheConfig(
    nsref="cache:parse_nsref",
    gref="lythonic.compose.namespace:_parse_nsref",
    min_ttl=0.5,   # days — fresh for 12 hours
    max_ttl=2.0,   # days — force refresh after 2 days
)
ns.register("lythonic.compose.namespace:_parse_nsref", nsref="cache:parse_nsref", config=cfg)
ns.mount(StorageConfig(cache_db=Path("cache.db")))

# First call hits the original function and caches the result
result = ns.cache.parse_nsref(nsref="market.data:fetch_prices")
print(result)  # (['market', 'data'], 'fetch_prices')

# Subsequent calls within min_ttl are served from cache
result = ns.cache.parse_nsref(nsref="market.data:fetch_prices")
```

TTL behavior:

- **age < `min_ttl`**: return cached value (fresh)
- **`min_ttl` <= age < `max_ttl`**: probabilistic refresh — the older the
  entry, the more likely it refreshes
- **age >= `max_ttl`** or cache miss: call the original function

TTL values are in days (e.g., `min_ttl=0.5` means 12 hours).

All cached method parameters must be "simple types" (primitives, `date`,
`datetime`, `Path`) so they can serve as cache key columns.
```

Update the "Next Steps" section (lines 270-271) — replace the reference to `register_cached_callable`:
```markdown
- [API Reference: lythonic.compose.cached](../reference/compose-cached.md)
  — mount_cached_node, TTL, pushback
```

- [ ] **Step 2: Update reference page**

Replace `docs/reference/compose-cached.md` with:

```markdown
# lythonic.compose.cached

Cache utilities for wrapping callables with SQLite-backed caching.

::: lythonic.compose.cached
    options:
      show_root_heading: false
      members:
        - mount_cached_node
        - CacheRefreshPushback
        - CacheRefreshSuppressed
        - CacheProhibitDirectCall
        - generate_cache_table_ddl
        - table_name_from_path
```

- [ ] **Step 3: Run lint**

Run: `make lint`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add docs/tutorials/compose-pipeline.md docs/reference/compose-cached.md
git commit -m "docs: update cache docs for mount-based flow"
```

---

### Task 11: Final verification

**Depends on:** All previous tasks

- [ ] **Step 1: Run full lint and test suite**

Run: `make lint && make test`
Expected: 0 lint errors, all tests pass

- [ ] **Step 2: Verify no remaining references to `register_cached_callable`**

Run: `grep -r "register_cached_callable" src/ tests/ docs/`
Expected: No matches (release notes may still reference it — that's historical and fine)

- [ ] **Step 3: Verify the new public API is importable**

Run:
```bash
uv run python -c "
from lythonic.compose.namespace import mountable, mount_required, Namespace, NsCacheConfig
from lythonic.compose.cached import mount_cached_node
from lythonic.compose.engine import StorageConfig
ns = Namespace()
print('is_mounted:', ns.is_mounted)
print('requires_mount:', ns.requires_mount)
print('has_mountable:', ns.has_mountable)
print('OK')
"
```
Expected: prints `is_mounted: False`, `requires_mount: False`, `has_mountable: False`, `OK`
