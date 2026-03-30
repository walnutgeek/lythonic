# Namespace Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Pydantic config models for persisting Namespace structure (callables, cached callables, DAGs), with `load_namespace()`, `dump_namespace()`, `validate_config()`, and subsume `CacheConfig`/`CacheRegistry` into the new system.

**Architecture:** New `namespace_config.py` contains the Pydantic models and three functions. `cached.py` is refactored: `CacheConfig`/`CacheRule`/`CacheRegistry` removed, replaced by a `register_cached_callable()` helper. `NamespaceNode` gains a `metadata` dict for round-trip serialization.

**Tech Stack:** Python 3.11+, Pydantic, SQLite, pytest, pytest-asyncio

**Spec:** `docs/superpowers/specs/2026-03-30-namespace-config-design.md`

---

### Task 1: Pydantic Config Models + NamespaceNode.metadata

**Files:**
- Create: `src/lythonic/compose/namespace_config.py`
- Modify: `src/lythonic/compose/namespace.py`
- Create: `tests/test_namespace_config.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_namespace_config.py`:

```python
from __future__ import annotations

from typing import Any


def test_config_model_callable_entry():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate({
        "entries": [
            {"nsref": "market:fetch", "gref": "json:dumps"},
        ]
    })
    assert len(config.entries) == 1
    assert config.entries[0].nsref == "market:fetch"
    assert config.entries[0].gref == "json:dumps"
    assert config.entries[0].cache is None
    assert config.entries[0].dag is None


def test_config_model_cached_entry():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate({
        "entries": [
            {
                "nsref": "market:fetch",
                "gref": "json:dumps",
                "cache": {"min_ttl": 0.5, "max_ttl": 2.0},
            },
        ]
    })
    assert config.entries[0].cache is not None
    assert config.entries[0].cache.min_ttl == 0.5
    assert config.entries[0].cache.max_ttl == 2.0


def test_config_model_dag_entry():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate({
        "entries": [
            {
                "nsref": "pipelines:daily",
                "dag": {
                    "nodes": [
                        {"label": "fetch", "nsref": "market:fetch"},
                        {"label": "compute", "nsref": "analysis:compute", "after": ["fetch"]},
                    ]
                },
            },
        ]
    })
    dag = config.entries[0].dag
    assert dag is not None
    assert len(dag.nodes) == 2
    assert dag.nodes[1].after == ["fetch"]


def test_config_model_storage():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate({
        "storage": {"cache_db": "cache.db", "dag_db": "runs.db"},
        "entries": [],
    })
    assert config.storage.cache_db == "cache.db"
    assert config.storage.dag_db == "runs.db"


def test_config_model_storage_defaults():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate({"entries": []})
    assert config.storage.cache_db is None
    assert config.storage.dag_db is None


def test_config_entry_validation_neither_gref_nor_dag():
    from pydantic import ValidationError

    from lythonic.compose.namespace_config import NamespaceConfig

    try:
        NamespaceConfig.model_validate({
            "entries": [{"nsref": "bad:entry"}]
        })
        raise AssertionError("Expected ValidationError")
    except ValidationError:
        pass


def test_config_entry_validation_both_gref_and_dag():
    from pydantic import ValidationError

    from lythonic.compose.namespace_config import NamespaceConfig

    try:
        NamespaceConfig.model_validate({
            "entries": [{
                "nsref": "bad:entry",
                "gref": "json:dumps",
                "dag": {"nodes": []},
            }]
        })
        raise AssertionError("Expected ValidationError")
    except ValidationError:
        pass


def test_namespace_node_metadata():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    def sample(x: int) -> int:
        return x

    ns = Namespace()
    method = Method(sample)
    node = NamespaceNode(method=method, nsref="t:sample", namespace=ns)
    assert node.metadata == {}

    node.metadata["cache"] = {"min_ttl": 0.5, "max_ttl": 2.0}
    assert node.metadata["cache"]["min_ttl"] == 0.5
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace_config.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write implementation**

Create `src/lythonic/compose/namespace_config.py`:

```python
"""
Namespace Config: Pydantic models for persisting Namespace structure.

Defines the configuration schema for callables, cached callables, and DAGs.
Format-agnostic — use `model_dump()`/`model_validate()` with any serializer.

Provides `load_namespace()` to build a live Namespace from config,
`dump_namespace()` to serialize a Namespace back to config, and
`validate_config()` for config-only validation.
"""

from __future__ import annotations

from pydantic import BaseModel, model_validator


class StorageConfig(BaseModel):
    """Storage paths for cache DB and DAG provenance DB."""

    cache_db: str | None = None
    dag_db: str | None = None


class CacheEntryConfig(BaseModel):
    """Cache TTL settings for a cached callable entry."""

    min_ttl: float
    max_ttl: float


class DagNodeConfig(BaseModel):
    """A node in a DAG, referencing a callable by nsref."""

    label: str
    nsref: str
    after: list[str] = []


class DagEntryConfig(BaseModel):
    """DAG definition with nodes and dependency declarations."""

    nodes: list[DagNodeConfig]


class NamespaceEntryConfig(BaseModel):
    """
    One entry in the namespace config. Determined by fields present:

    - `gref` set, no `dag` → callable (plain or cached)
    - `dag` set, no `gref` → DAG entry
    - Both or neither → validation error
    """

    nsref: str
    gref: str | None = None
    cache: CacheEntryConfig | None = None
    dag: DagEntryConfig | None = None

    @model_validator(mode="after")
    def _check_entry_type(self) -> NamespaceEntryConfig:
        has_gref = self.gref is not None
        has_dag = self.dag is not None
        if has_gref and has_dag:
            raise ValueError(f"Entry '{self.nsref}': cannot have both 'gref' and 'dag'")
        if not has_gref and not has_dag:
            raise ValueError(f"Entry '{self.nsref}': must have either 'gref' or 'dag'")
        if self.cache is not None and not has_gref:
            raise ValueError(f"Entry '{self.nsref}': 'cache' requires 'gref'")
        return self


class NamespaceConfig(BaseModel):
    """
    Top-level configuration for a Namespace. Format-agnostic — serialize
    with `model_dump()` and any writer (YAML, JSON, TOML).
    """

    storage: StorageConfig = StorageConfig()
    entries: list[NamespaceEntryConfig]
```

Add `metadata` field to `NamespaceNode.__init__` in `src/lythonic/compose/namespace.py`:

In the `__init__` method, after `self._decorated = decorated`, add:

```python
        self.metadata: dict[str, Any] = {}
```

And add the type annotation to the class body:

```python
    metadata: dict[str, Any]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace_config.py -v`
Expected: All PASS

- [ ] **Step 5: Run lint**

Run: `make lint`

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace_config.py src/lythonic/compose/namespace.py tests/test_namespace_config.py
git commit -m "feat(config): add Pydantic config models and NamespaceNode.metadata"
```

---

### Task 2: register_cached_callable() helper

**Files:**
- Modify: `src/lythonic/compose/cached.py`
- Modify: `tests/test_namespace_config.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_namespace_config.py`:

```python
import tempfile
from pathlib import Path

import tests.test_namespace_config as this_module


def _cached_fn(ticker: str) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction]
    this_module._cached_fn_count += 1  # pyright: ignore
    return {"price": 100.0, "ticker": ticker}


_cached_fn_count = 0


def test_register_cached_callable():
    from lythonic.compose.cached import register_cached_callable
    from lythonic.compose.namespace import Namespace

    this_module._cached_fn_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        node = register_cached_callable(
            ns,
            gref="tests.test_namespace_config:_cached_fn",
            nsref="market:fetch",
            min_ttl=1.0,
            max_ttl=2.0,
            db_path=db_path,
        )

        assert node.nsref == "market:fetch"
        assert node.metadata.get("cache") == {"min_ttl": 1.0, "max_ttl": 2.0}

        result = ns.market.fetch(ticker="AAPL")  # pyright: ignore
        assert result == {"price": 100.0, "ticker": "AAPL"}
        assert this_module._cached_fn_count == 1  # pyright: ignore

        # Second call from cache
        result2 = ns.market.fetch(ticker="AAPL")  # pyright: ignore
        assert result2 == {"price": 100.0, "ticker": "AAPL"}
        assert this_module._cached_fn_count == 1  # pyright: ignore
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_namespace_config.py::test_register_cached_callable -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write implementation**

Add to `src/lythonic/compose/cached.py`, after the `_build_async_wrapper` function and before `CacheRegistry`:

```python
def register_cached_callable(
    ns: Namespace,
    gref: str,
    nsref: str,
    min_ttl: float,
    max_ttl: float,
    db_path: Path,
) -> "NamespaceNode":
    """
    Register a callable with cache wrapping. Handles DDL generation,
    wrapper building, pushback table creation, and namespace registration.
    Stores cache config in `node.metadata["cache"]`.
    """
    from lythonic.compose.namespace import NamespaceNode

    gref_obj = GlobalRef(gref)
    method = Method(gref_obj)
    method.validate_simple_type_args()

    # Derive table name from nsref (convert : to __ like namespace_path)
    tbl_name = table_name_from_path(nsref.replace(":", "__").replace(".", "__"))

    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Create cache table and pushback table
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

    min_ttl_s = min_ttl * DAYS_TO_SECONDS
    max_ttl_s = max_ttl * DAYS_TO_SECONDS

    # namespace_path for pushback matching (use nsref leaf part with dots)
    namespace_path = nsref.replace(":", ".")

    if gref_obj.is_async():
        wrapper = _build_async_wrapper(
            method, tbl_name, db_path, min_ttl_s, max_ttl_s, namespace_path
        )
    else:
        wrapper = _build_sync_wrapper(
            method, tbl_name, db_path, min_ttl_s, max_ttl_s, namespace_path
        )

    node: NamespaceNode = ns.register(gref, nsref=nsref, decorate=lambda _: wrapper)
    node.metadata["cache"] = {"min_ttl": min_ttl, "max_ttl": max_ttl}
    return node
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace_config.py -v`
Expected: All PASS

- [ ] **Step 5: Run lint**

Run: `make lint`

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/cached.py tests/test_namespace_config.py
git commit -m "feat(cached): add register_cached_callable() helper"
```

---

### Task 3: load_namespace() function

**Files:**
- Modify: `src/lythonic/compose/namespace_config.py`
- Modify: `tests/test_namespace_config.py`

- [ ] **Step 1: Write the failing tests**

Add helper functions and tests to `tests/test_namespace_config.py`:

```python
def _plain_fn(x: int) -> int:  # pyright: ignore[reportUnusedFunction]
    return x * 2


def _another_plain_fn(value: int) -> str:  # pyright: ignore[reportUnusedFunction]
    return str(value)


def test_load_namespace_callable_entries():
    from lythonic.compose.namespace_config import NamespaceConfig, load_namespace

    config = NamespaceConfig.model_validate({
        "entries": [
            {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
        ]
    })

    with tempfile.TemporaryDirectory() as tmp:
        ns = load_namespace(config, Path(tmp))
        node = ns.get("math:double")
        assert node(x=5) == 10


def test_load_namespace_cached_entries():
    from lythonic.compose.namespace_config import NamespaceConfig, load_namespace

    this_module._cached_fn_count = 0  # pyright: ignore

    config = NamespaceConfig.model_validate({
        "storage": {"cache_db": "cache.db"},
        "entries": [
            {
                "nsref": "market:fetch",
                "gref": "tests.test_namespace_config:_cached_fn",
                "cache": {"min_ttl": 1.0, "max_ttl": 2.0},
            },
        ],
    })

    with tempfile.TemporaryDirectory() as tmp:
        ns = load_namespace(config, Path(tmp))
        result = ns.market.fetch(ticker="AAPL")  # pyright: ignore
        assert result == {"price": 100.0, "ticker": "AAPL"}
        assert this_module._cached_fn_count == 1  # pyright: ignore

        # Cached
        ns.market.fetch(ticker="AAPL")  # pyright: ignore
        assert this_module._cached_fn_count == 1  # pyright: ignore


def test_load_namespace_dag_entries():
    from lythonic.compose.namespace_config import NamespaceConfig, load_namespace

    config = NamespaceConfig.model_validate({
        "entries": [
            {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
            {"nsref": "fmt:to_str", "gref": "tests.test_namespace_config:_another_plain_fn"},
            {
                "nsref": "pipelines:convert",
                "dag": {
                    "nodes": [
                        {"label": "double", "nsref": "math:double"},
                        {"label": "format", "nsref": "fmt:to_str", "after": ["double"]},
                    ]
                },
            },
        ],
    })

    with tempfile.TemporaryDirectory() as tmp:
        ns = load_namespace(config, Path(tmp))
        node = ns.get("pipelines:convert")
        assert node is not None


def test_load_namespace_two_pass_order_independence():
    """DAG entry can appear before callable entries it references."""
    from lythonic.compose.namespace_config import NamespaceConfig, load_namespace

    config = NamespaceConfig.model_validate({
        "entries": [
            {
                "nsref": "pipelines:pipe",
                "dag": {
                    "nodes": [
                        {"label": "step", "nsref": "math:double"},
                    ]
                },
            },
            {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
        ],
    })

    with tempfile.TemporaryDirectory() as tmp:
        ns = load_namespace(config, Path(tmp))
        assert ns.get("math:double") is not None
        assert ns.get("pipelines:pipe") is not None


def test_load_namespace_duplicate_nsref_raises():
    from lythonic.compose.namespace_config import NamespaceConfig, load_namespace

    config = NamespaceConfig.model_validate({
        "entries": [
            {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
            {"nsref": "math:double", "gref": "tests.test_namespace_config:_another_plain_fn"},
        ],
    })

    with tempfile.TemporaryDirectory() as tmp:
        try:
            load_namespace(config, Path(tmp))
            raise AssertionError("Expected ValueError")
        except ValueError as e:
            assert "duplicate" in str(e).lower() or "already exists" in str(e).lower()


def test_load_namespace_dag_bad_nsref_raises():
    from lythonic.compose.namespace_config import NamespaceConfig, load_namespace

    config = NamespaceConfig.model_validate({
        "entries": [
            {
                "nsref": "pipelines:pipe",
                "dag": {
                    "nodes": [
                        {"label": "step", "nsref": "nonexistent:fn"},
                    ]
                },
            },
        ],
    })

    with tempfile.TemporaryDirectory() as tmp:
        try:
            load_namespace(config, Path(tmp))
            raise AssertionError("Expected KeyError or ValueError")
        except (KeyError, ValueError):
            pass
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace_config.py::test_load_namespace_callable_entries -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write implementation**

Add to `src/lythonic/compose/namespace_config.py`:

```python
from pathlib import Path
from typing import Any

from lythonic.compose.namespace import Dag, Namespace


def load_namespace(config: NamespaceConfig, config_dir: Path) -> Namespace:
    """
    Build a live `Namespace` from a `NamespaceConfig`. Two-pass: first
    registers all callables (plain and cached), then builds DAGs that
    reference them. Declaration order in config does not matter.
    """
    # Check for duplicate nsrefs
    nsrefs = [e.nsref for e in config.entries]
    seen: set[str] = set()
    for nsref in nsrefs:
        if nsref in seen:
            raise ValueError(f"Duplicate nsref: '{nsref}'")
        seen.add(nsref)

    ns = Namespace()

    # Resolve storage paths
    cache_db = (config_dir / config.storage.cache_db) if config.storage.cache_db else None
    dag_db = (config_dir / config.storage.dag_db) if config.storage.dag_db else None

    # Pass 1: register callable entries
    for entry in config.entries:
        if entry.gref is None:
            continue

        if entry.cache is not None:
            if cache_db is None:
                raise ValueError(
                    f"Entry '{entry.nsref}' has cache config but storage.cache_db is not set"
                )
            from lythonic.compose.cached import register_cached_callable

            register_cached_callable(
                ns,
                gref=entry.gref,
                nsref=entry.nsref,
                min_ttl=entry.cache.min_ttl,
                max_ttl=entry.cache.max_ttl,
                db_path=cache_db,
            )
        else:
            ns.register(entry.gref, nsref=entry.nsref)

    # Pass 2: build DAGs
    for entry in config.entries:
        if entry.dag is None:
            continue

        dag = Dag()
        for node_cfg in entry.dag.nodes:
            ns_node = ns.get(node_cfg.nsref)
            dag.node(ns_node, label=node_cfg.label)

        for node_cfg in entry.dag.nodes:
            for upstream_label in node_cfg.after:
                upstream = dag.nodes[upstream_label]
                downstream = dag.nodes[node_cfg.label]
                dag.add_edge(upstream, downstream)

        dag.validate()

        if dag_db is not None:
            dag.db_path = dag_db

        ns.register(dag, nsref=entry.nsref)

    return ns
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace_config.py -v`
Expected: All PASS

- [ ] **Step 5: Run lint**

Run: `make lint`

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace_config.py tests/test_namespace_config.py
git commit -m "feat(config): add load_namespace() two-pass loader"
```

---

### Task 4: validate_config() and dump_namespace()

**Files:**
- Modify: `src/lythonic/compose/namespace_config.py`
- Modify: `tests/test_namespace_config.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_namespace_config.py`:

```python
def test_validate_config_valid():
    from lythonic.compose.namespace_config import NamespaceConfig, validate_config

    config = NamespaceConfig.model_validate({
        "entries": [
            {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
            {
                "nsref": "pipelines:pipe",
                "dag": {
                    "nodes": [
                        {"label": "step", "nsref": "math:double"},
                    ]
                },
            },
        ],
    })
    errors = validate_config(config)
    assert errors == []


def test_validate_config_bad_gref():
    from lythonic.compose.namespace_config import NamespaceConfig, validate_config

    config = NamespaceConfig.model_validate({
        "entries": [
            {"nsref": "bad:fn", "gref": "nonexistent.module:fn"},
        ],
    })
    errors = validate_config(config)
    assert len(errors) > 0
    assert "nonexistent" in errors[0].lower() or "import" in errors[0].lower()


def test_validate_config_dag_nsref_not_in_config():
    from lythonic.compose.namespace_config import NamespaceConfig, validate_config

    config = NamespaceConfig.model_validate({
        "entries": [
            {
                "nsref": "pipelines:pipe",
                "dag": {
                    "nodes": [
                        {"label": "step", "nsref": "missing:fn"},
                    ]
                },
            },
        ],
    })
    errors = validate_config(config)
    assert len(errors) > 0
    assert "missing:fn" in errors[0]


def test_validate_config_duplicate_nsref():
    from lythonic.compose.namespace_config import NamespaceConfig, validate_config

    config = NamespaceConfig.model_validate({
        "entries": [
            {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
            {"nsref": "math:double", "gref": "tests.test_namespace_config:_another_plain_fn"},
        ],
    })
    errors = validate_config(config)
    assert len(errors) > 0
    assert "duplicate" in errors[0].lower()


def test_validate_config_duplicate_dag_label():
    from lythonic.compose.namespace_config import NamespaceConfig, validate_config

    config = NamespaceConfig.model_validate({
        "entries": [
            {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
            {
                "nsref": "pipelines:pipe",
                "dag": {
                    "nodes": [
                        {"label": "step", "nsref": "math:double"},
                        {"label": "step", "nsref": "math:double"},
                    ]
                },
            },
        ],
    })
    errors = validate_config(config)
    assert len(errors) > 0
    assert "step" in errors[0]


def test_dump_namespace_round_trip():
    from lythonic.compose.namespace_config import (
        NamespaceConfig,
        StorageConfig,
        dump_namespace,
        load_namespace,
    )

    config = NamespaceConfig.model_validate({
        "storage": {"cache_db": "cache.db"},
        "entries": [
            {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
            {
                "nsref": "market:fetch",
                "gref": "tests.test_namespace_config:_cached_fn",
                "cache": {"min_ttl": 1.0, "max_ttl": 2.0},
            },
        ],
    })

    with tempfile.TemporaryDirectory() as tmp:
        ns = load_namespace(config, Path(tmp))
        dumped = dump_namespace(ns, storage=StorageConfig(cache_db="cache.db"))

        assert len(dumped.entries) == 2
        # Sorted by nsref
        assert dumped.entries[0].nsref == "market:fetch"
        assert dumped.entries[1].nsref == "math:double"
        # Cache metadata preserved
        assert dumped.entries[0].cache is not None
        assert dumped.entries[0].cache.min_ttl == 1.0


def test_dump_namespace_serialization_order():
    from lythonic.compose.namespace_config import dump_namespace
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._plain_fn, nsref="z:last")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_plain_fn, nsref="a:first")  # pyright: ignore[reportPrivateUsage]

    dumped = dump_namespace(ns)
    assert dumped.entries[0].nsref == "a:first"
    assert dumped.entries[1].nsref == "z:last"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace_config.py::test_validate_config_valid tests/test_namespace_config.py::test_dump_namespace_round_trip -v`
Expected: FAIL

- [ ] **Step 3: Write implementation**

Add to `src/lythonic/compose/namespace_config.py`:

```python
def validate_config(config: NamespaceConfig) -> list[str]:
    """
    Validate config without building a Namespace. Returns a list of
    error messages (empty if valid).
    """
    from lythonic import GlobalRef
    from lythonic.compose import Method
    from lythonic.compose.namespace import _parse_nsref  # pyright: ignore[reportPrivateUsage]

    errors: list[str] = []

    # Check duplicate nsrefs
    seen_nsrefs: set[str] = set()
    for entry in config.entries:
        if entry.nsref in seen_nsrefs:
            errors.append(f"Duplicate nsref: '{entry.nsref}'")
        seen_nsrefs.add(entry.nsref)

    # Collect callable nsrefs for DAG reference checking
    callable_nsrefs = {e.nsref for e in config.entries if e.gref is not None}

    for entry in config.entries:
        # Validate grefs resolve
        if entry.gref is not None:
            try:
                gref = GlobalRef(entry.gref)
                gref.get_instance()
            except Exception as e:
                errors.append(f"Entry '{entry.nsref}': cannot resolve gref '{entry.gref}': {e}")
                continue

            # Validate cached entry parameters are simple_type
            if entry.cache is not None:
                try:
                    method = Method(GlobalRef(entry.gref))
                    method.validate_simple_type_args()
                except ValueError as e:
                    errors.append(f"Entry '{entry.nsref}': {e}")

        # Validate DAG entries
        if entry.dag is not None:
            dag_labels: set[str] = set()
            for node_cfg in entry.dag.nodes:
                # Check duplicate labels
                if node_cfg.label in dag_labels:
                    errors.append(
                        f"Entry '{entry.nsref}': duplicate label '{node_cfg.label}'"
                    )
                dag_labels.add(node_cfg.label)

                # Check nsref references a callable entry
                if node_cfg.nsref not in callable_nsrefs:
                    errors.append(
                        f"Entry '{entry.nsref}': DAG node '{node_cfg.label}' "
                        f"references '{node_cfg.nsref}' which is not a callable entry"
                    )

            # Check after references exist within the DAG
            for node_cfg in entry.dag.nodes:
                for upstream in node_cfg.after:
                    if upstream not in dag_labels:
                        errors.append(
                            f"Entry '{entry.nsref}': node '{node_cfg.label}' "
                            f"references unknown upstream '{upstream}'"
                        )

    return errors


def dump_namespace(
    ns: Namespace, storage: StorageConfig | None = None
) -> NamespaceConfig:
    """
    Export a live `Namespace` to a `NamespaceConfig`. Entries sorted
    alphabetically by nsref. DAG nodes sorted by label.
    """
    from lythonic.compose.namespace import Dag, NamespaceNode

    entries: list[NamespaceEntryConfig] = []

    def _collect_leaves(namespace: Namespace) -> list[NamespaceNode]:
        """Recursively collect all leaf nodes."""
        leaves: list[NamespaceNode] = []
        for node in namespace._leaves.values():  # pyright: ignore[reportPrivateUsage]
            leaves.append(node)
        for branch in namespace._branches.values():  # pyright: ignore[reportPrivateUsage]
            leaves.extend(_collect_leaves(branch))
        return leaves

    for node in _collect_leaves(ns):
        # Check if this is a DAG registration (method wraps a dag_wrapper)
        # DAG nodes have metadata with "dag" key set during registration
        if "dag" in node.metadata:
            dag_info = node.metadata["dag"]
            entries.append(NamespaceEntryConfig(
                nsref=node.nsref,
                dag=dag_info,
            ))
        elif "cache" in node.metadata:
            cache_info = node.metadata["cache"]
            entries.append(NamespaceEntryConfig(
                nsref=node.nsref,
                gref=str(node.method.gref),
                cache=CacheEntryConfig(**cache_info),
            ))
        else:
            entries.append(NamespaceEntryConfig(
                nsref=node.nsref,
                gref=str(node.method.gref),
            ))

    # Sort by nsref
    entries.sort(key=lambda e: e.nsref)

    return NamespaceConfig(
        storage=storage or StorageConfig(),
        entries=entries,
    )
```

Also update `load_namespace` to store DAG metadata on the node for round-trip. After `ns.register(dag, nsref=entry.nsref)`, add:

```python
        dag_node = ns.get(entry.nsref)
        # Store DAG config for round-trip serialization
        dag_entry_config = DagEntryConfig(
            nodes=sorted(
                [
                    DagNodeConfig(
                        label=dn.label,
                        nsref=dn.ns_node.nsref,
                        after=sorted([
                            e.upstream for e in dag.edges if e.downstream == dn.label
                        ]),
                    )
                    for dn in dag.nodes.values()
                ],
                key=lambda n: n.label,
            )
        )
        dag_node.metadata["dag"] = dag_entry_config
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace_config.py -v`
Expected: All PASS

- [ ] **Step 5: Run lint**

Run: `make lint`

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace_config.py tests/test_namespace_config.py
git commit -m "feat(config): add validate_config() and dump_namespace()"
```

---

### Task 5: Remove CacheConfig/CacheRule/CacheRegistry, migrate test_cached.py

**Files:**
- Modify: `src/lythonic/compose/cached.py`
- Modify: `tests/test_cached.py`
- Modify: `docs/reference/compose-cached.md`

This is the biggest task — removing the old API and migrating all tests.

- [ ] **Step 1: Remove CacheConfig, CacheRule, CacheRegistry from cached.py**

In `src/lythonic/compose/cached.py`:

Delete these classes and their methods:
- `class CacheRule(BaseModel)` (lines 156-162)
- `class CacheConfig(BaseModel)` (lines 165-169)
- `class CacheRegistry` (lines 410-477)

Remove the `yaml` import (no longer needed):
```python
import yaml
```

Remove the `GRef` import (no longer needed):
```python
from lythonic import GlobalRef, GRef
```

Change to:
```python
from lythonic import GlobalRef
```

Remove `from pydantic import BaseModel` if no longer used (but `BaseModel` is used in `_serialize_return_value` for `isinstance` check — keep it).

Update the module docstring to reflect the new API (remove CacheRegistry usage, show register_cached_callable).

- [ ] **Step 2: Migrate test_cached.py**

In `tests/test_cached.py`, the migration pattern for each test is:

**Old pattern:**
```python
from lythonic.compose.cached import CacheConfig, CacheRegistry, CacheRule

config = CacheConfig(
    rules=[CacheRule(gref="...", namespace_path="market.fetch", min_ttl=1.0, max_ttl=2.0)],
    cache_db="cache.db",
)
registry = CacheRegistry(config, config_dir=Path(tmp))
result = registry.cached.market.fetch(ticker="AAPL")
```

**New pattern:**
```python
from lythonic.compose.cached import register_cached_callable
from lythonic.compose.namespace import Namespace

ns = Namespace()
register_cached_callable(ns, "tests.test_cached:_fake_fetch", "market:fetch", 1.0, 2.0, Path(tmp) / "cache.db")
result = ns.market.fetch(ticker="AAPL")
```

Tests to **delete** (test old config parsing, no longer relevant):
- `test_cache_config_from_yaml`
- `test_from_yaml_file`

Tests to **migrate** (change CacheRegistry setup to register_cached_callable):
- `test_sync_wrapper_miss_fetches_and_caches`
- `test_sync_wrapper_expired_refetches`
- `test_async_wrapper_miss_fetches_and_caches`
- `test_pydantic_return_type_cached`
- `test_probabilistic_refresh_between_ttls`
- `test_default_namespace_path_uses_function_name`
- `test_pushback_table_created_on_registry_init`
- `test_pushback_suppresses_probabilistic_refresh`
- `test_async_pushback_suppresses_probabilistic_refresh`
- `test_pushback_recorded_on_exception`
- `test_past_max_ttl_with_pushback_raises_suppressed`
- `test_cache_miss_ignores_pushback`
- `test_default_scope_uses_method_namespace_path`

For each migrated test, the key change is replacing the `CacheConfig`/`CacheRegistry` setup block with `Namespace` + `register_cached_callable`, and replacing `registry.cached.xxx` with `ns.xxx`.

Note on `namespace_path` to `nsref` conversion:
- `"market.fetch"` → `"market:fetch"`
- `"async_market.fetch"` → `"async_market:fetch"`
- `"fetch2"` → `"fetch2"`
- `"prob"` → `"prob"`
- `"typed_fetch"` → `"typed_fetch"`
- `"api.rate_limited"` → `"api:rate_limited"`
- `"api.other"` → `"api:other"`
- `"market.pushback_fetch"` → `"market:pushback_fetch"`

Note on table names: the old `table_name_from_path` converted dots to `__`. The new `register_cached_callable` converts `:` and `.` to `__`. So `"market:fetch"` → `"market__fetch"`. The SQL table names in test assertions (UPDATE statements with explicit table names) need to match. Check that the table name derivation in `register_cached_callable` is consistent.

- [ ] **Step 3: Run all tests**

Run: `uv run pytest tests/test_cached.py -v`
Expected: All PASS

Run: `uv run pytest tests/test_namespace_config.py -v`
Expected: All PASS

- [ ] **Step 4: Update docs reference**

Update `docs/reference/compose-cached.md` — remove `CacheRegistry`, `CacheConfig`, `CacheRule` from members, add `register_cached_callable`:

```markdown
# lythonic.compose.cached

Cache utilities for wrapping callables with SQLite-backed caching.

::: lythonic.compose.cached
    options:
      show_root_heading: false
      members:
        - register_cached_callable
        - CacheRefreshPushback
        - CacheRefreshSuppressed
        - generate_cache_table_ddl
        - table_name_from_path
```

- [ ] **Step 5: Run lint**

Run: `make lint`

- [ ] **Step 6: Run full test suite**

Run: `make test`
Expected: All pass

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/compose/cached.py tests/test_cached.py docs/reference/compose-cached.md
git commit -m "refactor(cached): remove CacheRegistry, migrate tests to register_cached_callable"
```

---

### Task 6: Docs + final verification

**Files:**
- Create: `docs/reference/compose-namespace-config.md`
- Modify: `mkdocs.yml`

- [ ] **Step 1: Create reference page**

Create `docs/reference/compose-namespace-config.md`:

```markdown
# lythonic.compose.namespace_config

Pydantic models for persisting Namespace structure and loader/serializer functions.

::: lythonic.compose.namespace_config
    options:
      show_root_heading: false
      members:
        - NamespaceConfig
        - NamespaceEntryConfig
        - StorageConfig
        - CacheEntryConfig
        - DagEntryConfig
        - DagNodeConfig
        - load_namespace
        - dump_namespace
        - validate_config
```

- [ ] **Step 2: Update mkdocs.yml**

Add after `lythonic.compose.namespace`:

```yaml
      - lythonic.compose.namespace_config: reference/compose-namespace-config.md
```

- [ ] **Step 3: Run full test suite**

Run: `make lint && make test`
Expected: All pass, zero lint errors

- [ ] **Step 4: Commit**

```bash
git add docs/reference/compose-namespace-config.md mkdocs.yml
git commit -m "docs: add namespace_config reference page"
```
