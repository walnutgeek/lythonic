# Cache Wrapper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a YAML-configured caching layer to `lythonic.compose` that wraps sync/async download methods with SQLite-backed caching and probabilistic stale refresh.

**Architecture:** Config-driven registry pattern. A `CacheConfig` Pydantic model loaded from YAML defines cache rules. A `CacheRegistry` validates rules against method signatures (enforcing `simple_type` args), creates per-rule SQLite tables dynamically, builds sync/async wrapper callables, and installs them on a nested namespace object for attribute-based access.

**Tech Stack:** Python 3.11+, Pydantic, SQLite (via `lythonic.state`), PyYAML, asyncio

---

## File Structure

### Modified Files

- `src/lythonic/types.py` — Add `simple_type: bool` to `KnownTypeArgs` and `KnownType`. Auto-detect for primitives, date, datetime, Path during registration.
- `src/lythonic/compose/__init__.py` — Add `validate_simple_type_args()` method to `Method` for cache key validation.
- `pyproject.toml` — Add `pyyaml` dependency.

### New Files

- `src/lythonic/compose/cached.py` — `CacheRule`, `CacheConfig`, `Namespace`, `CacheRegistry`, wrapper factory.
- `tests/test_cached.py` — Integration tests for the cache wrapper system.

---

## Task 1: Add `simple_type` to `KnownTypeArgs` and `KnownType`

**Files:**
- Modify: `src/lythonic/types.py:238-251` (KnownTypeArgs)
- Modify: `src/lythonic/types.py:390-432` (KnownType)
- Modify: `src/lythonic/types.py:532-546` (KnownTypesMap.register, register_type)
- Modify: `src/lythonic/types.py:595-631` (KNOWN_TYPES.register call)

- [ ] **Step 1: Write the failing test**

Add to the bottom of `src/lythonic/types.py`, before the closing registration block, or at the end of file:

```python
## Tests

def test_simple_type_primitives():
    from lythonic.types import KNOWN_TYPES
    for t in (int, float, bool, str):
        kt = KNOWN_TYPES.resolve_type(t)
        assert kt.simple_type

def test_simple_type_date_datetime_path():
    from datetime import date, datetime
    from pathlib import Path
    from lythonic.types import KNOWN_TYPES
    for t in (date, datetime, Path):
        kt = KNOWN_TYPES.resolve_type(t)
        assert kt.simple_type

def test_non_simple_type():
    from lythonic.types import KNOWN_TYPES
    kt = KNOWN_TYPES.resolve_type(bytes)
    assert not kt.simple_type
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest -s src/lythonic/types.py::test_simple_type_primitives -v`
Expected: FAIL — `KnownType` has no attribute `simple_type`

- [ ] **Step 3: Add `simple_type` field to `KnownTypeArgs`**

In `src/lythonic/types.py`, add to `KnownTypeArgs` (after `aliases`):

```python
    simple_type: bool = False
```

- [ ] **Step 4: Add `simple_type` field to `KnownType` and set it from args**

In `KnownType.__init__`, after the line `self.aliases.add(alias.lower())` block, add:

```python
        self.simple_type = args.simple_type
```

- [ ] **Step 5: Auto-detect `simple_type` in `KnownTypesMap.register`**

In `KnownTypesMap.register`, before the `if not args.is_factory:` check, add auto-detection logic. The `register` method should become:

```python
    def register(self, *array_of_args: KnownTypeArgs) -> None:
        for args in array_of_args:
            if not args.simple_type and args.concrete_type is not None:
                if is_primitive(args.concrete_type) or args.concrete_type in (date, datetime, Path):
                    args.simple_type = True
            if not args.is_factory:
                self.register_type(KnownType(args))
            else:
                self.abstract_types.add(args)
```

This requires `date`, `datetime`, and `Path` imports which are already present at the top of `types.py`.

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest -s src/lythonic/types.py::test_simple_type_primitives src/lythonic/types.py::test_simple_type_date_datetime_path src/lythonic/types.py::test_non_simple_type -v`
Expected: All 3 PASS

- [ ] **Step 7: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero warnings, all tests pass

- [ ] **Step 8: Commit**

```bash
git add src/lythonic/types.py
git commit -m "feat: add simple_type flag to KnownTypeArgs and KnownType"
```

---

## Task 2: Add `validate_simple_type_args` to `Method`

**Files:**
- Modify: `src/lythonic/compose/__init__.py:92-162` (Method class)

- [ ] **Step 1: Write the failing test**

Add at the bottom of `src/lythonic/compose/__init__.py`:

```python
## Tests

def test_validate_simple_type_args_passes():
    from lythonic.compose import Method
    from lythonic import GlobalRef

    def fetch(ticker: str, year: int) -> dict:
        return {}

    m = Method(fetch)
    m.validate_simple_type_args()

def test_validate_simple_type_args_fails_on_non_simple():
    from lythonic.compose import Method

    def fetch(data: bytes) -> dict:
        return {}

    m = Method(fetch)
    try:
        m.validate_simple_type_args()
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "data" in str(e)
        assert "simple_type" in str(e)

def test_validate_simple_type_args_skips_optional():
    from lythonic.compose import Method

    def fetch(ticker: str, limit: int = 10) -> dict:
        return {}

    m = Method(fetch)
    # Optional args are still validated (they have simple types here)
    m.validate_simple_type_args()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest -s src/lythonic/compose/__init__.py::test_validate_simple_type_args_passes -v`
Expected: FAIL — `Method` has no attribute `validate_simple_type_args`

- [ ] **Step 3: Implement `validate_simple_type_args` on Method**

In `src/lythonic/compose/__init__.py`, add the import at the top (after existing imports):

```python
from lythonic.types import KNOWN_TYPES
```

Add this method to `Method`, after the `__call__` method:

```python
    def validate_simple_type_args(self) -> None:
        """
        Validate that all non-optional parameters have type annotations
        whose `KnownType` has `simple_type=True`. Raises `ValueError`
        if any parameter fails.
        """
        for arg in self.args:
            if arg.annotation is None:
                raise ValueError(
                    f"Parameter `{arg.name}` on `{self.gref}` has no type annotation, "
                    f"required for simple_type validation"
                )
            try:
                kt = KNOWN_TYPES.resolve_type(arg.annotation)
            except (ValueError, TypeError):
                raise ValueError(
                    f"Parameter `{arg.name}` on `{self.gref}` has type `{arg.annotation.__name__}` "
                    f"which is not a registered KnownType"
                )
            if not kt.simple_type:
                raise ValueError(
                    f"Parameter `{arg.name}` on `{self.gref}` has type `{arg.annotation.__name__}` "
                    f"which is not a simple_type (required for cache key)"
                )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest -s src/lythonic/compose/__init__.py::test_validate_simple_type_args_passes src/lythonic/compose/__init__.py::test_validate_simple_type_args_fails_on_non_simple src/lythonic/compose/__init__.py::test_validate_simple_type_args_skips_optional -v`
Expected: All 3 PASS

- [ ] **Step 5: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero warnings, all tests pass

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/__init__.py
git commit -m "feat: add validate_simple_type_args to Method"
```

---

## Task 3: Add PyYAML dependency

**Files:**
- Modify: `pyproject.toml:40-42` (dependencies)

- [ ] **Step 1: Add pyyaml dependency**

Run: `uv add pyyaml`

This updates `pyproject.toml` and `uv.lock`.

- [ ] **Step 2: Verify install**

Run: `uv run python -c "import yaml; print(yaml.__version__)"`
Expected: Prints a version number

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "feat: add pyyaml dependency for cache config"
```

---

## Task 4: Create `cached.py` — Config models and Namespace

**Files:**
- Create: `src/lythonic/compose/cached.py`

- [ ] **Step 1: Write the failing test for config loading**

Create `tests/test_cached.py`:

```python
from __future__ import annotations

from textwrap import dedent
from pathlib import Path
import tempfile

def test_cache_config_from_yaml():
    from lythonic.compose.cached import CacheConfig

    yaml_str = dedent("""
        rules:
          - gref: "json:dumps"
            namespace_path: "util.dumps"
            min_ttl: 0.5
            max_ttl: 2.0
          - gref: "json:loads"
            min_ttl: 0.25
            max_ttl: 1.0
    """).strip()

    import yaml
    data = yaml.safe_load(yaml_str)
    config = CacheConfig.model_validate(data)
    assert len(config.rules) == 2
    assert config.namespace == "lythonic.compose.cached"
    assert config.rules[0].namespace_path == "util.dumps"
    assert config.rules[0].min_ttl == 0.5
    assert config.rules[0].max_ttl == 2.0
    assert config.rules[1].namespace_path is None
    assert str(config.rules[1].gref) == "json:loads"

def test_namespace_nested_access():
    from lythonic.compose.cached import Namespace

    ns = Namespace()
    ns._install("market.fetch_prices", lambda: "ok")
    ns._install("get_data", lambda: "data")

    assert ns.market.fetch_prices() == "ok"
    assert ns.get_data() == "data"

def test_namespace_path_generates_table_name():
    from lythonic.compose.cached import table_name_from_path

    assert table_name_from_path("market.fetch_prices") == "market__fetch_prices"
    assert table_name_from_path("get_data") == "get_data"
    assert table_name_from_path("a.b.c") == "a__b__c"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest -s tests/test_cached.py::test_cache_config_from_yaml -v`
Expected: FAIL — cannot import `CacheConfig`

- [ ] **Step 3: Implement config models, Namespace, and table_name_from_path**

Create `src/lythonic/compose/cached.py`:

```python
"""
Cached method wrappers with YAML-configured TTL-based caching.

Wraps sync/async download methods with SQLite-backed caching.
Methods are exposed through a nested namespace object with attribute access.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import random
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

from lythonic import GRef, GlobalRef
from lythonic.compose import Method
from lythonic.state import execute_sql, open_sqlite_db
from lythonic.types import KNOWN_TYPES, DbTypeInfo


class CacheRule(BaseModel):
    """One cached method definition."""

    gref: GRef
    namespace_path: str | None = None
    min_ttl: float
    max_ttl: float


class CacheConfig(BaseModel):
    """Root of the YAML config file."""

    namespace: str = "lythonic.compose.cached"
    cache_db: str | None = None
    rules: list[CacheRule]


def table_name_from_path(path: str) -> str:
    """Convert a dot-separated namespace path to a SQL table name."""
    return path.replace(".", "__")


class Namespace:
    """
    Nested attribute namespace for cached method wrappers.

    Dot-separated paths create intermediate Namespace objects automatically.
    """

    def _install(self, path: str, func: Callable[..., Any]) -> None:
        parts = path.split(".")
        current: Namespace = self
        for part in parts[:-1]:
            if not hasattr(current, part):
                setattr(current, part, Namespace())
            current = getattr(current, part)
        setattr(current, parts[-1], func)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest -s tests/test_cached.py::test_cache_config_from_yaml tests/test_cached.py::test_namespace_nested_access tests/test_cached.py::test_namespace_path_generates_table_name -v`
Expected: All 3 PASS

- [ ] **Step 5: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero warnings, all tests pass

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/cached.py tests/test_cached.py
git commit -m "feat: add CacheConfig, CacheRule, Namespace models for cached module"
```

---

## Task 5: Dynamic DDL generation and table creation

**Files:**
- Modify: `src/lythonic/compose/cached.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cached.py`:

```python
def test_generate_ddl_single_param():
    from lythonic.compose.cached import generate_cache_table_ddl

    def fetch(ticker: str) -> dict:
        return {}

    ddl = generate_cache_table_ddl("market__fetch_prices", Method(fetch))
    assert "CREATE TABLE IF NOT EXISTS market__fetch_prices" in ddl
    assert "ticker TEXT NOT NULL" in ddl
    assert "value_json TEXT NOT NULL" in ddl
    assert "fetched_at REAL NOT NULL" in ddl
    assert "PRIMARY KEY (ticker)" in ddl

def test_generate_ddl_multiple_params():
    from lythonic.compose.cached import generate_cache_table_ddl

    def fetch(from_currency: str, to_currency: str) -> dict:
        return {}

    ddl = generate_cache_table_ddl("get_exchange_rate", Method(fetch))
    assert "PRIMARY KEY (from_currency, to_currency)" in ddl

from lythonic.compose import Method
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest -s tests/test_cached.py::test_generate_ddl_single_param -v`
Expected: FAIL — cannot import `generate_cache_table_ddl`

- [ ] **Step 3: Implement `generate_cache_table_ddl`**

Add to `src/lythonic/compose/cached.py`:

```python
def generate_cache_table_ddl(table_name: str, method: Method) -> str:
    """
    Generate CREATE TABLE DDL for a cache table based on the method's
    non-optional parameter types.
    """
    columns: list[str] = []
    param_names: list[str] = []

    for arg in method.args:
        kt = KNOWN_TYPES.resolve_type(arg.annotation)
        db_type = kt.db_type_info.name
        columns.append(f"    {arg.name} {db_type} NOT NULL")
        param_names.append(arg.name)

    columns.append("    value_json TEXT NOT NULL")
    columns.append("    fetched_at REAL NOT NULL")

    pk = ", ".join(param_names)
    columns.append(f"    PRIMARY KEY ({pk})")

    cols_str = ",\n".join(columns)
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{cols_str}\n)"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest -s tests/test_cached.py::test_generate_ddl_single_param tests/test_cached.py::test_generate_ddl_multiple_params -v`
Expected: Both PASS

- [ ] **Step 5: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero warnings, all tests pass

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/cached.py tests/test_cached.py
git commit -m "feat: add dynamic DDL generation for cache tables"
```

---

## Task 6: Sync wrapper with cache lookup and probabilistic refresh

**Files:**
- Modify: `src/lythonic/compose/cached.py`
- Modify: `tests/test_cached.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cached.py`:

```python
import tempfile
import time

def test_sync_wrapper_miss_fetches_and_caches():
    """On cache miss, the wrapper calls the original and stores the result."""
    from lythonic.compose.cached import CacheRegistry, CacheConfig, CacheRule
    from lythonic import GlobalRef

    call_count = 0
    def fake_fetch(ticker: str) -> dict:
        nonlocal call_count
        call_count += 1
        return {"price": 100.0, "ticker": ticker}

    # Temporarily register fake_fetch so GlobalRef can find it
    import tests.test_cached as this_module
    this_module._fake_fetch = fake_fetch  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref=GlobalRef("tests.test_cached:_fake_fetch"),
                    namespace_path="market.fetch",
                    min_ttl=1.0,
                    max_ttl=2.0,
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))

        result = registry.cached.market.fetch(ticker="AAPL")
        assert result == {"price": 100.0, "ticker": "AAPL"}
        assert call_count == 1

        # Second call should come from cache
        result2 = registry.cached.market.fetch(ticker="AAPL")
        assert result2 == {"price": 100.0, "ticker": "AAPL"}
        assert call_count == 1

def test_sync_wrapper_expired_refetches():
    """Past max_ttl, the wrapper must re-fetch."""
    from lythonic.compose.cached import CacheRegistry, CacheConfig, CacheRule, _cache_upsert
    from lythonic import GlobalRef

    call_count = 0
    def fake_fetch2(ticker: str) -> dict:
        nonlocal call_count
        call_count += 1
        return {"price": float(call_count)}

    import tests.test_cached as this_module
    this_module._fake_fetch2 = fake_fetch2  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref=GlobalRef("tests.test_cached:_fake_fetch2"),
                    namespace_path="fetch2",
                    min_ttl=0.0001,  # ~8.6 seconds
                    max_ttl=0.0002,  # ~17.3 seconds
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))

        result = registry.cached.fetch2(ticker="X")
        assert call_count == 1

        # Manually backdate the fetched_at to simulate expiry
        db_path = Path(tmp) / "cache.db"
        import sqlite3
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "UPDATE fetch2 SET fetched_at = ? WHERE ticker = ?",
                (time.time() - 86400 * 1, "X"),  # 1 day ago, way past max_ttl
            )
            conn.commit()

        result2 = registry.cached.fetch2(ticker="X")
        assert call_count == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest -s tests/test_cached.py::test_sync_wrapper_miss_fetches_and_caches -v`
Expected: FAIL — cannot import `CacheRegistry`

- [ ] **Step 3: Implement `CacheRegistry` and sync wrapper**

Add to `src/lythonic/compose/cached.py`:

```python
DAYS_TO_SECONDS = 86400.0


def _cache_lookup(
    conn: sqlite3.Connection,
    table_name: str,
    method: Method,
    kwargs: dict[str, Any],
) -> tuple[str, float] | None:
    """Look up a cache entry. Returns (value_json, fetched_at) or None."""
    where_parts: list[str] = []
    values: list[Any] = []
    for arg in method.args:
        kt = KNOWN_TYPES.resolve_type(arg.annotation)
        where_parts.append(f"{arg.name} = ?")
        values.append(kt.db.map_to(kwargs[arg.name]))

    where_clause = " AND ".join(where_parts)
    sql = f"SELECT value_json, fetched_at FROM {table_name} WHERE {where_clause}"
    cursor = conn.cursor()
    execute_sql(cursor, sql, tuple(values))
    row = cursor.fetchone()
    if row is None:
        return None
    return row[0], row[1]


def _cache_upsert(
    conn: sqlite3.Connection,
    table_name: str,
    method: Method,
    kwargs: dict[str, Any],
    value_json: str,
    fetched_at: float,
) -> None:
    """Insert or replace a cache entry."""
    col_names: list[str] = []
    values: list[Any] = []
    for arg in method.args:
        kt = KNOWN_TYPES.resolve_type(arg.annotation)
        col_names.append(arg.name)
        values.append(kt.db.map_to(kwargs[arg.name]))

    col_names.extend(["value_json", "fetched_at"])
    values.extend([value_json, fetched_at])

    placeholders = ", ".join(["?"] * len(values))
    cols = ", ".join(col_names)
    sql = f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})"
    cursor = conn.cursor()
    execute_sql(cursor, sql, tuple(values))
    conn.commit()


def _serialize_return_value(value: Any, return_type: Any) -> str:
    """Serialize a return value to JSON string."""
    if isinstance(value, BaseModel):
        return value.model_dump_json()
    return json.dumps(value)


def _deserialize_return_value(value_json: str, return_type: Any) -> Any:
    """Deserialize a JSON string back to the return type."""
    if return_type is not None and isinstance(return_type, type) and issubclass(return_type, BaseModel):
        return return_type.model_validate_json(value_json)
    return json.loads(value_json)


def _build_sync_wrapper(
    method: Method,
    table_name: str,
    db_path: Path,
    min_ttl_seconds: float,
    max_ttl_seconds: float,
) -> Callable[..., Any]:
    """Build a sync wrapper callable for a cached method."""

    def wrapper(**kwargs: Any) -> Any:
        with open_sqlite_db(db_path) as conn:
            cached = _cache_lookup(conn, table_name, method, kwargs)
            now = time.time()

            if cached is not None:
                value_json, fetched_at = cached
                age = now - fetched_at

                if age < min_ttl_seconds:
                    return _deserialize_return_value(value_json, method.return_annotation)

                if age < max_ttl_seconds:
                    # Probabilistic refresh
                    p = (age - min_ttl_seconds) / (max_ttl_seconds - min_ttl_seconds)
                    if random.random() >= p:
                        return _deserialize_return_value(value_json, method.return_annotation)
                    # Try refresh, fall back to stale on failure
                    try:
                        result = method.o(**kwargs)
                        result_json = _serialize_return_value(result, method.return_annotation)
                        _cache_upsert(conn, table_name, method, kwargs, result_json, time.time())
                        return result
                    except Exception:
                        return _deserialize_return_value(value_json, method.return_annotation)

            # Cache miss or expired past max_ttl
            result = method.o(**kwargs)
            result_json = _serialize_return_value(result, method.return_annotation)
            _cache_upsert(conn, table_name, method, kwargs, result_json, time.time())
            return result

    return wrapper


class CacheRegistry:
    """
    Loads cache config, validates rules, creates tables, builds wrappers,
    and installs them on a nested namespace.
    """

    cached: Namespace
    db_path: Path

    def __init__(self, config: CacheConfig, config_dir: Path) -> None:
        self.cached = Namespace()

        if config.cache_db is not None:
            self.db_path = config_dir / config.cache_db
        else:
            self.db_path = config_dir / "cache.db"

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        for rule in config.rules:
            self._register_rule(rule)

    def _register_rule(self, rule: CacheRule) -> None:
        gref = GlobalRef(rule.gref)
        method = Method(gref)

        method.validate_simple_type_args()

        namespace_path = rule.namespace_path if rule.namespace_path is not None else gref.name
        tbl_name = table_name_from_path(namespace_path)

        ddl = generate_cache_table_ddl(tbl_name, method)
        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(cursor, ddl)
            conn.commit()

        min_ttl_s = rule.min_ttl * DAYS_TO_SECONDS
        max_ttl_s = rule.max_ttl * DAYS_TO_SECONDS

        if gref.is_async():
            wrapper = _build_async_wrapper(method, tbl_name, self.db_path, min_ttl_s, max_ttl_s)
        else:
            wrapper = _build_sync_wrapper(method, tbl_name, self.db_path, min_ttl_s, max_ttl_s)

        self.cached._install(namespace_path, wrapper)

    @classmethod
    def from_yaml(cls, config_path: Path) -> CacheRegistry:
        """Load a CacheRegistry from a YAML config file."""
        raw = yaml.safe_load(config_path.read_text())
        config = CacheConfig.model_validate(raw)
        return cls(config, config_dir=config_path.parent)
```

Note: `_build_async_wrapper` is referenced but implemented in the next task.

- [ ] **Step 4: Add a stub for `_build_async_wrapper` so the module loads**

Add this placeholder right after `_build_sync_wrapper`:

```python
def _build_async_wrapper(
    method: Method,
    table_name: str,
    db_path: Path,
    min_ttl_seconds: float,
    max_ttl_seconds: float,
) -> Callable[..., Any]:
    """Build an async wrapper callable for a cached method. Implemented in Task 7."""
    raise NotImplementedError
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest -s tests/test_cached.py::test_sync_wrapper_miss_fetches_and_caches tests/test_cached.py::test_sync_wrapper_expired_refetches -v`
Expected: Both PASS

- [ ] **Step 6: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero warnings, all tests pass

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/compose/cached.py tests/test_cached.py
git commit -m "feat: add CacheRegistry with sync wrapper and cache lookup/upsert"
```

---

## Task 7: Async wrapper

**Files:**
- Modify: `src/lythonic/compose/cached.py`
- Modify: `tests/test_cached.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cached.py`:

```python
import asyncio

async def test_async_wrapper_miss_fetches_and_caches():
    from lythonic.compose.cached import CacheRegistry, CacheConfig, CacheRule
    from lythonic import GlobalRef

    call_count = 0
    async def fake_async_fetch(ticker: str) -> dict:
        nonlocal call_count
        call_count += 1
        return {"price": 200.0, "ticker": ticker}

    import tests.test_cached as this_module
    this_module._fake_async_fetch = fake_async_fetch  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref=GlobalRef("tests.test_cached:_fake_async_fetch"),
                    namespace_path="async_market.fetch",
                    min_ttl=1.0,
                    max_ttl=2.0,
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))

        result = await registry.cached.async_market.fetch(ticker="GOOG")
        assert result == {"price": 200.0, "ticker": "GOOG"}
        assert call_count == 1

        result2 = await registry.cached.async_market.fetch(ticker="GOOG")
        assert result2 == {"price": 200.0, "ticker": "GOOG"}
        assert call_count == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest -s tests/test_cached.py::test_async_wrapper_miss_fetches_and_caches -v`
Expected: FAIL — `NotImplementedError` from the stub

- [ ] **Step 3: Implement `_build_async_wrapper`**

Replace the stub in `src/lythonic/compose/cached.py`:

```python
def _build_async_wrapper(
    method: Method,
    table_name: str,
    db_path: Path,
    min_ttl_seconds: float,
    max_ttl_seconds: float,
) -> Callable[..., Any]:
    """Build an async wrapper callable for a cached method."""

    async def wrapper(**kwargs: Any) -> Any:
        with open_sqlite_db(db_path) as conn:
            cached = _cache_lookup(conn, table_name, method, kwargs)
            now = time.time()

            if cached is not None:
                value_json, fetched_at = cached
                age = now - fetched_at

                if age < min_ttl_seconds:
                    return _deserialize_return_value(value_json, method.return_annotation)

                if age < max_ttl_seconds:
                    p = (age - min_ttl_seconds) / (max_ttl_seconds - min_ttl_seconds)
                    if random.random() >= p:
                        return _deserialize_return_value(value_json, method.return_annotation)
                    try:
                        result = await method.o(**kwargs)
                        result_json = _serialize_return_value(result, method.return_annotation)
                        _cache_upsert(conn, table_name, method, kwargs, result_json, time.time())
                        return result
                    except Exception:
                        return _deserialize_return_value(value_json, method.return_annotation)

        # Cache miss or expired past max_ttl
        result = await method.o(**kwargs)
        result_json = _serialize_return_value(result, method.return_annotation)
        with open_sqlite_db(db_path) as conn:
            _cache_upsert(conn, table_name, method, kwargs, result_json, time.time())
        return result

    return wrapper
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest -s tests/test_cached.py::test_async_wrapper_miss_fetches_and_caches -v`
Expected: PASS

- [ ] **Step 5: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero warnings, all tests pass

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/cached.py tests/test_cached.py
git commit -m "feat: add async wrapper for cached methods"
```

---

## Task 8: Pydantic BaseModel return type support

**Files:**
- Modify: `tests/test_cached.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cached.py`:

```python
def test_pydantic_return_type_cached():
    from lythonic.compose.cached import CacheRegistry, CacheConfig, CacheRule
    from lythonic import GlobalRef
    from pydantic import BaseModel as PydanticBaseModel

    class PriceResult(PydanticBaseModel):
        ticker: str
        price: float

    import tests.test_cached as this_module
    this_module.PriceResult = PriceResult  # pyright: ignore

    def fetch_typed(ticker: str) -> PriceResult:
        return PriceResult(ticker=ticker, price=42.0)

    this_module._fetch_typed = fetch_typed  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref=GlobalRef("tests.test_cached:_fetch_typed"),
                    namespace_path="typed_fetch",
                    min_ttl=1.0,
                    max_ttl=2.0,
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))

        result = registry.cached.typed_fetch(ticker="MSFT")
        assert isinstance(result, PriceResult)
        assert result.ticker == "MSFT"
        assert result.price == 42.0

        # From cache
        result2 = registry.cached.typed_fetch(ticker="MSFT")
        assert isinstance(result2, PriceResult)
        assert result2.ticker == "MSFT"
```

- [ ] **Step 2: Run test to verify it passes**

Run: `uv run pytest -s tests/test_cached.py::test_pydantic_return_type_cached -v`
Expected: PASS (the implementation already handles BaseModel via `_serialize_return_value` / `_deserialize_return_value`)

If it fails, debug and fix the serialization logic.

- [ ] **Step 3: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero warnings, all tests pass

- [ ] **Step 4: Commit**

```bash
git add tests/test_cached.py
git commit -m "test: add Pydantic return type caching test"
```

---

## Task 9: `from_yaml` integration test and YAML file loading

**Files:**
- Modify: `tests/test_cached.py`

- [ ] **Step 1: Write the integration test**

Add to `tests/test_cached.py`:

```python
def test_from_yaml_file():
    from lythonic.compose.cached import CacheRegistry
    from lythonic import GlobalRef
    from textwrap import dedent

    call_count = 0
    def yaml_fetch(code: str) -> dict:
        nonlocal call_count
        call_count += 1
        return {"code": code, "value": 999}

    import tests.test_cached as this_module
    this_module._yaml_fetch = yaml_fetch  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        config_path = tmp_path / "cache.yaml"
        config_path.write_text(dedent("""
            rules:
              - gref: "tests.test_cached:_yaml_fetch"
                namespace_path: "codes.lookup"
                min_ttl: 1.0
                max_ttl: 5.0
        """).strip())

        registry = CacheRegistry.from_yaml(config_path)

        result = registry.cached.codes.lookup(code="ABC")
        assert result == {"code": "ABC", "value": 999}
        assert call_count == 1

        result2 = registry.cached.codes.lookup(code="ABC")
        assert result2 == {"code": "ABC", "value": 999}
        assert call_count == 1
```

- [ ] **Step 2: Run test**

Run: `uv run pytest -s tests/test_cached.py::test_from_yaml_file -v`
Expected: PASS

- [ ] **Step 3: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero warnings, all tests pass

- [ ] **Step 4: Commit**

```bash
git add tests/test_cached.py
git commit -m "test: add YAML file loading integration test"
```

---

## Task 10: Probabilistic refresh test

**Files:**
- Modify: `tests/test_cached.py`

- [ ] **Step 1: Write the test**

Add to `tests/test_cached.py`:

```python
def test_probabilistic_refresh_between_ttls():
    """Between min_ttl and max_ttl, refresh probability increases with age."""
    from lythonic.compose.cached import CacheRegistry, CacheConfig, CacheRule
    from lythonic import GlobalRef
    import sqlite3

    call_count = 0
    def prob_fetch(key: str) -> dict:
        nonlocal call_count
        call_count += 1
        return {"v": call_count}

    import tests.test_cached as this_module
    this_module._prob_fetch = prob_fetch  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref=GlobalRef("tests.test_cached:_prob_fetch"),
                    namespace_path="prob",
                    min_ttl=1.0,   # 1 day
                    max_ttl=3.0,   # 3 days
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))

        # Initial fetch
        registry.cached.prob(key="A")
        assert call_count == 1

        # Set fetched_at to exactly min_ttl ago (p=0, no refresh)
        db_path = Path(tmp) / "cache.db"
        min_ttl_secs = 1.0 * 86400
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "UPDATE prob SET fetched_at = ? WHERE key = ?",
                (time.time() - min_ttl_secs, "A"),
            )
            conn.commit()

        # With p=0 at exactly min_ttl, random.random() >= 0 is always true,
        # so no refresh should happen. But p=0 means random() >= 0 is always true...
        # Actually p=0 means we never refresh. random() >= 0 is always True, so
        # the condition `random.random() >= p` means "don't refresh".
        # Wait — the code says `if random.random() >= p: return stale`.
        # At p=0, random() >= 0 is always True, so always return stale. Correct.

        # Set fetched_at to almost max_ttl ago (p ~= 1, almost certain refresh)
        almost_max_secs = 2.99 * 86400
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "UPDATE prob SET fetched_at = ? WHERE key = ?",
                (time.time() - almost_max_secs, "A"),
            )
            conn.commit()

        # With p ~= 1, should almost certainly refresh
        # Run multiple times to confirm refresh happens
        refreshed = False
        for _ in range(20):
            before = call_count
            registry.cached.prob(key="A")
            if call_count > before:
                refreshed = True
                break
        assert refreshed
```

- [ ] **Step 2: Run test**

Run: `uv run pytest -s tests/test_cached.py::test_probabilistic_refresh_between_ttls -v`
Expected: PASS

- [ ] **Step 3: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero warnings, all tests pass

- [ ] **Step 4: Commit**

```bash
git add tests/test_cached.py
git commit -m "test: add probabilistic refresh test between min_ttl and max_ttl"
```

---

## Task 11: Default namespace path (no namespace_path configured)

**Files:**
- Modify: `tests/test_cached.py`

- [ ] **Step 1: Write the test**

Add to `tests/test_cached.py`:

```python
def test_default_namespace_path_uses_function_name():
    """When namespace_path is None, uses the original function name at root."""
    from lythonic.compose.cached import CacheRegistry, CacheConfig, CacheRule
    from lythonic import GlobalRef

    def my_download(tag: str) -> dict:
        return {"tag": tag}

    import tests.test_cached as this_module
    this_module._my_download = my_download  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref=GlobalRef("tests.test_cached:_my_download"),
                    min_ttl=1.0,
                    max_ttl=2.0,
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))

        result = registry.cached._my_download(tag="hello")
        assert result == {"tag": "hello"}
```

- [ ] **Step 2: Run test**

Run: `uv run pytest -s tests/test_cached.py::test_default_namespace_path_uses_function_name -v`
Expected: PASS

- [ ] **Step 3: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero warnings, all tests pass

- [ ] **Step 4: Commit**

```bash
git add tests/test_cached.py
git commit -m "test: verify default namespace path uses function name"
```

---

## Task 12: Final cleanup and full verification

**Files:**
- All modified files

- [ ] **Step 1: Run full lint and test suite**

Run: `make`
Expected: Zero warnings, all tests pass

- [ ] **Step 2: Review all changes**

Run: `git diff main --stat` and `git log --oneline main..HEAD`
Verify all files match the design spec.

- [ ] **Step 3: Final commit if any cleanup needed**

Only if lint or tests required fixes:

```bash
git add -u
git commit -m "chore: final cleanup for cache wrapper"
```
