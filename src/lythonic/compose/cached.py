"""
Cached: YAML-configured caching layer for download methods.

Wraps sync or async methods that return `dict` or Pydantic `BaseModel` with
SQLite-backed caching. Each cached method gets its own table with typed
parameter columns (derived from the method signature) and a composite
primary key.

## Configuration

Define cache rules in a YAML file (default: `./data/compose/cached/cache.yaml`):

```yaml
rules:
  - gref: "myapp.downloads:fetch_prices"
    namespace_path: "market.fetch_prices"
    min_ttl: 0.5    # days, fresh period
    max_ttl: 2.0    # days, hard expiry

  - gref: "myapp.downloads:get_exchange_rate"
    min_ttl: 0.25
    max_ttl: 1.0
```

## Usage

```python
from pathlib import Path
from lythonic.compose.cached import CacheRegistry

registry = CacheRegistry.from_yaml(Path("./data/compose/cached/cache.yaml"))

# Sync method — served from cache when fresh
result = registry.cached.market.fetch_prices(ticker="AAPL")

# Async method — awaited on cache miss or hard expiry
rate = await registry.cached.get_exchange_rate(from_currency="USD", to_currency="EUR")
```

## TTL Behavior

- **age < `min_ttl`**: return cached value (fresh)
- **`min_ttl` <= age < `max_ttl`**: probabilistic refresh — probability increases
  linearly from 0 to 1. On refresh failure, returns stale value.
- **age >= `max_ttl`** or cache miss: call original method. On failure, raises.

## Validation

All method parameters must have types registered as `simple_type` in
`KNOWN_TYPES` (primitives, date, datetime, Path). Validated at config load
time via `Method.validate_simple_type_args()`.

## Pushback

When a cached method raises `CacheRefreshPushback(days, namespace_prefix)`, all
probabilistic refreshes matching the scope are suppressed for the given duration.
If `namespace_prefix` is omitted, only the raising method is suppressed.

- During the probabilistic window with active pushback: returns stale data.
- Past `max_ttl` with active pushback: raises `CacheRefreshSuppressed`.
- Cache miss: always calls the method regardless of pushback.

## Namespace

Wrapped methods are installed on a `Namespace` object with nested attribute
access. Dot-separated `namespace_path` values create intermediate objects
(e.g., `"market.fetch_prices"` becomes `cached.market.fetch_prices(...)`).
When `namespace_path` is omitted, the original function name is used at root.
"""

from __future__ import annotations

import json
import random
import sqlite3
import time
import typing
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

from lythonic import GlobalRef, GRef
from lythonic.compose import Method
from lythonic.state import execute_sql, open_sqlite_db
from lythonic.types import KNOWN_TYPES

DAYS_TO_SECONDS = 86400.0


class CacheRefreshPushback(Exception):
    """
    Raise from a cached method to suppress probabilistic refreshes.
    Defaults to suppressing only the raising method; set `namespace_prefix`
    to suppress a group of methods.
    """

    days: float
    namespace_prefix: str | None

    def __init__(self, days: float, namespace_prefix: str | None = None):
        super().__init__(f"Cache refresh pushback for {days} days")
        self.days = days
        self.namespace_prefix = namespace_prefix


class CacheRefreshSuppressed(Exception):
    """
    Raised when a cache entry is past max_ttl but refresh is suppressed
    by an active pushback.
    """

    namespace_path: str
    suppressed_until: float

    def __init__(self, namespace_path: str, suppressed_until: float):
        super().__init__(f"Cache refresh suppressed for {namespace_path} until {suppressed_until}")
        self.namespace_path = namespace_path
        self.suppressed_until = suppressed_until


def _pushback_set(conn: sqlite3.Connection, namespace_prefix: str, suppressed_until: float) -> None:
    """Replace any existing pushback with a new one (single-row table)."""
    cursor = conn.cursor()
    execute_sql(cursor, "DELETE FROM _pushback")
    execute_sql(
        cursor,
        "INSERT INTO _pushback (namespace_prefix, suppressed_until) VALUES (?, ?)",
        (namespace_prefix, suppressed_until),
    )
    conn.commit()


def _pushback_check(conn: sqlite3.Connection, namespace_path: str) -> float | None:
    """
    Check if a pushback is active for the given namespace path.
    Returns `suppressed_until` timestamp if suppressed, `None` otherwise.
    Clears expired entries before checking.
    """
    cursor = conn.cursor()
    execute_sql(cursor, "DELETE FROM _pushback WHERE suppressed_until <= ?", (time.time(),))
    execute_sql(cursor, "SELECT namespace_prefix, suppressed_until FROM _pushback")
    row = cursor.fetchone()
    if row is None:
        return None
    prefix: str = row[0]
    suppressed_until: float = row[1]
    if namespace_path == prefix or namespace_path.startswith(prefix + "."):
        return suppressed_until
    return None


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

    def install(self, path: str, func: Callable[..., Any]) -> None:
        parts = path.split(".")
        current: Namespace = self
        for part in parts[:-1]:
            if not hasattr(current, part):
                setattr(current, part, Namespace())
            current = getattr(current, part)
        setattr(current, parts[-1], func)


def generate_cache_table_ddl(table_name: str, method: Method) -> str:
    """
    Generate CREATE TABLE DDL for a cache table based on the method's
    parameter types.
    """
    columns: list[str] = []
    param_names: list[str] = []

    for arg in method.args:
        assert arg.annotation is not None
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
        assert arg.annotation is not None
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
        assert arg.annotation is not None
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


def _resolve_return_type(method: Method) -> type | None:
    """Resolve the return type annotation of a method, handling string annotations."""
    try:
        hints = typing.get_type_hints(method.o)
        return hints.get("return")
    except Exception:
        rt = method.return_annotation
        if isinstance(rt, type):
            return rt
        return None


def _serialize_return_value(value: Any, return_type: Any) -> str:  # pyright: ignore[reportUnusedParameter]
    """Serialize a return value to JSON string."""
    if isinstance(value, BaseModel):
        return value.model_dump_json()
    return json.dumps(value)


def _deserialize_return_value(value_json: str, return_type: Any) -> Any:
    """Deserialize a JSON string back to the return type."""
    if (
        return_type is not None
        and isinstance(return_type, type)
        and issubclass(return_type, BaseModel)
    ):
        return return_type.model_validate_json(value_json)
    return json.loads(value_json)


def _build_sync_wrapper(
    method: Method,
    table_name: str,
    db_path: Path,
    min_ttl_seconds: float,
    max_ttl_seconds: float,
    namespace_path: str,
) -> Callable[..., Any]:
    """Build a sync wrapper callable for a cached method."""
    return_type = _resolve_return_type(method)

    def wrapper(**kwargs: Any) -> Any:
        with open_sqlite_db(db_path) as conn:
            cached = _cache_lookup(conn, table_name, method, kwargs)
            now = time.time()

            if cached is not None:
                value_json, fetched_at = cached
                age = now - fetched_at

                if age < min_ttl_seconds:
                    return _deserialize_return_value(value_json, return_type)

                if age < max_ttl_seconds:
                    # Probabilistic refresh: probability increases linearly from 0 to 1
                    p = (age - min_ttl_seconds) / (max_ttl_seconds - min_ttl_seconds)
                    if random.random() >= p:
                        return _deserialize_return_value(value_json, return_type)
                    if _pushback_check(conn, namespace_path):
                        return _deserialize_return_value(value_json, return_type)
                    try:
                        result = method.o(**kwargs)
                        result_json = _serialize_return_value(result, return_type)
                        _cache_upsert(conn, table_name, method, kwargs, result_json, time.time())
                        return result
                    except CacheRefreshPushback as e:
                        prefix = e.namespace_prefix or namespace_path
                        until = time.time() + e.days * DAYS_TO_SECONDS
                        _pushback_set(conn, prefix, until)
                        return _deserialize_return_value(value_json, return_type)
                    except Exception:
                        return _deserialize_return_value(value_json, return_type)

                # Past max_ttl with cached entry
                suppressed_until = _pushback_check(conn, namespace_path)
                if suppressed_until:
                    raise CacheRefreshSuppressed(namespace_path, suppressed_until)

            # Cache miss or expired past max_ttl (no pushback)
            result = method.o(**kwargs)
            result_json = _serialize_return_value(result, return_type)
            _cache_upsert(conn, table_name, method, kwargs, result_json, time.time())
            return result

    return wrapper


def _build_async_wrapper(
    method: Method,
    table_name: str,
    db_path: Path,
    min_ttl_seconds: float,
    max_ttl_seconds: float,
    namespace_path: str,
) -> Callable[..., Any]:
    """Build an async wrapper callable for a cached method."""
    return_type = _resolve_return_type(method)

    async def wrapper(**kwargs: Any) -> Any:
        with open_sqlite_db(db_path) as conn:
            cached = _cache_lookup(conn, table_name, method, kwargs)
            now = time.time()

            if cached is not None:
                value_json, fetched_at = cached
                age = now - fetched_at

                if age < min_ttl_seconds:
                    return _deserialize_return_value(value_json, return_type)

                if age < max_ttl_seconds:
                    p = (age - min_ttl_seconds) / (max_ttl_seconds - min_ttl_seconds)
                    if random.random() >= p:
                        return _deserialize_return_value(value_json, return_type)
                    if _pushback_check(conn, namespace_path):
                        return _deserialize_return_value(value_json, return_type)
                    try:
                        result = await method.o(**kwargs)
                        result_json = _serialize_return_value(result, return_type)
                        _cache_upsert(conn, table_name, method, kwargs, result_json, time.time())
                        return result
                    except CacheRefreshPushback as e:
                        prefix = e.namespace_prefix or namespace_path
                        until = time.time() + e.days * DAYS_TO_SECONDS
                        _pushback_set(conn, prefix, until)
                        return _deserialize_return_value(value_json, return_type)
                    except Exception:
                        return _deserialize_return_value(value_json, return_type)

                # Past max_ttl with cached entry
                suppressed_until = _pushback_check(conn, namespace_path)
                if suppressed_until:
                    raise CacheRefreshSuppressed(namespace_path, suppressed_until)

            # Cache miss or expired past max_ttl (no pushback)
            result = await method.o(**kwargs)
            result_json = _serialize_return_value(result, return_type)
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

        with open_sqlite_db(self.db_path) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "CREATE TABLE IF NOT EXISTS _pushback "
                "(namespace_prefix TEXT NOT NULL, suppressed_until REAL NOT NULL)",
            )
            conn.commit()

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
            wrapper = _build_async_wrapper(
                method, tbl_name, self.db_path, min_ttl_s, max_ttl_s, namespace_path
            )
        else:
            wrapper = _build_sync_wrapper(
                method, tbl_name, self.db_path, min_ttl_s, max_ttl_s, namespace_path
            )

        self.cached.install(namespace_path, wrapper)

    @classmethod
    def from_yaml(cls, config_path: Path) -> CacheRegistry:
        """Load a CacheRegistry from a YAML config file."""
        raw = yaml.safe_load(config_path.read_text())
        config = CacheConfig.model_validate(raw)
        return cls(config, config_dir=config_path.parent)
