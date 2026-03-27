"""
Cached method wrappers with YAML-configured TTL-based caching.

Wraps sync/async download methods with SQLite-backed caching.
Methods are exposed through a nested namespace object with attribute access.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel

from lythonic import GRef
from lythonic.compose import Method
from lythonic.types import KNOWN_TYPES


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
