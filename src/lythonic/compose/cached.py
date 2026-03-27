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
