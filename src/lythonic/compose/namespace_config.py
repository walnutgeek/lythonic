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

    - `gref` set, no `dag` -> callable (plain or cached)
    - `dag` set, no `gref` -> DAG entry
    - Both or neither -> validation error
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
