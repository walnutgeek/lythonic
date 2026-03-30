"""
Namespace Config: Pydantic models for persisting Namespace structure.

Defines the configuration schema for callables, cached callables, and DAGs.
Format-agnostic — use `model_dump()`/`model_validate()` with any serializer.

Provides `load_namespace()` to build a live Namespace from config,
`dump_namespace()` to serialize a Namespace back to config, and
`validate_config()` for config-only validation.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, model_validator

if TYPE_CHECKING:
    from lythonic.compose.namespace import Namespace


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


def load_namespace(config: NamespaceConfig, config_dir: Path) -> Namespace:
    """
    Build a live `Namespace` from a `NamespaceConfig`. Two-pass: first
    registers all callables (plain and cached), then builds DAGs that
    reference them. Declaration order in config does not matter.
    """
    from lythonic.compose.namespace import Dag, Namespace

    # Check for duplicate nsrefs
    seen: set[str] = set()
    for entry in config.entries:
        if entry.nsref in seen:
            raise ValueError(f"Duplicate nsref: '{entry.nsref}'")
        seen.add(entry.nsref)

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

        # Store DAG config for round-trip serialization
        dag_node = ns.get(entry.nsref)
        dag_entry_config = DagEntryConfig(
            nodes=sorted(
                [
                    DagNodeConfig(
                        label=dn.label,
                        nsref=dn.ns_node.nsref,
                        after=sorted([e.upstream for e in dag.edges if e.downstream == dn.label]),
                    )
                    for dn in dag.nodes.values()
                ],
                key=lambda n: n.label,
            )
        )
        dag_node.metadata["dag"] = dag_entry_config

    return ns
