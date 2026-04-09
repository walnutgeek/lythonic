"""
Namespace Config: Pydantic models for persisting Namespace structure.

.. deprecated::
    This module is obsolete. Use `NsNodeConfig` on `NamespaceNode` for
    structured config and `StorageConfig` from `lythonic.compose.engine`
    for storage paths. DAGs are now code-defined via `@dag_factory`.
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
    tags: list[str] | None = None

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

            node = register_cached_callable(
                ns,
                gref=entry.gref,
                nsref=entry.nsref,
                min_ttl=entry.cache.min_ttl,
                max_ttl=entry.cache.max_ttl,
                db_path=cache_db,
            )
            if entry.tags:
                node.config.tags = list(entry.tags)
        else:
            ns.register(entry.gref, nsref=entry.nsref, tags=entry.tags)

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

        ns.register(dag, nsref=entry.nsref, tags=entry.tags)

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


def validate_config(config: NamespaceConfig) -> list[str]:
    """
    Validate config without building a Namespace. Returns a list of
    error messages (empty if valid).
    """
    from lythonic import GlobalRef
    from lythonic.compose import Method

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
                    errors.append(f"Entry '{entry.nsref}': duplicate label '{node_cfg.label}'")
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


def dump_namespace(ns: Namespace, storage: StorageConfig | None = None) -> NamespaceConfig:
    """
    Export a live `Namespace` to a `NamespaceConfig`. Entries sorted
    alphabetically by nsref. DAG nodes sorted by label.
    """

    entries: list[NamespaceEntryConfig] = []

    for node in ns._all_leaves():  # pyright: ignore[reportPrivateUsage]
        if "dag" in node.metadata:
            dag_info = node.metadata["dag"]
            entries.append(
                NamespaceEntryConfig(
                    nsref=node.nsref,
                    dag=dag_info,
                    tags=sorted(node.tags) if node.tags else None,
                )
            )
        elif "cache" in node.metadata:
            cache_info = node.metadata["cache"]
            entries.append(
                NamespaceEntryConfig(
                    nsref=node.nsref,
                    gref=str(node.method.gref),
                    cache=CacheEntryConfig(**cache_info),
                    tags=sorted(node.tags) if node.tags else None,
                )
            )
        else:
            entries.append(
                NamespaceEntryConfig(
                    nsref=node.nsref,
                    gref=str(node.method.gref),
                    tags=sorted(node.tags) if node.tags else None,
                )
            )

    entries.sort(key=lambda e: e.nsref)

    return NamespaceConfig(
        storage=storage or StorageConfig(),
        entries=entries,
    )
