"""
Engine: Runtime configuration for the compose execution environment.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel

from lythonic.compose.namespace import NsNodeConfig


class StorageConfig(BaseModel):
    """Storage paths for cache DB, DAG provenance DB, and trigger DB."""

    cache_db: Path | None = None
    dag_db: Path | None = None
    trigger_db: Path | None = None


class EngineConfig(BaseModel):
    """
    Top-level configuration for the lyth engine. Loaded from `lyth.yaml`.
    Storage paths default to `{data_dir}/{name}.db` when not specified.
    """

    storage: StorageConfig = StorageConfig()
    namespace: list[NsNodeConfig] = []
