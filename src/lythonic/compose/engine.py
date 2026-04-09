"""
Engine: Runtime configuration for the compose execution environment.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel

from lythonic.compose.namespace import NsNodeConfig


class StorageConfig(BaseModel):
    """Storage paths for cache DB and DAG provenance DB."""

    cache_db: Path | None = None
    dag_db: Path | None = None
    trigger_db: Path | None = None


class EngineConfig(BaseModel):
    storage: StorageConfig
    namespace: list[NsNodeConfig]
