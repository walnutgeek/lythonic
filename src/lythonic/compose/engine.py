"""
Engine: Runtime configuration for the compose execution environment.
"""

from __future__ import annotations

from pydantic import BaseModel


class StorageConfig(BaseModel):
    """Storage paths for cache DB and DAG provenance DB."""

    cache_db: str | None = None
    dag_db: str | None = None
