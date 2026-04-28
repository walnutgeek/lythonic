"""
Engine: Runtime configuration for the compose execution environment.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel

from lythonic.compose.namespace import NsNodeConfig


class LogConfig(BaseModel):
    """Logging configuration. Call `setup_logging()` to activate."""

    log_file: Path | None = None
    log_level: str = "DEBUG"
    loggers: dict[str, str] = {}

    def setup_logging(self) -> None:
        """Activate file logging if log_file is configured. Global (root logger)."""
        if self.log_file is None:
            return

        import logging

        from lythonic.compose.log_context import NodeRunLogFilter

        self.log_file.parent.mkdir(parents=True, exist_ok=True)

        handler = logging.FileHandler(self.log_file)
        handler.addFilter(NodeRunLogFilter())
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-8s [%(name)s] run=%(run_id)s node=%(node_label)s %(message)s"
            )
        )

        root = logging.getLogger()
        root.addHandler(handler)
        root.setLevel(getattr(logging, self.log_level.upper(), logging.DEBUG))

        for logger_name, level_name in self.loggers.items():
            logging.getLogger(logger_name).setLevel(
                getattr(logging, level_name.upper(), logging.DEBUG)
            )


class StorageConfig(LogConfig):
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
