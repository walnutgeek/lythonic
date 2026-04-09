"""
lyth: CLI for the lythonic compose engine.

Commands:
    lyth start          Start the engine (triggers, poll loop)
    lyth stop           Stop a running engine instance
    lyth run <nsref>    Run a DAG or callable once
    lyth fire <trigger> Fire a trigger manually
    lyth status         Show trigger and run status
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from lythonic import GlobalRef
from lythonic.compose.cli import ActionTree, RunContext
from lythonic.compose.engine import EngineConfig


class LythMain(BaseModel):
    """lyth — lythonic compose engine CLI"""

    help: bool = Field(default=False, description="Show help")
    data: str = Field(default="./data", description="Data directory")
    config: str = Field(default="", description="Config file (default: {data}/lyth.yaml)")


def _resolve_config(ctx: RunContext) -> tuple[Path, EngineConfig]:
    """Load EngineConfig from the config file, resolving paths relative to data dir."""
    root: LythMain = ctx.path.get("/")  # pyright: ignore[reportAssignmentType]
    data_dir = Path(root.data)
    config_path = Path(root.config) if root.config else data_dir / "lyth.yaml"

    if not config_path.exists():
        ctx.run_result.print(f"Config file not found: {config_path}")
        sys.exit(1)

    raw = yaml.safe_load(config_path.read_text())
    engine_config = EngineConfig.model_validate(raw)

    # Apply storage defaults relative to data dir
    storage = engine_config.storage
    if storage.cache_db is None:
        storage.cache_db = data_dir / "cache.db"
    elif not storage.cache_db.is_absolute():
        storage.cache_db = data_dir / storage.cache_db
    if storage.dag_db is None:
        storage.dag_db = data_dir / "dags.db"
    elif not storage.dag_db.is_absolute():
        storage.dag_db = data_dir / storage.dag_db
    if storage.trigger_db is None:
        storage.trigger_db = data_dir / "triggers.db"
    elif not storage.trigger_db.is_absolute():
        storage.trigger_db = data_dir / storage.trigger_db
    if storage.log_file is None:
        storage.log_file = data_dir / "lyth.log"
    elif not storage.log_file.is_absolute():
        storage.log_file = data_dir / storage.log_file

    return data_dir, engine_config


def _setup_file_logging(log_file: Path) -> None:
    """Set up file logging with NodeRunLogFilter and context-aware formatter."""
    from lythonic.compose.log_context import NodeRunLogFilter

    log_file.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_file)
    handler.addFilter(NodeRunLogFilter())
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)-8s [%(name)s] run=%(run_id)s node=%(node_label)s %(message)s"
        )
    )
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)


def _build_namespace(engine_config: EngineConfig) -> Any:
    """Build a live Namespace from EngineConfig."""
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    for entry in engine_config.namespace:
        if entry.gref is not None:
            gref = GlobalRef(str(entry.gref))
            ns.register(gref.get_instance(), nsref=entry.nsref, config=entry)
    return ns


def _pid_file(data_dir: Path) -> Path:
    return data_dir / "lyth.pid"


# CLI setup

main = ActionTree(LythMain)


@main.actions.wrap
def start(ctx: RunContext) -> None:
    """Start the engine: load config, activate triggers, run poll loop."""
    data_dir, engine_config = _resolve_config(ctx)
    data_dir.mkdir(parents=True, exist_ok=True)

    ns = _build_namespace(engine_config)
    storage = engine_config.storage

    # Set up file logging
    assert storage.log_file is not None
    _setup_file_logging(storage.log_file)
    ctx.run_result.print(f"Logging to {storage.log_file}")

    # Write PID file
    pid_path = _pid_file(data_dir)
    pid_path.write_text(str(os.getpid()))

    ctx.run_result.print(f"Starting lyth engine (pid={os.getpid()}, data={data_dir})")

    async def _run() -> None:
        from lythonic.compose.dag_provenance import DagProvenance
        from lythonic.compose.trigger import TriggerManager, TriggerStore

        assert storage.trigger_db is not None
        assert storage.dag_db is not None

        trigger_store = TriggerStore(storage.trigger_db)
        provenance = DagProvenance(storage.dag_db)
        manager = TriggerManager(namespace=ns, store=trigger_store, provenance=provenance)

        # Activate all triggers from node configs
        for node in ns._all_leaves():  # pyright: ignore[reportPrivateUsage]
            for tc in node.config.triggers:
                manager.activate(tc.name)
                ctx.run_result.print(f"  Activated trigger: {tc.name} ({tc.type})")

        manager.start()
        ctx.run_result.print("Poll loop started. Press Ctrl+C to stop.")

        # Handle SIGTERM for graceful shutdown
        shutdown = asyncio.Event()

        def _handle_signal() -> None:
            ctx.run_result.print("\nShutting down...")
            manager.stop()
            shutdown.set()

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, _handle_signal)
        loop.add_signal_handler(signal.SIGINT, _handle_signal)

        await shutdown.wait()

    try:
        asyncio.run(_run())
    finally:
        pid_path = _pid_file(data_dir)
        if pid_path.exists():
            pid_path.unlink()

    ctx.run_result.success = True


@main.actions.wrap
def stop(ctx: RunContext) -> None:
    """Stop a running lyth engine instance."""
    root: LythMain = ctx.path.get("/")  # pyright: ignore[reportAssignmentType]
    data_dir = Path(root.data)
    pid_path = _pid_file(data_dir)

    if not pid_path.exists():
        ctx.run_result.print("No running instance found (no PID file)")
        return

    pid = int(pid_path.read_text().strip())
    ctx.run_result.print(f"Sending SIGTERM to process {pid}")

    try:
        os.kill(pid, signal.SIGTERM)
        ctx.run_result.success = True
    except ProcessLookupError:
        ctx.run_result.print(f"Process {pid} not found, removing stale PID file")
        pid_path.unlink()


@main.actions.wrap
def run(ctx: RunContext, nsref: str) -> None:
    """Run a DAG or callable once."""
    _, engine_config = _resolve_config(ctx)
    ns = _build_namespace(engine_config)

    try:
        node = ns.get(nsref)
    except KeyError:
        ctx.run_result.print(f"'{nsref}' not found in namespace")
        return

    async def _run() -> None:
        result = await node()
        ctx.run_result.print(json.dumps(result.model_dump(), indent=2, default=str))

    asyncio.run(_run())
    ctx.run_result.success = True


@main.actions.wrap
def fire(ctx: RunContext, trigger_name: str) -> None:
    """Fire a trigger manually."""
    _, engine_config = _resolve_config(ctx)
    ns = _build_namespace(engine_config)
    storage = engine_config.storage

    async def _fire() -> None:
        from lythonic.compose.dag_provenance import DagProvenance
        from lythonic.compose.trigger import TriggerManager, TriggerStore

        assert storage.trigger_db is not None
        assert storage.dag_db is not None

        trigger_store = TriggerStore(storage.trigger_db)
        provenance = DagProvenance(storage.dag_db)
        manager = TriggerManager(namespace=ns, store=trigger_store, provenance=provenance)
        manager.activate(trigger_name)

        result = await manager.fire(trigger_name)
        ctx.run_result.print(f"Status: {result.status}")
        ctx.run_result.print(f"Run ID: {result.run_id}")
        if result.outputs:
            ctx.run_result.print(f"Outputs: {json.dumps(result.outputs, default=str)}")
        if result.error:
            ctx.run_result.print(f"Error: {result.error}")

    asyncio.run(_fire())
    ctx.run_result.success = True


@main.actions.wrap
def status(ctx: RunContext) -> None:
    """Show trigger and run status."""
    data_dir, engine_config = _resolve_config(ctx)
    storage = engine_config.storage

    assert storage.trigger_db is not None
    assert storage.dag_db is not None

    # Check PID
    pid_path = _pid_file(data_dir)
    if pid_path.exists():
        pid = pid_path.read_text().strip()
        ctx.run_result.print(f"Engine running (pid={pid})")
    else:
        ctx.run_result.print("Engine not running")

    # Show trigger activations

    if storage.trigger_db.exists():
        # Read all activations for a complete picture
        from lythonic.state import execute_sql, open_sqlite_db

        with open_sqlite_db(storage.trigger_db) as conn:
            cursor = conn.cursor()
            execute_sql(
                cursor,
                "SELECT name, trigger_type, status, last_run_at, last_run_id FROM trigger_activations",
            )
            cols = [d[0] for d in cursor.description]
            rows = [dict(zip(cols, row, strict=False)) for row in cursor.fetchall()]

        if rows:
            ctx.run_result.print(f"\nTriggers ({len(rows)}):")
            for row in rows:
                last = f", last_run={row['last_run_id'][:8]}..." if row.get("last_run_id") else ""
                last_at = ""
                if row.get("last_run_at"):
                    import datetime

                    dt = datetime.datetime.fromtimestamp(row["last_run_at"], tz=datetime.UTC)
                    last_at = f" at {dt:%Y-%m-%d %H:%M:%S}"
                ctx.run_result.print(
                    f"  {row['name']} [{row['trigger_type']}] {row['status']}{last_at}{last}"
                )
        else:
            ctx.run_result.print("\nNo triggers configured")
    else:
        ctx.run_result.print("\nNo trigger database found")

    ctx.run_result.success = True


def cli_main() -> None:
    """Entry point for the lyth CLI."""
    result = main.run_args(sys.argv)
    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    cli_main()
