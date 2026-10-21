"""Who started a run, or changed a trigger: `initiated_by` and `run_initiator`."""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import time
from contextlib import closing
from pathlib import Path

import tests.test_run_initiator as this_module
from lythonic.compose.dag_provenance import (
    SCHEDULER,
    DagProvenance,
    initiated_by,
    run_initiator,
)
from lythonic.compose.dag_runner import DagRunner
from lythonic.compose.namespace import Dag, Namespace, NsNodeConfig, TriggerConfig
from lythonic.compose.trigger import TriggerManager, TriggerStore


async def _split(text: str) -> list[str]:  # pyright: ignore[reportUnusedFunction]
    return text.split(",")


async def _upper(text: str) -> str:  # pyright: ignore[reportUnusedFunction]
    return text.upper()


async def _join(values: list[str]) -> str:  # pyright: ignore[reportUnusedFunction]
    return "|".join(values)


_fail_next = False


async def _flaky(text: str) -> str:  # pyright: ignore[reportUnusedFunction]
    if this_module._fail_next:  # pyright: ignore[reportPrivateUsage]
        this_module._fail_next = False  # pyright: ignore[reportPrivateUsage]
        raise RuntimeError("flaky")
    return text


_ticked: asyncio.Event | None = None
_seen_by_sync_node: list[str | None] = []


def _sync_tick() -> str:  # pyright: ignore[reportUnusedFunction]
    """Runs on an executor thread, so it checks that the context reaches it."""
    this_module._seen_by_sync_node.append(run_initiator.get())  # pyright: ignore[reportPrivateUsage]
    return "tick"


async def _signal_tick(value: str) -> str:  # pyright: ignore[reportUnusedFunction]
    if this_module._ticked is not None:  # pyright: ignore[reportPrivateUsage]
        this_module._ticked.set()  # pyright: ignore[reportPrivateUsage]
    return value


def _mapped_dag() -> Dag:
    ns = Namespace()
    ns.register(this_module._split, nsref="t:split")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._upper, nsref="t:upper")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._join, nsref="t:join")  # pyright: ignore[reportPrivateUsage]
    sub = Dag()
    sub.node(ns.get("t:upper"))
    parent = Dag()
    s = parent.node(ns.get("t:split"))
    m = parent.map(sub, label="mapped")
    j = parent.node(ns.get("t:join"))
    s >> m >> j  # pyright: ignore[reportUnusedExpression]
    return parent


def _flaky_dag() -> Dag:
    ns = Namespace()
    ns.register(this_module._flaky, nsref="t:flaky")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._upper, nsref="t:upper")  # pyright: ignore[reportPrivateUsage]
    dag = Dag()
    dag.node(ns.get("t:flaky")) >> dag.node(ns.get("t:upper"))  # pyright: ignore[reportUnusedExpression]
    return dag


async def test_run_records_its_initiator_and_children_inherit_it():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        prov = DagProvenance(Path(tmp) / "runs.db")
        runner = DagRunner(_mapped_dag(), provenance=prov)
        with initiated_by("usr_ann"):
            result = await runner.run(source_inputs={"split": {"text": "a,b"}}, dag_nsref="t:m")
        dag_run = prov.inspect_run(result.run_id)
        assert dag_run is not None and dag_run.initiated_by == "usr_ann"
        children = prov.get_child_runs(result.run_id)
        assert len(children) == 2
        assert {c.initiated_by for c in children} == {"usr_ann"}
        assert prov.get_run(result.run_id)["initiated_by"] == "usr_ann"  # pyright: ignore[reportOptionalSubscript]


async def test_run_without_initiator_records_none():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        prov = DagProvenance(Path(tmp) / "runs.db")
        result = await DagRunner(_mapped_dag(), provenance=prov).run(
            source_inputs={"split": {"text": "a"}}, dag_nsref="t:m"
        )
        [recent] = [r for r in prov.get_recent_runs() if r.run_id == result.run_id]
        assert recent.initiated_by is None


async def test_initiated_by_none_clears_an_outer_initiator():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        prov = DagProvenance(Path(tmp) / "runs.db")
        with initiated_by("usr_ann"), initiated_by(None):
            result = await DagRunner(_mapped_dag(), provenance=prov).run(
                source_inputs={"split": {"text": "a"}}, dag_nsref="t:m"
            )
        assert prov.get_run(result.run_id)["initiated_by"] is None  # pyright: ignore[reportOptionalSubscript]


async def test_replay_is_attributed_to_its_caller_and_restart_keeps_the_original():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        prov = DagProvenance(Path(tmp) / "runs.db")
        runner = DagRunner(_flaky_dag(), provenance=prov)
        this_module._fail_next = True  # pyright: ignore[reportPrivateUsage]
        with initiated_by("usr_ann"):
            failed = await runner.run(source_inputs={"flaky": {"text": "x"}}, dag_nsref="t:f")
        assert failed.status == "failed"

        with initiated_by("usr_bob"):
            restarted = await runner.restart(failed.run_id)
            replayed = await runner.replay(failed.run_id, rerun_nodes={"upper"})
        assert restarted.run_id == failed.run_id and restarted.status == "completed"
        assert prov.get_run(failed.run_id)["initiated_by"] == "usr_ann"  # pyright: ignore[reportOptionalSubscript]
        assert replayed.run_id != failed.run_id
        assert prov.get_run(replayed.run_id)["initiated_by"] == "usr_bob"  # pyright: ignore[reportOptionalSubscript]


def test_existing_runs_database_gains_the_column():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        db = Path(tmp) / "runs.db"
        with closing(sqlite3.connect(db)) as conn:
            conn.execute(
                "CREATE TABLE dag_runs (run_id TEXT PRIMARY KEY, dag_nsref TEXT NOT NULL, "
                "parent_run_id TEXT, status TEXT NOT NULL, started_at REAL NOT NULL, "
                "finished_at REAL, source_inputs_json TEXT, sink_outputs_json TEXT)"
            )
            conn.execute(
                "INSERT INTO dag_runs (run_id, dag_nsref, status, started_at) "
                "VALUES ('old', 'p:d', 'completed', ?)",
                (time.time(),),
            )
            conn.commit()
        prov = DagProvenance(db)
        old = prov.inspect_run("old")
        assert old is not None and old.initiated_by is None
        with initiated_by("usr_bob"):
            prov.create_run("new", "p:d", {})
        new = prov.inspect_run("new")
        assert new is not None and new.initiated_by == "usr_bob"


def _trigger_namespace() -> Namespace:
    ns = Namespace()
    ns.register(this_module._sync_tick, nsref="t:tick")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._signal_tick, nsref="t:signal")  # pyright: ignore[reportPrivateUsage]
    dag = Dag()
    dag.node(ns.get("t:tick")) >> dag.node(ns.get("t:signal"))  # pyright: ignore[reportUnusedExpression]
    config = NsNodeConfig(
        nsref="pipelines:tick",
        triggers=[
            TriggerConfig(name="hook", type="push"),
            TriggerConfig(name="daily", type="poll", schedule="5 0 * * *"),
        ],
    )
    ns.register(dag, nsref="pipelines:tick", config=config)
    return ns


def test_existing_triggers_database_gains_the_columns():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        db = Path(tmp) / "triggers.db"
        with closing(sqlite3.connect(db)) as conn:
            conn.execute(
                "CREATE TABLE trigger_activations (name TEXT PRIMARY KEY, "
                "dag_nsref TEXT NOT NULL, trigger_type TEXT NOT NULL, status TEXT NOT NULL, "
                "last_run_at REAL, next_run_at REAL, last_run_id TEXT, "
                "created_at REAL NOT NULL, config_json TEXT)"
            )
            conn.execute(
                "INSERT INTO trigger_activations (name, dag_nsref, trigger_type, status, "
                "created_at) VALUES ('hook', 'pipelines:tick', 'push', 'active', ?)",
                (time.time(),),
            )
            conn.commit()
        store = TriggerStore(db)
        assert store.get_activation("hook")["changed_by"] is None  # pyright: ignore[reportOptionalSubscript]
        with initiated_by("usr_ann"):
            TriggerManager(namespace=_trigger_namespace(), store=store).deactivate("hook")
        assert store.get_activation("hook")["changed_by"] == "usr_ann"  # pyright: ignore[reportOptionalSubscript]


async def test_fired_and_scheduled_runs_name_their_initiator():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        prov = DagProvenance(Path(tmp) / "runs.db")
        manager = TriggerManager(namespace=_trigger_namespace(), store=store, provenance=prov)
        this_module._seen_by_sync_node.clear()  # pyright: ignore[reportPrivateUsage]
        manager.activate("hook")
        with initiated_by("usr_ann"):
            fired = await manager.fire("hook")
        assert prov.get_run(fired.run_id)["initiated_by"] == "usr_ann"  # pyright: ignore[reportOptionalSubscript]

        manager.activate("daily")
        with closing(sqlite3.connect(store.db_path)) as conn:
            conn.execute(
                "UPDATE trigger_activations SET last_run_at = ? WHERE name = 'daily'",
                (time.time() - 3 * 86400,),
            )
            conn.commit()
        this_module._ticked = asyncio.Event()  # pyright: ignore[reportPrivateUsage]
        manager.start()
        try:
            await asyncio.wait_for(this_module._ticked.wait(), timeout=5)  # pyright: ignore[reportPrivateUsage]
        finally:
            manager.stop()
            this_module._ticked = None  # pyright: ignore[reportPrivateUsage]
        scheduled = [r for r in prov.get_recent_runs() if r.run_id != fired.run_id]
        assert [r.initiated_by for r in scheduled] == [SCHEDULER]
        assert this_module._seen_by_sync_node == ["usr_ann", SCHEDULER]  # pyright: ignore[reportPrivateUsage]


def test_trigger_changes_record_who_made_them():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        manager = TriggerManager(namespace=_trigger_namespace(), store=store)

        def change() -> tuple[str | None, float | None]:
            a = store.get_activation("hook")
            assert a is not None
            return a["changed_by"], a["changed_at"]

        with initiated_by("usr_ann"):
            manager.activate("hook")
        by, ann_at = change()
        assert by == "usr_ann" and ann_at is not None

        # Activation at process start names no one and changes nothing.
        manager.activate_configured()
        assert change() == ("usr_ann", ann_at)

        with initiated_by("usr_bob"):
            manager.deactivate("hook")
        by, bob_at = change()
        assert by == "usr_bob" and bob_at is not None and bob_at >= ann_at

        assert manager.activate_configured().left_disabled == ["hook"]
        assert change() == ("usr_bob", bob_at)

        # Re-enabling by no one must not leave Bob's name on a state he did not set.
        manager.activate("hook")
        by, at = change()
        assert store.get_activation("hook")["status"] == "active"  # pyright: ignore[reportOptionalSubscript]
        assert by is None and at is not None and at >= bob_at
