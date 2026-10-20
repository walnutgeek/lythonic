"""Who started a run, or changed a trigger: `initiated_by` and `run_initiator`."""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import time
from contextlib import closing
from pathlib import Path

import tests.test_dag_runner as dag_tests
import tests.test_trigger as trigger_tests
from lythonic.compose.dag_provenance import SCHEDULER, DagProvenance, initiated_by
from lythonic.compose.dag_runner import DagRunner
from lythonic.compose.namespace import Dag, Namespace, NsNodeConfig, TriggerConfig
from lythonic.compose.trigger import TriggerManager, TriggerStore


def _mapped_dag() -> Dag:
    ns = Namespace()
    ns.register(dag_tests._async_split, nsref="t:split")  # pyright: ignore[reportPrivateUsage]
    ns.register(dag_tests._async_upper, nsref="t:upper")  # pyright: ignore[reportPrivateUsage]
    ns.register(dag_tests._async_join, nsref="t:join")  # pyright: ignore[reportPrivateUsage]
    sub = Dag()
    sub.node(ns.get("t:upper"))
    parent = Dag()
    s = parent.node(ns.get("t:split"))
    m = parent.map(sub, label="mapped")
    j = parent.node(ns.get("t:join"))
    s >> m >> j  # pyright: ignore[reportUnusedExpression]
    return parent


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


def _push_and_poll_namespace() -> Namespace:
    ns = Namespace()
    ns.register(trigger_tests._async_tick, nsref="t:tick")  # pyright: ignore[reportPrivateUsage]
    dag = Dag()
    dag.node(ns.get("t:tick"))
    config = NsNodeConfig(
        nsref="pipelines:tick",
        triggers=[
            TriggerConfig(name="hook", type="push"),
            TriggerConfig(name="daily", type="poll", schedule="5 0 * * *"),
        ],
    )
    ns.register(dag, nsref="pipelines:tick", config=config)
    return ns


async def test_fired_and_scheduled_runs_name_their_initiator():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        prov = DagProvenance(Path(tmp) / "runs.db")
        manager = TriggerManager(namespace=_push_and_poll_namespace(), store=store, provenance=prov)
        manager.activate("hook")
        with initiated_by("usr_ann"):
            fired = await manager.fire("hook")
        assert prov.get_run(fired.run_id)["initiated_by"] == "usr_ann"  # pyright: ignore[reportOptionalSubscript]

        manager.activate("daily")
        with sqlite3.connect(store.db_path) as conn:
            conn.execute(
                "UPDATE trigger_activations SET last_run_at = ? WHERE name = 'daily'",
                (time.time() - 3 * 86400,),
            )
        manager.start()
        try:
            for _ in range(100):
                events = store.get_events("daily")
                if events and events[0]["run_id"]:
                    break
                await asyncio.sleep(0.05)
        finally:
            manager.stop()
        run_id = store.get_events("daily")[0]["run_id"]
        assert prov.get_run(run_id)["initiated_by"] == SCHEDULER  # pyright: ignore[reportOptionalSubscript]


def test_trigger_changes_record_who_made_them():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        manager = TriggerManager(namespace=_push_and_poll_namespace(), store=store)
        manager.activate("hook")
        assert store.get_activation("hook")["changed_by"] is None  # pyright: ignore[reportOptionalSubscript]
        with initiated_by("usr_ann"):
            manager.deactivate("hook")
        activation = store.get_activation("hook")
        assert activation is not None
        assert activation["changed_by"] == "usr_ann"
        assert activation["changed_at"] is not None
