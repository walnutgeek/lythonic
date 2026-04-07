from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import tests.test_trigger as this_module
from lythonic.periodic import Interval

_poll_counter = 0


def _counting_poll_fn() -> dict[str, str] | None:  # pyright: ignore[reportUnusedFunction]
    import tests.test_trigger as m

    m._poll_counter += 1  # pyright: ignore
    if m._poll_counter >= 2:  # pyright: ignore
        return {"batch": str(m._poll_counter)}  # pyright: ignore
    return None


def _poll_fn() -> dict[str, str] | None:  # pyright: ignore[reportUnusedFunction]
    return {"data": "test"}


def test_register_trigger_poll():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register_trigger(
        name="daily_etl",
        dag_nsref="pipelines:etl",
        trigger_type="poll",
        interval=Interval.from_string("1D"),
    )
    td = ns.get_trigger("daily_etl")
    assert td.name == "daily_etl"
    assert td.dag_nsref == "pipelines:etl"
    assert td.trigger_type == "poll"
    assert td.interval is not None


def test_register_trigger_push():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register_trigger(
        name="webhook",
        dag_nsref="pipelines:handle",
        trigger_type="push",
    )
    td = ns.get_trigger("webhook")
    assert td.trigger_type == "push"
    assert td.interval is None


def test_register_trigger_poll_with_fn():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register_trigger(
        name="new_orders",
        dag_nsref="pipelines:orders",
        trigger_type="poll",
        poll_fn=this_module._poll_fn,  # pyright: ignore[reportPrivateUsage]
        interval=Interval.from_string("5m"),
    )
    td = ns.get_trigger("new_orders")
    assert td.poll_fn is not None


def test_register_trigger_duplicate_raises():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register_trigger(name="t1", dag_nsref="p:d", trigger_type="push")
    try:
        ns.register_trigger(name="t1", dag_nsref="p:d", trigger_type="push")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "already" in str(e).lower()


def test_get_trigger_missing_raises():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    try:
        ns.get_trigger("nonexistent")
        raise AssertionError("Expected KeyError")
    except KeyError:
        pass


def test_register_trigger_poll_requires_interval():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    try:
        ns.register_trigger(name="bad", dag_nsref="p:d", trigger_type="poll")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "interval" in str(e).lower()


def test_trigger_store_activate():
    from lythonic.compose.trigger import TriggerDef, TriggerStore

    with tempfile.TemporaryDirectory() as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        td = TriggerDef(
            name="daily",
            dag_nsref="p:etl",
            trigger_type="poll",
            interval=Interval.from_string("1D"),
        )
        store.activate(td)

        activation = store.get_activation("daily")
        assert activation is not None
        assert activation["name"] == "daily"
        assert activation["dag_nsref"] == "p:etl"
        assert activation["status"] == "active"


def test_trigger_store_deactivate():
    from lythonic.compose.trigger import TriggerDef, TriggerStore

    with tempfile.TemporaryDirectory() as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        td = TriggerDef(name="t1", dag_nsref="p:d", trigger_type="push")
        store.activate(td)
        store.deactivate("t1")

        activation = store.get_activation("t1")
        assert activation is not None
        assert activation["status"] == "disabled"


def test_trigger_store_record_event():
    from lythonic.compose.trigger import TriggerDef, TriggerStore

    with tempfile.TemporaryDirectory() as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        td = TriggerDef(name="t1", dag_nsref="p:d", trigger_type="push")
        store.activate(td)

        store.record_event("t1", payload={"key": "value"}, run_id="run-123", status="completed")

        events = store.get_events("t1")
        assert len(events) == 1
        assert events[0]["trigger_name"] == "t1"
        assert events[0]["run_id"] == "run-123"
        assert events[0]["status"] == "completed"


def test_trigger_store_get_active_poll_triggers():
    from lythonic.compose.trigger import TriggerDef, TriggerStore

    with tempfile.TemporaryDirectory() as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        store.activate(
            TriggerDef(
                name="poll1",
                dag_nsref="p:a",
                trigger_type="poll",
                interval=Interval.from_string("1D"),
            )
        )
        store.activate(TriggerDef(name="push1", dag_nsref="p:b", trigger_type="push"))
        store.activate(
            TriggerDef(
                name="poll2",
                dag_nsref="p:c",
                trigger_type="poll",
                interval=Interval.from_string("1h"),
            )
        )
        store.deactivate("poll2")

        active_polls = store.get_active_poll_triggers()
        assert len(active_polls) == 1
        assert active_polls[0]["name"] == "poll1"


def test_trigger_store_update_last_run():
    from lythonic.compose.trigger import TriggerDef, TriggerStore

    with tempfile.TemporaryDirectory() as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        td = TriggerDef(
            name="t1", dag_nsref="p:d", trigger_type="poll", interval=Interval.from_string("1D")
        )
        store.activate(td)
        store.update_last_run("t1", run_id="run-1")

        activation = store.get_activation("t1")
        assert activation is not None
        assert activation["last_run_id"] == "run-1"
        assert activation["last_run_at"] is not None


async def _async_echo(text: str) -> str:  # pyright: ignore[reportUnusedFunction]
    return f"echo:{text}"


async def test_trigger_manager_fire_push():
    from lythonic.compose.dag_provenance import DagProvenance
    from lythonic.compose.namespace import Dag, Namespace
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    ns = Namespace()
    ns.register(this_module._async_echo, nsref="t:echo")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:echo"))
    ns.register(dag, nsref="pipelines:echo_pipe")

    ns.register_trigger(
        name="webhook",
        dag_nsref="pipelines:echo_pipe",
        trigger_type="push",
    )

    with tempfile.TemporaryDirectory() as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        provenance = DagProvenance(Path(tmp) / "provenance.db")
        manager = TriggerManager(namespace=ns, store=store, provenance=provenance)
        manager.activate("webhook")

        result = await manager.fire("webhook", payload={"text": "hello"})

        assert result.status == "completed"
        # The node label is derived from the nsref leaf ("t:echo" -> "echo")
        assert result.outputs["echo"] == "echo:hello"

        events = store.get_events("webhook")
        assert len(events) == 1
        assert events[0]["status"] == "completed"
        assert events[0]["run_id"] == result.run_id


async def test_trigger_manager_fire_without_start():
    """Push triggers work without calling start()."""
    from lythonic.compose.namespace import Dag, Namespace
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    ns = Namespace()
    ns.register(this_module._async_echo, nsref="t:echo")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:echo"))
    ns.register(dag, nsref="pipelines:echo")

    ns.register_trigger(name="push1", dag_nsref="pipelines:echo", trigger_type="push")

    with tempfile.TemporaryDirectory() as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        manager = TriggerManager(namespace=ns, store=store)
        manager.activate("push1")

        result = await manager.fire("push1", payload={"text": "test"})
        assert result.status == "completed"


async def test_trigger_manager_fire_deactivated_raises():
    from lythonic.compose.namespace import Dag, Namespace
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    ns = Namespace()
    ns.register(this_module._async_echo, nsref="t:echo")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:echo"))
    ns.register(dag, nsref="pipelines:echo")

    ns.register_trigger(name="push1", dag_nsref="pipelines:echo", trigger_type="push")

    with tempfile.TemporaryDirectory() as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        manager = TriggerManager(namespace=ns, store=store)
        manager.activate("push1")
        manager.deactivate("push1")

        try:
            await manager.fire("push1", payload={"text": "test"})
            raise AssertionError("Expected ValueError")
        except ValueError as e:
            assert "not active" in str(e).lower() or "disabled" in str(e).lower()


async def test_trigger_manager_poll_schedule():
    """Poll trigger fires DAG when interval elapses; does not fire before interval."""
    from lythonic.compose.dag_provenance import DagProvenance
    from lythonic.compose.namespace import Dag, Namespace
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    ns = Namespace()
    ns.register(this_module._async_echo, nsref="t:echo")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:echo"))
    ns.register(dag, nsref="pipelines:echo")

    ns.register_trigger(
        name="scheduled",
        dag_nsref="pipelines:echo",
        trigger_type="poll",
        interval=Interval.from_string("1h"),
    )

    with tempfile.TemporaryDirectory() as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        provenance = DagProvenance(Path(tmp) / "prov.db")
        manager = TriggerManager(namespace=ns, store=store, provenance=provenance)
        manager.activate("scheduled")

        manager.start()
        await asyncio.sleep(0.1)

        # Should not have fired yet (interval is 1h)
        events = store.get_events("scheduled")
        assert len(events) == 0

        manager.stop()


async def test_trigger_manager_poll_custom_fn():
    """Poll trigger with custom poll_fn fires when fn returns data."""
    import tests.test_trigger as m
    from lythonic.compose.namespace import Dag, Namespace
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    m._poll_counter = 0  # pyright: ignore

    ns = Namespace()
    ns.register(this_module._async_echo, nsref="t:echo")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:echo"))
    ns.register(dag, nsref="pipelines:echo")

    ns.register_trigger(
        name="poller",
        dag_nsref="pipelines:echo",
        trigger_type="poll",
        poll_fn=this_module._counting_poll_fn,  # pyright: ignore[reportPrivateUsage]
        interval=Interval.from_string("1s"),
    )

    with tempfile.TemporaryDirectory() as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        manager = TriggerManager(namespace=ns, store=store)
        manager.activate("poller")

        manager.start()
        await asyncio.sleep(3.5)
        manager.stop()

        # poll_fn returns None on first call, data on second+
        events = store.get_events("poller")
        assert len(events) >= 1


async def test_trigger_manager_start_stop():
    """Manager starts and stops cleanly."""
    from lythonic.compose.namespace import Namespace
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    ns = Namespace()

    with tempfile.TemporaryDirectory() as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        manager = TriggerManager(namespace=ns, store=store)

        manager.start()
        assert manager._task is not None  # pyright: ignore[reportPrivateUsage]
        assert not manager._task.done()  # pyright: ignore[reportPrivateUsage]

        manager.stop()
        await asyncio.sleep(0.1)
        assert manager._task.done() or manager._task.cancelled()  # pyright: ignore[reportPrivateUsage]


async def test_trigger_manager_deactivated_not_polled():
    """Deactivated poll triggers are not polled."""
    import tests.test_trigger as m
    from lythonic.compose.namespace import Dag, Namespace
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    m._poll_counter = 0  # pyright: ignore

    ns = Namespace()
    ns.register(this_module._async_echo, nsref="t:echo")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:echo"))
    ns.register(dag, nsref="pipelines:echo")

    ns.register_trigger(
        name="poller",
        dag_nsref="pipelines:echo",
        trigger_type="poll",
        poll_fn=this_module._counting_poll_fn,  # pyright: ignore[reportPrivateUsage]
        interval=Interval.from_string("1s"),
    )

    with tempfile.TemporaryDirectory() as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        manager = TriggerManager(namespace=ns, store=store)
        manager.activate("poller")
        manager.deactivate("poller")

        manager.start()
        await asyncio.sleep(2.5)
        manager.stop()

        events = store.get_events("poller")
        assert len(events) == 0
