from __future__ import annotations

import asyncio
import logging
import tempfile
from pathlib import Path

from typing_extensions import override

import tests.test_trigger as this_module

_poll_counter = 0


def _counting_poll_fn() -> dict[str, str] | None:  # pyright: ignore[reportUnusedFunction]
    import tests.test_trigger as m

    m._poll_counter += 1  # pyright: ignore
    if m._poll_counter >= 2:  # pyright: ignore
        return {"batch": str(m._poll_counter)}  # pyright: ignore
    return None


def _poll_fn() -> dict[str, str] | None:  # pyright: ignore[reportUnusedFunction]
    return {"data": "test"}


def test_trigger_config_on_node():
    from lythonic.compose.namespace import Namespace, NsNodeConfig, TriggerConfig

    ns = Namespace()
    config = NsNodeConfig(
        nsref="pipelines:etl",
        triggers=[
            TriggerConfig(name="daily_etl", type="poll", schedule="0 0 * * *"),
            TriggerConfig(name="webhook", type="push"),
        ],
    )
    ns.register(lambda: None, nsref="pipelines:etl", config=config)

    node, tc = ns.get_trigger("daily_etl")
    assert tc.name == "daily_etl"
    assert tc.type == "poll"
    assert tc.schedule == "0 0 * * *"
    assert node.nsref == "pipelines:etl"

    node2, tc2 = ns.get_trigger("webhook")
    assert tc2.type == "push"
    assert node2.nsref == "pipelines:etl"


def test_get_trigger_missing_raises():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    try:
        ns.get_trigger("nonexistent")
        raise AssertionError("Expected KeyError")
    except KeyError:
        pass


def test_trigger_config_with_poll_fn():
    from lythonic.compose.namespace import Namespace, NsNodeConfig, TriggerConfig

    ns = Namespace()
    config = NsNodeConfig(
        nsref="pipelines:orders",
        triggers=[
            TriggerConfig(
                name="new_orders",
                type="poll",
                schedule="*/5 * * * *",
                poll_fn="tests.test_trigger:_poll_fn",  # pyright: ignore[reportArgumentType]
            ),
        ],
    )
    ns.register(lambda: None, nsref="pipelines:orders", config=config)

    _, tc = ns.get_trigger("new_orders")
    assert tc.poll_fn is not None


def test_trigger_store_activate():
    from lythonic.compose.namespace import TriggerConfig
    from lythonic.compose.trigger import TriggerStore

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        tc = TriggerConfig(name="daily", type="poll", schedule="0 0 * * *")
        store.activate(tc, dag_nsref="p:etl")

        activation = store.get_activation("daily")
        assert activation is not None
        assert activation["name"] == "daily"
        assert activation["dag_nsref"] == "p:etl"
        assert activation["status"] == "active"


def test_trigger_store_deactivate():
    from lythonic.compose.namespace import TriggerConfig
    from lythonic.compose.trigger import TriggerStore

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        tc = TriggerConfig(name="t1", type="push")
        store.activate(tc, dag_nsref="p:d")
        store.deactivate("t1")

        activation = store.get_activation("t1")
        assert activation is not None
        assert activation["status"] == "disabled"


def test_trigger_store_record_event():
    from lythonic.compose.namespace import TriggerConfig
    from lythonic.compose.trigger import TriggerStore

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        tc = TriggerConfig(name="t1", type="push")
        store.activate(tc, dag_nsref="p:d")

        store.record_event("t1", payload={"key": "value"}, run_id="run-123", status="completed")

        events = store.get_events("t1")
        assert len(events) == 1
        assert events[0]["trigger_name"] == "t1"
        assert events[0]["run_id"] == "run-123"
        assert events[0]["status"] == "completed"


def test_trigger_store_get_active_poll_triggers():
    from lythonic.compose.namespace import TriggerConfig
    from lythonic.compose.trigger import TriggerStore

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        store.activate(
            TriggerConfig(name="poll1", type="poll", schedule="0 0 * * *"), dag_nsref="p:a"
        )
        store.activate(TriggerConfig(name="push1", type="push"), dag_nsref="p:b")
        store.activate(
            TriggerConfig(name="poll2", type="poll", schedule="0 * * * *"), dag_nsref="p:c"
        )
        store.deactivate("poll2")

        active_polls = store.get_active_poll_triggers()
        assert len(active_polls) == 1
        assert active_polls[0]["name"] == "poll1"


def test_trigger_store_update_last_run():
    from lythonic.compose.namespace import TriggerConfig
    from lythonic.compose.trigger import TriggerStore

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        tc = TriggerConfig(name="t1", type="poll", schedule="0 0 * * *")
        store.activate(tc, dag_nsref="p:d")
        store.update_last_run("t1", run_id="run-1")

        activation = store.get_activation("t1")
        assert activation is not None
        assert activation["last_run_id"] == "run-1"
        assert activation["last_run_at"] is not None


def test_trigger_store_reactivate_keeps_run_history():
    from lythonic.compose.namespace import TriggerConfig
    from lythonic.compose.trigger import TriggerStore

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        store.activate(TriggerConfig(name="t1", type="poll", schedule="0 0 * * *"), dag_nsref="p:d")
        store.update_last_run("t1", run_id="run-1")
        before = store.get_activation("t1")
        assert before is not None

        store.activate(TriggerConfig(name="t1", type="poll", schedule="5 0 * * *"), dag_nsref="p:e")

        after = store.get_activation("t1")
        assert after is not None
        assert after["last_run_at"] == before["last_run_at"]
        assert after["last_run_id"] == "run-1"
        assert after["created_at"] == before["created_at"]
        assert after["dag_nsref"] == "p:e"
        assert after["config_json"] == '{"schedule": "5 0 * * *"}'
        assert after["status"] == "active"


def test_trigger_store_reactivate_after_deactivate_resets_last_run():
    import time

    from lythonic.compose.namespace import TriggerConfig
    from lythonic.compose.trigger import TriggerStore

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        tc = TriggerConfig(name="t1", type="poll", schedule="0 0 * * *")
        store.activate(tc, dag_nsref="p:d")
        store.update_last_run("t1", run_id="run-1")
        before = store.get_activation("t1")
        assert before is not None
        store.deactivate("t1")

        reactivated_at = time.time()
        store.activate(tc, dag_nsref="p:d")

        after = store.get_activation("t1")
        assert after is not None
        assert after["status"] == "active"
        assert after["last_run_at"] >= reactivated_at
        assert after["created_at"] == before["created_at"]


async def _async_echo(text: str) -> str:  # pyright: ignore[reportUnusedFunction]
    return f"echo:{text}"


async def test_trigger_manager_fire_push():
    from lythonic.compose.dag_provenance import DagProvenance
    from lythonic.compose.namespace import Dag, Namespace, NsNodeConfig, TriggerConfig
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    ns = Namespace()
    ns.register(this_module._async_echo, nsref="t:echo")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:echo"))

    # Register DAG with a push trigger on the node config
    dag_config = NsNodeConfig(
        nsref="pipelines:echo_pipe",
        triggers=[TriggerConfig(name="webhook", type="push")],
    )
    ns.register(dag, nsref="pipelines:echo_pipe", config=dag_config)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        provenance = DagProvenance(Path(tmp) / "provenance.db")
        manager = TriggerManager(namespace=ns, store=store, provenance=provenance)
        manager.activate("webhook")

        result = await manager.fire("webhook", payload={"text": "hello"})

        assert result.status == "completed"
        assert result.outputs["echo"] == "echo:hello"

        events = store.get_events("webhook")
        assert len(events) == 1
        assert events[0]["status"] == "completed"
        assert events[0]["run_id"] == result.run_id


async def test_trigger_config_default_payload():
    """Config payload is used when fire() payload is None."""
    from lythonic.compose.namespace import Dag, Namespace, NsNodeConfig, TriggerConfig
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    ns = Namespace()
    ns.register(this_module._async_echo, nsref="t:echo")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:echo"))

    dag_config = NsNodeConfig(
        nsref="pipelines:echo",
        triggers=[TriggerConfig(name="with_default", type="push", payload={"text": "default_msg"})],
    )
    ns.register(dag, nsref="pipelines:echo", config=dag_config)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        manager = TriggerManager(namespace=ns, store=store)
        manager.activate("with_default")

        # Fire without payload — should use config default
        result = await manager.fire("with_default")
        assert result.status == "completed"
        assert result.outputs["echo"] == "echo:default_msg"

        # Fire with explicit payload — should override
        result2 = await manager.fire("with_default", payload={"text": "override"})
        assert result2.outputs["echo"] == "echo:override"


async def test_trigger_manager_fire_without_start():
    """Push triggers work without calling start()."""
    from lythonic.compose.namespace import Dag, Namespace, NsNodeConfig, TriggerConfig
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    ns = Namespace()
    ns.register(this_module._async_echo, nsref="t:echo")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:echo"))

    dag_config = NsNodeConfig(
        nsref="pipelines:echo",
        triggers=[TriggerConfig(name="push1", type="push")],
    )
    ns.register(dag, nsref="pipelines:echo", config=dag_config)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        manager = TriggerManager(namespace=ns, store=store)
        manager.activate("push1")

        result = await manager.fire("push1", payload={"text": "test"})
        assert result.status == "completed"


async def test_trigger_manager_fire_deactivated_raises():
    from lythonic.compose.namespace import Dag, Namespace, NsNodeConfig, TriggerConfig
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    ns = Namespace()
    ns.register(this_module._async_echo, nsref="t:echo")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:echo"))

    dag_config = NsNodeConfig(
        nsref="pipelines:echo",
        triggers=[TriggerConfig(name="push1", type="push")],
    )
    ns.register(dag, nsref="pipelines:echo", config=dag_config)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
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
    """Poll trigger does not fire before schedule elapses."""
    from lythonic.compose.dag_provenance import DagProvenance
    from lythonic.compose.namespace import Dag, Namespace, NsNodeConfig, TriggerConfig
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    ns = Namespace()
    ns.register(this_module._async_echo, nsref="t:echo")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:echo"))

    dag_config = NsNodeConfig(
        nsref="pipelines:echo",
        triggers=[TriggerConfig(name="scheduled", type="poll", schedule="0 * * * *")],
    )
    ns.register(dag, nsref="pipelines:echo", config=dag_config)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        provenance = DagProvenance(Path(tmp) / "prov.db")
        manager = TriggerManager(namespace=ns, store=store, provenance=provenance)
        manager.activate("scheduled")

        manager.start()
        await asyncio.sleep(0.1)

        # Should not have fired yet (schedule is hourly)
        events = store.get_events("scheduled")
        assert len(events) == 0

        manager.stop()


_ticked: asyncio.Event | None = None


async def _async_tick() -> str:  # pyright: ignore[reportUnusedFunction]
    if this_module._ticked is not None:  # pyright: ignore[reportPrivateUsage]
        this_module._ticked.set()  # pyright: ignore[reportPrivateUsage]
    return "tick"


async def test_trigger_manager_catches_up_missed_fires_once_after_restart():
    """Fires missed while down run once on start, not once per missed fire."""
    import time

    from lythonic.compose.namespace import Dag, Namespace, NsNodeConfig, TriggerConfig
    from lythonic.compose.trigger import TriggerManager, TriggerStore
    from lythonic.state import open_sqlite_db

    ns = Namespace()
    ns.register(this_module._async_tick, nsref="t:tick")  # pyright: ignore[reportPrivateUsage]
    dag = Dag()
    dag.node(ns.get("t:tick"))
    dag_config = NsNodeConfig(
        nsref="pipelines:tick",
        triggers=[TriggerConfig(name="daily", type="poll", schedule="5 0 * * *")],
    )
    ns.register(dag, nsref="pipelines:tick", config=dag_config)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        TriggerManager(namespace=ns, store=store).activate("daily")
        three_days_ago = time.time() - 3 * 86400
        with open_sqlite_db(store.db_path) as conn:
            conn.execute(
                "UPDATE trigger_activations SET last_run_at = ? WHERE name = ?",
                (three_days_ago, "daily"),
            )
            conn.commit()

        this_module._ticked = asyncio.Event()  # pyright: ignore[reportPrivateUsage]
        manager = TriggerManager(namespace=ns, store=store)
        manager.activate("daily")
        manager.start()
        await asyncio.wait_for(this_module._ticked.wait(), timeout=5)  # pyright: ignore[reportPrivateUsage]
        # Observation window spanning another poll loop pass (1s period) that must not fire.
        await asyncio.sleep(1.5)
        manager.stop()

        events = store.get_events("daily")
        assert len(events) == 1
        assert events[0]["status"] == "completed"
        activation = store.get_activation("daily")
        assert activation is not None
        assert activation["last_run_at"] > three_days_ago + 2 * 86400


async def _async_slow() -> str:  # pyright: ignore[reportUnusedFunction]
    await asyncio.sleep(2.2)
    return "slow"


class _WarningSignal(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.records: list[logging.LogRecord] = []
        self.received: asyncio.Event = asyncio.Event()

    @override
    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        self.received.set()


async def test_trigger_manager_warns_when_run_overruns_next_fire():
    """A run that outlasts the next scheduled fire time logs a warning naming the trigger."""
    from lythonic.compose.namespace import Dag, Namespace, NsNodeConfig, TriggerConfig
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    ns = Namespace()
    ns.register(this_module._async_slow, nsref="t:slow")  # pyright: ignore[reportPrivateUsage]
    dag = Dag()
    dag.node(ns.get("t:slow"))
    dag_config = NsNodeConfig(
        nsref="pipelines:slow",
        triggers=[TriggerConfig(name="every_sec", type="poll", schedule="* * * * * *")],
    )
    ns.register(dag, nsref="pipelines:slow", config=dag_config)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        manager = TriggerManager(namespace=ns, store=store)
        manager.activate("every_sec")
        signal = _WarningSignal()
        trigger_log = logging.getLogger("lythonic.compose.trigger")
        trigger_log.addHandler(signal)
        try:
            manager.start()
            await asyncio.wait_for(signal.received.wait(), timeout=6)
            manager.stop()
        finally:
            trigger_log.removeHandler(signal)

        assert "every_sec" in signal.records[0].getMessage()


async def test_trigger_manager_poll_custom_fn():
    """Poll trigger with custom poll_fn fires when fn returns data."""
    import tests.test_trigger as m
    from lythonic.compose.namespace import Dag, Namespace, NsNodeConfig, TriggerConfig
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    m._poll_counter = 0  # pyright: ignore

    ns = Namespace()
    ns.register(this_module._async_echo, nsref="t:echo")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:echo"))

    dag_config = NsNodeConfig(
        nsref="pipelines:echo",
        triggers=[
            TriggerConfig(
                name="poller",
                type="poll",
                poll_fn="tests.test_trigger:_counting_poll_fn",  # pyright: ignore[reportArgumentType]
                schedule="* * * * * */1",
            ),
        ],
    )
    ns.register(dag, nsref="pipelines:echo", config=dag_config)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        manager = TriggerManager(namespace=ns, store=store)
        manager.activate("poller")

        manager.start()
        await asyncio.sleep(3.5)
        manager.stop()

        events = store.get_events("poller")
        assert len(events) >= 1


async def test_trigger_manager_start_stop():
    """Manager starts and stops cleanly."""
    from lythonic.compose.namespace import Namespace
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    ns = Namespace()

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
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
    from lythonic.compose.namespace import Dag, Namespace, NsNodeConfig, TriggerConfig
    from lythonic.compose.trigger import TriggerManager, TriggerStore

    m._poll_counter = 0  # pyright: ignore

    ns = Namespace()
    ns.register(this_module._async_echo, nsref="t:echo")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:echo"))

    dag_config = NsNodeConfig(
        nsref="pipelines:echo",
        triggers=[
            TriggerConfig(
                name="poller",
                type="poll",
                poll_fn="tests.test_trigger:_counting_poll_fn",  # pyright: ignore[reportArgumentType]
                schedule="* * * * * */1",
            ),
        ],
    )
    ns.register(dag, nsref="pipelines:echo", config=dag_config)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = TriggerStore(Path(tmp) / "triggers.db")
        manager = TriggerManager(namespace=ns, store=store)
        manager.activate("poller")
        manager.deactivate("poller")

        manager.start()
        await asyncio.sleep(2.5)
        manager.stop()

        events = store.get_events("poller")
        assert len(events) == 0
