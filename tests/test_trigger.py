from __future__ import annotations

import tests.test_trigger as this_module
from lythonic.periodic import Interval


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
