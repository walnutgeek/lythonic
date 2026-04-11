from __future__ import annotations

import asyncio
import json
import logging as _logging
import tempfile
import threading
from pathlib import Path

import tests.test_dag_runner as this_module
from lythonic.compose.dag_provenance import DagProvenance
from lythonic.compose.namespace import DagContext, inline


def test_provenance_create_and_get_run():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "pipelines:daily", {"fetch": {"ticker": "AAPL"}})

        run = prov.get_run("run-1")
        assert run is not None
        assert run["run_id"] == "run-1"
        assert run["dag_nsref"] == "pipelines:daily"
        assert run["status"] == "running"
        assert run["started_at"] > 0
        assert json.loads(run["source_inputs_json"]) == {"fetch": {"ticker": "AAPL"}}


def test_provenance_update_and_finish_run():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.update_run_status("run-1", "paused")
        assert prov.get_run("run-1")["status"] == "paused"  # pyright: ignore

        prov.finish_run("run-1", "completed")
        run = prov.get_run("run-1")
        assert run is not None
        assert run["status"] == "completed"
        assert run["finished_at"] is not None


def test_provenance_node_skipped():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.record_node_skipped("run-1", "fetch", '{"price": 100.0}')
        execs = prov.get_node_executions("run-1")
        assert execs[0]["status"] == "skipped"
        assert execs[0]["output_json"] == '{"price": 100.0}'


def test_provenance_get_missing_run():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        assert prov.get_run("nonexistent") is None


def test_provenance_node_type():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})
        prov.record_node_start("run-1", "fetch", "{}", node_type="source")
        prov.record_node_start("run-1", "compute", "{}", node_type="internal")
        prov.record_node_start("run-1", "report", "{}", node_type="sink")

        nodes = prov.get_node_executions("run-1")
        types = {n["node_label"]: n["node_type"] for n in nodes}
        assert types["fetch"] == "source"
        assert types["compute"] == "internal"
        assert types["report"] == "sink"


async def test_edge_traversals_recorded_during_dag_run():
    """Edge traversals are recorded when running a DAG with provenance."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_double, nsref="t:double")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_format, nsref="t:format")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    s = dag.node(ns.get("t:source"))
    d = dag.node(ns.get("t:double"))
    f = dag.node(ns.get("t:format"))
    s >> d >> f  # pyright: ignore[reportUnusedExpression]

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        prov = DagProvenance(Path(tmp) / "runs.db")
        runner = DagRunner(dag, provenance=prov)
        result = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:edge_test",
        )

        assert result.status == "completed"

        # Check edge traversals
        edges = prov.get_edge_traversals(result.run_id)
        edge_pairs = [(e["upstream_label"], e["downstream_label"]) for e in edges]
        assert ("source", "double") in edge_pairs
        assert ("double", "format") in edge_pairs
        assert len(edges) == 2

        # Check node types
        nodes = prov.get_node_executions(result.run_id)
        types = {n["node_label"]: n["node_type"] for n in nodes}
        assert types["source"] == "source"
        assert types["double"] == "internal"
        assert types["format"] == "sink"


async def _ctx_node(ctx: DagContext, value: float) -> str:  # pyright: ignore[reportUnusedFunction]
    return f"{ctx.node_label}:{ctx.run_id[:8]}:{value}"


async def _async_report(value: float) -> str:  # pyright: ignore[reportUnusedFunction]
    return f"report:{value}"


async def _async_archive(value: float) -> str:  # pyright: ignore[reportUnusedFunction]
    return f"archive:{value}"


async def _async_merge(values: list[str]) -> str:  # pyright: ignore[reportUnusedFunction]
    return "+".join(sorted(values))


async def _async_source(ticker: str) -> float:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    return 100.0


async def _async_double(value: float) -> float:  # pyright: ignore[reportUnusedFunction]
    return value * 2


async def _async_format(value: float) -> str:  # pyright: ignore[reportUnusedFunction]
    return f"result={value}"


async def _async_split(text: str) -> list[str]:  # pyright: ignore[reportUnusedFunction]
    return text.split(",")


async def _async_upper(text: str) -> str:  # pyright: ignore[reportUnusedFunction]
    return text.upper()


async def _async_join(values: list[str]) -> str:  # pyright: ignore[reportUnusedFunction]
    return "|".join(values)


async def _async_double_str(text: str) -> str:  # pyright: ignore[reportUnusedFunction]
    return text + text


async def _async_make_text() -> str:  # pyright: ignore[reportUnusedFunction]
    return "hello"


def _capture_thread(value: int) -> str:  # pyright: ignore[reportUnusedFunction]
    """Sync node that captures which thread it runs on."""
    return f"{threading.current_thread().name}:{value * 2}"


@inline
def _capture_thread_inline(value: int) -> str:  # pyright: ignore[reportUnusedFunction]
    """Inline sync node that captures which thread it runs on."""
    return f"{threading.current_thread().name}:{value * 2}"


async def _capture_thread_async(value: int) -> str:  # pyright: ignore[reportUnusedFunction]
    """Async node that captures which thread it runs on."""
    return f"{threading.current_thread().name}:{value * 2}"


def _capture_thread_with_ctx(ctx: DagContext, value: int) -> str:  # pyright: ignore[reportUnusedFunction]
    """Sync node with DagContext that captures which thread it runs on."""
    return f"{threading.current_thread().name}:{ctx.node_label}:{value * 2}"


def _log_and_return(value: int) -> str:  # pyright: ignore[reportUnusedFunction]
    """Sync node that logs and returns a value."""
    _logging.getLogger("test.node").info("processing %d", value)
    return f"done:{value}"


@inline
def _log_and_return_inline(value: int) -> str:  # pyright: ignore[reportUnusedFunction]
    """Inline sync node that logs and returns a value."""
    _logging.getLogger("test.node").info("processing %d", value)
    return f"done:{value}"


async def _log_and_return_async(value: int) -> str:  # pyright: ignore[reportUnusedFunction]
    """Async node that logs and returns a value."""
    _logging.getLogger("test.node").info("processing %d", value)
    return f"done:{value}"


async def test_linear_dag_execution():
    """Three nodes in a line: source -> double -> format."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_double, nsref="t:double")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_format, nsref="t:format")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        d = dag.node(ns.get("t:double"))
        f = dag.node(ns.get("t:format"))
        _ = s >> d >> f

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        runner = DagRunner(dag, provenance=DagProvenance(Path(tmp) / "runs.db"))
        result = await runner.run(
            source_inputs={"source": {"ticker": "AAPL"}},
            dag_nsref="test:pipeline",
        )

        assert result.status == "completed"
        assert result.outputs["format"] == "result=200.0"
        assert result.failed_node is None


async def test_provenance_recorded_during_run():
    """Verify provenance DB is populated during execution."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_double, nsref="t:double")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        d = dag.node(ns.get("t:double"))
        _ = s >> d

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        db_path = Path(tmp) / "runs.db"
        runner = DagRunner(dag, provenance=DagProvenance(db_path))
        result = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:pipe",
        )

        run = runner.provenance.get_run(result.run_id)
        assert run is not None
        assert run["status"] == "completed"

        execs = runner.provenance.get_node_executions(result.run_id)
        assert len(execs) == 2
        labels = {e["node_label"] for e in execs}
        assert labels == {"source", "double"}
        for e in execs:
            assert e["status"] == "completed"
            assert e["output_json"] is not None


async def test_dag_context_injection():
    """Node with DagContext first param receives injected context."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._ctx_node, nsref="t:ctx_node")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        c = dag.node(ns.get("t:ctx_node"))
        _ = s >> c

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        runner = DagRunner(dag, provenance=DagProvenance(Path(tmp) / "runs.db"))
        result = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="test:ctx_pipe",
        )

        assert result.status == "completed"
        output = result.outputs["ctx_node"]
        assert output.startswith("ctx_node:")
        assert ":100.0" in output


async def test_fan_out_fan_in():
    """Fan-out from source, fan-in collecting list of values."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_report, nsref="t:report")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_archive, nsref="t:archive")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_merge, nsref="t:merge")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        r = dag.node(ns.get("t:report"))
        a = dag.node(ns.get("t:archive"))
        m = dag.node(ns.get("t:merge"))
        _ = s >> r >> m
        _ = s >> a >> m

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        runner = DagRunner(dag, provenance=DagProvenance(Path(tmp) / "runs.db"))
        result = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:fanio",
        )

        assert result.status == "completed"
        assert result.outputs["merge"] == "archive:100.0+report:100.0"


async def _async_fail(value: float) -> str:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    raise RuntimeError("intentional failure")


async def test_node_failure():
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_fail, nsref="t:fail_node")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        f = dag.node(ns.get("t:fail_node"))
        _ = s >> f

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        runner = DagRunner(dag, provenance=DagProvenance(Path(tmp) / "runs.db"))
        result = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:fail_pipe",
        )

        assert result.status == "failed"
        assert result.failed_node == "fail_node"
        assert "intentional failure" in (result.error or "")

        run = runner.provenance.get_run(result.run_id)
        assert run is not None
        assert run["status"] == "failed"


async def _async_pause_node(value: float) -> str:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    from lythonic.compose.dag_runner import DagPause

    raise DagPause()


async def test_node_driven_pause():
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_pause_node, nsref="t:pauser")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_format, nsref="t:fmt")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        p = dag.node(ns.get("t:pauser"))
        f = dag.node(ns.get("t:fmt"))
        _ = s >> p >> f

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        runner = DagRunner(dag, provenance=DagProvenance(Path(tmp) / "runs.db"))
        result = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:pause_pipe",
        )

        assert result.status == "paused"
        assert result.failed_node is None
        # "fmt" should not have run
        assert "fmt" not in result.outputs


_ext_pause_call_count = 0
_ext_pause_event: asyncio.Event | None = None


async def _slow_step1(ticker: str) -> float:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    global _ext_pause_call_count  # noqa: PLW0603
    _ext_pause_call_count += 1
    # Signal that step1 has started so the pause task can fire
    if _ext_pause_event is not None:
        _ext_pause_event.set()
    # Yield control to let the pause task run
    import asyncio

    await asyncio.sleep(0)
    return 1.0


async def _slow_step2(value: float) -> str:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    global _ext_pause_call_count  # noqa: PLW0603
    _ext_pause_call_count += 1
    return "done"


async def test_external_pause():
    import asyncio

    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    global _ext_pause_call_count, _ext_pause_event  # noqa: PLW0603
    _ext_pause_call_count = 0
    _ext_pause_event = asyncio.Event()

    ns = Namespace()
    ns.register(this_module._slow_step1, nsref="t:step1")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._slow_step2, nsref="t:step2")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s1 = dag.node(ns.get("t:step1"))
        s2 = dag.node(ns.get("t:step2"))
        _ = s1 >> s2

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        runner = DagRunner(dag, provenance=DagProvenance(Path(tmp) / "runs.db"))

        async def pause_when_step1_starts():
            assert _ext_pause_event is not None
            await _ext_pause_event.wait()
            runner.pause()

        asyncio.create_task(pause_when_step1_starts())
        result = await runner.run(
            source_inputs={"step1": {"ticker": "X"}},
            dag_nsref="t:ext_pause",
        )

        assert result.status == "paused"
        # step1 completed but step2 should not have run
        assert _ext_pause_call_count == 1


_pause_once_count = 0


async def _pause_once(value: float) -> float:  # pyright: ignore[reportUnusedFunction]
    from lythonic.compose.dag_runner import DagPause

    this_module._pause_once_count += 1  # pyright: ignore
    if this_module._pause_once_count == 1:  # pyright: ignore
        raise DagPause()
    return value * 2


_fail_once_count = 0


async def _fail_once(value: float) -> str:  # pyright: ignore[reportUnusedFunction]
    this_module._fail_once_count += 1  # pyright: ignore
    if this_module._fail_once_count == 1:  # pyright: ignore
        raise RuntimeError("first attempt fails")
    return f"ok:{value}"


async def test_restart_paused_run():
    """Restart a paused run -- resumes from the paused node."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    this_module._pause_once_count = 0  # pyright: ignore

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._pause_once, nsref="t:maybe_pause")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_format, nsref="t:fmt")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        p = dag.node(ns.get("t:maybe_pause"))
        f = dag.node(ns.get("t:fmt"))
        _ = s >> p >> f

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        runner = DagRunner(dag, provenance=DagProvenance(Path(tmp) / "runs.db"))

        # First run pauses
        result1 = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:restart_pipe",
        )
        assert result1.status == "paused"

        # Restart completes
        result2 = await runner.restart(result1.run_id)
        assert result2.status == "completed"
        assert result2.run_id == result1.run_id
        assert result2.outputs["fmt"] == "result=200.0"


async def test_restart_failed_run():
    """Restart a failed run -- re-executes from the failed node."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    this_module._fail_once_count = 0  # pyright: ignore

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._fail_once, nsref="t:maybe_fail")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        f = dag.node(ns.get("t:maybe_fail"))
        _ = s >> f

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        runner = DagRunner(dag, provenance=DagProvenance(Path(tmp) / "runs.db"))

        result1 = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:restart_fail",
        )
        assert result1.status == "failed"

        result2 = await runner.restart(result1.run_id)
        assert result2.status == "completed"
        assert result2.outputs["maybe_fail"] == "ok:100.0"


_replay_source_count = 0


async def _replay_source(ticker: str) -> float:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    this_module._replay_source_count += 1  # pyright: ignore
    return float(this_module._replay_source_count) * 100  # pyright: ignore


async def test_replay_selective_reexecution():
    """Replay: rerun 'double' and 'fmt' but keep 'source' output from previous run."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    this_module._replay_source_count = 0  # pyright: ignore

    ns = Namespace()
    ns.register(this_module._replay_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_double, nsref="t:double")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_format, nsref="t:fmt")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        d = dag.node(ns.get("t:double"))
        f = dag.node(ns.get("t:fmt"))
        _ = s >> d >> f

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        runner = DagRunner(dag, provenance=DagProvenance(Path(tmp) / "runs.db"))

        # First run
        result1 = await runner.run(
            source_inputs={"source": {"ticker": "X"}},
            dag_nsref="t:replay",
        )
        assert result1.status == "completed"
        assert result1.outputs["fmt"] == "result=200.0"
        assert this_module._replay_source_count == 1  # pyright: ignore

        # Replay: rerun double and fmt, but keep source output
        result2 = await runner.replay(result1.run_id, rerun_nodes={"double", "fmt"})
        assert result2.status == "completed"
        # Source was NOT re-run (count stays 1), but its output (100.0) was reused
        assert this_module._replay_source_count == 1  # pyright: ignore
        assert result2.outputs["fmt"] == "result=200.0"
        assert result2.run_id != result1.run_id

        # Verify source node was recorded as skipped
        execs = runner.provenance.get_node_executions(result2.run_id)
        source_exec = [e for e in execs if e["node_label"] == "source"][0]
        assert source_exec["status"] == "skipped"


async def test_dag_registered_in_namespace():
    """A Dag registered in Namespace becomes callable."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_double, nsref="t:double")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        d = dag.node(ns.get("t:double"))
        _ = s >> d

    ns.register(dag, nsref="pipelines:my_pipe")

    node = ns.get("pipelines:my_pipe")
    result = await node(source={"ticker": "X"})

    assert result.run_id is not None  # pyright: ignore
    assert result.status == "completed"  # pyright: ignore
    assert result.outputs["double"] == 200.0  # pyright: ignore


async def test_dag_registered_without_provenance():
    """Dag registered in Namespace uses NullProvenance."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_double, nsref="t:double")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        d = dag.node(ns.get("t:double"))
        s >> d  # pyright: ignore[reportUnusedExpression]

    ns.register(dag, nsref="pipelines:no_persist")

    node = ns.get("pipelines:no_persist")
    result = await node(source={"ticker": "X"})
    assert result.run_id is not None  # pyright: ignore
    assert result.status == "completed"  # pyright: ignore
    assert result.outputs["double"] == 200.0  # pyright: ignore


async def test_runner_without_persistence():
    """DagRunner without explicit provenance executes successfully."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_double, nsref="t:double")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        d = dag.node(ns.get("t:double"))
        s >> d  # pyright: ignore[reportUnusedExpression]

    runner = DagRunner(dag)
    result = await runner.run(
        source_inputs={"source": {"ticker": "X"}},
        dag_nsref="t:no_persist",
    )
    assert result.status == "completed"
    assert result.outputs["double"] == 200.0


async def test_runner_without_persistence_restart_raises():
    """restart() with NullProvenance raises ValueError (run not found)."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        dag.node(ns.get("t:source"))

    runner = DagRunner(dag)
    result = await runner.run(
        source_inputs={"source": {"ticker": "X"}},
    )

    try:
        await runner.restart(result.run_id)
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "not found" in str(e)


async def test_runner_without_persistence_replay_raises():
    """replay() with NullProvenance raises ValueError (run not found)."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        dag.node(ns.get("t:source"))

    runner = DagRunner(dag)
    result = await runner.run(
        source_inputs={"source": {"ticker": "X"}},
    )

    try:
        await runner.replay(result.run_id, rerun_nodes={"source"})
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "not found" in str(e)


async def test_map_over_list():
    """Map a sub-DAG over each element of a list."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_split, nsref="t:split")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_upper, nsref="t:upper")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_join, nsref="t:join")  # pyright: ignore[reportPrivateUsage]

    # Sub-DAG: upper each element
    sub = Dag()
    sub.node(ns.get("t:upper"))

    # Parent: split -> map(upper) -> join
    parent = Dag()
    s = parent.node(ns.get("t:split"))
    m = parent.map(sub, label="mapped")
    j = parent.node(ns.get("t:join"))
    s >> m >> j  # pyright: ignore[reportUnusedExpression]

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        runner = DagRunner(parent, provenance=DagProvenance(Path(tmp) / "runs.db"))
        result = await runner.run(
            source_inputs={"split": {"text": "hello,world,foo"}},
            dag_nsref="t:map_test",
        )

        assert result.status == "completed"
        assert result.outputs["join"] == "HELLO|WORLD|FOO"


async def _async_make_dict(text: str) -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
    return {"us": text + "_us", "eu": text + "_eu"}


async def _async_make_int() -> int:  # pyright: ignore[reportUnusedFunction]
    return 42


async def _async_fail_str(text: str) -> str:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    raise RuntimeError("intentional failure")


async def test_map_over_dict():
    """Map a sub-DAG over each value of a dict, preserving keys."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_make_dict, nsref="t:make_dict")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_upper, nsref="t:upper")  # pyright: ignore[reportPrivateUsage]

    sub = Dag()
    sub.node(ns.get("t:upper"))

    parent = Dag()
    s = parent.node(ns.get("t:make_dict"))
    m = parent.map(sub, label="regions")
    s >> m  # pyright: ignore[reportUnusedExpression]

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        runner = DagRunner(parent, provenance=DagProvenance(Path(tmp) / "runs.db"))
        result = await runner.run(
            source_inputs={"make_dict": {"text": "hello"}},
            dag_nsref="t:dict_map",
        )

        assert result.status == "completed"
        assert result.outputs["regions"] == {"us": "HELLO_US", "eu": "HELLO_EU"}


async def test_map_invalid_input_type_raises():
    """MapNode raises TypeError if upstream output is not list or dict."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_make_int, nsref="t:make_int")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_upper, nsref="t:upper")  # pyright: ignore[reportPrivateUsage]

    sub = Dag()
    sub.node(ns.get("t:upper"))

    parent = Dag()
    s = parent.node(ns.get("t:make_int"))
    m = parent.map(sub, label="mapped")
    s >> m  # pyright: ignore[reportUnusedExpression]

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        runner = DagRunner(parent, provenance=DagProvenance(Path(tmp) / "runs.db"))
        result = await runner.run(
            source_inputs={},
            dag_nsref="t:type_test",
        )

        assert result.status == "failed"
        assert result.failed_node == "mapped"
        assert "expected list or dict" in (result.error or "").lower()


async def test_map_iteration_failure_propagates():
    """If a sub-DAG iteration fails, MapNode reports failure."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_split, nsref="t:split")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_fail_str, nsref="t:fail_str")  # pyright: ignore[reportPrivateUsage]

    sub = Dag()
    sub.node(ns.get("t:fail_str"))

    parent = Dag()
    s = parent.node(ns.get("t:split"))
    m = parent.map(sub, label="mapped")
    s >> m  # pyright: ignore[reportUnusedExpression]

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        runner = DagRunner(parent, provenance=DagProvenance(Path(tmp) / "runs.db"))
        result = await runner.run(
            source_inputs={"split": {"text": "a,b"}},
            dag_nsref="t:fail_map",
        )

        assert result.status == "failed"
        assert result.failed_node == "mapped"


async def test_sync_node_runs_in_executor_thread():
    """Sync DAG node should run in a separate thread, not the event loop thread."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._capture_thread, nsref="t:capture")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        dag.node(ns.get("t:capture"))

    runner = DagRunner(dag)
    result = await runner.run(
        source_inputs={"capture": {"value": 5}},
        dag_nsref="test:executor",
    )

    assert result.status == "completed"
    output = result.outputs["capture"]
    thread_name, value = output.rsplit(":", 1)
    assert value == "10"
    # Must NOT be the main thread (MainThread) -- should be a ThreadPoolExecutor thread
    assert thread_name != threading.current_thread().name


async def test_inline_sync_node_runs_on_event_loop_thread():
    """@inline sync DAG node should run on the event loop thread."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._capture_thread_inline, nsref="t:capture_inline")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        dag.node(ns.get("t:capture_inline"))

    runner = DagRunner(dag)
    result = await runner.run(
        source_inputs={"capture_inline": {"value": 5}},
        dag_nsref="test:inline",
    )

    assert result.status == "completed"
    output = result.outputs["capture_inline"]
    thread_name, value = output.rsplit(":", 1)
    assert value == "10"
    # Must be the same thread as the event loop
    assert thread_name == threading.current_thread().name


async def test_async_node_runs_on_event_loop_thread():
    """Async DAG node should always run on the event loop thread (regression guard)."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._capture_thread_async, nsref="t:capture_async")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        dag.node(ns.get("t:capture_async"))

    runner = DagRunner(dag)
    result = await runner.run(
        source_inputs={"capture_async": {"value": 5}},
        dag_nsref="test:async",
    )

    assert result.status == "completed"
    output = result.outputs["capture_async"]
    thread_name, value = output.rsplit(":", 1)
    assert value == "10"
    assert thread_name == threading.current_thread().name


async def test_sync_node_with_dag_context_runs_in_executor_thread():
    """Sync DAG node with DagContext should also run in a separate thread."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._capture_thread_with_ctx, nsref="t:capture_ctx")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        dag.node(ns.get("t:capture_ctx"))

    runner = DagRunner(dag)
    result = await runner.run(
        source_inputs={"capture_ctx": {"value": 5}},
        dag_nsref="test:executor_ctx",
    )

    assert result.status == "completed"
    output = result.outputs["capture_ctx"]
    thread_name, label, value = output.rsplit(":", 2)
    assert label == "capture_ctx"
    assert value == "10"
    assert thread_name != threading.current_thread().name


async def test_call_node_execution():
    """CallNode runs a sub-DAG once with upstream output."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_double_str, nsref="t:double")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_make_text, nsref="t:make_text")  # pyright: ignore[reportPrivateUsage]

    sub = Dag()
    sub.node(ns.get("t:double"))

    parent = Dag()
    s = parent.node(ns.get("t:make_text"))
    c = parent.node(sub, label="doubler")
    s >> c  # pyright: ignore[reportUnusedExpression]

    runner = DagRunner(parent)
    result = await runner.run(dag_nsref="t:call_test")

    assert result.status == "completed"
    assert result.outputs["doubler"] == "hellohello"


async def test_call_node_in_chain():
    """CallNode works in a multi-step chain: source -> call -> sink."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_upper, nsref="t:upper")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_double_str, nsref="t:double")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_make_text, nsref="t:make_text")  # pyright: ignore[reportPrivateUsage]

    # Sub-DAG: upper
    sub = Dag()
    sub.node(ns.get("t:upper"))

    # Parent: make_text -> call(upper) -> double
    parent = Dag()
    s = parent.node(ns.get("t:make_text"))
    c = parent.node(sub, label="shout")
    d = parent.node(ns.get("t:double"))
    s >> c >> d  # pyright: ignore[reportUnusedExpression]

    runner = DagRunner(parent)
    result = await runner.run(dag_nsref="t:chain_test")

    assert result.status == "completed"
    assert result.outputs["double"] == "HELLOHELLO"


async def test_call_node_error_propagation():
    """If call sub-DAG fails, parent reports failure."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_fail_str, nsref="t:fail")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_make_text, nsref="t:make_text")  # pyright: ignore[reportPrivateUsage]

    sub = Dag()
    sub.node(ns.get("t:fail"))

    parent = Dag()
    s = parent.node(ns.get("t:make_text"))
    c = parent.node(sub, label="broken")
    s >> c  # pyright: ignore[reportUnusedExpression]

    runner = DagRunner(parent)
    result = await runner.run(dag_nsref="t:fail_call")

    assert result.status == "failed"
    assert result.failed_node == "broken"


async def test_log_context_sync_node_in_executor():
    """Sync node in executor thread should have run_id and node_label in log records."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.log_context import NodeRunLogFilter
    from lythonic.compose.namespace import Dag, Namespace

    logger = _logging.getLogger("test.node")
    f = NodeRunLogFilter()
    logger.addFilter(f)

    records: list[_logging.LogRecord] = []
    handler = _logging.Handler()
    handler.emit = lambda record: records.append(record)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    logger.addHandler(handler)
    logger.setLevel(_logging.DEBUG)

    try:
        ns = Namespace()
        ns.register(this_module._log_and_return, nsref="t:log_sync")  # pyright: ignore[reportPrivateUsage]

        with Dag() as dag:
            dag.node(ns.get("t:log_sync"))

        runner = DagRunner(dag)
        result = await runner.run(
            source_inputs={"log_sync": {"value": 42}},
            dag_nsref="test:log_ctx",
        )

        assert result.status == "completed"
        assert len(records) == 1
        assert records[0].run_id != ""  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
        assert records[0].node_label == "log_sync"  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    finally:
        logger.removeHandler(handler)
        logger.removeFilter(f)


async def test_log_context_async_node():
    """Async node should have run_id and node_label in log records."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.log_context import NodeRunLogFilter
    from lythonic.compose.namespace import Dag, Namespace

    logger = _logging.getLogger("test.node")
    f = NodeRunLogFilter()
    logger.addFilter(f)

    records: list[_logging.LogRecord] = []
    handler = _logging.Handler()
    handler.emit = lambda record: records.append(record)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    logger.addHandler(handler)
    logger.setLevel(_logging.DEBUG)

    try:
        ns = Namespace()
        ns.register(this_module._log_and_return_async, nsref="t:log_async")  # pyright: ignore[reportPrivateUsage]

        with Dag() as dag:
            dag.node(ns.get("t:log_async"))

        runner = DagRunner(dag)
        result = await runner.run(
            source_inputs={"log_async": {"value": 42}},
            dag_nsref="test:log_ctx",
        )

        assert result.status == "completed"
        assert len(records) == 1
        assert records[0].run_id != ""  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
        assert records[0].node_label == "log_async"  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    finally:
        logger.removeHandler(handler)
        logger.removeFilter(f)


async def test_log_context_inline_node():
    """Inline node should have run_id and node_label in log records."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.log_context import NodeRunLogFilter
    from lythonic.compose.namespace import Dag, Namespace

    logger = _logging.getLogger("test.node")
    f = NodeRunLogFilter()
    logger.addFilter(f)

    records: list[_logging.LogRecord] = []
    handler = _logging.Handler()
    handler.emit = lambda record: records.append(record)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    logger.addHandler(handler)
    logger.setLevel(_logging.DEBUG)

    try:
        ns = Namespace()
        ns.register(this_module._log_and_return_inline, nsref="t:log_inline")  # pyright: ignore[reportPrivateUsage]

        with Dag() as dag:
            dag.node(ns.get("t:log_inline"))

        runner = DagRunner(dag)
        result = await runner.run(
            source_inputs={"log_inline": {"value": 42}},
            dag_nsref="test:log_ctx",
        )

        assert result.status == "completed"
        assert len(records) == 1
        assert records[0].run_id != ""  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
        assert records[0].node_label == "log_inline"  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    finally:
        logger.removeHandler(handler)
        logger.removeFilter(f)


def test_log_context_outside_node():
    """Log records outside node execution should have empty context fields."""
    from lythonic.compose.log_context import NodeRunLogFilter

    logger = _logging.getLogger("test.node.outside")
    f = NodeRunLogFilter()
    logger.addFilter(f)

    records: list[_logging.LogRecord] = []
    handler = _logging.Handler()
    handler.emit = lambda record: records.append(record)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    logger.addHandler(handler)
    logger.setLevel(_logging.DEBUG)

    try:
        logger.info("no context")
        assert len(records) == 1
        assert records[0].run_id == ""  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
        assert records[0].node_label == ""  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    finally:
        logger.removeHandler(handler)
        logger.removeFilter(f)


# Runtime resolution with CacheProhibitDirectCall


def _guarded_fetch(ticker: str) -> dict[str, float]:  # pyright: ignore[reportUnusedFunction]
    """A fetch function that refuses direct calls — must go through cache wrapper."""
    from lythonic.compose.cached import CacheProhibitDirectCall

    CacheProhibitDirectCall.require()
    return {"price": 100.0, "ticker": ticker}  # pyright: ignore[reportReturnType]


async def _format_price(data: dict[str, float]) -> str:  # pyright: ignore[reportUnusedFunction]
    return f"{data['ticker']}={data['price']}"


async def test_runtime_resolution_detached_dag_fails_with_guard():
    """A detached DAG calling a guarded function should fail."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag

    # ns = Namespace()
    # ns.register(this_module._guarded_fetch, nsref="market:fetch")  # pyright: ignore[reportPrivateUsage]
    # ns.register(this_module._format_price, nsref="market:format")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    f = dag.node(this_module._guarded_fetch)  # pyright: ignore[reportPrivateUsage]
    p = dag.node(this_module._format_price)  # pyright: ignore[reportPrivateUsage]
    f >> p  # pyright: ignore[reportUnusedExpression]

    # Detached DAG (no parent_namespace) calls the raw function directly
    runner = DagRunner(dag)
    result = await runner.run(
        source_inputs={"_guarded_fetch": {"ticker": "AAPL"}},
        dag_nsref="test:detached",
    )
    assert result.status == "failed"
    assert "cache wrapper" in (result.error or "").lower()


async def test_runtime_resolution_attached_dag_succeeds_with_cache():
    """An attached DAG resolves to the cached version and succeeds."""
    from lythonic.compose.cached import register_cached_callable
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        db_path = Path(tmp) / "cache.db"

        # Register guarded function WITH cache wrapping in the namespace
        register_cached_callable(
            ns,
            gref="tests.test_dag_runner:_guarded_fetch",
            min_ttl=1.0,
            max_ttl=2.0,
            db_path=db_path,
        )

        # Build a DAG using the namespace nodes

        dag = Dag()
        f = dag.node(this_module._guarded_fetch)  # pyright: ignore[reportPrivateUsage]
        p = dag.node(this_module._format_price)  # pyright: ignore[reportPrivateUsage]
        f >> p  # pyright: ignore[reportUnusedExpression]

        # Register DAG in namespace (sets parent_namespace for runtime resolution)
        ns.register(dag, nsref="pipelines:prices")

        # Run via the registered DAG — runner resolves via parent_namespace
        runner = DagRunner(dag)
        result = await runner.run(
            source_inputs={"_guarded_fetch": {"ticker": "AAPL"}},
            dag_nsref="test:attached",
        )
        assert result.status == "completed"
        print(result.outputs)
        assert result.outputs["_format_price"] == "AAPL=100.0"
