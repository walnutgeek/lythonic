from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path

import tests.test_dag_runner as this_module
from lythonic.compose.namespace import DagContext


def test_provenance_create_and_get_run():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
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

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.update_run_status("run-1", "paused")
        assert prov.get_run("run-1")["status"] == "paused"  # pyright: ignore

        prov.finish_run("run-1", "completed")
        run = prov.get_run("run-1")
        assert run is not None
        assert run["status"] == "completed"
        assert run["finished_at"] is not None


def test_provenance_node_lifecycle():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.record_node_start("run-1", "fetch", '{"ticker": "AAPL"}')
        execs = prov.get_node_executions("run-1")
        assert len(execs) == 1
        assert execs[0]["status"] == "running"

        prov.record_node_complete("run-1", "fetch", '{"price": 150.0}')
        execs = prov.get_node_executions("run-1")
        assert execs[0]["status"] == "completed"
        assert execs[0]["output_json"] == '{"price": 150.0}'
        assert execs[0]["finished_at"] is not None


def test_provenance_node_failed():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.record_node_start("run-1", "fetch", "{}")
        prov.record_node_failed("run-1", "fetch", "Connection timeout")

        execs = prov.get_node_executions("run-1")
        assert execs[0]["status"] == "failed"
        assert execs[0]["error"] == "Connection timeout"


def test_provenance_node_skipped():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.record_node_skipped("run-1", "fetch", '{"price": 100.0}')
        execs = prov.get_node_executions("run-1")
        assert execs[0]["status"] == "skipped"
        assert execs[0]["output_json"] == '{"price": 100.0}'


def test_provenance_get_node_output():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.record_node_start("run-1", "fetch", "{}")
        prov.record_node_complete("run-1", "fetch", '{"v": 1}')

        assert prov.get_node_output("run-1", "fetch") == '{"v": 1}'
        assert prov.get_node_output("run-1", "missing") is None


def test_provenance_get_pending_nodes():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        prov.create_run("run-1", "p:d", {})

        prov.record_node_start("run-1", "fetch", "{}")
        prov.record_node_complete("run-1", "fetch", "{}")
        prov.record_node_start("run-1", "compute", "{}")
        prov.record_node_failed("run-1", "compute", "err")

        pending = prov.get_pending_nodes("run-1")
        assert "compute" in pending
        assert "fetch" not in pending


def test_provenance_get_missing_run():
    from lythonic.compose.dag_provenance import DagProvenance

    with tempfile.TemporaryDirectory() as tmp:
        prov = DagProvenance(Path(tmp) / "test.db")
        assert prov.get_run("nonexistent") is None


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

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")
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

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "runs.db"
        runner = DagRunner(dag, db_path)
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

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")
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

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")
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

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")
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

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")
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

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")

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

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")

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

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")

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

    with tempfile.TemporaryDirectory() as tmp:
        runner = DagRunner(dag, Path(tmp) / "runs.db")

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

    with tempfile.TemporaryDirectory() as tmp:
        dag.db_path = Path(tmp) / "runs.db"
        ns.register(dag, nsref="pipelines:my_pipe")

        node = ns.get("pipelines:my_pipe")
        result = await node(source={"ticker": "X"})

        assert result.run_id is not None  # pyright: ignore
        assert result.status == "completed"  # pyright: ignore
        assert result.outputs["double"] == 200.0  # pyright: ignore


async def test_dag_registered_without_db_path():
    """Dag with no db_path registered in Namespace uses NullProvenance."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._async_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._async_double, nsref="t:double")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s = dag.node(ns.get("t:source"))
        d = dag.node(ns.get("t:double"))
        s >> d  # pyright: ignore[reportUnusedExpression]

    # No db_path set
    ns.register(dag, nsref="pipelines:no_persist")

    node = ns.get("pipelines:no_persist")
    result = await node(source={"ticker": "X"})
    assert result.run_id is not None  # pyright: ignore
    assert result.status == "completed"  # pyright: ignore
    assert result.outputs["double"] == 200.0  # pyright: ignore


async def test_runner_without_persistence():
    """DagRunner with db_path=None executes successfully."""
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
