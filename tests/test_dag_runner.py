from __future__ import annotations

import json
import tempfile
from pathlib import Path

import tests.test_dag_runner as this_module


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
