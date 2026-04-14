"""
End-to-end test for the lyth CLI engine.

Extracts the YAML config from do_sleep_repeat.py's docstring, runs
`lyth start` in a temp directory, waits for triggers to fire, then
stops and verifies provenance records.

Marked @slow — not included in default `make test`.
Run with: `uv run pytest tests/test_lyth_e2e.py -m slow -v`
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest
import yaml

from lythonic.compose.dag_provenance import DagProvenance


def _extract_yaml_from_docstring() -> str:
    """Extract the YAML config from do_sleep_repeat.py's docstring."""
    import lythonic.examples.do_sleep_repeat as mod

    doc = mod.__doc__
    assert doc is not None, "do_sleep_repeat.py has no docstring"
    # Extract everything after the "---" marker
    parts = doc.split("---", 1)
    assert len(parts) == 2, "Expected '---' separator in docstring"
    return parts[1].strip()


@pytest.mark.slow
def test_lyth_engine_e2e():
    """
    Full lifecycle: start engine -> wait for triggers -> stop -> verify provenance.
    """
    yaml_content = _extract_yaml_from_docstring()

    # Validate the YAML parses
    config = yaml.safe_load(yaml_content)
    assert "namespace" in config, "YAML must have 'namespace' key"

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        data_dir = Path(tmp)
        config_path = data_dir / "lyth.yaml"
        config_path.write_text(yaml_content)

        # Start lyth engine as a subprocess
        env = os.environ.copy()
        proc = subprocess.Popen(
            [sys.executable, "-m", "lythonic.compose.lyth", "--data", str(data_dir), "start"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        try:
            # Wait a moment for startup
            time.sleep(3)

            # Verify process is running
            assert proc.poll() is None, f"lyth start exited early with code {proc.returncode}"

            # Verify PID file exists
            pid_path = data_dir / "lyth.pid"
            assert pid_path.exists(), "PID file not created"
            pid = int(pid_path.read_text().strip())
            assert pid == proc.pid, f"PID mismatch: file={pid}, proc={proc.pid}"

            # Wait for triggers to fire (2 minutes)
            # Schedules are */19s, */17s, */23s — should fire multiple times
            time.sleep(120)

            # Verify still running
            assert proc.poll() is None, "lyth died during run"

            # Stop via SIGTERM (same as `lyth stop`)
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=10)

            assert proc.returncode == 0, f"lyth exited with code {proc.returncode}"

            # Verify PID file cleaned up
            assert not pid_path.exists(), "PID file not cleaned up after stop"

        except Exception:
            # Kill if something went wrong
            proc.kill()
            proc.wait()
            raise

        # Verify provenance records
        dags_db = data_dir / "dags.db"
        assert dags_db.exists(), "dags.db not created"

        prov = DagProvenance(dags_db)
        runs = prov.get_recent_runs(limit=100)

        assert len(runs) > 0, "No DAG runs recorded"

        # Check that we have runs for different DAGs
        nsrefs = {r.dag_nsref for r in runs}
        assert "examples:task1" in nsrefs, f"No task1 runs. Found: {nsrefs}"
        assert "examples:dag1" in nsrefs or "examples:dag1__" in nsrefs, (
            f"No dag1 runs. Found: {nsrefs}"
        )

        # Verify run structure
        for run in runs:
            assert run.run_id, "Empty run_id"
            assert run.status in ("completed", "failed", "running"), (
                f"Unexpected status: {run.status}"
            )
            assert run.started_at is not None, "No started_at"

            # Check latest_update works
            latest = run.latest_update()
            assert latest >= run.started_at, "latest_update < started_at"

        # Check completed runs have node executions
        completed = [r for r in runs if r.status == "completed"]
        assert len(completed) > 0, "No completed runs"

        for run in completed:
            assert len(run.nodes) > 0, f"Completed run {run.run_id} has no nodes"
            assert run.finished_at is not None, f"Completed run {run.run_id} has no finished_at"

            # Each node should have a status
            for node in run.nodes:
                assert node.status in ("completed", "skipped"), (
                    f"Node {node.node_label} status: {node.status}"
                )
                assert node.node_type is not None, f"Node {node.node_label} has no node_type"

        # Verify trigger DB
        triggers_db = data_dir / "triggers.db"
        assert triggers_db.exists(), "triggers.db not created"

        # Verify log file
        log_file = data_dir / "lyth.log"
        assert log_file.exists(), "lyth.log not created"
        log_content = log_file.read_text()
        assert "Starting" in log_content or "task1" in log_content, "Log seems empty"

        # Summary
        print("\nE2E Results:")
        print(f"  Total runs: {len(runs)}")
        print(f"  Completed: {len(completed)}")
        print(f"  DAG types: {nsrefs}")
        print(f"  Log size: {len(log_content)} bytes")
