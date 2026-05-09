"""
End-to-end test for the lyth CLI engine.

Extracts the YAML config from do_sleep_repeat.py's docstring, runs
`lyth start` in a temp directory, waits for triggers to fire, then
stops and verifies provenance records.

Marked @slow — not included in default `make test`.
Run with: `uv run pytest tests/test_lyth_e2e.py -m slow -v`
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path

import pytest
import yaml

from lythonic.compose.dag_provenance import DagProvenance


def _extract_yaml_from_docstring() -> str:
    """Extract the YAML config from do_sleep_repeat.py's docstring."""
    import lythonic.examples.do_sleep_repeat as mod

    doc = mod.__doc__
    assert doc is not None, "do_sleep_repeat.py has no docstring"
    # Extract everything between the "---" markers
    parts = doc.split("---")
    assert len(parts) >= 3, "Expected '---' separators in docstring"
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
        runs = prov.get_recent_runs(limit=200)

        assert len(runs) > 0, "No DAG runs recorded"

        # Check that we have runs for different DAGs
        nsrefs = {r.dag_nsref for r in runs}
        assert "examples:task1" in nsrefs, f"No task1 runs. Found: {nsrefs}"
        assert "examples:dag1" in nsrefs or "examples:dag1__" in nsrefs, (
            f"No dag1 runs. Found: {nsrefs}"
        )

        # Fragment-registered DAGs should have runs
        assert "frag:frag_dag__" in nsrefs, f"No frag_dag runs. Found: {nsrefs}"
        assert "transforms:double_dag__" in nsrefs, (
            f"No transforms:double_dag runs. Found: {nsrefs}"
        )

        # -- always_failing_task: should have failed runs with traceback --
        fail_nsref = "lythonic.examples.do_sleep_repeat:always_failing_task"
        assert fail_nsref in nsrefs, f"No always_failing_task runs. Found: {nsrefs}"
        fail_runs = [r for r in runs if r.dag_nsref == fail_nsref]
        assert all(r.status == "failed" for r in fail_runs), (
            f"Expected all always_failing_task runs to be failed, got: "
            f"{[r.status for r in fail_runs]}"
        )
        # Node error should contain a full traceback, not just the message
        for run in fail_runs:
            failed_nodes = [n for n in run.nodes.values() if n.status == "failed"]
            assert len(failed_nodes) > 0, f"Failed run {run.run_id} has no failed nodes"
            for node in failed_nodes:
                assert node.error is not None, f"Failed node {node.node_label} has no error"
                assert "Traceback" in node.error, (
                    f"Error for {node.node_label} missing traceback:\n{node.error}"
                )
                assert "ValueError" in node.error, (
                    f"Error for {node.node_label} missing ValueError:\n{node.error}"
                )

        # -- play_with_ctx: should succeed (ns_call works) --
        ctx_nsref = "lythonic.examples.do_sleep_repeat:play_with_ctx"
        assert ctx_nsref in nsrefs, f"No play_with_ctx runs. Found: {nsrefs}"
        ctx_runs = [r for r in runs if r.dag_nsref == ctx_nsref]
        ctx_completed = [r for r in ctx_runs if r.status == "completed"]
        assert len(ctx_completed) > 0, (
            f"No completed play_with_ctx runs. Statuses: {[r.status for r in ctx_runs]}"
        )

        # -- Cache: get_timestamp should produce different values over 120s --
        # TTLs: min_ttl=0.0005 days (~43s), max_ttl=0.001 days (~86s)
        # Over 120s the cache should refresh at least once.
        cache_db = data_dir / "cache.db"
        assert cache_db.exists(), "cache.db not created"
        with closing(sqlite3.connect(str(cache_db))) as conn:
            cursor = conn.cursor()
            # The table name derives from nsref "examples:get_timestamp"
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='examples__get_timestamp'"
            )
            assert cursor.fetchone() is not None, "Cache table examples__get_timestamp not created"
            cursor.execute("SELECT value_json, fetched_at FROM examples__get_timestamp")
            row = cursor.fetchone()
            assert row is not None, "No cache entry for get_timestamp"
            # The current cached value exists; verify it's a valid timestamp
            cached_ts = json.loads(row[0])
            assert isinstance(cached_ts, float), f"Cached value is not a float: {cached_ts}"

        # Verify cache refreshes via the log: get_timestamp should have been
        # called fresh more than once during the 120s window (min_ttl ~43s).
        log_file = data_dir / "lyth.log"
        assert log_file.exists(), "lyth.log not created"
        log_content = log_file.read_text()
        fresh_calls = [
            line for line in log_content.splitlines() if "get_timestamp called (fresh)" in line
        ]
        assert len(fresh_calls) >= 2, (
            f"Expected get_timestamp to refresh at least twice over 120s, "
            f"got {len(fresh_calls)} fresh calls"
        )
        # Extract timestamps from log lines to verify they differ
        fresh_timestamps: list[float] = []
        for line in fresh_calls:
            # Format: "get_timestamp called (fresh): 1234567890.123"
            ts_str = line.rsplit(":", 1)[-1].strip()
            try:
                fresh_timestamps.append(float(ts_str))
            except ValueError:
                pass
        if len(fresh_timestamps) >= 2:
            assert fresh_timestamps[0] != fresh_timestamps[-1], (
                f"Cache never refreshed: all timestamps are {fresh_timestamps[0]}"
            )

        # -- Log file: traceback for always_failing_task should appear --
        assert "Traceback" in log_content, "No traceback found in log"
        assert "ValueError" in log_content, "No ValueError found in log"
        assert "Failing" in log_content, "ValueError message 'Failing' not in log"

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
            for node in run.nodes.values():
                assert node.status in ("completed", "skipped"), (
                    f"Node {node.node_label} status: {node.status}"
                )
                assert isinstance(node.is_source, bool), f"Node {node.node_label} missing is_source"
                assert isinstance(node.is_sink, bool), f"Node {node.node_label} missing is_sink"

        # Verify trigger DB
        triggers_db = data_dir / "triggers.db"
        assert triggers_db.exists(), "triggers.db not created"

        # Verify failed runs have proper structure
        failed = [r for r in runs if r.status == "failed"]
        assert len(failed) > 0, "No failed runs (expected from always_failing_task)"
        for run in failed:
            assert run.finished_at is not None, f"Failed run {run.run_id} has no finished_at"

        # Summary
        failed_nsrefs = {r.dag_nsref for r in failed}
        print("\nE2E Results:")
        print(f"  Total runs: {len(runs)}")
        print(f"  Completed: {len(completed)}")
        print(f"  Failed: {len(failed)} ({failed_nsrefs})")
        print(f"  DAG types: {nsrefs}")
        print(f"  Cache fresh calls: {len(fresh_calls)}")
        print(f"  Log size: {len(log_content)} bytes")
