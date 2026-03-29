"""
DagRunner: Async execution engine for DAGs with provenance tracking.

Executes DAG nodes in topological order, wiring outputs to inputs by type.
Supports DagContext injection, pause/restart, and selective replay.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from lythonic.compose.dag_provenance import DagProvenance
from lythonic.compose.namespace import Dag, DagContext, DagNode


class DagPause(Exception):
    """Raise from a DAG node to signal the runner should pause."""


class DagRunResult(BaseModel):
    """Result of a DAG execution."""

    run_id: str
    status: str
    outputs: dict[str, Any]
    failed_node: str | None = None
    error: str | None = None


class DagRunner:
    """
    Async execution engine for DAGs with provenance tracking.

    Executes nodes in topological order, wires outputs to inputs by type
    (with fan-in list collection), injects `DagContext` when expected,
    and supports pause/restart/replay.
    """

    dag: Dag
    provenance: DagProvenance
    _pause_requested: bool

    def __init__(self, dag: Dag, db_path: Path) -> None:
        self.dag = dag
        self.provenance = DagProvenance(db_path)
        self._pause_requested = False

    def pause(self) -> None:
        """Request pause after the current node completes."""
        self._pause_requested = True

    async def run(
        self,
        source_inputs: dict[str, dict[str, Any]] | None = None,
        dag_nsref: str | None = None,
    ) -> DagRunResult:
        """Execute the DAG from scratch."""
        run_id = str(uuid.uuid4())
        nsref = dag_nsref or "unknown"
        self.provenance.create_run(run_id, nsref, source_inputs or {})
        self._pause_requested = False

        return await self._execute(run_id, nsref, source_inputs or {}, completed_outputs={})

    async def _execute(
        self,
        run_id: str,
        dag_nsref: str,
        source_inputs: dict[str, dict[str, Any]],
        completed_outputs: dict[str, Any],
    ) -> DagRunResult:
        """Core execution loop shared by run, restart, and replay."""
        node_outputs: dict[str, Any] = dict(completed_outputs)
        order = self.dag.topological_order()
        sink_labels = {n.label for n in self.dag.sinks()}

        for dag_node in order:
            if dag_node.label in node_outputs:
                continue

            kwargs = self._wire_inputs(dag_node, node_outputs, source_inputs)
            self.provenance.record_node_start(
                run_id, dag_node.label, json.dumps(kwargs, default=str)
            )

            try:
                result = await self._call_node(dag_node, run_id, dag_nsref, kwargs)
                node_outputs[dag_node.label] = result
                self.provenance.record_node_complete(
                    run_id, dag_node.label, json.dumps(result, default=str)
                )
            except DagPause:
                self.provenance.update_run_status(run_id, "paused")
                return DagRunResult(
                    run_id=run_id,
                    status="paused",
                    outputs={lb: node_outputs[lb] for lb in sink_labels if lb in node_outputs},
                )
            except Exception as e:
                self.provenance.record_node_failed(run_id, dag_node.label, str(e))
                self.provenance.finish_run(run_id, "failed")
                return DagRunResult(
                    run_id=run_id,
                    status="failed",
                    outputs={lb: node_outputs[lb] for lb in sink_labels if lb in node_outputs},
                    failed_node=dag_node.label,
                    error=str(e),
                )

            if self._pause_requested:
                self.provenance.update_run_status(run_id, "paused")
                return DagRunResult(
                    run_id=run_id,
                    status="paused",
                    outputs={lb: node_outputs[lb] for lb in sink_labels if lb in node_outputs},
                )

        self.provenance.finish_run(run_id, "completed")
        return DagRunResult(
            run_id=run_id,
            status="completed",
            outputs={lb: node_outputs[lb] for lb in sink_labels if lb in node_outputs},
        )

    async def _call_node(
        self,
        dag_node: DagNode,
        run_id: str,
        dag_nsref: str,
        kwargs: dict[str, Any],
    ) -> Any:
        """Call a node's function, injecting DagContext if expected."""
        fn = dag_node.ns_node._decorated or dag_node.ns_node.method.o  # pyright: ignore[reportPrivateUsage]

        if dag_node.ns_node.expects_dag_context():
            ctx_type = dag_node.ns_node.dag_context_type() or DagContext
            ctx = ctx_type(
                dag_nsref=dag_nsref,
                node_label=dag_node.label,
                run_id=run_id,
            )
            if asyncio.iscoroutinefunction(fn):
                return await fn(ctx, **kwargs)
            return fn(ctx, **kwargs)

        if asyncio.iscoroutinefunction(fn):
            return await fn(**kwargs)
        return fn(**kwargs)

    async def restart(self, run_id: str) -> DagRunResult:
        """Restart a paused or failed run from where it left off."""
        run_info = self.provenance.get_run(run_id)
        if run_info is None:
            raise ValueError(f"Run '{run_id}' not found")
        if run_info["status"] not in ("paused", "failed"):
            raise ValueError(
                f"Run '{run_id}' has status '{run_info['status']}', expected 'paused' or 'failed'"
            )

        # Load completed node outputs
        completed_outputs: dict[str, Any] = {}
        for node_exec in self.provenance.get_node_executions(run_id):
            if node_exec["status"] == "completed" and node_exec["output_json"]:
                completed_outputs[node_exec["node_label"]] = json.loads(node_exec["output_json"])

        self.provenance.update_run_status(run_id, "running")
        self._pause_requested = False

        source_inputs_json: str = run_info.get("source_inputs_json", "{}")
        source_inputs: dict[str, dict[str, Any]] = (
            json.loads(source_inputs_json) if source_inputs_json else {}
        )

        return await self._execute(run_id, run_info["dag_nsref"], source_inputs, completed_outputs)

    async def replay(
        self,
        run_id: str,
        rerun_nodes: set[str],
    ) -> DagRunResult:
        """
        Re-execute selected nodes using a previous run's outputs for
        the rest. Creates a new run.
        """
        old_run = self.provenance.get_run(run_id)
        if old_run is None:
            raise ValueError(f"Run '{run_id}' not found")

        # Load outputs from old run
        old_outputs: dict[str, Any] = {}
        for node_exec in self.provenance.get_node_executions(run_id):
            if node_exec["status"] in ("completed", "skipped") and node_exec["output_json"]:
                old_outputs[node_exec["node_label"]] = json.loads(node_exec["output_json"])

        # Create new run
        new_run_id = str(uuid.uuid4())
        source_inputs_json: str = old_run.get("source_inputs_json", "{}")
        source_inputs: dict[str, dict[str, Any]] = (
            json.loads(source_inputs_json) if source_inputs_json else {}
        )
        self.provenance.create_run(new_run_id, old_run["dag_nsref"], source_inputs)
        self._pause_requested = False

        # Pre-populate outputs for nodes NOT being re-run
        completed_outputs: dict[str, Any] = {}
        for label, output in old_outputs.items():
            if label not in rerun_nodes:
                completed_outputs[label] = output
                self.provenance.record_node_skipped(
                    new_run_id, label, json.dumps(output, default=str)
                )

        return await self._execute(
            new_run_id, old_run["dag_nsref"], source_inputs, completed_outputs
        )

    @staticmethod
    def _extract_list_element_type(annotation: Any) -> str | None:
        """If annotation is 'list[X]', return 'X'. Otherwise None."""
        if isinstance(annotation, str):
            s = annotation.strip()
            if s.startswith("list[") and s.endswith("]"):
                return s[5:-1]
        return None

    def _wire_inputs(
        self,
        dag_node: DagNode,
        node_outputs: dict[str, Any],
        source_inputs: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Build kwargs for a node by wiring upstream outputs to parameters."""
        upstream_edges = [e for e in self.dag.edges if e.downstream == dag_node.label]

        # Source nodes get inputs from source_inputs
        if not upstream_edges:
            return dict(source_inputs.get(dag_node.label, {}))

        args = dag_node.ns_node.method.args
        if dag_node.ns_node.expects_dag_context() and len(args) > 0:
            args = args[1:]

        kwargs: dict[str, Any] = {}
        for arg in args:
            # Check if parameter expects a list (fan-in collection)
            list_elem_type = self._extract_list_element_type(arg.annotation)

            matching_values: list[Any] = []
            for edge in upstream_edges:
                if edge.upstream in node_outputs:
                    upstream_node = self.dag.nodes[edge.upstream]
                    upstream_return = upstream_node.ns_node.method.return_annotation
                    if upstream_return is None:
                        continue
                    # Direct type match or list element type match for fan-in
                    if upstream_return == arg.annotation:
                        matching_values.append(node_outputs[edge.upstream])
                    elif list_elem_type and str(upstream_return) == list_elem_type:
                        matching_values.append(node_outputs[edge.upstream])

            if len(matching_values) == 1:
                # For list-typed params, always wrap in a list
                kwargs[arg.name] = [matching_values[0]] if list_elem_type else matching_values[0]
            elif len(matching_values) > 1:
                kwargs[arg.name] = matching_values

        return kwargs
