# pyright: reportImportCycles=false
"""
DagRunner: Async execution engine for DAGs with provenance tracking.

Executes DAG nodes in topological order, wiring outputs to inputs by type.
Supports `DagContext` injection, pause/restart, and selective replay.

Constructor: `DagRunner(dag, provenance=...)`. When `provenance` is omitted,
defaults to `NullProvenance` (no persistence). Pass a `DagProvenance(path)`
for SQLite-backed run history.

Sync node functions are dispatched to a thread executor by default so they
don't block the async event loop. Use the `@inline` decorator
(`lythonic.compose._inline.inline`) to opt out for lightweight pure-computation
functions.

Handles `CompositeDagNode` subtypes: `MapNode` runs a sub-DAG on each element
of a collection (concurrent via `asyncio.gather`), and `CallNode` runs a
sub-DAG once as a single pipeline step.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import json
import uuid
from typing import Any, cast

from pydantic import BaseModel

from lythonic.compose.dag_provenance import DagProvenance, NullProvenance


def _json_default(obj: Any) -> Any:
    """JSON serialization fallback for Pydantic models and other types."""
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    return str(obj)


from lythonic.compose.log_context import reset_node_run_context, set_node_run_context
from lythonic.compose.namespace import (
    CallNode,
    CompositeDagNode,
    Dag,
    DagContext,
    DagNode,
    LabelSwitch,
    MapNode,
    MapSwitchNode,
    SwitchNode,
)


def _extract_switch(node_label: str, upstream: Any) -> tuple[LabelSwitch, Any]:  # pyright: ignore[reportUnknownParameterType]
    """Extract LabelSwitch and data from upstream output."""
    if isinstance(upstream, dict) and "__switch__" in upstream:
        return str(upstream.pop("__switch__")), upstream  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType,reportUnknownVariableType]
    if isinstance(upstream, (list, tuple)) and len(upstream) >= 2:  # pyright: ignore[reportUnknownArgumentType]
        data = upstream[1] if len(upstream) == 2 else upstream[1:]  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
        return str(upstream[0]), data  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
    raise TypeError(
        f"SwitchNode '{node_label}' requires upstream to provide a LabelSwitch. "
        f"Got {type(upstream).__name__}"  # pyright: ignore[reportUnknownArgumentType]
    )


def _single_element_inputs(dag: Dag, source_label: str, element: Any) -> dict[str, Any]:
    """Build source_inputs dict for a single sub-DAG source from one element."""
    source_node = dag.nodes[source_label]
    args = source_node.ns_node.method.args
    if source_node.ns_node.expects_dag_context() and len(args) > 0:
        args = args[1:]
    if len(args) == 1:
        return {args[0].name: element}
    if isinstance(element, dict):
        return element  # pyright: ignore[reportUnknownVariableType]
    return {args[0].name: element} if args else {}


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
    provenance: DagProvenance | NullProvenance
    _pause_requested: bool

    def __init__(self, dag: Dag, provenance: DagProvenance | NullProvenance | None = None) -> None:
        self.dag = dag
        self.provenance = provenance or NullProvenance()
        self._pause_requested = False

    def pause(self) -> None:
        """Request pause after the current node completes."""
        self._pause_requested = True

    async def run(
        self,
        source_inputs: dict[str, dict[str, Any]] | None = None,
        dag_nsref: str | None = None,
        parent_run_id: str | None = None,
    ) -> DagRunResult:
        """Execute the DAG from scratch."""
        run_id = str(uuid.uuid4())
        nsref = dag_nsref or "unknown"
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, self.provenance.create_run, run_id, nsref, source_inputs or {}, parent_run_id
        )
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
        source_labels = {n.label for n in self.dag.sources()}
        sink_labels = {n.label for n in self.dag.sinks()}

        def _node_type(label: str) -> str:
            if isinstance(self.dag.nodes[label], CompositeDagNode):
                return "composite"
            if label in source_labels:
                return "source"
            if label in sink_labels:
                return "sink"
            return "internal"

        def _outgoing_edges(label: str) -> list[tuple[str, str]]:
            return [(e.upstream, e.downstream) for e in self.dag.edges if e.upstream == label]

        loop = asyncio.get_running_loop()

        for dag_node in order:
            if dag_node.label in node_outputs:
                continue

            nt = _node_type(dag_node.label)

            if isinstance(dag_node, CompositeDagNode):
                await loop.run_in_executor(
                    None,
                    self.provenance.record_node_start,
                    run_id,
                    dag_node.label,
                    json.dumps({"composite": type(dag_node).__name__}, default=_json_default),
                    nt,
                )
                try:
                    if isinstance(dag_node, MapNode):
                        result = await self._execute_map_node(
                            dag_node, run_id, dag_nsref, node_outputs
                        )
                    elif isinstance(dag_node, CallNode):
                        result = await self._execute_call_node(
                            dag_node, run_id, dag_nsref, node_outputs
                        )
                    elif isinstance(dag_node, MapSwitchNode):
                        result = await self._execute_map_switch_node(
                            dag_node, run_id, dag_nsref, node_outputs
                        )
                    elif isinstance(dag_node, SwitchNode):
                        result = await self._execute_switch_node(
                            dag_node, run_id, dag_nsref, node_outputs
                        )
                    else:
                        raise ValueError(f"Unknown composite node type: {type(dag_node)}")
                    node_outputs[dag_node.label] = result
                    output_json = json.dumps(result, default=_json_default)
                    edges = _outgoing_edges(dag_node.label)
                    await loop.run_in_executor(
                        None,
                        self.provenance.complete_node_with_edges,
                        run_id,
                        dag_node.label,
                        output_json,
                        edges,
                    )
                except Exception as e:
                    await loop.run_in_executor(
                        None,
                        self.provenance.fail_node_and_finish_run,
                        run_id,
                        dag_node.label,
                        str(e),
                    )
                    return DagRunResult(
                        run_id=run_id,
                        status="failed",
                        outputs={lb: node_outputs[lb] for lb in sink_labels if lb in node_outputs},
                        failed_node=dag_node.label,
                        error=str(e),
                    )

                if self._pause_requested:
                    await loop.run_in_executor(
                        None, self.provenance.update_run_status, run_id, "paused"
                    )
                    return DagRunResult(
                        run_id=run_id,
                        status="paused",
                        outputs={lb: node_outputs[lb] for lb in sink_labels if lb in node_outputs},
                    )
                continue

            kwargs = self._wire_inputs(dag_node, node_outputs, source_inputs)
            await loop.run_in_executor(
                None,
                self.provenance.record_node_start,
                run_id,
                dag_node.label,
                json.dumps(kwargs, default=_json_default),
                nt,
            )

            try:
                result = await self._call_node(dag_node, run_id, dag_nsref, kwargs)
                node_outputs[dag_node.label] = result
                output_json = json.dumps(result, default=_json_default)
                edges = _outgoing_edges(dag_node.label)
                await loop.run_in_executor(
                    None,
                    self.provenance.complete_node_with_edges,
                    run_id,
                    dag_node.label,
                    output_json,
                    edges,
                )
            except DagPause:
                await loop.run_in_executor(
                    None, self.provenance.update_run_status, run_id, "paused"
                )
                return DagRunResult(
                    run_id=run_id,
                    status="paused",
                    outputs={lb: node_outputs[lb] for lb in sink_labels if lb in node_outputs},
                )
            except Exception as e:
                await loop.run_in_executor(
                    None,
                    self.provenance.fail_node_and_finish_run,
                    run_id,
                    dag_node.label,
                    str(e),
                )
                return DagRunResult(
                    run_id=run_id,
                    status="failed",
                    outputs={lb: node_outputs[lb] for lb in sink_labels if lb in node_outputs},
                    failed_node=dag_node.label,
                    error=str(e),
                )

            if self._pause_requested:
                await loop.run_in_executor(
                    None, self.provenance.update_run_status, run_id, "paused"
                )
                return DagRunResult(
                    run_id=run_id,
                    status="paused",
                    outputs={lb: node_outputs[lb] for lb in sink_labels if lb in node_outputs},
                )

        await loop.run_in_executor(None, self.provenance.finish_run, run_id, "completed")
        return DagRunResult(
            run_id=run_id,
            status="completed",
            outputs={lb: node_outputs[lb] for lb in sink_labels if lb in node_outputs},
        )

    def _get_upstream_output(
        self,
        composite_node: CompositeDagNode,
        node_outputs: dict[str, Any],
    ) -> Any:
        """Get the single upstream output for a composite node."""
        upstream_edges = [e for e in self.dag.edges if e.downstream == composite_node.label]
        if len(upstream_edges) != 1:
            raise ValueError(
                f"CompositeDagNode '{composite_node.label}' must have exactly one upstream edge, "
                f"found {len(upstream_edges)}"
            )
        return node_outputs[upstream_edges[0].upstream]

    async def _run_sub_dag(
        self,
        sub_dag: Dag,
        element: Any,
        key: str,
        dag_nsref: str,
        parent_run_id: str,
    ) -> Any:
        """Run a sub-DAG once with a single element as input."""
        sub_sources = sub_dag.sources()
        sub_sinks = sub_dag.sinks()
        source_label = sub_sources[0].label
        sink_label = sub_sinks[0].label

        sub_runner = DagRunner(sub_dag, provenance=self.provenance)
        iter_nsref = f"{dag_nsref}/{key}"
        sub_result = await sub_runner.run(
            source_inputs={source_label: _single_element_inputs(sub_dag, source_label, element)},
            dag_nsref=iter_nsref,
            parent_run_id=parent_run_id,
        )
        if sub_result.status != "completed":
            raise RuntimeError(f"Sub-DAG [{key}] failed: {sub_result.error}")
        return sub_result.outputs[sink_label]

    async def _execute_map_node(
        self,
        map_node: MapNode,
        run_id: str,
        dag_nsref: str,
        node_outputs: dict[str, Any],
    ) -> Any:
        """Execute a MapNode by running sub-DAG on each collection element."""
        collection = self._get_upstream_output(map_node, node_outputs)

        if isinstance(collection, list):
            items: list[tuple[str, Any]] = [
                (str(i), v) for i, v in enumerate(cast(list[Any], collection))
            ]
            is_dict = False
        elif isinstance(collection, dict):
            items = [(str(k), v) for k, v in cast(dict[str, Any], collection).items()]
            is_dict = True
        else:
            raise TypeError(
                f"MapNode '{map_node.label}' received {type(collection).__name__}, "
                f"expected list or dict"
            )

        results = await asyncio.gather(
            *(
                self._run_sub_dag(map_node.sub_dag, v, f"{map_node.label}[{k}]", dag_nsref, run_id)
                for k, v in items
            )
        )

        if is_dict:
            return {items[i][0]: results[i] for i in range(len(items))}
        # Flatmap: if sub-DAG returns a list, flatten into the result
        flat: list[Any] = []
        for r in results:
            if isinstance(r, list):
                flat.extend(r)  # pyright: ignore[reportUnknownArgumentType]
            else:
                flat.append(r)
        return flat

    async def _execute_call_node(
        self,
        call_node: CallNode,
        run_id: str,
        dag_nsref: str,
        node_outputs: dict[str, Any],
    ) -> Any:
        """Execute a CallNode by running sub-DAG once with upstream output."""
        upstream = self._get_upstream_output(call_node, node_outputs)
        return await self._run_sub_dag(
            call_node.sub_dag, upstream, f"{call_node.label}/call", dag_nsref, run_id
        )

    async def _execute_switch_node(
        self,
        switch_node: SwitchNode,
        run_id: str,
        dag_nsref: str,
        node_outputs: dict[str, Any],
    ) -> Any:
        """Execute a SwitchNode by routing to the branch matching LabelSwitch."""
        upstream = self._get_upstream_output(switch_node, node_outputs)

        # Extract LabelSwitch and data from upstream
        switch_label, data = _extract_switch(switch_node.label, upstream)

        if switch_label not in switch_node.branches:
            raise ValueError(
                f"SwitchNode '{switch_node.label}': no branch for label '{switch_label}'. "
                f"Available: {list(switch_node.branches.keys())}"
            )

        branch_dag = switch_node.branches[switch_label]
        sub_sources = branch_dag.sources()
        sub_sinks = branch_dag.sinks()
        source_label = sub_sources[0].label
        sink_label = sub_sinks[0].label

        sub_runner = DagRunner(branch_dag, provenance=self.provenance)
        iter_nsref = f"{dag_nsref}/{switch_node.label}[{switch_label}]"
        sub_result = await sub_runner.run(
            source_inputs={source_label: _single_element_inputs(branch_dag, source_label, data)},
            dag_nsref=iter_nsref,
            parent_run_id=run_id,
        )
        if sub_result.status != "completed":
            raise RuntimeError(f"Switch branch [{switch_label}] failed: {sub_result.error}")
        return sub_result.outputs[sink_label]

    async def _execute_map_switch_node(
        self,
        node: MapSwitchNode,
        run_id: str,
        dag_nsref: str,
        node_outputs: dict[str, Any],
    ) -> Any:
        """Map over a collection, routing each element through a SwitchNode."""
        collection = self._get_upstream_output(node, node_outputs)

        if not isinstance(collection, list):
            raise TypeError(
                f"MapSwitchNode '{node.label}' received {type(collection).__name__}, expected list"
            )

        switch = node.switch_node

        async def run_one(idx: int, element: Any) -> Any:
            switch_label, data = _extract_switch(node.label, element)
            if switch_label not in switch.branches:
                raise ValueError(
                    f"MapSwitchNode '{node.label}': no branch for '{switch_label}'. "
                    f"Available: {list(switch.branches.keys())}"
                )
            branch_dag = switch.branches[switch_label]
            return await self._run_sub_dag(
                branch_dag, data, f"{node.label}[{idx}:{switch_label}]", dag_nsref, run_id
            )

        results = await asyncio.gather(*(run_one(i, elem) for i, elem in enumerate(collection)))  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]

        # Flatmap: flatten list results
        flat: list[Any] = []
        for r in results:
            if isinstance(r, list):
                flat.extend(r)  # pyright: ignore[reportUnknownArgumentType]
            else:
                flat.append(r)
        return flat

    async def _call_node(
        self,
        dag_node: DagNode,
        run_id: str,
        dag_nsref: str,
        kwargs: dict[str, Any],
    ) -> Any:
        """Call a node's function, injecting DagContext if expected."""
        # Runtime resolution: try parent namespace first for overrides (e.g., cache wrappers)
        resolved = dag_node.ns_node
        if self.dag.parent_namespace is not None:
            try:
                resolved = self.dag.resolve(dag_node.ns_node.nsref)
            except KeyError:
                pass
        fn = resolved._decorated or resolved.method.o  # pyright: ignore[reportPrivateUsage]

        is_inline = getattr(fn, "_lythonic_inline", False)

        token = set_node_run_context(run_id=run_id, node_label=dag_node.label)
        try:
            if resolved.expects_dag_context():
                ctx_type = resolved.dag_context_type() or DagContext
                ctx = ctx_type(
                    dag_nsref=dag_nsref,
                    node_label=dag_node.label,
                    run_id=run_id,
                )
                if asyncio.iscoroutinefunction(fn):
                    return await fn(ctx, **kwargs)
                if is_inline:
                    return fn(ctx, **kwargs)
                loop = asyncio.get_running_loop()
                # Copy context so ContextVar values (incl. NodeRunContext) propagate to thread.
                copied = contextvars.copy_context()
                return await loop.run_in_executor(
                    None, copied.run, functools.partial(fn, ctx, **kwargs)
                )

            if asyncio.iscoroutinefunction(fn):
                return await fn(**kwargs)
            if is_inline:
                return fn(**kwargs)
            loop = asyncio.get_running_loop()
            # Copy context so ContextVar values (incl. NodeRunContext) propagate to thread.
            copied = contextvars.copy_context()
            return await loop.run_in_executor(None, copied.run, functools.partial(fn, **kwargs))
        finally:
            reset_node_run_context(token)

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
                    new_run_id, label, json.dumps(output, default=_json_default)
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

        composite_edges = [
            e
            for e in upstream_edges
            if isinstance(self.dag.nodes[e.upstream], CompositeDagNode)
            and e.upstream in node_outputs
        ]
        if composite_edges and args:
            for edge, arg in zip(composite_edges, args, strict=False):
                kwargs[arg.name] = node_outputs[edge.upstream]
            return kwargs

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
