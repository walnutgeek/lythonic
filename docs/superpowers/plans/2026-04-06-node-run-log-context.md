# Node Run Log Context Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automatically attach `run_id` and `node_label` to all log entries produced during DAG node execution, across async, executor, and inline paths.

**Architecture:** A `ContextVar` holds `NodeRunContext(run_id, node_label)`. `DagRunner._call_node` sets it before each node invocation. A `logging.Filter` reads it and injects the fields into every `LogRecord`. Stdlib only, no dependencies.

**Tech Stack:** Python `contextvars`, `logging`, `dataclasses`

**Spec:** `docs/superpowers/specs/2026-04-06-node-run-log-context-design.md`

---

### Task 1: Create `log_context.py` with data model, ContextVar, filter, and inline tests

**Files:**
- Create: `src/lythonic/compose/log_context.py`

- [ ] **Step 1: Write the failing inline tests**

Create `src/lythonic/compose/log_context.py` with only the test functions (implementation comes next). The tests import from the same module, so they will fail until the implementation exists:

```python
"""
Node run log context: ContextVar-based logging context for DAG node execution.

Provides `NodeRunContext` to identify which run and node produced a log entry,
a `logging.Filter` to inject this into log records, and a context manager for
setup/teardown. Works across async tasks, executor threads, and inline calls.
"""

from __future__ import annotations


## Tests


def test_node_run_context_fields():
    ctx = NodeRunContext(run_id="r1", node_label="fetch")
    assert ctx.run_id == "r1"
    assert ctx.node_label == "fetch"


def test_context_manager_sets_and_resets():
    assert get_node_run_context() is None
    with node_run_context("r1", "fetch"):
        ctx = get_node_run_context()
        assert ctx is not None
        assert ctx.run_id == "r1"
        assert ctx.node_label == "fetch"
    assert get_node_run_context() is None


def test_filter_enriches_record_with_context():
    import logging

    f = NodeRunLogFilter()
    record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)

    with node_run_context("r1", "fetch"):
        f.filter(record)
    assert record.run_id == "r1"  # pyright: ignore[reportAttributeAccessIssue]
    assert record.node_label == "fetch"  # pyright: ignore[reportAttributeAccessIssue]


def test_filter_sets_empty_strings_without_context():
    import logging

    f = NodeRunLogFilter()
    record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)
    f.filter(record)
    assert record.run_id == ""  # pyright: ignore[reportAttributeAccessIssue]
    assert record.node_label == ""  # pyright: ignore[reportAttributeAccessIssue]


def test_filter_always_returns_true():
    import logging

    f = NodeRunLogFilter()
    record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)
    assert f.filter(record) is True
    with node_run_context("r1", "fetch"):
        assert f.filter(record) is True


def test_install_on_root_logger():
    import logging

    root = logging.getLogger()
    original_filters = list(root.filters)
    try:
        f = install_node_run_log_filter()
        assert f in root.filters
    finally:
        root.filters = original_filters  # pyright: ignore[reportAttributeAccessIssue]


def test_install_on_specific_handler():
    import logging

    handler = logging.StreamHandler()
    f = install_node_run_log_filter(handler)
    assert f in handler.filters
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/lythonic/compose/log_context.py -v`
Expected: FAIL with `NameError: name 'NodeRunContext' is not defined`

- [ ] **Step 3: Implement the module**

Add the implementation above the `## Tests` section in `src/lythonic/compose/log_context.py`:

```python
import logging
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from collections.abc import Iterator


@dataclass(frozen=True)
class NodeRunContext:
    """Identifies the DAG run and node that produced a log entry."""

    run_id: str
    node_label: str


_current_node_run: ContextVar[NodeRunContext | None] = ContextVar(
    "_current_node_run", default=None
)


def get_node_run_context() -> NodeRunContext | None:
    """Return the current node run context, or `None` if not inside a node."""
    return _current_node_run.get()


@contextmanager
def node_run_context(run_id: str, node_label: str) -> Iterator[NodeRunContext]:
    """Set node run context for the duration of a block."""
    ctx = NodeRunContext(run_id=run_id, node_label=node_label)
    token = _current_node_run.set(ctx)
    try:
        yield ctx
    finally:
        _current_node_run.reset(token)


class NodeRunLogFilter(logging.Filter):
    """
    Logging filter that injects `run_id` and `node_label` from the current
    `NodeRunContext` into every log record. Always returns `True` — enrichment
    only, no suppression. Sets empty strings when no context is active.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # pyright: ignore[reportImplicitOverride]
        ctx = _current_node_run.get()
        if ctx is not None:
            record.run_id = ctx.run_id  # pyright: ignore[reportAttributeAccessIssue]
            record.node_label = ctx.node_label  # pyright: ignore[reportAttributeAccessIssue]
        else:
            record.run_id = ""  # pyright: ignore[reportAttributeAccessIssue]
            record.node_label = ""  # pyright: ignore[reportAttributeAccessIssue]
        return True


def install_node_run_log_filter(
    handler: logging.Handler | None = None,
) -> NodeRunLogFilter:
    """
    Install `NodeRunLogFilter` on a handler. If `handler` is `None`,
    installs on the root logger directly.
    """
    f = NodeRunLogFilter()
    if handler is not None:
        handler.addFilter(f)
    else:
        logging.getLogger().addFilter(f)
    return f
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/lythonic/compose/log_context.py -v`
Expected: all 7 tests PASS

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: zero warnings/errors

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/log_context.py
git commit -m "feat: add node run log context module with ContextVar + logging.Filter"
```

---

### Task 2: Integrate into `DagRunner._call_node` and add integration tests

**Files:**
- Modify: `src/lythonic/compose/dag_runner.py:275-306`
- Modify: `tests/test_dag_runner.py`

- [ ] **Step 1: Write the failing integration tests**

Add module-level helper functions to `tests/test_dag_runner.py` after the existing `_capture_thread_async` helper (around line 193). These helpers log a message and return it, so the test can verify the log record was enriched:

```python
import logging as _logging


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
```

Then add the test functions at the end of the file:

```python
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
    handler.emit = lambda record: records.append(record)  # pyright: ignore[reportAttributeAccessIssue]
    logger.addHandler(handler)
    logger.setLevel(_logging.DEBUG)

    try:
        ns = Namespace()
        ns.register(this_module._log_and_return, nsref="t:log_sync")

        with Dag() as dag:
            dag.node(ns.get("t:log_sync"))

        runner = DagRunner(dag)
        result = await runner.run(
            source_inputs={"log_sync": {"value": 42}},
            dag_nsref="test:log_ctx",
        )

        assert result.status == "completed"
        assert len(records) == 1
        assert records[0].run_id != ""  # pyright: ignore[reportAttributeAccessIssue]
        assert records[0].node_label == "log_sync"  # pyright: ignore[reportAttributeAccessIssue]
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
    handler.emit = lambda record: records.append(record)  # pyright: ignore[reportAttributeAccessIssue]
    logger.addHandler(handler)
    logger.setLevel(_logging.DEBUG)

    try:
        ns = Namespace()
        ns.register(this_module._log_and_return_async, nsref="t:log_async")

        with Dag() as dag:
            dag.node(ns.get("t:log_async"))

        runner = DagRunner(dag)
        result = await runner.run(
            source_inputs={"log_async": {"value": 42}},
            dag_nsref="test:log_ctx",
        )

        assert result.status == "completed"
        assert len(records) == 1
        assert records[0].run_id != ""  # pyright: ignore[reportAttributeAccessIssue]
        assert records[0].node_label == "log_async"  # pyright: ignore[reportAttributeAccessIssue]
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
    handler.emit = lambda record: records.append(record)  # pyright: ignore[reportAttributeAccessIssue]
    logger.addHandler(handler)
    logger.setLevel(_logging.DEBUG)

    try:
        ns = Namespace()
        ns.register(this_module._log_and_return_inline, nsref="t:log_inline")

        with Dag() as dag:
            dag.node(ns.get("t:log_inline"))

        runner = DagRunner(dag)
        result = await runner.run(
            source_inputs={"log_inline": {"value": 42}},
            dag_nsref="test:log_ctx",
        )

        assert result.status == "completed"
        assert len(records) == 1
        assert records[0].run_id != ""  # pyright: ignore[reportAttributeAccessIssue]
        assert records[0].node_label == "log_inline"  # pyright: ignore[reportAttributeAccessIssue]
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
    handler.emit = lambda record: records.append(record)  # pyright: ignore[reportAttributeAccessIssue]
    logger.addHandler(handler)
    logger.setLevel(_logging.DEBUG)

    try:
        logger.info("no context")
        assert len(records) == 1
        assert records[0].run_id == ""  # pyright: ignore[reportAttributeAccessIssue]
        assert records[0].node_label == ""  # pyright: ignore[reportAttributeAccessIssue]
    finally:
        logger.removeHandler(handler)
        logger.removeFilter(f)
```

- [ ] **Step 2: Run integration tests to verify they fail**

Run: `uv run pytest tests/test_dag_runner.py::test_log_context_sync_node_in_executor tests/test_dag_runner.py::test_log_context_async_node tests/test_dag_runner.py::test_log_context_inline_node tests/test_dag_runner.py::test_log_context_outside_node -v`
Expected: the first three FAIL (log records won't have `run_id`/`node_label` attributes since `_call_node` doesn't set the context yet). The fourth should PASS (filter sets empty strings without context).

- [ ] **Step 3: Modify `_call_node` to set the ContextVar**

In `src/lythonic/compose/dag_runner.py`, add the import at the top (after the existing imports):

```python
from lythonic.compose.log_context import NodeRunContext, _current_node_run
```

Replace the `_call_node` method (lines 275-306) with:

```python
    async def _call_node(
        self,
        dag_node: DagNode,
        run_id: str,
        dag_nsref: str,
        kwargs: dict[str, Any],
    ) -> Any:
        """Call a node's function, injecting DagContext if expected."""
        fn = dag_node.ns_node._decorated or dag_node.ns_node.method.o  # pyright: ignore[reportPrivateUsage]

        is_inline = getattr(fn, "_lythonic_inline", False)

        token = _current_node_run.set(NodeRunContext(run_id=run_id, node_label=dag_node.label))
        try:
            if dag_node.ns_node.expects_dag_context():
                ctx_type = dag_node.ns_node.dag_context_type() or DagContext
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
                return await loop.run_in_executor(None, functools.partial(fn, ctx, **kwargs))

            if asyncio.iscoroutinefunction(fn):
                return await fn(**kwargs)
            if is_inline:
                return fn(**kwargs)
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, functools.partial(fn, **kwargs))
        finally:
            _current_node_run.reset(token)
```

- [ ] **Step 4: Run integration tests to verify they pass**

Run: `uv run pytest tests/test_dag_runner.py::test_log_context_sync_node_in_executor tests/test_dag_runner.py::test_log_context_async_node tests/test_dag_runner.py::test_log_context_inline_node tests/test_dag_runner.py::test_log_context_outside_node -v`
Expected: all 4 PASS

- [ ] **Step 5: Run full test suite**

Run: `make test`
Expected: all tests PASS

- [ ] **Step 6: Run lint**

Run: `make lint`
Expected: zero warnings/errors

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/compose/dag_runner.py tests/test_dag_runner.py
git commit -m "feat: integrate node run log context into DagRunner._call_node"
```
