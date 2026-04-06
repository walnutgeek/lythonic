# Sync Executor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run sync DAG nodes in a thread executor by default so they never block the event loop, with `@inline` opt-out for lightweight functions.

**Architecture:** Add an `inline` decorator that stamps `_lythonic_inline = True` on a callable. Modify `DagRunner._call_node()` to dispatch sync functions via `loop.run_in_executor(None, functools.partial(fn, ...))` unless the function has the inline attribute. Async nodes are unaffected.

**Tech Stack:** Python asyncio, `functools.partial`, `loop.run_in_executor`

**Spec:** `docs/superpowers/specs/2026-04-06-sync-executor-design.md`

---

### Task 1: Add the `inline` decorator

**Files:**
- Modify: `src/lythonic/compose/namespace.py` (add function after line 74, before `DagContext` class)
- Modify: `src/lythonic/compose/__init__.py` (add re-export)

- [ ] **Step 1: Write the failing test**

Create `tests/test_inline_decorator.py`:

```python
from __future__ import annotations

from lythonic.compose.namespace import inline


def test_inline_sets_attribute():
    @inline
    def add(a: int, b: int) -> int:
        return a + b

    assert getattr(add, "_lythonic_inline", False) is True


def test_inline_preserves_callable():
    @inline
    def add(a: int, b: int) -> int:
        return a + b

    assert add(2, 3) == 5


def test_unmarked_function_has_no_attribute():
    def add(a: int, b: int) -> int:
        return a + b

    assert getattr(add, "_lythonic_inline", False) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_inline_decorator.py -v`
Expected: FAIL with `ImportError: cannot import name 'inline'`

- [ ] **Step 3: Implement the `inline` decorator**

In `src/lythonic/compose/namespace.py`, add after the existing imports (before the `DagContext` class at line 76):

```python
def inline(fn: Callable[..., Any]) -> Callable[..., Any]:
    """
    Mark a sync DAG node to run on the event loop instead of in a thread
    executor. Use for lightweight pure-computation functions that won't
    block the loop.
    """
    fn._lythonic_inline = True  # pyright: ignore[reportFunctionMemberAccess]
    return fn
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_inline_decorator.py -v`
Expected: all 3 tests PASS

- [ ] **Step 5: Export `inline` from `lythonic.compose`**

In `src/lythonic/compose/__init__.py`, add to the imports at the top:

```python
from lythonic.compose.namespace import inline as inline
```

(The `as inline` makes the re-export explicit for type checkers.)

- [ ] **Step 6: Run lint**

Run: `make lint`
Expected: zero warnings/errors

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/compose/namespace.py src/lythonic/compose/__init__.py tests/test_inline_decorator.py
git commit -m "feat: add @inline decorator for sync DAG node opt-out"
```

---

### Task 2: Modify `_call_node` to use executor for sync functions

**Files:**
- Modify: `src/lythonic/compose/dag_runner.py:1-264` (add `functools` import, rewrite `_call_node`)

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_dag_runner.py`. These tests need module-level helper functions (required by `GlobalRef`/`Method` which inspect module-level callables).

Add these helper functions near the other helpers at the top of the file (after the existing `_async_join` function around line 168):

```python
import threading


def _capture_thread(value: int) -> str:
    """Sync node that captures which thread it runs on."""
    return f"{threading.current_thread().name}:{value * 2}"


@inline
def _capture_thread_inline(value: int) -> str:
    """Inline sync node that captures which thread it runs on."""
    return f"{threading.current_thread().name}:{value * 2}"


async def _capture_thread_async(value: int) -> str:
    """Async node that captures which thread it runs on."""
    return f"{threading.current_thread().name}:{value * 2}"
```

Also add the import at the top of the file:

```python
from lythonic.compose.namespace import inline
```

Then add the test functions:

```python
async def test_sync_node_runs_in_executor_thread():
    """Sync DAG node should run in a separate thread, not the event loop thread."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._capture_thread, nsref="t:capture")

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
    # Must NOT be the main thread (MainThread) — should be a ThreadPoolExecutor thread
    assert thread_name != threading.current_thread().name


async def test_inline_sync_node_runs_on_event_loop_thread():
    """@inline sync DAG node should run on the event loop thread."""
    from lythonic.compose.dag_runner import DagRunner
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._capture_thread_inline, nsref="t:capture_inline")

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
    ns.register(this_module._capture_thread_async, nsref="t:capture_async")

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dag_runner.py::test_sync_node_runs_in_executor_thread tests/test_dag_runner.py::test_inline_sync_node_runs_on_event_loop_thread tests/test_dag_runner.py::test_async_node_runs_on_event_loop_thread -v`
Expected: `test_sync_node_runs_in_executor_thread` FAILS (sync node currently runs on the main thread, so `thread_name == threading.current_thread().name` is true, but the test asserts `!=`). The other two should PASS (inline and async both run on the event loop thread, which is current behavior).

- [ ] **Step 3: Modify `_call_node` to use executor**

In `src/lythonic/compose/dag_runner.py`:

Add `import functools` to the imports at the top (after `import asyncio`).

Replace the `_call_node` method (lines 241-264) with:

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
```

- [ ] **Step 4: Run the three new tests to verify they pass**

Run: `uv run pytest tests/test_dag_runner.py::test_sync_node_runs_in_executor_thread tests/test_dag_runner.py::test_inline_sync_node_runs_on_event_loop_thread tests/test_dag_runner.py::test_async_node_runs_on_event_loop_thread -v`
Expected: all 3 PASS

- [ ] **Step 5: Run full test suite**

Run: `make test`
Expected: all tests PASS (existing tests use async helpers, so unaffected; sync helpers now run in executor but behavior is identical)

- [ ] **Step 6: Run lint**

Run: `make lint`
Expected: zero warnings/errors

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/compose/dag_runner.py tests/test_dag_runner.py
git commit -m "feat: run sync DAG nodes in thread executor by default"
```
