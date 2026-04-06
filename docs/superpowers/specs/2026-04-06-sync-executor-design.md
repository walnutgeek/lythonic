# Sync DAG Nodes: Executor-Based Execution

## Problem

`DagRunner._call_node()` calls sync functions directly on the event loop thread.
This blocks the loop during execution, defeating concurrency in `MapNode`
(which uses `asyncio.gather()`) and preventing other async work from progressing.

## Design

### Default behavior: sync nodes run in a thread executor

All sync DAG node callables are dispatched via `loop.run_in_executor(None, ...)`,
using asyncio's default `ThreadPoolExecutor`. This ensures they never block the
event loop.

`functools.partial` wraps the call since `run_in_executor` requires a zero-arg
callable.

### Opt-out: the `@inline` decorator

For lightweight pure-computation functions where executor overhead is undesirable,
the `@inline` decorator marks a function to run directly on the event loop:

```python
from lythonic.compose import inline

@inline
def add(a: int, b: int) -> int:
    return a + b
```

The decorator sets `fn._lythonic_inline = True`. `_call_node` checks this attribute
before deciding whether to use the executor. The attribute name is namespaced to
avoid collisions.

### Async nodes are unaffected

`asyncio.iscoroutinefunction()` is checked first, as today. Only sync functions
go through the executor path.

## Changes

### `lythonic/compose/namespace.py`

Add the `inline` decorator:

```python
def inline(fn: Callable[..., Any]) -> Callable[..., Any]:
    """
    Mark a sync DAG node to run on the event loop instead of in a thread
    executor. Use for lightweight pure-computation functions that won't
    block the loop.
    """
    fn._lythonic_inline = True
    return fn
```

### `lythonic/compose/dag_runner.py`

Modify `_call_node` to wrap sync calls in `run_in_executor` unless `_lythonic_inline`
is set:

```python
async def _call_node(self, dag_node, run_id, dag_nsref, kwargs):
    fn = dag_node.ns_node._decorated or dag_node.ns_node.method.o

    is_inline = getattr(fn, "_lythonic_inline", False)

    if dag_node.ns_node.expects_dag_context():
        ctx_type = dag_node.ns_node.dag_context_type() or DagContext
        ctx = ctx_type(dag_nsref=dag_nsref, node_label=dag_node.label, run_id=run_id)
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

### `lythonic/compose/__init__.py`

Export `inline` alongside existing symbols.

## Testing

Tests in `tests/test_dag_runner.py`:

1. **Sync node runs in a separate thread.** A sync node captures
   `threading.current_thread()` and returns it. Assert the thread is not the
   main event loop thread.

2. **`@inline` sync node runs on the event loop thread.** Same pattern with
   `@inline` applied. Assert the thread is the event loop thread.

3. **Async nodes are unaffected.** An async node runs on the event loop thread
   (regression guard).
