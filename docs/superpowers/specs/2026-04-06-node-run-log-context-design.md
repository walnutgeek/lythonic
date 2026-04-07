# Node Run Log Context: ContextVar + logging.Filter

## Problem

DAG nodes run concurrently across async tasks, executor threads, and inline on
the event loop. Log entries from node code (and third-party libraries called by
nodes) have no way to identify which run and node produced them. Passing
`run_id`/`node_label` explicitly to every logging call is impractical,
especially for code we don't control.

## Design

Stdlib-only solution using `contextvars.ContextVar` and `logging.Filter`.

### Data model and ContextVar

New module `src/lythonic/compose/log_context.py`:

```python
@dataclass(frozen=True)
class NodeRunContext:
    run_id: str
    node_label: str
```

A `ContextVar[NodeRunContext | None]` with default `None`. A context manager
`node_run_context(run_id, node_label)` for external use, and a public
`get_node_run_context()` reader.

### logging.Filter

`NodeRunLogFilter` reads the `ContextVar` and injects `run_id` and
`node_label` into every `LogRecord`:

```python
class NodeRunLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        ctx = _current_node_run.get()
        if ctx is not None:
            record.run_id = ctx.run_id
            record.node_label = ctx.node_label
        else:
            record.run_id = ""
            record.node_label = ""
        return True
```

Always returns `True` — enrichment only, no suppression. Empty strings when
no context is active so formatters don't break.

A convenience `install_node_run_log_filter(handler=None)` installs the filter
on a given handler or the root logger.

### DagRunner integration

`_call_node` sets the `ContextVar` before invocation via the raw token API
(avoids `contextlib` overhead in a hot path), resets in `finally`:

```python
token = _current_node_run.set(NodeRunContext(run_id=run_id, node_label=dag_node.label))
try:
    # ... existing async/inline/executor dispatch ...
finally:
    _current_node_run.reset(token)
```

This works across all three execution paths:
- **async**: `ContextVar` is native to asyncio task context
- **executor**: `contextvars.copy_context().run()` wraps the call to propagate
  `ContextVar` values to the worker thread (`run_in_executor` does not do
  this automatically)
- **inline**: same thread, same context

### Exports

`NodeRunContext`, `NodeRunLogFilter`, `node_run_context`,
`get_node_run_context`, and `install_node_run_log_filter` from
`lythonic.compose.log_context`. No re-export from `__init__` — users import
the module directly.

## Files Changed

| File | Change |
|------|--------|
| `src/lythonic/compose/log_context.py` | New module: `NodeRunContext`, `ContextVar`, `NodeRunLogFilter`, context manager, convenience installer |
| `src/lythonic/compose/dag_runner.py` | `_call_node` sets/resets `ContextVar` around node invocation |
| `tests/test_dag_runner.py` | Tests for context propagation across async, executor, and inline paths |

## Testing

Inline tests in `log_context.py` for the data model and filter. Integration
tests in `tests/test_dag_runner.py`:

1. Sync node in executor — log call captures `run_id` and `node_label`
2. Async node — same verification
3. Inline node — same verification
4. Outside node context — fields are empty strings
