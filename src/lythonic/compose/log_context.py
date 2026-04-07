"""
Node run log context: ContextVar-based logging context for DAG node execution.

Provides `NodeRunContext` to identify which run and node produced a log entry,
a `logging.Filter` to inject this into log records, and a context manager for
setup/teardown. Works across async tasks, executor threads, and inline calls.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass(frozen=True)
class NodeRunContext:
    """Identifies the DAG run and node that produced a log entry."""

    run_id: str
    node_label: str


_current_node_run: ContextVar[NodeRunContext | None] = ContextVar("_current_node_run", default=None)


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
    f = NodeRunLogFilter()
    record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)

    with node_run_context("r1", "fetch"):
        f.filter(record)
    assert record.run_id == "r1"  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    assert record.node_label == "fetch"  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]


def test_filter_sets_empty_strings_without_context():
    f = NodeRunLogFilter()
    record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)
    f.filter(record)
    assert record.run_id == ""  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    assert record.node_label == ""  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]


def test_filter_always_returns_true():
    f = NodeRunLogFilter()
    record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)
    assert f.filter(record) is True
    with node_run_context("r1", "fetch"):
        assert f.filter(record) is True


def test_install_on_root_logger():
    root = logging.getLogger()
    original_filters = list(root.filters)
    try:
        f = install_node_run_log_filter()
        assert f in root.filters
    finally:
        root.filters = original_filters  # pyright: ignore[reportAttributeAccessIssue]


def test_install_on_specific_handler():
    handler = logging.StreamHandler()
    f = install_node_run_log_filter(handler)
    assert f in handler.filters
