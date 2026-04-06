from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

_F = TypeVar("_F", bound=Callable[..., Any])


def inline(fn: _F) -> _F:
    """
    Mark a sync DAG node to run on the event loop instead of in a thread
    executor. Use for lightweight pure-computation functions that won't
    block the loop.
    """
    fn._lythonic_inline = True  # pyright: ignore[reportFunctionMemberAccess]
    return fn
