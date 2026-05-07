"""Sample module used as a namespace fragment in tests."""

from __future__ import annotations

from typing import Any, cast

from lythonic.compose.namespace import nsnode, require_cache


@require_cache
@nsnode(tags=["api"])
def normalize(data: dict[str, Any]) -> dict[str, Any]:
    return {"normalized": True, **data}


@nsnode()
def flatten(data: list[Any]) -> list[Any]:
    result: list[Any] = []
    for sublist in data:
        if isinstance(sublist, list):
            result.extend(cast(list[Any], sublist))
        else:
            result.append(sublist)
    return result
