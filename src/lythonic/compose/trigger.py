"""
Trigger: Event-driven DAG execution.

Provides `TriggerDef` for declarative trigger definitions,
`TriggerStore` for activation state persistence, and
`TriggerManager` for runtime coordination.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, model_validator

from lythonic.periodic import Interval


class TriggerDef(BaseModel):
    """
    Declarative trigger definition. Registered in a `Namespace` via
    `register_trigger()`. Does not start anything -- purely metadata.
    """

    name: str
    dag_nsref: str
    trigger_type: str  # "poll" or "push"
    interval: Interval | None = None
    poll_fn: Callable[[], Any] | None = None

    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def _validate_trigger(self) -> TriggerDef:
        if self.trigger_type == "poll" and self.interval is None:
            raise ValueError(f"Trigger '{self.name}': poll triggers require an interval")
        return self
