"""


Run it with lyth.yaml:
---
namespace:
  - nsref: "examples:task1"
    gref: "lythonic.examples.do_sleep_repeat:task1"
    triggers:
      - name: "task1_repeat"
        type: "poll"
        schedule: "*/19 * * * * *"
  - gref: "lythonic.examples.do_sleep_repeat:always_failing_task"
    triggers:
      - "*/37 * * * * *"
  - type: cache
    nsref: "examples:get_timestamp"
    gref: "lythonic.examples.do_sleep_repeat:get_timestamp"
    min_ttl: 0.0005 # less then 1 min
    max_ttl: 0.001 # less then 2 min
  - gref: "lythonic.examples.do_sleep_repeat:play_with_ctx"
    triggers:
      - "*/13 * * * * *"
  - nsref: "examples:dag1"
    gref: "lythonic.examples.do_sleep_repeat:dag1__"
    triggers:
      - name: dag1_repeat
        type: "poll"
        schedule: "*/17 * * * * *"
  - nsref: "examples:map_switch_flat_map"
    gref: "lythonic.examples.do_sleep_repeat:map_switch_flat_map__"
    triggers:
      - name: map_switch_flat_map
        type: "poll"
        schedule: "*/23 * * * * *"
  - type: fragment
    gref: "lythonic.examples.do_sleep_repeat:SleepFragment"
    nsref: "frag:"
    init:
      delay: 0.3
    configs:
      frag_dag:
        triggers:
          - name: frag_dag_repeat
            type: "poll"
            schedule: "*/29 * * * * *"
  - type: fragment
    gref: "lythonic.examples.transforms"
    nsref: "transforms:"
    configs:
      double_dag:
        triggers:
          - name: double_dag_repeat
            type: "poll"
            schedule: "*/31 * * * * *"
---

Quick runthru:
```
rm data/*{db,log}; uv run lyth --verbose start
sqlite3 data/dags.db 'select * from dag_runs'
sqlite3 data/dags.db "select * from node_executions WHERE status = 'failed'"
sqlite3 data/cache.db 'select * from examples__get_timestamp'
grep ValueError data/*log
```
"""

import asyncio
import logging
import threading
import time

from pydantic import BaseModel

from lythonic.compose.log_context import get_node_run_context
from lythonic.compose.namespace import (
    Dag,
    DagContext,
    LabelSwitch,
    NamespaceFragment,
    dag_factory,
    inline,
    nsnode,
    require_cache,
)

_log = logging.getLogger(__name__)


def info() -> str:
    run, label = ("", "")
    ctx = get_node_run_context()
    if ctx is not None:
        run, label = ctx.run_id, ctx.node_label
    return f"thread={threading.current_thread().name} {run=} {label=}"


async def always_failing_task():
    raise ValueError("Failing")


async def task1():
    _log.info("Starting task1")
    await asyncio.sleep(1.0)
    _log.debug(f"After sleep task1 {info()}")
    await asyncio.sleep(1.0)
    _log.warning(f"Done task1 {info()}")


@require_cache
def get_timestamp() -> float:
    """Returns current time; cached so repeated calls within TTL return the same value."""
    ts = time.time()
    _log.info(f"get_timestamp called (fresh): {ts}")
    return ts


async def play_with_ctx(ctx: DagContext) -> DagContext:
    ts = ctx.ns_call("examples:get_timestamp")
    _log.warning(f"Done play_with_ctx {info()} cached_ts={ts} {ctx}")
    return ctx


@dag_factory
def dag1():
    with Dag() as dag:
        dag.node(task1)
    return dag


class S12(BaseModel):
    s1: str
    s2: str


@inline
def split(ss: S12) -> list[str]:
    _log.info(f"split {info()}")
    return f"{ss.s1}/{ss.s2}".split("/")


def join(ss: S12) -> str:
    _log.info(f"join {info()}")
    time.sleep(1.0)
    return f"{ss.s1} {ss.s2}"


@dag_factory
def join_dag():
    with Dag() as dag:
        dag.node(join)
    return dag


@dag_factory
def split_dag():
    with Dag() as dag:
        dag.node(split)
    return dag


def input() -> list[tuple[LabelSwitch, S12]]:
    return [
        ("split_dag", S12(s1="a/b", s2="d/e")),
        ("split", S12(s1="w/x", s2="y/z")),
        ("join_dag", S12(s1="A", s2="B C")),
        ("join", S12(s1="X Y", s2="Z")),
    ]


@dag_factory
def map_switch_flat_map():
    with Dag() as dag:
        dag.node(input) >> dag.map(  # pyright: ignore[reportUnusedExpression]
            dag.switch([split_dag, split, join_dag, join], label="router"), label="map"
        )
    return dag


class SleepFragment(NamespaceFragment):
    delay: float

    def __init__(self, delay: float = 0.5):
        self.delay = delay

    @nsnode(tags=["fragment"])
    async def quick_task(self) -> str:
        _log.info(f"quick_task delay={self.delay}")
        await asyncio.sleep(self.delay)
        return f"done in {self.delay}s"

    @dag_factory
    def frag_dag(self):
        with Dag() as dag:
            dag.node(self.quick_task)
        return dag


async def main():
    print(await map_switch_flat_map()())


if __name__ == "__main__":
    asyncio.run(main())
