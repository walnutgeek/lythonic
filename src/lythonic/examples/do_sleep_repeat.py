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

"""

import asyncio
import logging
import threading
import time

from pydantic import BaseModel

from lythonic.compose._inline import inline
from lythonic.compose.log_context import get_node_run_context
from lythonic.compose.namespace import Dag, LabelSwitch, dag_factory

_log = logging.getLogger(__name__)


def info() -> str:
    run, label = ("", "")
    ctx = get_node_run_context()
    if ctx is not None:
        run, label = ctx.run_id, ctx.node_label
    return f"thread={threading.current_thread().name} {run=} {label=}"


async def task1():
    _log.info("Starting task1")
    await asyncio.sleep(1.0)
    _log.debug(f"After sleep task1 {info()}")
    await asyncio.sleep(1.0)
    _log.warning(f"Done task1 {info()}")


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


async def main():
    print(await map_switch_flat_map()())


if __name__ == "__main__":
    asyncio.run(main())
