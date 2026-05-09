"""Module-as-fragment example: standalone decorated functions."""

from __future__ import annotations

import logging
import random

from lythonic.compose.namespace import Dag, dag_factory, nsnode

_log = logging.getLogger(__name__)


@nsnode(tags=["transform"])
def double(value: int) -> int:
    _log.info(f"double({value})")
    return value * 2


@nsnode(tags=["transform"])
def negate(value: int) -> int:
    _log.info(f"negate({value})")
    return -value


def random_value() -> int:
    return random.randint(1, 100)


@dag_factory
def double_dag():
    with Dag() as dag:
        dag.node(random_value) >> dag.node(double)  # pyright: ignore[reportUnusedExpression]
    return dag
