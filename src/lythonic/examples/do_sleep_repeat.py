import asyncio
import logging
import threading

from lythonic.compose.namespace import Dag, dag_factory

_log = logging.getLogger(__name__)


async def task1():
    print(">", __name__)
    _log.info("Starting task1")
    await asyncio.sleep(1.0)
    _log.debug(f"After sleep task1 {threading.current_thread().name}")
    await asyncio.sleep(1.0)
    _log.warning(f"Done task1 {threading.current_thread().name}")
    print("<", __name__)


@dag_factory
def dag1():
    with Dag() as dag:
        dag.node(task1)
    return dag
