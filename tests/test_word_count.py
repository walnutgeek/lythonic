import lythonic.examples.word_count as wc
from lythonic.compose.dag_runner import DagRunner


async def test_run():
    dag = wc.main_dag()
    drr = await DagRunner(dag).run()
    results = drr.outputs["reduce"]
    assert "conn" in results and "python" in results and "author" in results


async def test_callable():
    dag = wc.main_dag()
    drr = await dag()
    results = drr.outputs["reduce"]
    assert "conn" in results and "python" in results and "author" in results
