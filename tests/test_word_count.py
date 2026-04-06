import lythonic.examples.word_count as wc
from lythonic.compose.dag_runner import DagRunner


async def test_run():
    drr = await DagRunner(wc.main_dag).run()
    results = drr.outputs["reduce"]
    assert "conn" in results and "python" in results and "author" in results


async def test_callable():
    import lythonic.examples.word_count as wc

    drr = await wc.main_dag()
    results = drr.outputs["reduce"]
    assert "conn" in results and "python" in results and "author" in results
