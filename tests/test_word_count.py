from lythonic.compose.dag_runner import DagRunner
import lythonic.examples.word_count as wc

async def test_run():
    drr = await DagRunner(wc.main_dag, None).run()
    results = drr.outputs['reduce']
    assert 'conn' in results and 'python' in results and 'author' in results


    