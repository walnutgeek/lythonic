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


def test_register_dag_flattens_sub_dag_callables():
    """Registering a composed DAG should flatten all sub-DAG callables into the namespace."""
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(wc.main_dag, nsref="pipelines:wc__")

    # Callables from the main DAG should be in the namespace
    assert ns.get("lythonic.examples.word_count:get_text") is not None
    assert ns.get("lythonic.examples.word_count:split_text") is not None
    assert ns.get("lythonic.examples.word_count:reduce") is not None

    # Callables from the sub-DAG (chunks) should also be flattened
    assert ns.get("lythonic.examples.word_count:tokenize") is not None
    assert ns.get("lythonic.examples.word_count:count") is not None
