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


async def test_register_dag_keeps_namespaces_separate():
    """Registering a composed DAG sets parent_namespace but does not copy callables."""
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(wc.main_dag)

    # The DAG entry is in the parent namespace
    dag_node = ns.get("lythonic.examples.word_count:main_dag__")
    assert dag_node is not None

    # Callables are NOT copied to the parent namespace
    try:
        ns.get("lythonic.examples.word_count:get_text")
        raise AssertionError("Should not be in parent namespace")
    except KeyError:
        pass

    # But the DAG still runs correctly via its own namespace
    drr = await dag_node()
    results = drr.outputs["reduce"]
    assert "conn" in results and "python" in results and "author" in results
