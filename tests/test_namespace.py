from __future__ import annotations

from typing import Any

import tests.test_namespace as this_module
from lythonic.compose.namespace import DagContext


def test_parse_nsref_with_branch_and_leaf():
    from lythonic.compose.namespace import _parse_nsref  # pyright: ignore[reportPrivateUsage]

    assert _parse_nsref("market:fetch_prices") == (["market"], "fetch_prices")
    assert _parse_nsref("market.data:fetch_prices") == (["market", "data"], "fetch_prices")
    assert _parse_nsref("fetch_prices") == ([], "fetch_prices")
    assert _parse_nsref(":fetch_prices") == ([], "fetch_prices")


def test_dag_context_fields():
    from lythonic.compose.namespace import DagContext

    ctx = DagContext(dag_nsref="pipelines:daily", node_label="fetch", run_id="abc123")
    assert ctx.dag_nsref == "pipelines:daily"
    assert ctx.node_label == "fetch"
    assert ctx.run_id == "abc123"


def _sample_fn(ticker: str, limit: int = 10) -> dict[str, Any]:
    """Fetch some data."""
    return {"ticker": ticker, "limit": limit}


def _another_fn(code: str) -> str:  # pyright: ignore[reportUnusedParameter]
    return code.upper()


def _ctx_fn(ctx: DagContext, value: float) -> float:  # pyright: ignore[reportUnusedParameter]
    """A DAG-aware function."""
    return value * 2


def _get_sample_fn():  # pyright: ignore[reportUnusedFunction]
    return this_module._sample_fn  # pyright: ignore[reportPrivateUsage]


def _get_ctx_fn():  # pyright: ignore[reportUnusedFunction]
    return this_module._ctx_fn  # pyright: ignore[reportPrivateUsage]


def test_namespace_node_callable():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(_get_sample_fn())
    ns = Namespace()
    node = NamespaceNode(method=method, nsref="test:sample_fn", namespace=ns)

    result = node(ticker="AAPL")
    assert result == {"ticker": "AAPL", "limit": 10}


def test_namespace_node_metadata():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(_get_sample_fn())
    ns = Namespace()
    node = NamespaceNode(method=method, nsref="test:sample_fn", namespace=ns)

    assert node.nsref == "test:sample_fn"
    assert len(node.method.args) == 2
    assert node.method.args[0].name == "ticker"
    assert node.method.doc == "Fetch some data."


def test_namespace_node_decorated():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(_get_sample_fn())
    ns = Namespace()
    node = NamespaceNode(
        method=method,
        nsref="test:sample_fn",
        namespace=ns,
        decorated=lambda **kw: {"decorated": True},  # pyright: ignore[reportUnknownLambdaType, reportUnknownArgumentType]
    )

    result = node(ticker="AAPL")
    assert result == {"decorated": True}
    # Metadata still reflects the original
    assert node.method.args[0].name == "ticker"


def test_namespace_node_expects_dag_context():
    from lythonic.compose import Method
    from lythonic.compose.namespace import DagContext, Namespace, NamespaceNode

    ns = Namespace()

    plain_node = NamespaceNode(method=Method(_get_sample_fn()), nsref="t:a", namespace=ns)
    assert not plain_node.expects_dag_context()
    assert plain_node.dag_context_type() is None

    ctx_node = NamespaceNode(method=Method(_get_ctx_fn()), nsref="t:b", namespace=ns)
    assert ctx_node.expects_dag_context()
    assert ctx_node.dag_context_type() is DagContext


# Task 2: register and get


def test_register_by_callable():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    node = ns.register(this_module._sample_fn, nsref="test:sample_fn")  # pyright: ignore[reportPrivateUsage]
    assert node.nsref == "test:sample_fn"
    assert node(ticker="X") == {"ticker": "X", "limit": 10}


def test_register_by_string_gref():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    node = ns.register("tests.test_namespace:_sample_fn", nsref="test:sample")
    assert node(ticker="Y") == {"ticker": "Y", "limit": 10}


def test_register_derives_nsref():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    node = ns.register(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]
    assert node.nsref == "tests.test_namespace:_sample_fn"


def test_get_retrieves_node():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market:fetch")  # pyright: ignore[reportPrivateUsage]
    node = ns.get("market:fetch")
    assert node.nsref == "market:fetch"
    assert node(ticker="Z") == {"ticker": "Z", "limit": 10}


def test_get_nested_branch():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market.data:fetch")  # pyright: ignore[reportPrivateUsage]
    node = ns.get("market.data:fetch")
    assert node.nsref == "market.data:fetch"


def test_get_root_level():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="fetch")  # pyright: ignore[reportPrivateUsage]
    node = ns.get("fetch")
    assert node.nsref == "fetch"


def test_get_missing_raises_key_error():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    try:
        ns.get("nonexistent:thing")
        raise AssertionError("Expected KeyError")
    except KeyError:
        pass


# Task 3: register_all, __getattr__, decorate, error cases


def test_register_all():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    nodes = ns.register_all(this_module._sample_fn, this_module._another_fn)  # pyright: ignore[reportPrivateUsage]
    assert len(nodes) == 2
    assert ns.get("tests.test_namespace:_sample_fn") is not None
    assert ns.get("tests.test_namespace:_another_fn") is not None


def test_register_with_decorate():
    from lythonic.compose.namespace import Namespace

    def uppercase_decorator(fn: Any) -> Any:  # pyright: ignore[reportUnusedParameter]
        def wrapper(**kwargs: Any) -> dict[str, Any]:  # pyright: ignore[reportUnusedParameter]
            return {"decorated": True}

        return wrapper

    ns = Namespace()
    node = ns.register(this_module._sample_fn, nsref="test:sample", decorate=uppercase_decorator)  # pyright: ignore[reportPrivateUsage]
    result = node(ticker="X")
    assert result == {"decorated": True}
    assert node.method.args[0].name == "ticker"


def test_getattr_leaf():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market:fetch")  # pyright: ignore[reportPrivateUsage]
    node = ns.market.fetch  # pyright: ignore
    assert node.nsref == "market:fetch"
    assert node(ticker="A") == {"ticker": "A", "limit": 10}


def test_getattr_nested():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market.data:fetch")  # pyright: ignore[reportPrivateUsage]
    node = ns.market.data.fetch  # pyright: ignore
    assert node.nsref == "market.data:fetch"


def test_getattr_root_level():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="_my_download")  # pyright: ignore[reportPrivateUsage]
    node = ns._my_download  # pyright: ignore
    assert node.nsref == "_my_download"


def test_getattr_missing_raises():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    try:
        _ = ns.nonexistent  # pyright: ignore
        raise AssertionError("Expected AttributeError")
    except AttributeError:
        pass


def test_register_duplicate_leaf_raises():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="test:fetch")  # pyright: ignore[reportPrivateUsage]
    try:
        ns.register(this_module._another_fn, nsref="test:fetch")  # pyright: ignore[reportPrivateUsage]
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "already exists" in str(e)


def test_register_leaf_on_branch_raises():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market.data:fetch")  # pyright: ignore[reportPrivateUsage]
    try:
        ns.register(this_module._another_fn, nsref="market")  # pyright: ignore[reportPrivateUsage]
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "branch" in str(e).lower() or "already exists" in str(e)


# Task 4: DagEdge, DagNode, and >> operator


def test_dag_edge_fields():
    from lythonic.compose.namespace import DagEdge

    edge = DagEdge(upstream="fetch", downstream="compute")
    assert edge.upstream == "fetch"
    assert edge.downstream == "compute"


def test_dag_node_rshift_creates_edge():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="test:fetch")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="test:compute")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    f = dag.node(ns.get("test:fetch"))
    c = dag.node(ns.get("test:compute"))

    result = f >> c
    assert result is c
    assert len(dag.edges) == 1
    assert dag.edges[0].upstream == "fetch"
    assert dag.edges[0].downstream == "compute"


def test_dag_node_rshift_chaining():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:step1")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:step2")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="a:step3")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    s1 = dag.node(ns.get("a:step1"))
    s2 = dag.node(ns.get("a:step2"))
    s3 = dag.node(ns.get("a:step3"))

    s1 >> s2 >> s3  # pyright: ignore[reportUnusedExpression]

    assert len(dag.edges) == 2
    assert dag.edges[0].upstream == "step1"
    assert dag.edges[0].downstream == "step2"
    assert dag.edges[1].upstream == "step2"
    assert dag.edges[1].downstream == "step3"


# Task 5: Dag fan-out/fan-in, duplicate labels, callable source


def test_dag_fan_out_fan_in():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:report")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="a:archive")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:summarize")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    f = dag.node(ns.get("a:fetch"))
    r = dag.node(ns.get("a:report"))
    a = dag.node(ns.get("a:archive"))
    s = dag.node(ns.get("a:summarize"))

    f >> r >> s  # pyright: ignore[reportUnusedExpression]
    f >> a >> s  # pyright: ignore[reportUnusedExpression]

    assert len(dag.edges) == 4
    upstream_of_s = [e.upstream for e in dag.edges if e.downstream == "summarize"]
    assert sorted(upstream_of_s) == ["archive", "report"]
    downstream_of_f = [e.downstream for e in dag.edges if e.upstream == "fetch"]
    assert sorted(downstream_of_f) == ["archive", "report"]


def test_dag_same_callable_different_labels():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market:fetch")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    f1 = dag.node(ns.get("market:fetch"), label="fetch_us")
    f2 = dag.node(ns.get("market:fetch"), label="fetch_eu")

    assert f1.label == "fetch_us"
    assert f2.label == "fetch_eu"
    assert len(dag.nodes) == 2
    # Both wrap the same NamespaceNode
    assert f1.ns_node is f2.ns_node


def test_dag_duplicate_auto_label_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="market:fetch")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("market:fetch"))
    try:
        dag.node(ns.get("market:fetch"))
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "already exists" in str(e)


def test_dag_node_from_callable():
    from lythonic.compose.namespace import Dag

    dag = Dag()
    n = dag.node(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]
    assert n.label == "_sample_fn"
    assert n.ns_node.method.doc == "Fetch some data."
