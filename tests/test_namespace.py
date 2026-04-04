from __future__ import annotations

from typing import Any

import tests.test_namespace as this_module
from lythonic.compose.namespace import DagContext


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


# Task 6: Dag validation (cycle detection + type compatibility)


def _typed_source() -> float:
    return 1.0


def _typed_sink(value: float) -> str:  # pyright: ignore[reportUnusedParameter]
    return "ok"


def _incompatible_sink(data: list[int]) -> str:  # pyright: ignore[reportUnusedParameter, reportUnusedFunction]
    return "ok"


def test_dag_validate_passes_for_valid_dag():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:step1")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:step2")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    s1 = dag.node(ns.get("a:step1"))
    s2 = dag.node(ns.get("a:step2"))
    s1 >> s2  # pyright: ignore[reportUnusedExpression]
    dag.validate()


def test_dag_validate_detects_cycle():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:n1")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:n2")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="a:n3")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    n1 = dag.node(ns.get("a:n1"))
    n2 = dag.node(ns.get("a:n2"))
    n3 = dag.node(ns.get("a:n3"))

    n1 >> n2 >> n3  # pyright: ignore[reportUnusedExpression]
    # Create a cycle
    dag.add_edge(n3, n1)

    try:
        dag.validate()
        raise AssertionError("Expected ValueError for cycle")
    except ValueError as e:
        assert "cycle" in str(e).lower()


def test_dag_validate_type_compatible():
    """Type-compatible edges should pass validation."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._typed_source, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._typed_sink, nsref="t:sink")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    src = dag.node(ns.get("t:source"))
    snk = dag.node(ns.get("t:sink"))
    src >> snk  # pyright: ignore[reportUnusedExpression]
    dag.validate()


# Task 7: Dag introspection and context manager


def test_dag_topological_order():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:compute")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="a:report")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    f = dag.node(ns.get("a:fetch"))
    c = dag.node(ns.get("a:compute"))
    r = dag.node(ns.get("a:report"))
    f >> c >> r  # pyright: ignore[reportUnusedExpression]

    order = dag.topological_order()
    labels = [n.label for n in order]
    assert labels.index("fetch") < labels.index("compute")
    assert labels.index("compute") < labels.index("report")


def test_dag_sources_and_sinks():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:compute")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="a:report")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    f = dag.node(ns.get("a:fetch"))
    c = dag.node(ns.get("a:compute"))
    r = dag.node(ns.get("a:report"))
    f >> c >> r  # pyright: ignore[reportUnusedExpression]

    source_labels = sorted([n.label for n in dag.sources()])
    sink_labels = sorted([n.label for n in dag.sinks()])
    assert source_labels == ["fetch"]
    assert sink_labels == ["report"]


def test_dag_sources_sinks_fan_out():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:report")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="a:archive")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    f = dag.node(ns.get("a:fetch"))
    r = dag.node(ns.get("a:report"))
    a = dag.node(ns.get("a:archive"))
    f >> r  # pyright: ignore[reportUnusedExpression]
    f >> a  # pyright: ignore[reportUnusedExpression]

    assert len(dag.sources()) == 1
    sink_labels = sorted([n.label for n in dag.sinks()])
    assert sink_labels == ["archive", "report"]


def test_dag_context_manager_validates():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:step1")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:step2")  # pyright: ignore[reportPrivateUsage]

    with Dag() as dag:
        s1 = dag.node(ns.get("a:step1"))
        s2 = dag.node(ns.get("a:step2"))
        s1 >> s2  # pyright: ignore[reportUnusedExpression]


def test_dag_context_manager_catches_cycle():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:n1")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:n2")  # pyright: ignore[reportPrivateUsage]

    try:
        with Dag() as dag:
            n1 = dag.node(ns.get("a:n1"))
            n2 = dag.node(ns.get("a:n2"))
            n1 >> n2  # pyright: ignore[reportUnusedExpression]
            dag.add_edge(n2, n1)
        raise AssertionError("Expected ValueError for cycle")
    except ValueError as e:
        assert "cycle" in str(e).lower()


# Task 2 (plan): Dag.clone(prefix)


def test_dag_clone_prefixes_labels():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:compute")  # pyright: ignore[reportPrivateUsage]

    with Dag() as template:
        f = template.node(ns.get("a:fetch"))
        c = template.node(ns.get("a:compute"))
        f >> c  # pyright: ignore[reportUnusedExpression]

    clone = template.clone("etl")
    assert "etl/fetch" in clone.nodes
    assert "etl/compute" in clone.nodes
    assert len(clone.nodes) == 2


def test_dag_clone_remaps_edges():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:compute")  # pyright: ignore[reportPrivateUsage]

    with Dag() as template:
        f = template.node(ns.get("a:fetch"))
        c = template.node(ns.get("a:compute"))
        f >> c  # pyright: ignore[reportUnusedExpression]

    clone = template.clone("etl")
    assert len(clone.edges) == 1
    assert clone.edges[0].upstream == "etl/fetch"
    assert clone.edges[0].downstream == "etl/compute"


def test_dag_clone_shares_ns_node():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")  # pyright: ignore[reportPrivateUsage]

    with Dag() as template:
        template.node(ns.get("a:fetch"))

    clone = template.clone("sub")
    assert clone.nodes["sub/fetch"].ns_node is template.nodes["fetch"].ns_node


def test_dag_clone_leaves_original_unmodified():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:compute")  # pyright: ignore[reportPrivateUsage]

    with Dag() as template:
        f = template.node(ns.get("a:fetch"))
        c = template.node(ns.get("a:compute"))
        f >> c  # pyright: ignore[reportUnusedExpression]

    original_labels = set(template.nodes.keys())
    original_edges = list(template.edges)

    template.clone("etl")

    assert set(template.nodes.keys()) == original_labels
    assert template.edges == original_edges


def test_dag_clone_empty_prefix_raises():
    from lythonic.compose.namespace import Dag

    dag = Dag()
    try:
        dag.clone("")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "non-empty" in str(e)


# Task 3 (plan): DagNode >> Dag merge and wire


def test_rshift_dag_merges_and_wires():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:setup")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="a:fetch")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:compute")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:report")  # pyright: ignore[reportPrivateUsage]

    with Dag() as template:
        f = template.node(ns.get("a:fetch"))
        c = template.node(ns.get("a:compute"))
        f >> c  # pyright: ignore[reportUnusedExpression]

    sub = template.clone("etl")

    with Dag() as parent:
        setup = parent.node(ns.get("a:setup"))
        report = parent.node(ns.get("a:report"))
        _ = setup >> sub >> report

    assert "setup" in parent.nodes
    assert "etl/fetch" in parent.nodes
    assert "etl/compute" in parent.nodes
    assert "report" in parent.nodes
    assert len(parent.nodes) == 4

    edge_pairs = [(e.upstream, e.downstream) for e in parent.edges]
    assert ("setup", "etl/fetch") in edge_pairs
    assert ("etl/fetch", "etl/compute") in edge_pairs
    assert ("etl/compute", "report") in edge_pairs


def test_rshift_dag_chained_composition():
    """setup >> sub1 >> sub2 >> report"""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:setup")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="a:step")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:report")  # pyright: ignore[reportPrivateUsage]

    with Dag() as template:
        template.node(ns.get("a:step"))

    sub1 = template.clone("stage1")
    sub2 = template.clone("stage2")

    with Dag() as parent:
        setup = parent.node(ns.get("a:setup"))
        report = parent.node(ns.get("a:report"))
        _ = setup >> sub1 >> sub2 >> report

    assert len(parent.nodes) == 4
    labels = sorted(parent.nodes.keys())
    assert labels == ["report", "setup", "stage1/step", "stage2/step"]

    edge_pairs = [(e.upstream, e.downstream) for e in parent.edges]
    assert ("setup", "stage1/step") in edge_pairs
    assert ("stage1/step", "stage2/step") in edge_pairs
    assert ("stage2/step", "report") in edge_pairs


def test_rshift_dag_empty_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:setup")  # pyright: ignore[reportPrivateUsage]

    parent = Dag()
    setup = parent.node(ns.get("a:setup"))

    empty_dag = Dag()
    try:
        _ = setup >> empty_dag
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "empty" in str(e).lower()


def test_rshift_dag_multi_source_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:setup")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="a:s1")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:s2")  # pyright: ignore[reportPrivateUsage]

    multi = Dag()
    multi.node(ns.get("a:s1"))
    multi.node(ns.get("a:s2"))

    parent = Dag()
    setup = parent.node(ns.get("a:setup"))

    try:
        _ = setup >> multi
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "one source" in str(e).lower()


def test_rshift_dag_multi_sink_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:setup")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="a:src")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:sink1")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:sink2")  # pyright: ignore[reportPrivateUsage]

    multi = Dag()
    src = multi.node(ns.get("a:src"))
    s1 = multi.node(ns.get("a:sink1"))
    s2 = multi.node(ns.get("a:sink2"))
    _ = src >> s1
    _ = src >> s2

    parent = Dag()
    setup = parent.node(ns.get("a:setup"))

    try:
        _ = setup >> multi
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "one sink" in str(e).lower()


def test_rshift_dag_label_collision_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fetch")  # pyright: ignore[reportPrivateUsage]

    sub = Dag()
    sub.node(ns.get("a:fetch"))

    parent = Dag()
    parent.node(ns.get("a:fetch"))

    try:
        _ = parent.nodes["fetch"] >> sub
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "already exists" in str(e)


# Tags


def test_namespace_node_tags_default():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(_get_sample_fn())
    ns = Namespace()
    node = NamespaceNode(method=method, nsref="test:sample_fn", namespace=ns)
    assert node.tags == frozenset()


def test_namespace_node_tags_set():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(_get_sample_fn())
    ns = Namespace()
    node = NamespaceNode(
        method=method, nsref="test:sample_fn", namespace=ns, tags=frozenset({"slow", "market"})
    )
    assert node.tags == frozenset({"slow", "market"})


def test_namespace_node_tags_rejects_string():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(_get_sample_fn())
    ns = Namespace()
    try:
        NamespaceNode(
            method=method,
            nsref="test:sample_fn",
            namespace=ns,
            tags="slow",  # pyright: ignore[reportArgumentType]
        )
        raise AssertionError("Expected TypeError")
    except TypeError as e:
        assert "str" in str(e)


def test_namespace_node_tags_rejects_invalid_chars():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(_get_sample_fn())
    ns = Namespace()
    for bad_tag in ["slow&fast", "a|b", "~experimental", "has space"]:
        try:
            NamespaceNode(
                method=method, nsref="test:sample_fn", namespace=ns, tags=frozenset({bad_tag})
            )
            raise AssertionError(f"Expected ValueError for tag {bad_tag!r}")
        except ValueError as e:
            assert "tag" in str(e).lower()


def test_register_with_tags():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    node = ns.register(
        this_module._sample_fn,  # pyright: ignore[reportPrivateUsage]
        nsref="test:fetch",
        tags={"slow", "market"},
    )
    assert node.tags == frozenset({"slow", "market"})


def test_register_without_tags():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    node = ns.register(this_module._sample_fn, nsref="test:fetch")  # pyright: ignore[reportPrivateUsage]
    assert node.tags == frozenset()


def test_register_all_with_tags():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    nodes = ns.register_all(
        this_module._sample_fn,  # pyright: ignore[reportPrivateUsage]
        this_module._another_fn,  # pyright: ignore[reportPrivateUsage]
        tags={"batch"},
    )
    assert all(n.tags == frozenset({"batch"}) for n in nodes)


def test_register_tags_validation():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    try:
        ns.register(this_module._sample_fn, nsref="test:fetch", tags="slow")  # pyright: ignore[reportPrivateUsage, reportArgumentType]
        raise AssertionError("Expected TypeError")
    except TypeError:
        pass
