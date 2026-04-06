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


def _constant() -> str:
    return "hello"


def _boom() -> str:
    raise RuntimeError("intentional failure")


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


# Dag-namespace registration


def test_register_dag_copies_nodes_to_parent():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    dag = Dag()
    dag.node(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]
    dag.node(this_module._another_fn)  # pyright: ignore[reportPrivateUsage]

    ns.register(dag, nsref="pipelines:my_dag")

    # Nodes from the DAG should now be in the parent namespace
    sample_node = ns.get("tests.test_namespace:_sample_fn")
    assert sample_node is not None
    another_node = ns.get("tests.test_namespace:_another_fn")
    assert another_node is not None


def test_register_dag_updates_dagnode_references():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    dag = Dag()
    dag.node(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]

    ns.register(dag, nsref="pipelines:my_dag")

    # DagNode.ns_node should now point to the parent namespace's copy
    dag_node = dag.nodes["_sample_fn"]
    assert dag_node.ns_node.namespace is ns


def test_register_dag_skip_identical():
    """If parent already has the same callable, skip without error."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]

    # Should not raise -- same callable
    ns.register(dag, nsref="pipelines:my_dag")

    # DagNode should now reference the parent's node
    dag_node = dag.nodes["_sample_fn"]
    assert dag_node.ns_node.namespace is ns


def test_register_dag_conflict_raises():
    """If parent has same nsref but different callable, raise ValueError."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    # Register _sample_fn under the nsref that _another_fn will use
    ns.register(
        this_module._sample_fn,  # pyright: ignore[reportPrivateUsage]
        nsref="tests.test_namespace:_another_fn",
    )

    dag = Dag()
    dag.node(this_module._another_fn)  # pyright: ignore[reportPrivateUsage]

    try:
        ns.register(dag, nsref="pipelines:my_dag")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "conflict" in str(e).lower() or "different" in str(e).lower()


def test_register_dag_double_registration_raises():
    """Registering a DAG to the same namespace twice raises ValueError."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    dag = Dag()
    dag.node(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]

    ns.register(dag, nsref="pipelines:first")

    try:
        ns.register(dag, nsref="pipelines:second")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "already registered" in str(e).lower()


def test_dag_node_auto_registers_callable():
    """dag.node(callable) auto-registers the callable in dag.namespace."""
    from lythonic.compose.namespace import Dag

    dag = Dag()
    dag.node(this_module._sample_fn)  # pyright: ignore[reportPrivateUsage]

    # Should be in the DAG's internal namespace
    node = dag.namespace.get("tests.test_namespace:_sample_fn")
    assert node is not None
    assert node(ticker="AAPL") == {"ticker": "AAPL", "limit": 10}


def test_dag_node_reuses_namespace_node():
    """Calling dag.node(same_fn) twice reuses the same NamespaceNode."""
    from lythonic.compose.namespace import Dag

    dag = Dag()
    d1 = dag.node(this_module._sample_fn, label="first")  # pyright: ignore[reportPrivateUsage]
    d2 = dag.node(this_module._sample_fn, label="second")  # pyright: ignore[reportPrivateUsage]

    assert d1.ns_node is d2.ns_node
    assert len(dag.nodes) == 2


def test_dag_node_with_namespace_node_skips_registration():
    """Passing a NamespaceNode directly does not re-register."""
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    registered = ns.register(this_module._sample_fn, nsref="market:fetch")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    d = dag.node(registered)

    assert d.ns_node is registered
    # DAG's own namespace should remain empty
    assert dag.namespace._all_leaves() == []  # pyright: ignore[reportPrivateUsage]


# Dag.map()


def test_dag_map_creates_map_node():
    from lythonic.compose.namespace import Dag, MapNode, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="t:process")  # pyright: ignore[reportPrivateUsage]

    sub_dag = Dag()
    sub_dag.node(ns.get("t:process"))

    parent = Dag()
    m = parent.map(sub_dag, label="chunks")

    assert isinstance(m, MapNode)
    assert m.label == "chunks"
    assert m.sub_dag is sub_dag
    assert "chunks" in parent.nodes


def test_dag_map_wires_with_rshift():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="t:sink")  # pyright: ignore[reportPrivateUsage]

    sub_dag = Dag()
    sub_dag.node(ns.get("t:sink"))

    parent = Dag()
    s = parent.node(ns.get("t:source"))
    m = parent.map(sub_dag, label="mapped")
    s >> m  # pyright: ignore[reportUnusedExpression]

    assert len(parent.edges) == 1
    assert parent.edges[0].upstream == "source"
    assert parent.edges[0].downstream == "mapped"


def test_dag_map_requires_label():
    from lythonic.compose.namespace import Dag

    sub_dag = Dag()
    parent = Dag()
    try:
        parent.map(sub_dag, label="")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "label" in str(e).lower()


def test_dag_map_duplicate_label_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="t:source")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="t:process")  # pyright: ignore[reportPrivateUsage]

    sub_dag = Dag()
    sub_dag.node(ns.get("t:process"))

    parent = Dag()
    parent.node(ns.get("t:source"), label="chunks")
    try:
        parent.map(sub_dag, label="chunks")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "already exists" in str(e)


def test_dag_map_multi_source_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="t:a")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="t:b")  # pyright: ignore[reportPrivateUsage]

    sub_dag = Dag()
    sub_dag.node(ns.get("t:a"))
    sub_dag.node(ns.get("t:b"))

    parent = Dag()
    try:
        parent.map(sub_dag, label="bad")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "one source" in str(e).lower()


def test_dag_map_multi_sink_raises():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="t:src")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="t:sink1")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="t:sink2")  # pyright: ignore[reportPrivateUsage]

    sub_dag = Dag()
    s = sub_dag.node(ns.get("t:src"))
    s1 = sub_dag.node(ns.get("t:sink1"))
    s2 = sub_dag.node(ns.get("t:sink2"))
    s >> s1  # pyright: ignore[reportUnusedExpression]
    s >> s2  # pyright: ignore[reportUnusedExpression]

    parent = Dag()
    try:
        parent.map(sub_dag, label="bad")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "one sink" in str(e).lower()


# Dag.__call__


async def test_dag_callable_no_inputs():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._constant, nsref="t:constant")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:constant"))

    result = await dag()
    assert result.status == "completed"
    assert result.outputs["constant"] == "hello"


async def test_dag_callable_with_kwargs():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="t:fetch")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:fetch"))

    result = await dag(ticker="AAPL")
    assert result.status == "completed"
    assert result.outputs["fetch"] == {"ticker": "AAPL", "limit": 10}


async def test_dag_callable_error_propagation():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._boom, nsref="t:boom")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    dag.node(ns.get("t:boom"))

    result = await dag()
    assert result.status == "failed"
    assert "intentional failure" in (result.error or "")


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


# Namespace.query()


def _setup_tagged_namespace():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fast_market", tags={"fast", "market"})  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:slow_market", tags={"slow", "market"})  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="b:fast_exp", tags={"fast", "experimental"})  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="b:untagged")  # pyright: ignore[reportPrivateUsage]
    return ns


def test_query_single_tag():
    ns = _setup_tagged_namespace()
    result = ns.query("slow")
    assert [n.nsref for n in result] == ["a:slow_market"]


def test_query_and():
    ns = _setup_tagged_namespace()
    result = ns.query("fast & market")
    assert [n.nsref for n in result] == ["a:fast_market"]


def test_query_or():
    ns = _setup_tagged_namespace()
    result = ns.query("slow | experimental")
    nsrefs = sorted(n.nsref for n in result)
    assert nsrefs == ["a:slow_market", "b:fast_exp"]


def test_query_not():
    ns = _setup_tagged_namespace()
    result = ns.query("~market")
    nsrefs = sorted(n.nsref for n in result)
    assert nsrefs == ["b:fast_exp", "b:untagged"]


def test_query_combined_precedence():
    """slow & ~experimental | fast evaluates as (slow & ~experimental) | fast"""
    ns = _setup_tagged_namespace()
    result = ns.query("slow & ~experimental | fast")
    nsrefs = sorted(n.nsref for n in result)
    assert nsrefs == ["a:fast_market", "a:slow_market", "b:fast_exp"]


def test_query_no_matches():
    ns = _setup_tagged_namespace()
    result = ns.query("nonexistent")
    assert result == []


def test_query_empty_raises():
    ns = _setup_tagged_namespace()
    try:
        ns.query("")
        raise AssertionError("Expected ValueError")
    except ValueError:
        pass

    try:
        ns.query("   ")
        raise AssertionError("Expected ValueError")
    except ValueError:
        pass


def test_query_malformed_raises():
    ns = _setup_tagged_namespace()
    for bad_expr in ["& slow", "slow &", "~ &", "| fast"]:
        try:
            ns.query(bad_expr)
            raise AssertionError(f"Expected ValueError for {bad_expr!r}")
        except ValueError:
            pass


def test_query_recursive_across_branches():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="deep.nested.branch:fn", tags={"deep"})  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="top:fn", tags={"deep"})  # pyright: ignore[reportPrivateUsage]
    result = ns.query("deep")
    nsrefs = sorted(n.nsref for n in result)
    assert nsrefs == ["deep.nested.branch:fn", "top:fn"]


def test_query_untagged_nodes_match_not():
    """Nodes with no tags match ~sometag (they lack the tag)."""
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:tagged", tags={"x"})  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:untagged")  # pyright: ignore[reportPrivateUsage]
    result = ns.query("~x")
    assert [n.nsref for n in result] == ["a:untagged"]


def test_query_or_short_circuit():
    """Ensure OR doesn't short-circuit and skip token consumption."""
    ns = _setup_tagged_namespace()
    # a:fast_market has {fast, market}, b:fast_exp has {fast, experimental}
    # "market | experimental" should match both without parser errors
    result = ns.query("market | experimental")
    nsrefs = sorted(n.nsref for n in result)
    assert nsrefs == ["a:fast_market", "a:slow_market", "b:fast_exp"]


def test_query_and_short_circuit():
    """Ensure AND doesn't short-circuit and skip token consumption."""
    ns = _setup_tagged_namespace()
    # Node b:untagged has no tags. "nonexistent & market" should match nothing
    # without parser errors from unconsumed tokens.
    result = ns.query("nonexistent & market")
    assert result == []
