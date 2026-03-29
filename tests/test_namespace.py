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
