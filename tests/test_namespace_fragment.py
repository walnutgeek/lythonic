from __future__ import annotations

import tests.test_namespace_fragment as this_module
from lythonic.compose.namespace import (
    Dag,
    Namespace,
    NsCacheConfig,
    dag_factory,
    nsnode,
    require_cache,
)


def test_nsnode_sets_flag_and_tags():
    @nsnode(tags=["api", "market"])
    def fetch(ticker: str) -> dict[str, str]:
        return {"ticker": ticker}

    assert getattr(fetch, "_is_nsnode", False) is True
    assert getattr(fetch, "_nsnode_tags", []) == ["api", "market"]
    assert fetch(ticker="X") == {"ticker": "X"}


def test_nsnode_empty_tags():
    @nsnode()
    def transform(data: dict[str, str]) -> dict[str, str]:
        return data

    assert getattr(transform, "_is_nsnode", False) is True
    assert getattr(transform, "_nsnode_tags", []) == []


def test_require_cache_sets_flag():
    @require_cache
    def expensive(key: str) -> dict[str, str]:
        return {"key": key}

    assert getattr(expensive, "_require_cache", False) is True
    assert expensive(key="X") == {"key": "X"}


def test_require_cache_stacks_with_nsnode():
    @require_cache
    @nsnode(tags=["api"])
    def fetch(ticker: str) -> dict[str, str]:
        return {"ticker": ticker}

    assert getattr(fetch, "_is_nsnode", False) is True
    assert getattr(fetch, "_require_cache", False) is True
    assert getattr(fetch, "_nsnode_tags", []) == ["api"]


def test_require_cache_stacks_with_dag_factory():
    @require_cache
    @dag_factory
    def my_pipeline() -> Dag:
        dag = Dag()
        dag.node(lambda: "x", label="src")
        return dag

    assert getattr(my_pipeline, "_is_dag_factory", False) is True
    assert getattr(my_pipeline, "_require_cache", False) is True


@require_cache
def _cached_fn(key: str) -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
    return {"key": key}


def _plain_fn(key: str) -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
    return {"key": key}


def test_register_require_cache_without_config_raises():
    ns = Namespace()
    try:
        ns.register(this_module._cached_fn, nsref="t:cached")  # pyright: ignore[reportPrivateUsage]
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "require_cache" in str(e).lower() or "cache" in str(e).lower()


def test_register_require_cache_with_cache_config_succeeds():
    ns = Namespace()
    cfg = NsCacheConfig(nsref="t:cached", min_ttl=0.5, max_ttl=2.0)
    node = ns.register(this_module._cached_fn, nsref="t:cached", config=cfg)  # pyright: ignore[reportPrivateUsage]
    assert node.nsref == "t:cached"


def test_register_plain_fn_without_config_still_works():
    ns = Namespace()
    node = ns.register(this_module._plain_fn, nsref="t:plain")  # pyright: ignore[reportPrivateUsage]
    assert node.nsref == "t:plain"


@require_cache
@dag_factory
def _cached_dag_factory():  # pyright: ignore[reportUnusedFunction]
    from lythonic.compose.namespace import Dag

    dag = Dag()
    dag.node(lambda: "x", label="src")
    return dag


def test_register_require_cache_dag_factory_without_config_raises():
    ns = Namespace()
    try:
        ns.register(this_module._cached_dag_factory)  # pyright: ignore[reportPrivateUsage]
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "cache" in str(e).lower()


def test_register_require_cache_dag_factory_with_config_succeeds():
    ns = Namespace()
    cfg = NsCacheConfig(nsref="t:cached_dag__", min_ttl=1.0, max_ttl=5.0)
    node = ns.register(this_module._cached_dag_factory, nsref="t:cached_dag__", config=cfg)  # pyright: ignore[reportPrivateUsage]
    assert node.nsref == "t:cached_dag__"
