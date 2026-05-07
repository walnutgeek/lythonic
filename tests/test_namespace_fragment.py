from __future__ import annotations

from lythonic.compose.namespace import Dag, dag_factory, nsnode, require_cache


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
