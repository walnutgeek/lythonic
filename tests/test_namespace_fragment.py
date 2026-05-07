from __future__ import annotations

from typing import Any

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


def test_ns_fragment_config_validates():
    from lythonic import GlobalRef
    from lythonic.compose.namespace import NsFragmentConfig

    cfg = NsFragmentConfig(
        gref=GlobalRef("tests.test_namespace_fragment:SampleFragment"),
        nsref="downloads:",
        init={"api_key": "abc123"},
        configs={
            "fetch_prices": {"min_ttl": 0.5, "max_ttl": 2.0},
        },
    )
    assert cfg.type == "fragment"
    assert cfg.init == {"api_key": "abc123"}
    assert cfg.configs["fetch_prices"]["min_ttl"] == 0.5


def test_ns_fragment_config_in_config_types():
    from lythonic.compose.namespace import (
        _CONFIG_TYPES,  # pyright: ignore[reportPrivateUsage]
        NsFragmentConfig,
    )

    assert _CONFIG_TYPES["fragment"] is NsFragmentConfig  # pyright: ignore[reportPrivateUsage]


def test_namespace_fragment_is_marker_class():
    from lythonic.compose.namespace import NamespaceFragment

    class MyFrag(NamespaceFragment):
        pass

    frag = MyFrag()
    assert isinstance(frag, NamespaceFragment)


from lythonic.compose.namespace import NamespaceFragment


def _pipeline_step1(data: str) -> str:
    return data.upper()


def _pipeline_step2(data: str) -> str:
    return data + "!"


class SampleFragment(NamespaceFragment):
    api_key: str
    base_url: str

    def __init__(self, api_key: str, base_url: str = "https://api.example.com"):
        self.api_key = api_key
        self.base_url = base_url

    @require_cache
    @nsnode(tags=["api"])
    def fetch_prices(self, ticker: str) -> dict[str, str]:
        return {"ticker": ticker, "key": self.api_key}

    @nsnode()
    def transform(self, data: dict[str, Any]) -> dict[str, Any]:
        return {"processed": data}

    @dag_factory
    def pipeline(self) -> Dag:
        dag = Dag()
        _ = dag.node(_pipeline_step1) >> dag.node(_pipeline_step2)
        return dag

    def _private_helper(self) -> None:
        pass


def test_class_fragment_registers_all_methods():
    ns = Namespace.from_dict(
        [
            {
                "type": "fragment",
                "gref": "tests.test_namespace_fragment:SampleFragment",
                "nsref": "downloads:",
                "init": {"api_key": "abc123"},
                "configs": {
                    "fetch_prices": {"min_ttl": 0.5, "max_ttl": 2.0},
                },
            }
        ]
    )

    fetch_node = ns.get("downloads:fetch_prices")
    assert fetch_node(ticker="AAPL") == {"ticker": "AAPL", "key": "abc123"}
    assert fetch_node.tags == frozenset({"api"})

    transform_node = ns.get("downloads:transform")
    assert transform_node(data={"x": 1}) == {"processed": {"x": 1}}

    pipeline_node = ns.get("downloads:pipeline__")
    assert pipeline_node.dag is not None

    # Private helper not registered
    try:
        ns.get("downloads:_private_helper")
        raise AssertionError("Should not find private helper")
    except KeyError:
        pass


def test_class_fragment_cache_config_applied():
    ns = Namespace.from_dict(
        [
            {
                "type": "fragment",
                "gref": "tests.test_namespace_fragment:SampleFragment",
                "nsref": "dl:",
                "init": {"api_key": "key1"},
                "configs": {
                    "fetch_prices": {"min_ttl": 0.5, "max_ttl": 2.0},
                },
            }
        ]
    )

    fetch_node = ns.get("dl:fetch_prices")
    assert isinstance(fetch_node.config, NsCacheConfig)
    assert fetch_node.config.min_ttl == 0.5
    assert fetch_node.config.max_ttl == 2.0


def test_class_fragment_require_cache_missing_raises():
    try:
        Namespace.from_dict(
            [
                {
                    "type": "fragment",
                    "gref": "tests.test_namespace_fragment:SampleFragment",
                    "nsref": "dl:",
                    "init": {"api_key": "key1"},
                    "configs": {},
                }
            ]
        )
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "fetch_prices" in str(e) or "require_cache" in str(e).lower()


def test_class_fragment_constructor_args():
    ns = Namespace.from_dict(
        [
            {
                "type": "fragment",
                "gref": "tests.test_namespace_fragment:SampleFragment",
                "nsref": "x:",
                "init": {"api_key": "secret", "base_url": "https://custom.api"},
                "configs": {
                    "fetch_prices": {"min_ttl": 1.0, "max_ttl": 5.0},
                },
            }
        ]
    )

    result = ns.get("x:fetch_prices")(ticker="MSFT")
    assert result == {"ticker": "MSFT", "key": "secret"}


import logging

import pytest


def test_fragment_configs_nonexistent_method_warns(caplog: pytest.LogCaptureFixture):
    """configs entry for a method that doesn't exist logs a warning."""
    with caplog.at_level(logging.WARNING):
        Namespace.from_dict(
            [
                {
                    "type": "fragment",
                    "gref": "tests.test_namespace_fragment:SampleFragment",
                    "nsref": "w:",
                    "init": {"api_key": "k"},
                    "configs": {
                        "fetch_prices": {"min_ttl": 0.5, "max_ttl": 2.0},
                        "nonexistent_method": {"min_ttl": 1.0, "max_ttl": 3.0},
                    },
                }
            ]
        )
    assert any("nonexistent_method" in r.message for r in caplog.records)


def test_fragment_with_triggers():
    ns = Namespace.from_dict(
        [
            {
                "type": "fragment",
                "gref": "tests.test_namespace_fragment:SampleFragment",
                "nsref": "trig:",
                "init": {"api_key": "k"},
                "configs": {
                    "fetch_prices": {"min_ttl": 0.5, "max_ttl": 2.0},
                    "pipeline": {
                        "triggers": [
                            {"name": "pipe_repeat", "type": "poll", "schedule": "*/30 * * * * *"}
                        ]
                    },
                },
            }
        ]
    )

    pipeline_node = ns.get("trig:pipeline__")
    assert len(pipeline_node.config.triggers) == 1
    assert pipeline_node.config.triggers[0].name == "pipe_repeat"


def test_fragment_nsref_must_end_with_colon():
    try:
        Namespace.from_dict(
            [
                {
                    "type": "fragment",
                    "gref": "tests.test_namespace_fragment:SampleFragment",
                    "nsref": "bad_prefix",
                    "init": {"api_key": "k"},
                    "configs": {
                        "fetch_prices": {"min_ttl": 0.5, "max_ttl": 2.0},
                    },
                }
            ]
        )
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert ":" in str(e)


def test_module_fragment_discovers_functions():
    ns = Namespace.from_dict(
        [
            {
                "type": "fragment",
                "gref": "tests.sample_module_fragment",
                "nsref": "transforms:",
                "configs": {
                    "normalize": {"min_ttl": 1.0, "max_ttl": 5.0},
                },
            }
        ]
    )

    norm = ns.get("transforms:normalize")
    assert norm(data={"x": 1}) == {"normalized": True, "x": 1}
    assert norm.tags == frozenset({"api"})
    assert isinstance(norm.config, NsCacheConfig)

    flat = ns.get("transforms:flatten")
    assert flat(data=[[1, 2], [3]]) == [1, 2, 3]


def test_module_fragment_with_init_raises():
    try:
        Namespace.from_dict(
            [
                {
                    "type": "fragment",
                    "gref": "tests.sample_module_fragment",
                    "nsref": "t:",
                    "init": {"key": "val"},
                    "configs": {
                        "normalize": {"min_ttl": 1.0, "max_ttl": 5.0},
                    },
                }
            ]
        )
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "module" in str(e).lower() and "init" in str(e).lower()


def test_module_fragment_require_cache_missing_raises():
    try:
        Namespace.from_dict(
            [
                {
                    "type": "fragment",
                    "gref": "tests.sample_module_fragment",
                    "nsref": "t:",
                    "configs": {},
                }
            ]
        )
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "normalize" in str(e) or "require_cache" in str(e).lower()
