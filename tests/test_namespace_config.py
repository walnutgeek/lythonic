from __future__ import annotations


def test_config_model_callable_entry():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {"nsref": "market:fetch", "gref": "json:dumps"},
            ]
        }
    )
    assert len(config.entries) == 1
    assert config.entries[0].nsref == "market:fetch"
    assert config.entries[0].gref == "json:dumps"
    assert config.entries[0].cache is None
    assert config.entries[0].dag is None


def test_config_model_cached_entry():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {
                    "nsref": "market:fetch",
                    "gref": "json:dumps",
                    "cache": {"min_ttl": 0.5, "max_ttl": 2.0},
                },
            ]
        }
    )
    assert config.entries[0].cache is not None
    assert config.entries[0].cache.min_ttl == 0.5
    assert config.entries[0].cache.max_ttl == 2.0


def test_config_model_dag_entry():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {
                    "nsref": "pipelines:daily",
                    "dag": {
                        "nodes": [
                            {"label": "fetch", "nsref": "market:fetch"},
                            {"label": "compute", "nsref": "analysis:compute", "after": ["fetch"]},
                        ]
                    },
                },
            ]
        }
    )
    dag = config.entries[0].dag
    assert dag is not None
    assert len(dag.nodes) == 2
    assert dag.nodes[1].after == ["fetch"]


def test_config_model_storage():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate(
        {
            "storage": {"cache_db": "cache.db", "dag_db": "runs.db"},
            "entries": [],
        }
    )
    assert config.storage.cache_db == "cache.db"
    assert config.storage.dag_db == "runs.db"


def test_config_model_storage_defaults():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate({"entries": []})
    assert config.storage.cache_db is None
    assert config.storage.dag_db is None


def test_config_entry_validation_neither_gref_nor_dag():
    from pydantic import ValidationError

    from lythonic.compose.namespace_config import NamespaceConfig

    try:
        NamespaceConfig.model_validate({"entries": [{"nsref": "bad:entry"}]})
        raise AssertionError("Expected ValidationError")
    except ValidationError:
        pass


def test_config_entry_validation_both_gref_and_dag():
    from pydantic import ValidationError

    from lythonic.compose.namespace_config import NamespaceConfig

    try:
        NamespaceConfig.model_validate(
            {
                "entries": [
                    {
                        "nsref": "bad:entry",
                        "gref": "json:dumps",
                        "dag": {"nodes": []},
                    }
                ]
            }
        )
        raise AssertionError("Expected ValidationError")
    except ValidationError:
        pass


def test_namespace_node_metadata():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    def sample(x: int) -> int:
        return x

    ns = Namespace()
    method = Method(sample)
    node = NamespaceNode(method=method, nsref="t:sample", namespace=ns)
    assert node.metadata == {}

    node.metadata["cache"] = {"min_ttl": 0.5, "max_ttl": 2.0}
    assert node.metadata["cache"]["min_ttl"] == 0.5


import tempfile
from pathlib import Path
from typing import Any

import tests.test_namespace_config as this_module


def _cached_fn(ticker: str) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction]
    this_module._cached_fn_count += 1  # pyright: ignore
    return {"price": 100.0, "ticker": ticker}


_cached_fn_count = 0


def test_register_cached_callable():
    from lythonic.compose.cached import register_cached_callable
    from lythonic.compose.namespace import Namespace

    this_module._cached_fn_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        node = register_cached_callable(
            ns,
            gref="tests.test_namespace_config:_cached_fn",
            nsref="market:fetch",
            min_ttl=1.0,
            max_ttl=2.0,
            db_path=db_path,
        )

        assert node.nsref == "market:fetch"
        assert node.metadata.get("cache") == {"min_ttl": 1.0, "max_ttl": 2.0}

        result = ns.market.fetch(ticker="AAPL")  # pyright: ignore
        assert result == {"price": 100.0, "ticker": "AAPL"}
        assert this_module._cached_fn_count == 1  # pyright: ignore

        # Second call from cache
        result2 = ns.market.fetch(ticker="AAPL")  # pyright: ignore
        assert result2 == {"price": 100.0, "ticker": "AAPL"}
        assert this_module._cached_fn_count == 1  # pyright: ignore


def _plain_fn(x: int) -> int:  # pyright: ignore[reportUnusedFunction]
    return x * 2


def _another_plain_fn(value: int) -> str:  # pyright: ignore[reportUnusedFunction]
    return str(value)


def test_load_namespace_callable_entries():
    from lythonic.compose.namespace_config import NamespaceConfig, load_namespace

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
            ]
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        ns = load_namespace(config, Path(tmp))
        node = ns.get("math:double")
        assert node(x=5) == 10


def test_load_namespace_cached_entries():
    from lythonic.compose.namespace_config import NamespaceConfig, load_namespace

    this_module._cached_fn_count = 0  # pyright: ignore

    config = NamespaceConfig.model_validate(
        {
            "storage": {"cache_db": "cache.db"},
            "entries": [
                {
                    "nsref": "market:fetch",
                    "gref": "tests.test_namespace_config:_cached_fn",
                    "cache": {"min_ttl": 1.0, "max_ttl": 2.0},
                },
            ],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        ns = load_namespace(config, Path(tmp))
        result = ns.market.fetch(ticker="AAPL")  # pyright: ignore
        assert result == {"price": 100.0, "ticker": "AAPL"}
        assert this_module._cached_fn_count == 1  # pyright: ignore

        # Cached
        ns.market.fetch(ticker="AAPL")  # pyright: ignore
        assert this_module._cached_fn_count == 1  # pyright: ignore


def test_load_namespace_dag_entries():
    from lythonic.compose.namespace_config import NamespaceConfig, load_namespace

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
                {"nsref": "fmt:to_str", "gref": "tests.test_namespace_config:_another_plain_fn"},
                {
                    "nsref": "pipelines:convert",
                    "dag": {
                        "nodes": [
                            {"label": "double", "nsref": "math:double"},
                            {"label": "format", "nsref": "fmt:to_str", "after": ["double"]},
                        ]
                    },
                },
            ],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        ns = load_namespace(config, Path(tmp))
        node = ns.get("pipelines:convert")
        assert node is not None


def test_load_namespace_two_pass_order_independence():
    """DAG entry can appear before callable entries it references."""
    from lythonic.compose.namespace_config import NamespaceConfig, load_namespace

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {
                    "nsref": "pipelines:pipe",
                    "dag": {
                        "nodes": [
                            {"label": "step", "nsref": "math:double"},
                        ]
                    },
                },
                {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
            ],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        ns = load_namespace(config, Path(tmp))
        assert ns.get("math:double") is not None
        assert ns.get("pipelines:pipe") is not None


def test_load_namespace_duplicate_nsref_raises():
    from lythonic.compose.namespace_config import NamespaceConfig, load_namespace

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
                {"nsref": "math:double", "gref": "tests.test_namespace_config:_another_plain_fn"},
            ],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        try:
            load_namespace(config, Path(tmp))
            raise AssertionError("Expected ValueError")
        except ValueError as e:
            assert "duplicate" in str(e).lower() or "already exists" in str(e).lower()


def test_load_namespace_dag_bad_nsref_raises():
    from lythonic.compose.namespace_config import NamespaceConfig, load_namespace

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {
                    "nsref": "pipelines:pipe",
                    "dag": {
                        "nodes": [
                            {"label": "step", "nsref": "nonexistent:fn"},
                        ]
                    },
                },
            ],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        try:
            load_namespace(config, Path(tmp))
            raise AssertionError("Expected KeyError or ValueError")
        except (KeyError, ValueError):
            pass


def test_validate_config_valid():
    from lythonic.compose.namespace_config import NamespaceConfig, validate_config

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
                {
                    "nsref": "pipelines:pipe",
                    "dag": {
                        "nodes": [
                            {"label": "step", "nsref": "math:double"},
                        ]
                    },
                },
            ],
        }
    )
    errors = validate_config(config)
    assert errors == []


def test_validate_config_bad_gref():
    from lythonic.compose.namespace_config import NamespaceConfig, validate_config

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {"nsref": "bad:fn", "gref": "nonexistent.module:fn"},
            ],
        }
    )
    errors = validate_config(config)
    assert len(errors) > 0
    assert "nonexistent" in errors[0].lower() or "import" in errors[0].lower()


def test_validate_config_dag_nsref_not_in_config():
    from lythonic.compose.namespace_config import NamespaceConfig, validate_config

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {
                    "nsref": "pipelines:pipe",
                    "dag": {
                        "nodes": [
                            {"label": "step", "nsref": "missing:fn"},
                        ]
                    },
                },
            ],
        }
    )
    errors = validate_config(config)
    assert len(errors) > 0
    assert "missing:fn" in errors[0]


def test_validate_config_duplicate_nsref():
    from lythonic.compose.namespace_config import NamespaceConfig, validate_config

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
                {"nsref": "math:double", "gref": "tests.test_namespace_config:_another_plain_fn"},
            ],
        }
    )
    errors = validate_config(config)
    assert len(errors) > 0
    assert "duplicate" in errors[0].lower()


def test_validate_config_duplicate_dag_label():
    from lythonic.compose.namespace_config import NamespaceConfig, validate_config

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
                {
                    "nsref": "pipelines:pipe",
                    "dag": {
                        "nodes": [
                            {"label": "step", "nsref": "math:double"},
                            {"label": "step", "nsref": "math:double"},
                        ]
                    },
                },
            ],
        }
    )
    errors = validate_config(config)
    assert len(errors) > 0
    assert "step" in errors[0]


def test_dump_namespace_round_trip():
    from lythonic.compose.namespace_config import (
        NamespaceConfig,
        StorageConfig,
        dump_namespace,
        load_namespace,
    )

    config = NamespaceConfig.model_validate(
        {
            "storage": {"cache_db": "cache.db"},
            "entries": [
                {"nsref": "math:double", "gref": "tests.test_namespace_config:_plain_fn"},
                {
                    "nsref": "market:fetch",
                    "gref": "tests.test_namespace_config:_cached_fn",
                    "cache": {"min_ttl": 1.0, "max_ttl": 2.0},
                },
            ],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        ns = load_namespace(config, Path(tmp))
        dumped = dump_namespace(ns, storage=StorageConfig(cache_db="cache.db"))

        assert len(dumped.entries) == 2
        # Sorted by nsref
        assert dumped.entries[0].nsref == "market:fetch"
        assert dumped.entries[1].nsref == "math:double"
        # Cache metadata preserved
        assert dumped.entries[0].cache is not None
        assert dumped.entries[0].cache.min_ttl == 1.0


def test_dump_namespace_serialization_order():
    from lythonic.compose.namespace import Namespace
    from lythonic.compose.namespace_config import dump_namespace

    ns = Namespace()
    ns.register(this_module._plain_fn, nsref="z:last")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_plain_fn, nsref="a:first")  # pyright: ignore[reportPrivateUsage]

    dumped = dump_namespace(ns)
    assert dumped.entries[0].nsref == "a:first"
    assert dumped.entries[1].nsref == "z:last"
