from __future__ import annotations

import sqlite3
import tempfile
import time
from pathlib import Path
from textwrap import dedent
from typing import Any

import tests.test_cached as this_module


def test_cache_config_from_yaml():
    from lythonic.compose.cached import CacheConfig

    yaml_str = dedent("""
        rules:
          - gref: "json:dumps"
            namespace_path: "util.dumps"
            min_ttl: 0.5
            max_ttl: 2.0
          - gref: "json:loads"
            min_ttl: 0.25
            max_ttl: 1.0
    """).strip()

    import yaml

    data = yaml.safe_load(yaml_str)
    config = CacheConfig.model_validate(data)
    assert len(config.rules) == 2
    assert config.namespace == "lythonic.compose.cached"
    assert config.rules[0].namespace_path == "util.dumps"
    assert config.rules[0].min_ttl == 0.5
    assert config.rules[0].max_ttl == 2.0
    assert config.rules[1].namespace_path is None
    assert str(config.rules[1].gref) == "json:loads"


def test_namespace_nested_access():
    from lythonic.compose.cached import Namespace

    ns = Namespace()
    ns.install("market.fetch_prices", lambda: "ok")
    ns.install("get_data", lambda: "data")

    assert ns.market.fetch_prices() == "ok"  # pyright: ignore
    assert ns.get_data() == "data"  # pyright: ignore


def test_namespace_path_generates_table_name():
    from lythonic.compose.cached import table_name_from_path

    assert table_name_from_path("market.fetch_prices") == "market__fetch_prices"
    assert table_name_from_path("get_data") == "get_data"
    assert table_name_from_path("a.b.c") == "a__b__c"


def test_generate_ddl_single_param():
    from lythonic.compose import Method
    from lythonic.compose.cached import generate_cache_table_ddl

    def fetch(ticker: str) -> dict[str, float]:  # pyright: ignore[reportUnusedParameter]
        return {}

    ddl = generate_cache_table_ddl("market__fetch_prices", Method(fetch))
    assert "CREATE TABLE IF NOT EXISTS market__fetch_prices" in ddl
    assert "ticker TEXT NOT NULL" in ddl
    assert "value_json TEXT NOT NULL" in ddl
    assert "fetched_at REAL NOT NULL" in ddl
    assert "PRIMARY KEY (ticker)" in ddl


def test_generate_ddl_multiple_params():
    from lythonic.compose import Method
    from lythonic.compose.cached import generate_cache_table_ddl

    def fetch(from_currency: str, to_currency: str) -> dict[str, float]:  # pyright: ignore[reportUnusedParameter]
        return {}

    ddl = generate_cache_table_ddl("get_exchange_rate", Method(fetch))
    assert "PRIMARY KEY (from_currency, to_currency)" in ddl


# Referenced via GlobalRef in tests below
def _fake_fetch(ticker: str) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction]
    this_module._fake_fetch_count += 1  # pyright: ignore
    return {"price": 100.0, "ticker": ticker}


_fake_fetch_count = 0


# Referenced via GlobalRef in tests below
def _fake_fetch2(ticker: str) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    this_module._fake_fetch2_count += 1  # pyright: ignore
    return {"price": float(this_module._fake_fetch2_count)}  # pyright: ignore


_fake_fetch2_count = 0


def test_sync_wrapper_miss_fetches_and_caches():
    """On cache miss, the wrapper calls the original and stores the result."""
    from lythonic.compose.cached import CacheConfig, CacheRegistry, CacheRule

    this_module._fake_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref="tests.test_cached:_fake_fetch",  # pyright: ignore
                    namespace_path="market.fetch",
                    min_ttl=1.0,
                    max_ttl=2.0,
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))

        result = registry.cached.market.fetch(ticker="AAPL")  # pyright: ignore
        assert result == {"price": 100.0, "ticker": "AAPL"}
        assert this_module._fake_fetch_count == 1  # pyright: ignore

        # Second call should come from cache
        result2 = registry.cached.market.fetch(ticker="AAPL")  # pyright: ignore
        assert result2 == {"price": 100.0, "ticker": "AAPL"}
        assert this_module._fake_fetch_count == 1  # pyright: ignore


def test_sync_wrapper_expired_refetches():
    """Past max_ttl, the wrapper must re-fetch."""
    from lythonic.compose.cached import CacheConfig, CacheRegistry, CacheRule

    this_module._fake_fetch2_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref="tests.test_cached:_fake_fetch2",  # pyright: ignore
                    namespace_path="fetch2",
                    min_ttl=0.0001,
                    max_ttl=0.0002,
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))

        registry.cached.fetch2(ticker="X")  # pyright: ignore
        assert this_module._fake_fetch2_count == 1  # pyright: ignore

        # Backdate fetched_at to simulate expiry
        db_path = Path(tmp) / "cache.db"
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "UPDATE fetch2 SET fetched_at = ? WHERE ticker = ?",
                (time.time() - 86400 * 1, "X"),
            )
            conn.commit()

        registry.cached.fetch2(ticker="X")  # pyright: ignore
        assert this_module._fake_fetch2_count == 2  # pyright: ignore


# Referenced via GlobalRef
async def _fake_async_fetch(ticker: str) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction]
    this_module._fake_async_count += 1  # pyright: ignore
    return {"price": 200.0, "ticker": ticker}


_fake_async_count = 0


async def test_async_wrapper_miss_fetches_and_caches():
    from lythonic.compose.cached import CacheConfig, CacheRegistry, CacheRule

    this_module._fake_async_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref="tests.test_cached:_fake_async_fetch",  # pyright: ignore
                    namespace_path="async_market.fetch",
                    min_ttl=1.0,
                    max_ttl=2.0,
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))

        result = await registry.cached.async_market.fetch(ticker="GOOG")  # pyright: ignore
        assert result == {"price": 200.0, "ticker": "GOOG"}
        assert this_module._fake_async_count == 1  # pyright: ignore

        result2 = await registry.cached.async_market.fetch(ticker="GOOG")  # pyright: ignore
        assert result2 == {"price": 200.0, "ticker": "GOOG"}
        assert this_module._fake_async_count == 1  # pyright: ignore
