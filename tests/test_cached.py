from __future__ import annotations

import sqlite3
import tempfile
import time
from contextlib import closing
from pathlib import Path
from typing import Any

import tests.test_cached as this_module


def _ns_test_ok() -> str:  # pyright: ignore[reportUnusedFunction]
    return "ok"


def _ns_test_data() -> str:  # pyright: ignore[reportUnusedFunction]
    return "data"


def test_namespace_nested_access():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._ns_test_ok, nsref="market:fetch_prices")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._ns_test_data, nsref="get_data")  # pyright: ignore[reportPrivateUsage]

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
    from lythonic.compose.cached import register_cached_callable
    from lythonic.compose.namespace import Namespace

    this_module._fake_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        register_cached_callable(
            ns, "tests.test_cached:_fake_fetch", "market:fetch", 1.0, 2.0, db_path
        )

        result = ns.market.fetch(ticker="AAPL")  # pyright: ignore
        assert result == {"price": 100.0, "ticker": "AAPL"}
        assert this_module._fake_fetch_count == 1  # pyright: ignore

        # Second call should come from cache
        result2 = ns.market.fetch(ticker="AAPL")  # pyright: ignore
        assert result2 == {"price": 100.0, "ticker": "AAPL"}
        assert this_module._fake_fetch_count == 1  # pyright: ignore


def test_sync_wrapper_expired_refetches():
    """Past max_ttl, the wrapper must re-fetch."""
    from lythonic.compose.cached import register_cached_callable
    from lythonic.compose.namespace import Namespace

    this_module._fake_fetch2_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        register_cached_callable(
            ns, "tests.test_cached:_fake_fetch2", "fetch2", 0.0001, 0.0002, db_path
        )

        ns.fetch2(ticker="X")  # pyright: ignore
        assert this_module._fake_fetch2_count == 1  # pyright: ignore

        # Backdate fetched_at to simulate expiry
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.execute(
                "UPDATE fetch2 SET fetched_at = ? WHERE ticker = ?",
                (time.time() - 86400 * 1, "X"),
            )
            conn.commit()

        ns.fetch2(ticker="X")  # pyright: ignore
        assert this_module._fake_fetch2_count == 2  # pyright: ignore


# Referenced via GlobalRef
async def _fake_async_fetch(ticker: str) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction]
    this_module._fake_async_count += 1  # pyright: ignore
    return {"price": 200.0, "ticker": ticker}


_fake_async_count = 0


async def test_async_wrapper_miss_fetches_and_caches():
    from lythonic.compose.cached import register_cached_callable
    from lythonic.compose.namespace import Namespace

    this_module._fake_async_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        register_cached_callable(
            ns, "tests.test_cached:_fake_async_fetch", "async_market:fetch", 1.0, 2.0, db_path
        )

        result = await ns.async_market.fetch(ticker="GOOG")  # pyright: ignore
        assert result == {"price": 200.0, "ticker": "GOOG"}
        assert this_module._fake_async_count == 1  # pyright: ignore

        result2 = await ns.async_market.fetch(ticker="GOOG")  # pyright: ignore
        assert result2 == {"price": 200.0, "ticker": "GOOG"}
        assert this_module._fake_async_count == 1  # pyright: ignore


# Task 8: Pydantic BaseModel return type

from pydantic import BaseModel as PydanticBaseModel


class PriceResult(PydanticBaseModel):
    ticker: str
    price: float


# Referenced via GlobalRef
def _fetch_typed(ticker: str) -> PriceResult:  # pyright: ignore[reportUnusedFunction]
    return PriceResult(ticker=ticker, price=42.0)


def test_pydantic_return_type_cached():
    from lythonic.compose.cached import register_cached_callable
    from lythonic.compose.namespace import Namespace

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        register_cached_callable(
            ns, "tests.test_cached:_fetch_typed", "typed_fetch", 1.0, 2.0, db_path
        )

        result = ns.typed_fetch(ticker="MSFT")  # pyright: ignore
        assert isinstance(result, PriceResult)
        assert result.ticker == "MSFT"
        assert result.price == 42.0

        result2 = ns.typed_fetch(ticker="MSFT")  # pyright: ignore
        assert isinstance(result2, PriceResult)
        assert result2.ticker == "MSFT"


# Task 10: Probabilistic refresh


# Referenced via GlobalRef
def _prob_fetch(key: str) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    this_module._prob_fetch_count += 1  # pyright: ignore
    return {"v": this_module._prob_fetch_count}  # pyright: ignore


_prob_fetch_count = 0


def test_probabilistic_refresh_between_ttls():
    """Between min_ttl and max_ttl, refresh probability increases with age."""
    from lythonic.compose.cached import register_cached_callable
    from lythonic.compose.namespace import Namespace

    this_module._prob_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        register_cached_callable(ns, "tests.test_cached:_prob_fetch", "prob", 1.0, 3.0, db_path)

        ns.prob(key="A")  # pyright: ignore
        assert this_module._prob_fetch_count == 1  # pyright: ignore

        # Set fetched_at to almost max_ttl ago (p ~= 1, almost certain refresh)
        almost_max_secs = 2.99 * 86400
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.execute(
                "UPDATE prob SET fetched_at = ? WHERE key = ?",
                (time.time() - almost_max_secs, "A"),
            )
            conn.commit()

        # With p ~= 0.995, should almost certainly refresh within 20 tries
        refreshed = False
        for _ in range(20):
            before = this_module._prob_fetch_count  # pyright: ignore
            ns.prob(key="A")  # pyright: ignore
            if this_module._prob_fetch_count > before:  # pyright: ignore
                refreshed = True
                break
        assert refreshed


# Task 11: Default namespace path


# Referenced via GlobalRef
def _my_download(tag: str) -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
    return {"tag": tag}


def test_default_namespace_path_uses_function_name():
    """When namespace_path is None, uses the original function name at root."""
    from lythonic.compose.cached import register_cached_callable
    from lythonic.compose.namespace import Namespace

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        register_cached_callable(
            ns, "tests.test_cached:_my_download", "_my_download", 1.0, 2.0, db_path
        )

        result = ns._my_download(tag="hello")  # pyright: ignore
        assert result == {"tag": "hello"}


def test_pushback_exception_has_fields():
    from lythonic.compose.cached import CacheRefreshPushback

    ex = CacheRefreshPushback(days=1.5, namespace_prefix="market")
    assert ex.days == 1.5
    assert ex.namespace_prefix == "market"

    ex2 = CacheRefreshPushback(days=0.5)
    assert ex2.days == 0.5
    assert ex2.namespace_prefix is None


def test_suppressed_exception_has_fields():
    from lythonic.compose.cached import CacheRefreshSuppressed

    ex = CacheRefreshSuppressed(namespace_path="market.fetch", suppressed_until=1000.0)
    assert ex.namespace_path == "market.fetch"
    assert ex.suppressed_until == 1000.0


def test_pushback_set_and_check():
    """_pushback_set writes a row; _pushback_check matches by prefix."""
    from lythonic.compose.cached import (
        _pushback_check,  # pyright: ignore[reportPrivateUsage]
        _pushback_set,  # pyright: ignore[reportPrivateUsage]
    )

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _pushback "
                "(namespace_prefix TEXT NOT NULL, suppressed_until REAL NOT NULL)"
            )
            now = time.time()

            _pushback_set(conn, "market", now + 86400)

            # Exact match
            assert _pushback_check(conn, "market") is not None
            # Prefix match with dot boundary
            assert _pushback_check(conn, "market.fetch_prices") is not None
            # No match
            assert _pushback_check(conn, "weather.forecast") is None
            # "market" should NOT match "marketplace"
            assert _pushback_check(conn, "marketplace.something") is None


def test_pushback_replacement():
    """A new _pushback_set replaces the previous row (single-row table)."""
    from lythonic.compose.cached import (
        _pushback_check,  # pyright: ignore[reportPrivateUsage]
        _pushback_set,  # pyright: ignore[reportPrivateUsage]
    )

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _pushback "
                "(namespace_prefix TEXT NOT NULL, suppressed_until REAL NOT NULL)"
            )
            now = time.time()

            _pushback_set(conn, "market", now + 86400)
            assert _pushback_check(conn, "market.fetch") is not None

            # Replace with a different prefix
            _pushback_set(conn, "weather", now + 86400)
            assert _pushback_check(conn, "market.fetch") is None
            assert _pushback_check(conn, "weather.forecast") is not None

            row_count = conn.execute("SELECT COUNT(*) FROM _pushback").fetchone()[0]
            assert row_count == 1


def test_pushback_expired_not_matched():
    """Expired pushback entries are cleaned up and not matched."""
    from lythonic.compose.cached import (
        _pushback_check,  # pyright: ignore[reportPrivateUsage]
        _pushback_set,  # pyright: ignore[reportPrivateUsage]
    )

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _pushback "
                "(namespace_prefix TEXT NOT NULL, suppressed_until REAL NOT NULL)"
            )

            _pushback_set(conn, "market", time.time() - 1.0)
            assert _pushback_check(conn, "market.fetch") is None


def test_pushback_table_created_on_register():
    """register_cached_callable creates the _pushback table."""
    from lythonic.compose.cached import register_cached_callable
    from lythonic.compose.namespace import Namespace

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        register_cached_callable(
            ns, "tests.test_cached:_fake_fetch", "market:fetch", 1.0, 2.0, db_path
        )

        with closing(sqlite3.connect(str(db_path))) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM _pushback")
            assert cursor.fetchone()[0] == 0


# Referenced via GlobalRef
def _pushback_fetch(ticker: str) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    this_module._pushback_fetch_count += 1  # pyright: ignore
    return {"price": float(this_module._pushback_fetch_count)}  # pyright: ignore


_pushback_fetch_count = 0


def test_pushback_suppresses_probabilistic_refresh():
    """When pushback is active, probabilistic refresh is skipped and stale data returned."""
    from lythonic.compose.cached import (
        _pushback_set,  # pyright: ignore[reportPrivateUsage]
        register_cached_callable,
    )
    from lythonic.compose.namespace import Namespace

    this_module._pushback_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        register_cached_callable(
            ns, "tests.test_cached:_pushback_fetch", "market:pushback_fetch", 1.0, 3.0, db_path
        )

        # Initial fetch to populate cache
        result = ns.market.pushback_fetch(ticker="AAPL")  # pyright: ignore
        assert this_module._pushback_fetch_count == 1  # pyright: ignore
        assert result == {"price": 1.0}

        # Backdate to middle of probabilistic window (2 days old, p=0.5)
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.execute(
                "UPDATE market__pushback_fetch SET fetched_at = ? WHERE ticker = ?",
                (time.time() - 86400 * 2, "AAPL"),
            )
            conn.commit()

        # Set pushback on "market" prefix
        with closing(sqlite3.connect(str(db_path))) as conn:
            _pushback_set(conn, "market", time.time() + 86400)

        # Call many times — method should never be called due to pushback
        for _ in range(50):
            result = ns.market.pushback_fetch(ticker="AAPL")  # pyright: ignore
        assert this_module._pushback_fetch_count == 1  # pyright: ignore
        assert result == {"price": 1.0}


# Referenced via GlobalRef
async def _async_pushback_fetch(ticker: str) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    this_module._async_pushback_fetch_count += 1  # pyright: ignore
    return {"price": float(this_module._async_pushback_fetch_count)}  # pyright: ignore


_async_pushback_fetch_count = 0


async def test_async_pushback_suppresses_probabilistic_refresh():
    """Async wrapper: pushback suppresses probabilistic refresh."""
    from lythonic.compose.cached import (
        _pushback_set,  # pyright: ignore[reportPrivateUsage]
        register_cached_callable,
    )
    from lythonic.compose.namespace import Namespace

    this_module._async_pushback_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        register_cached_callable(
            ns,
            "tests.test_cached:_async_pushback_fetch",
            "async_market:pushback_fetch",
            1.0,
            3.0,
            db_path,
        )

        result = await ns.async_market.pushback_fetch(ticker="GOOG")  # pyright: ignore
        assert this_module._async_pushback_fetch_count == 1  # pyright: ignore

        # Backdate to probabilistic window
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.execute(
                "UPDATE async_market__pushback_fetch SET fetched_at = ? WHERE ticker = ?",
                (time.time() - 86400 * 2, "GOOG"),
            )
            conn.commit()

        with closing(sqlite3.connect(str(db_path))) as conn:
            _pushback_set(conn, "async_market", time.time() + 86400)

        for _ in range(50):
            result = await ns.async_market.pushback_fetch(ticker="GOOG")  # pyright: ignore
        assert this_module._async_pushback_fetch_count == 1  # pyright: ignore
        assert result == {"price": 1.0}


from lythonic.compose.cached import CacheRefreshPushback


# Referenced via GlobalRef
def _rate_limited_fetch(ticker: str) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction, reportUnusedParameter]
    this_module._rate_limited_count += 1  # pyright: ignore
    if this_module._rate_limited_count > 1:  # pyright: ignore
        raise CacheRefreshPushback(days=1.0)
    return {"price": 50.0}


_rate_limited_count = 0


def test_pushback_recorded_on_exception():
    """When a method raises CacheRefreshPushback, pushback is recorded and stale returned."""
    from lythonic.compose.cached import (
        _pushback_check,  # pyright: ignore[reportPrivateUsage]
        register_cached_callable,
    )
    from lythonic.compose.namespace import Namespace

    this_module._rate_limited_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        register_cached_callable(
            ns, "tests.test_cached:_rate_limited_fetch", "api:rate_limited", 1.0, 3.0, db_path
        )

        # First call succeeds
        result = ns.api.rate_limited(ticker="X")  # pyright: ignore
        assert result == {"price": 50.0}
        assert this_module._rate_limited_count == 1  # pyright: ignore

        # Backdate to probabilistic window with p ~= 1 (near-certain refresh)
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.execute(
                "UPDATE api__rate_limited SET fetched_at = ? WHERE ticker = ?",
                (time.time() - 86400 * 2.99, "X"),
            )
            conn.commit()

        # Next call triggers refresh, method raises CacheRefreshPushback.
        # Should get stale data back.
        result = ns.api.rate_limited(ticker="X")  # pyright: ignore
        assert result == {"price": 50.0}
        assert this_module._rate_limited_count == 2  # pyright: ignore

        # Pushback should now be recorded
        with closing(sqlite3.connect(str(db_path))) as conn:
            assert _pushback_check(conn, "api.rate_limited") is not None


def test_past_max_ttl_with_pushback_raises_suppressed():
    """Past max_ttl with active pushback raises CacheRefreshSuppressed."""
    import pytest

    from lythonic.compose.cached import (
        CacheRefreshSuppressed,
        _pushback_set,  # pyright: ignore[reportPrivateUsage]
        register_cached_callable,
    )
    from lythonic.compose.namespace import Namespace

    this_module._pushback_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        register_cached_callable(
            ns, "tests.test_cached:_pushback_fetch", "market:pushback_fetch", 1.0, 3.0, db_path
        )

        # Populate cache
        ns.market.pushback_fetch(ticker="AAPL")  # pyright: ignore
        assert this_module._pushback_fetch_count == 1  # pyright: ignore

        # Backdate past max_ttl
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.execute(
                "UPDATE market__pushback_fetch SET fetched_at = ? WHERE ticker = ?",
                (time.time() - 86400 * 4, "AAPL"),
            )
            conn.commit()

        # Set pushback
        with closing(sqlite3.connect(str(db_path))) as conn:
            _pushback_set(conn, "market", time.time() + 86400)

        with pytest.raises(CacheRefreshSuppressed) as exc_info:
            ns.market.pushback_fetch(ticker="AAPL")  # pyright: ignore

        assert exc_info.value.namespace_path == "market.pushback_fetch"
        assert exc_info.value.suppressed_until > time.time()
        # Method should not have been called again
        assert this_module._pushback_fetch_count == 1  # pyright: ignore


def test_cache_miss_ignores_pushback():
    """On cache miss, method is called even if pushback is active."""
    from lythonic.compose.cached import (
        _pushback_set,  # pyright: ignore[reportPrivateUsage]
        register_cached_callable,
    )
    from lythonic.compose.namespace import Namespace

    this_module._pushback_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        register_cached_callable(
            ns, "tests.test_cached:_pushback_fetch", "market:pushback_fetch", 1.0, 3.0, db_path
        )

        # Set pushback before any cache entry exists
        with closing(sqlite3.connect(str(db_path))) as conn:
            _pushback_set(conn, "market", time.time() + 86400)

        # Cache miss — should call method despite pushback
        result = ns.market.pushback_fetch(ticker="NEW")  # pyright: ignore
        assert this_module._pushback_fetch_count == 1  # pyright: ignore
        assert result["price"] == 1.0


def test_default_scope_uses_method_namespace_path():
    """CacheRefreshPushback with no prefix scopes to the raising method only."""
    from lythonic.compose.cached import (
        _pushback_check,  # pyright: ignore[reportPrivateUsage]
        register_cached_callable,
    )
    from lythonic.compose.namespace import Namespace

    this_module._rate_limited_count = 0  # pyright: ignore
    this_module._pushback_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        ns = Namespace()
        db_path = Path(tmp) / "cache.db"
        register_cached_callable(
            ns, "tests.test_cached:_rate_limited_fetch", "api:rate_limited", 1.0, 3.0, db_path
        )
        register_cached_callable(
            ns, "tests.test_cached:_pushback_fetch", "api:other", 1.0, 3.0, db_path
        )

        # Populate both caches
        ns.api.rate_limited(ticker="X")  # pyright: ignore
        ns.api.other(ticker="Y")  # pyright: ignore

        # Backdate both to probabilistic window (p ~= 1)
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.execute(
                "UPDATE api__rate_limited SET fetched_at = ?",
                (time.time() - 86400 * 2.99,),
            )
            conn.execute(
                "UPDATE api__other SET fetched_at = ?",
                (time.time() - 86400 * 2.99,),
            )
            conn.commit()

        # Trigger rate_limited to raise CacheRefreshPushback(days=1) with no prefix
        ns.api.rate_limited(ticker="X")  # pyright: ignore

        # Pushback should be scoped to "api.rate_limited" only
        with closing(sqlite3.connect(str(db_path))) as conn:
            assert _pushback_check(conn, "api.rate_limited") is not None
            assert _pushback_check(conn, "api.other") is None


def _guarded_sync(ticker: str) -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
    """Sync method that refuses direct calls."""
    from lythonic.compose.cached import CacheProhibitDirectCall

    CacheProhibitDirectCall.require()
    return {"ticker": ticker, "price": "100"}


def test_require_cache_context_raises_outside_wrapper():
    """CacheProhibitDirectCall.require() raises when called directly."""
    from lythonic.compose.cached import CacheProhibitDirectCall

    try:
        CacheProhibitDirectCall.require()
        raise AssertionError("Expected CacheProhibitDirectCall")
    except CacheProhibitDirectCall:
        pass


def test_require_cache_context_passes_through_wrapper():
    """CacheProhibitDirectCall.require() does not raise inside cache wrapper."""
    from lythonic.compose.cached import register_cached_callable
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    with tempfile.TemporaryDirectory() as tmp:
        register_cached_callable(
            ns,
            gref="tests.test_cached:_guarded_sync",
            nsref="t:guarded",
            min_ttl=1.0,
            max_ttl=2.0,
            db_path=Path(tmp) / "cache.db",
        )
        result = ns.t.guarded(ticker="AAPL")  # pyright: ignore
        assert result == {"ticker": "AAPL", "price": "100"}


def test_guarded_method_fails_when_called_directly():
    """A guarded method raises CacheProhibitDirectCall when called without wrapper."""
    from lythonic.compose.cached import CacheProhibitDirectCall

    try:
        _guarded_sync(ticker="AAPL")
        raise AssertionError("Expected CacheProhibitDirectCall")
    except CacheProhibitDirectCall:
        pass
