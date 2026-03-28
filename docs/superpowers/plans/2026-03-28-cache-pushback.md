# Cache Refresh Pushback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pushback mechanism to `lythonic.compose.cached` that suppresses probabilistic cache refreshes when downstream APIs are rate-limiting.

**Architecture:** A special exception (`CacheRefreshPushback`) raised by user code triggers a time-limited suppression stored in a `_pushback` SQLite table (single row). During the probabilistic refresh window, suppressed methods return stale data. Past `max_ttl`, a `CacheRefreshSuppressed` exception is raised to the caller.

**Tech Stack:** Python 3.11+, SQLite, Pydantic, pytest

**Spec:** `docs/superpowers/specs/2026-03-28-cache-pushback-design.md`

---

### Task 1: Add Exception Classes

**Files:**
- Modify: `src/lythonic/compose/cached.py:80` (after `DAYS_TO_SECONDS`)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cached.py` at the end of the file:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cached.py::test_pushback_exception_has_fields tests/test_cached.py::test_suppressed_exception_has_fields -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write minimal implementation**

Add after line 80 (`DAYS_TO_SECONDS = 86400.0`) in `src/lythonic/compose/cached.py`:

```python
class CacheRefreshPushback(Exception):
    """
    Raise from a cached method to suppress probabilistic refreshes.
    Defaults to suppressing only the raising method; set `namespace_prefix`
    to suppress a group of methods.
    """

    def __init__(self, days: float, namespace_prefix: str | None = None):
        super().__init__(f"Cache refresh pushback for {days} days")
        self.days = days
        self.namespace_prefix = namespace_prefix


class CacheRefreshSuppressed(Exception):
    """
    Raised when a cache entry is past max_ttl but refresh is suppressed
    by an active pushback.
    """

    def __init__(self, namespace_path: str, suppressed_until: float):
        super().__init__(
            f"Cache refresh suppressed for {namespace_path} until {suppressed_until}"
        )
        self.namespace_path = namespace_path
        self.suppressed_until = suppressed_until
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_cached.py::test_pushback_exception_has_fields tests/test_cached.py::test_suppressed_exception_has_fields -v`
Expected: PASS

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/cached.py tests/test_cached.py
git commit -m "feat(cached): add CacheRefreshPushback and CacheRefreshSuppressed exceptions"
```

---

### Task 2: Add Pushback DB Operations

**Files:**
- Modify: `src/lythonic/compose/cached.py` (add functions after exception classes, before `CacheRule`)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cached.py`:

```python
def test_pushback_set_and_check():
    """_pushback_set writes a row; _pushback_check matches by prefix."""
    from lythonic.compose.cached import _pushback_check, _pushback_set

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with sqlite3.connect(str(db_path)) as conn:
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
            # No match — different namespace
            assert _pushback_check(conn, "weather.forecast") is None
            # No match — "market" should NOT match "marketplace"
            assert _pushback_check(conn, "marketplace.something") is None


def test_pushback_replacement():
    """A new _pushback_set replaces the previous row (single-row table)."""
    from lythonic.compose.cached import _pushback_check, _pushback_set

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with sqlite3.connect(str(db_path)) as conn:
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

            # Verify single row
            row_count = conn.execute("SELECT COUNT(*) FROM _pushback").fetchone()[0]
            assert row_count == 1


def test_pushback_expired_not_matched():
    """Expired pushback entries are cleaned up and not matched."""
    from lythonic.compose.cached import _pushback_check, _pushback_set

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _pushback "
                "(namespace_prefix TEXT NOT NULL, suppressed_until REAL NOT NULL)"
            )

            # Set pushback that already expired
            _pushback_set(conn, "market", time.time() - 1.0)
            assert _pushback_check(conn, "market.fetch") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cached.py::test_pushback_set_and_check tests/test_cached.py::test_pushback_replacement tests/test_cached.py::test_pushback_expired_not_matched -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write minimal implementation**

Add in `src/lythonic/compose/cached.py` after the exception classes and before `class CacheRule`:

```python
def _pushback_set(conn: sqlite3.Connection, namespace_prefix: str, suppressed_until: float) -> None:
    """Replace any existing pushback with a new one (single-row table)."""
    cursor = conn.cursor()
    execute_sql(cursor, "DELETE FROM _pushback")
    execute_sql(cursor, "INSERT INTO _pushback (namespace_prefix, suppressed_until) VALUES (?, ?)",
                (namespace_prefix, suppressed_until))
    conn.commit()


def _pushback_check(conn: sqlite3.Connection, namespace_path: str) -> float | None:
    """
    Check if a pushback is active for the given namespace path.
    Returns `suppressed_until` timestamp if suppressed, `None` otherwise.
    Clears expired entries before checking.
    """
    cursor = conn.cursor()
    execute_sql(cursor, "DELETE FROM _pushback WHERE suppressed_until <= ?", (time.time(),))
    execute_sql(cursor, "SELECT namespace_prefix, suppressed_until FROM _pushback")
    row = cursor.fetchone()
    if row is None:
        return None
    prefix: str = row[0]
    suppressed_until: float = row[1]
    if namespace_path == prefix or namespace_path.startswith(prefix + "."):
        return suppressed_until
    return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cached.py::test_pushback_set_and_check tests/test_cached.py::test_pushback_replacement tests/test_cached.py::test_pushback_expired_not_matched -v`
Expected: PASS

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/cached.py tests/test_cached.py
git commit -m "feat(cached): add _pushback_set and _pushback_check DB operations"
```

---

### Task 3: Create Pushback Table in CacheRegistry

**Files:**
- Modify: `src/lythonic/compose/cached.py:326-337` (`CacheRegistry.__init__`)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cached.py`:

```python
def test_pushback_table_created_on_registry_init():
    """CacheRegistry.__init__ creates the _pushback table."""
    from lythonic.compose.cached import CacheConfig, CacheRegistry, CacheRule

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

        db_path = Path(tmp) / "cache.db"
        with sqlite3.connect(str(db_path)) as conn:
            # Table should exist and be queryable
            cursor = conn.execute("SELECT COUNT(*) FROM _pushback")
            assert cursor.fetchone()[0] == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cached.py::test_pushback_table_created_on_registry_init -v`
Expected: FAIL with `OperationalError: no such table: _pushback`

- [ ] **Step 3: Write minimal implementation**

In `CacheRegistry.__init__`, add pushback table creation after setting `self.db_path` and before the rule loop. The `__init__` method should become:

```python
def __init__(self, config: CacheConfig, config_dir: Path) -> None:
    self.cached = Namespace()

    if config.cache_db is not None:
        self.db_path = config_dir / config.cache_db
    else:
        self.db_path = config_dir / "cache.db"

    self.db_path.parent.mkdir(parents=True, exist_ok=True)

    with open_sqlite_db(self.db_path) as conn:
        cursor = conn.cursor()
        execute_sql(
            cursor,
            "CREATE TABLE IF NOT EXISTS _pushback "
            "(namespace_prefix TEXT NOT NULL, suppressed_until REAL NOT NULL)",
        )
        conn.commit()

    for rule in config.rules:
        self._register_rule(rule)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_cached.py::test_pushback_table_created_on_registry_init -v`
Expected: PASS

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/cached.py tests/test_cached.py
git commit -m "feat(cached): create _pushback table in CacheRegistry.__init__"
```

---

### Task 4: Modify Sync Wrapper — Pushback Suppresses Probabilistic Refresh

**Files:**
- Modify: `src/lythonic/compose/cached.py` (`_build_sync_wrapper`)
- Test: `tests/test_cached.py`

The wrapper functions currently don't receive `namespace_path`. We need to add it as a parameter.

- [ ] **Step 1: Write the failing test**

Add a new fake function and test to `tests/test_cached.py`:

```python
# Referenced via GlobalRef
def _pushback_fetch(ticker: str) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction]
    this_module._pushback_fetch_count += 1  # pyright: ignore
    return {"price": float(this_module._pushback_fetch_count)}  # pyright: ignore


_pushback_fetch_count = 0


def test_pushback_suppresses_probabilistic_refresh():
    """When pushback is active, probabilistic refresh is skipped and stale data returned."""
    from lythonic.compose.cached import CacheConfig, CacheRegistry, CacheRule, _pushback_set

    this_module._pushback_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref="tests.test_cached:_pushback_fetch",  # pyright: ignore
                    namespace_path="market.pushback_fetch",
                    min_ttl=1.0,
                    max_ttl=3.0,
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))
        db_path = Path(tmp) / "cache.db"

        # Initial fetch to populate cache
        result = registry.cached.market.pushback_fetch(ticker="AAPL")  # pyright: ignore
        assert this_module._pushback_fetch_count == 1  # pyright: ignore
        assert result == {"price": 1.0}

        # Backdate to middle of probabilistic window (2 days old, p=0.5)
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "UPDATE market__pushback_fetch SET fetched_at = ? WHERE ticker = ?",
                (time.time() - 86400 * 2, "AAPL"),
            )
            conn.commit()

        # Set pushback on "market" prefix
        with sqlite3.connect(str(db_path)) as conn:
            _pushback_set(conn, "market", time.time() + 86400)

        # Call many times — method should never be called due to pushback
        for _ in range(50):
            result = registry.cached.market.pushback_fetch(ticker="AAPL")  # pyright: ignore
        assert this_module._pushback_fetch_count == 1  # pyright: ignore
        assert result == {"price": 1.0}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cached.py::test_pushback_suppresses_probabilistic_refresh -v`
Expected: FAIL — the method will be called during probabilistic refresh since pushback isn't checked yet

- [ ] **Step 3: Implement pushback check in sync wrapper**

Modify `_build_sync_wrapper` to accept `namespace_path` and check pushback. The full updated function:

```python
def _build_sync_wrapper(
    method: Method,
    table_name: str,
    db_path: Path,
    min_ttl_seconds: float,
    max_ttl_seconds: float,
    namespace_path: str,
) -> Callable[..., Any]:
    """Build a sync wrapper callable for a cached method."""
    return_type = _resolve_return_type(method)

    def wrapper(**kwargs: Any) -> Any:
        with open_sqlite_db(db_path) as conn:
            cached = _cache_lookup(conn, table_name, method, kwargs)
            now = time.time()

            if cached is not None:
                value_json, fetched_at = cached
                age = now - fetched_at

                if age < min_ttl_seconds:
                    return _deserialize_return_value(value_json, return_type)

                if age < max_ttl_seconds:
                    # Probabilistic refresh: probability increases linearly from 0 to 1
                    p = (age - min_ttl_seconds) / (max_ttl_seconds - min_ttl_seconds)
                    if random.random() >= p:
                        return _deserialize_return_value(value_json, return_type)
                    if _pushback_check(conn, namespace_path):
                        return _deserialize_return_value(value_json, return_type)
                    try:
                        result = method.o(**kwargs)
                        result_json = _serialize_return_value(result, return_type)
                        _cache_upsert(conn, table_name, method, kwargs, result_json, time.time())
                        return result
                    except CacheRefreshPushback as e:
                        prefix = e.namespace_prefix or namespace_path
                        until = time.time() + e.days * DAYS_TO_SECONDS
                        _pushback_set(conn, prefix, until)
                        return _deserialize_return_value(value_json, return_type)
                    except Exception:
                        return _deserialize_return_value(value_json, return_type)

                # Past max_ttl with cached entry
                suppressed_until = _pushback_check(conn, namespace_path)
                if suppressed_until:
                    raise CacheRefreshSuppressed(namespace_path, suppressed_until)

            # Cache miss or expired past max_ttl (no pushback)
            result = method.o(**kwargs)
            result_json = _serialize_return_value(result, return_type)
            _cache_upsert(conn, table_name, method, kwargs, result_json, time.time())
            return result

    return wrapper
```

Also update the call site in `_register_rule` to pass `namespace_path`:

```python
if gref.is_async():
    wrapper = _build_async_wrapper(method, tbl_name, self.db_path, min_ttl_s, max_ttl_s, namespace_path)
else:
    wrapper = _build_sync_wrapper(method, tbl_name, self.db_path, min_ttl_s, max_ttl_s, namespace_path)
```

Note: `_build_async_wrapper` signature is also updated here so it won't break, but its body is changed in Task 5.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_cached.py::test_pushback_suppresses_probabilistic_refresh -v`
Expected: PASS

- [ ] **Step 5: Run all existing tests**

Run: `uv run pytest tests/test_cached.py -v`
Expected: All PASS (no regressions)

- [ ] **Step 6: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/compose/cached.py tests/test_cached.py
git commit -m "feat(cached): add pushback check to sync wrapper"
```

---

### Task 5: Modify Async Wrapper — Same Pushback Logic

**Files:**
- Modify: `src/lythonic/compose/cached.py` (`_build_async_wrapper`)
- Test: `tests/test_cached.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cached.py`:

```python
# Referenced via GlobalRef
async def _async_pushback_fetch(ticker: str) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction]
    this_module._async_pushback_fetch_count += 1  # pyright: ignore
    return {"price": float(this_module._async_pushback_fetch_count)}  # pyright: ignore


_async_pushback_fetch_count = 0


async def test_async_pushback_suppresses_probabilistic_refresh():
    """Async wrapper: pushback suppresses probabilistic refresh."""
    from lythonic.compose.cached import CacheConfig, CacheRegistry, CacheRule, _pushback_set

    this_module._async_pushback_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref="tests.test_cached:_async_pushback_fetch",  # pyright: ignore
                    namespace_path="async_market.pushback_fetch",
                    min_ttl=1.0,
                    max_ttl=3.0,
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))
        db_path = Path(tmp) / "cache.db"

        result = await registry.cached.async_market.pushback_fetch(ticker="GOOG")  # pyright: ignore
        assert this_module._async_pushback_fetch_count == 1  # pyright: ignore

        # Backdate to probabilistic window
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "UPDATE async_market__pushback_fetch SET fetched_at = ? WHERE ticker = ?",
                (time.time() - 86400 * 2, "GOOG"),
            )
            conn.commit()

        with sqlite3.connect(str(db_path)) as conn:
            _pushback_set(conn, "async_market", time.time() + 86400)

        for _ in range(50):
            result = await registry.cached.async_market.pushback_fetch(ticker="GOOG")  # pyright: ignore
        assert this_module._async_pushback_fetch_count == 1  # pyright: ignore
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cached.py::test_async_pushback_suppresses_probabilistic_refresh -v`
Expected: FAIL — async wrapper doesn't check pushback yet

- [ ] **Step 3: Implement pushback check in async wrapper**

Update `_build_async_wrapper` to mirror the sync wrapper changes:

```python
def _build_async_wrapper(
    method: Method,
    table_name: str,
    db_path: Path,
    min_ttl_seconds: float,
    max_ttl_seconds: float,
    namespace_path: str,
) -> Callable[..., Any]:
    """Build an async wrapper callable for a cached method."""
    return_type = _resolve_return_type(method)

    async def wrapper(**kwargs: Any) -> Any:
        with open_sqlite_db(db_path) as conn:
            cached = _cache_lookup(conn, table_name, method, kwargs)
            now = time.time()

            if cached is not None:
                value_json, fetched_at = cached
                age = now - fetched_at

                if age < min_ttl_seconds:
                    return _deserialize_return_value(value_json, return_type)

                if age < max_ttl_seconds:
                    p = (age - min_ttl_seconds) / (max_ttl_seconds - min_ttl_seconds)
                    if random.random() >= p:
                        return _deserialize_return_value(value_json, return_type)
                    if _pushback_check(conn, namespace_path):
                        return _deserialize_return_value(value_json, return_type)
                    try:
                        result = await method.o(**kwargs)
                        result_json = _serialize_return_value(result, return_type)
                        _cache_upsert(conn, table_name, method, kwargs, result_json, time.time())
                        return result
                    except CacheRefreshPushback as e:
                        prefix = e.namespace_prefix or namespace_path
                        until = time.time() + e.days * DAYS_TO_SECONDS
                        _pushback_set(conn, prefix, until)
                        return _deserialize_return_value(value_json, return_type)
                    except Exception:
                        return _deserialize_return_value(value_json, return_type)

                # Past max_ttl with cached entry
                suppressed_until = _pushback_check(conn, namespace_path)
                if suppressed_until:
                    raise CacheRefreshSuppressed(namespace_path, suppressed_until)

            # Cache miss or expired past max_ttl (no pushback)
            result = await method.o(**kwargs)
            result_json = _serialize_return_value(result, return_type)
            _cache_upsert(conn, table_name, method, kwargs, result_json, time.time())
            return result

    return wrapper
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_cached.py::test_async_pushback_suppresses_probabilistic_refresh -v`
Expected: PASS

- [ ] **Step 5: Run all tests**

Run: `uv run pytest tests/test_cached.py -v`
Expected: All PASS

- [ ] **Step 6: Run lint**

Run: `make lint`
Expected: No errors

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/compose/cached.py tests/test_cached.py
git commit -m "feat(cached): add pushback check to async wrapper"
```

---

### Task 6: Test CacheRefreshPushback Recording from Method

**Files:**
- Test: `tests/test_cached.py`

- [ ] **Step 1: Write the test**

Add a fake method that raises `CacheRefreshPushback` and a test:

```python
from lythonic.compose.cached import CacheRefreshPushback


# Referenced via GlobalRef
def _rate_limited_fetch(ticker: str) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction]
    this_module._rate_limited_count += 1  # pyright: ignore
    if this_module._rate_limited_count > 1:  # pyright: ignore
        raise CacheRefreshPushback(days=1.0)
    return {"price": 50.0}


_rate_limited_count = 0


def test_pushback_recorded_on_exception():
    """When a method raises CacheRefreshPushback, pushback is recorded and stale returned."""
    from lythonic.compose.cached import CacheConfig, CacheRegistry, CacheRule, _pushback_check

    this_module._rate_limited_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref="tests.test_cached:_rate_limited_fetch",  # pyright: ignore
                    namespace_path="api.rate_limited",
                    min_ttl=1.0,
                    max_ttl=3.0,
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))
        db_path = Path(tmp) / "cache.db"

        # First call succeeds
        result = registry.cached.api.rate_limited(ticker="X")  # pyright: ignore
        assert result == {"price": 50.0}
        assert this_module._rate_limited_count == 1  # pyright: ignore

        # Backdate to probabilistic window with p=1 (certain refresh)
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "UPDATE api__rate_limited SET fetched_at = ? WHERE ticker = ?",
                (time.time() - 86400 * 2.99, "X"),
            )
            conn.commit()

        # Next call will trigger refresh, method raises CacheRefreshPushback
        # Should get stale data back
        result = registry.cached.api.rate_limited(ticker="X")  # pyright: ignore
        assert result == {"price": 50.0}
        assert this_module._rate_limited_count == 2  # pyright: ignore

        # Pushback should now be recorded
        with sqlite3.connect(str(db_path)) as conn:
            assert _pushback_check(conn, "api.rate_limited") is not None
```

- [ ] **Step 2: Run test**

Run: `uv run pytest tests/test_cached.py::test_pushback_recorded_on_exception -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_cached.py
git commit -m "test(cached): add test for CacheRefreshPushback recording"
```

---

### Task 7: Test Past max_ttl with Active Pushback

**Files:**
- Test: `tests/test_cached.py`

- [ ] **Step 1: Write the test**

```python
def test_past_max_ttl_with_pushback_raises_suppressed():
    """Past max_ttl with active pushback raises CacheRefreshSuppressed."""
    import pytest

    from lythonic.compose.cached import (
        CacheConfig,
        CacheRefreshSuppressed,
        CacheRegistry,
        CacheRule,
        _pushback_set,
    )

    this_module._pushback_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref="tests.test_cached:_pushback_fetch",  # pyright: ignore
                    namespace_path="market.pushback_fetch",
                    min_ttl=1.0,
                    max_ttl=3.0,
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))
        db_path = Path(tmp) / "cache.db"

        # Populate cache
        registry.cached.market.pushback_fetch(ticker="AAPL")  # pyright: ignore
        assert this_module._pushback_fetch_count == 1  # pyright: ignore

        # Backdate past max_ttl
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "UPDATE market__pushback_fetch SET fetched_at = ? WHERE ticker = ?",
                (time.time() - 86400 * 4, "AAPL"),
            )
            conn.commit()

        # Set pushback
        with sqlite3.connect(str(db_path)) as conn:
            _pushback_set(conn, "market", time.time() + 86400)

        with pytest.raises(CacheRefreshSuppressed) as exc_info:
            registry.cached.market.pushback_fetch(ticker="AAPL")  # pyright: ignore

        assert exc_info.value.namespace_path == "market.pushback_fetch"
        assert exc_info.value.suppressed_until > time.time()
        # Method should not have been called again
        assert this_module._pushback_fetch_count == 1  # pyright: ignore
```

- [ ] **Step 2: Run test**

Run: `uv run pytest tests/test_cached.py::test_past_max_ttl_with_pushback_raises_suppressed -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_cached.py
git commit -m "test(cached): add test for CacheRefreshSuppressed on past max_ttl"
```

---

### Task 8: Test Cache Miss Ignores Pushback and Default Scope

**Files:**
- Test: `tests/test_cached.py`

- [ ] **Step 1: Write the tests**

```python
def test_cache_miss_ignores_pushback():
    """On cache miss, method is called even if pushback is active."""
    from lythonic.compose.cached import CacheConfig, CacheRegistry, CacheRule, _pushback_set

    this_module._pushback_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref="tests.test_cached:_pushback_fetch",  # pyright: ignore
                    namespace_path="market.pushback_fetch",
                    min_ttl=1.0,
                    max_ttl=3.0,
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))
        db_path = Path(tmp) / "cache.db"

        # Set pushback before any cache entry exists
        with sqlite3.connect(str(db_path)) as conn:
            _pushback_set(conn, "market", time.time() + 86400)

        # Cache miss — should call method despite pushback
        result = registry.cached.market.pushback_fetch(ticker="NEW")  # pyright: ignore
        assert this_module._pushback_fetch_count == 1  # pyright: ignore
        assert result["price"] == 1.0


def test_default_scope_uses_method_namespace_path():
    """CacheRefreshPushback with no prefix scopes to the raising method only."""
    from lythonic.compose.cached import CacheConfig, CacheRegistry, CacheRule, _pushback_check

    this_module._rate_limited_count = 0  # pyright: ignore
    this_module._pushback_fetch_count = 0  # pyright: ignore

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref="tests.test_cached:_rate_limited_fetch",  # pyright: ignore
                    namespace_path="api.rate_limited",
                    min_ttl=1.0,
                    max_ttl=3.0,
                ),
                CacheRule(
                    gref="tests.test_cached:_pushback_fetch",  # pyright: ignore
                    namespace_path="api.other",
                    min_ttl=1.0,
                    max_ttl=3.0,
                ),
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))
        db_path = Path(tmp) / "cache.db"

        # Populate both caches
        registry.cached.api.rate_limited(ticker="X")  # pyright: ignore
        registry.cached.api.other(ticker="Y")  # pyright: ignore

        # Backdate both to probabilistic window (p ~= 1)
        with sqlite3.connect(str(db_path)) as conn:
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
        registry.cached.api.rate_limited(ticker="X")  # pyright: ignore

        # Pushback should be scoped to "api.rate_limited" only
        with sqlite3.connect(str(db_path)) as conn:
            assert _pushback_check(conn, "api.rate_limited") is not None
            assert _pushback_check(conn, "api.other") is None
```

- [ ] **Step 2: Run tests**

Run: `uv run pytest tests/test_cached.py::test_cache_miss_ignores_pushback tests/test_cached.py::test_default_scope_uses_method_namespace_path -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_cached.py
git commit -m "test(cached): add tests for cache miss ignoring pushback and default scope"
```

---

### Task 9: Update Module Docstring and Final Verification

**Files:**
- Modify: `src/lythonic/compose/cached.py:1-59` (module docstring)

- [ ] **Step 1: Update module docstring**

Add a section to the module docstring, after the existing `## Validation` section and before `## Namespace`:

```python
## Pushback

When a cached method raises `CacheRefreshPushback(days, namespace_prefix)`, all
probabilistic refreshes matching the scope are suppressed for the given duration.
If `namespace_prefix` is omitted, only the raising method is suppressed.

- During the probabilistic window with active pushback: returns stale data.
- Past `max_ttl` with active pushback: raises `CacheRefreshSuppressed`.
- Cache miss: always calls the method regardless of pushback.
```

- [ ] **Step 2: Run full test suite**

Run: `make lint && make test`
Expected: All pass, zero lint errors

- [ ] **Step 3: Commit**

```bash
git add src/lythonic/compose/cached.py
git commit -m "docs(cached): document pushback mechanism in module docstring"
```
