# Cache Prohibit Direct Call Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let cached methods detect and refuse direct invocation outside the cache wrapper.

**Architecture:** A `ContextVar[bool]` set by both sync and async cache wrappers before calling `method.o(...)`. `CacheProhibitDirectCall.require()` checks it and raises if not inside a wrapper.

**Tech Stack:** Python `contextvars`

**Spec:** `docs/superpowers/specs/2026-04-08-cache-prohibit-direct-call-design.md`

---

### Task 1: Add ContextVar, update CacheProhibitDirectCall, wrap call sites, test

**Files:**
- Modify: `src/lythonic/compose/cached.py`
- Modify: `tests/test_cached.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_cached.py`:

```python
def test_require_cache_context_raises_outside_wrapper():
    """CacheProhibitDirectCall.require() raises when called directly."""
    from lythonic.compose.cached import CacheProhibitDirectCall

    try:
        CacheProhibitDirectCall.require()
        raise AssertionError("Expected CacheProhibitDirectCall")
    except CacheProhibitDirectCall:
        pass


def _guarded_sync(ticker: str) -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
    """Sync method that refuses direct calls."""
    from lythonic.compose.cached import CacheProhibitDirectCall

    CacheProhibitDirectCall.require()
    return {"ticker": ticker, "price": "100"}


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cached.py::test_require_cache_context_raises_outside_wrapper tests/test_cached.py::test_require_cache_context_passes_through_wrapper tests/test_cached.py::test_guarded_method_fails_when_called_directly -v`
Expected: first and third PASS (require() doesn't exist yet so ImportError, or if it does exist with `pass` body it won't raise). Second FAIL.

- [ ] **Step 3: Add `ContextVar` import and variable**

In `src/lythonic/compose/cached.py`, add `from contextvars import ContextVar` to the imports (after `import typing`, line 69).

After `DAYS_TO_SECONDS = 86400.0` (line 85), add:

```python
_in_cache_wrapper: ContextVar[bool] = ContextVar("_in_cache_wrapper", default=False)
```

- [ ] **Step 4: Update `CacheProhibitDirectCall`**

Replace the `CacheProhibitDirectCall` class (lines 87-92) with:

```python
class CacheProhibitDirectCall(Exception):
    """
    Raised when a cached method is called directly instead of through
    the cache wrapper. Call `CacheProhibitDirectCall.require()` from
    a method body to enforce this.
    """

    @staticmethod
    def require() -> None:
        """Raise if not inside a cache wrapper."""
        if not _in_cache_wrapper.get():
            raise CacheProhibitDirectCall(
                "Method must be called through cache wrapper"
            )
```

- [ ] **Step 5: Wrap sync wrapper call sites**

In `_build_sync_wrapper`, there are two `method.o(**kwargs)` calls.

Replace line 301 (`result = method.o(**kwargs)`) and its surrounding try block (lines 300-311):

```python
                    try:
                        token = _in_cache_wrapper.set(True)
                        try:
                            result = method.o(**kwargs)
                        finally:
                            _in_cache_wrapper.reset(token)
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
```

Replace line 319 (`result = method.o(**kwargs)`) and the lines through 322:

```python
            # Cache miss or expired past max_ttl (no pushback)
            token = _in_cache_wrapper.set(True)
            try:
                result = method.o(**kwargs)
            finally:
                _in_cache_wrapper.reset(token)
            result_json = _serialize_return_value(result, return_type)
            _cache_upsert(conn, table_name, method, kwargs, result_json, time.time())
            return result
```

- [ ] **Step 6: Wrap async wrapper call sites**

In `_build_async_wrapper`, same pattern but with `await`.

Replace line 357 (`result = await method.o(**kwargs)`) and its surrounding try block (lines 356-367):

```python
                    try:
                        token = _in_cache_wrapper.set(True)
                        try:
                            result = await method.o(**kwargs)
                        finally:
                            _in_cache_wrapper.reset(token)
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
```

Replace lines 375-379 (the TODO comment and `result = await method.o(**kwargs)` block):

```python
            # Cache miss or expired past max_ttl (no pushback)
            token = _in_cache_wrapper.set(True)
            try:
                result = await method.o(**kwargs)
            finally:
                _in_cache_wrapper.reset(token)
            result_json = _serialize_return_value(result, return_type)
            _cache_upsert(conn, table_name, method, kwargs, result_json, time.time())
            return result
```

- [ ] **Step 7: Run the new tests**

Run: `uv run pytest tests/test_cached.py::test_require_cache_context_raises_outside_wrapper tests/test_cached.py::test_require_cache_context_passes_through_wrapper tests/test_cached.py::test_guarded_method_fails_when_called_directly -v`
Expected: all 3 PASS

- [ ] **Step 8: Run full test suite and lint**

Run: `make test && make lint`
Expected: all tests PASS, zero lint warnings/errors

- [ ] **Step 9: Commit**

```bash
git add src/lythonic/compose/cached.py tests/test_cached.py
git commit -m "feat: add CacheProhibitDirectCall.require() with ContextVar guard"
```
