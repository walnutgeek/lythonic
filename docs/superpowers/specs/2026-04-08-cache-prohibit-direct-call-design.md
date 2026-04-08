# Cache Prohibit Direct Call

## Problem

Cached methods may perform expensive operations (API calls, heavy computation)
that should only happen through the cache wrapper, which provides TTL-based
freshness, probabilistic refresh, and pushback. If a method is called directly
(bypassing the cache), it wastes resources and defeats the caching strategy.
Methods need a way to detect and refuse direct invocation.

## Design

### ContextVar

A `ContextVar[bool]` named `_in_cache_wrapper` in `cached.py`, default `False`.
Both `_build_sync_wrapper` and `_build_async_wrapper` set it to `True` before
calling `method.o(...)` and reset it after, using the token API.

### Guard as static method

`CacheProhibitDirectCall` gets a `require()` static method that checks the
`ContextVar` and raises if not inside a cache wrapper:

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

Methods opt in by calling `CacheProhibitDirectCall.require()` in their body.

### Wrapper integration

All 4 `method.o(...)` call sites in the sync and async wrappers (lines 301,
319, 357, 376) are wrapped with set/reset:

```python
token = _in_cache_wrapper.set(True)
try:
    result = method.o(**kwargs)  # or await
finally:
    _in_cache_wrapper.reset(token)
```

### Cleanup

- Remove the TODO comment at line 375 of `cached.py`
- Update `CacheProhibitDirectCall` docstring (remove placeholder)
- Remove `pass` from the class body

## Files Changed

| File | Change |
|------|--------|
| `src/lythonic/compose/cached.py` | Add `ContextVar`, update `CacheProhibitDirectCall` with `require()`, wrap 4 call sites, remove TODO |

## Testing

- Test that `CacheProhibitDirectCall.require()` raises outside a cache wrapper
- Test that it does not raise when called through a registered cached callable
