# Cache Refresh Pushback Design

## Problem

The `lythonic.compose.cached` module uses probabilistic refresh between `min_ttl`
and `max_ttl` to keep cached data fresh. When downstream APIs start rate-limiting,
these refresh attempts become wasteful and can worsen the rate-limiting situation.
There is no mechanism to temporarily suppress refresh attempts.

## Solution

A pushback mechanism that suppresses probabilistic refreshes for a configurable
duration, scoped to a specific method or a group of methods by namespace prefix.
Triggered by a special exception raised from the cached method.

## Exception Classes

### `CacheRefreshPushback`

Raised by user code (the wrapped method) to signal that refreshes should be
suppressed.

```python
class CacheRefreshPushback(Exception):
    """
    Raise from a cached method to suppress probabilistic refreshes.
    Defaults to suppressing only the raising method; set `namespace_prefix`
    to suppress a group of methods.
    """
    def __init__(self, days: float, namespace_prefix: str | None = None):
        self.days = days
        self.namespace_prefix = namespace_prefix
```

- `days`: How long to suppress refreshes (converted to seconds internally).
- `namespace_prefix`: If `None`, defaults to the exact `namespace_path` of the
  method that raised it. If set, suppresses all methods whose `namespace_path`
  starts with this prefix (with dot-boundary matching to avoid `"market"` matching
  `"marketplace"`).

### `CacheRefreshSuppressed`

Raised by the cache layer when a cache entry is past `max_ttl` but an active
pushback prevents refresh.

```python
class CacheRefreshSuppressed(Exception):
    """
    Raised when a cache entry is past max_ttl but refresh is suppressed
    by an active pushback.
    """
    def __init__(self, namespace_path: str, suppressed_until: float):
        self.namespace_path = namespace_path
        self.suppressed_until = suppressed_until
```

## Pushback Storage

### Table Schema

A single-row table in the same SQLite database used for cache data:

```sql
CREATE TABLE IF NOT EXISTS _pushback (
    namespace_prefix TEXT NOT NULL,
    suppressed_until REAL NOT NULL
)
```

Only one pushback is active at a time. A new pushback replaces any existing one.

The table is created in `CacheRegistry.__init__` alongside the per-method tables.

### DB Operations

Three functions:

- **`_pushback_set(conn, namespace_prefix, suppressed_until)`**: Deletes any
  existing row, inserts the new one. Called when `CacheRefreshPushback` is caught.

- **`_pushback_check(conn, namespace_path) -> float | None`**: Queries the table.
  Returns `suppressed_until` if there is an active (non-expired) pushback whose
  prefix matches the given `namespace_path`. Returns `None` otherwise. Match logic:
  `namespace_path == prefix or namespace_path.startswith(prefix + ".")`.

- **`_pushback_clear_expired(conn)`**: Deletes rows where
  `suppressed_until <= time.time()`. Called at the start of `_pushback_check` so
  expired entries are cleaned up before matching.

## Modified Wrapper Logic

Changes apply identically to `_build_sync_wrapper` and `_build_async_wrapper`.

### Probabilistic Window (min_ttl <= age < max_ttl)

Current behavior: random check, then try refresh, fall back to stale on any
exception.

New behavior:

```
if age < max_ttl:
    p = (age - min_ttl) / (max_ttl - min_ttl)
    if random.random() >= p:
        return stale
    # If pushback is active, skip refresh entirely
    if _pushback_check(conn, namespace_path):
        return stale
    try:
        result = method()
        upsert(result)
        return result
    except CacheRefreshPushback as e:
        prefix = e.namespace_prefix or namespace_path
        until = time.time() + e.days * DAYS_TO_SECONDS
        _pushback_set(conn, prefix, until)
        return stale
    except Exception:
        return stale
```

### Hard Expiry (age >= max_ttl, cached entry exists)

New behavior: check pushback before calling the method.

```
suppressed_until = _pushback_check(conn, namespace_path)
if suppressed_until:
    raise CacheRefreshSuppressed(namespace_path, suppressed_until)
# Otherwise call method normally (existing behavior)
```

### Cache Miss (no cached entry)

No change. Call the method, propagate exceptions as before. Pushback state is
ignored since there is no stale data to serve.

## Behavior Summary

| Scenario | Pushback Active | Result |
|----------|----------------|--------|
| Probabilistic window | No | Try refresh (existing behavior) |
| Probabilistic window | Yes | Return stale, skip refresh |
| Probabilistic window, method raises `CacheRefreshPushback` | N/A | Record pushback, return stale |
| Probabilistic window, method raises other exception | N/A | Return stale (existing behavior) |
| Past max_ttl, cached entry exists | No | Call method (existing behavior) |
| Past max_ttl, cached entry exists | Yes | Raise `CacheRefreshSuppressed` |
| Cache miss | Any | Call method (existing behavior) |

## Testing

Tests to add to `tests/test_cached.py`:

1. **Pushback recorded**: Method raises `CacheRefreshPushback` during probabilistic
   window. Verify pushback row is written and stale value is returned.

2. **Pushback suppresses refresh**: With active pushback, calls in probabilistic
   window return stale without calling the method.

3. **Prefix matching**: Pushback on `"market"` suppresses `"market.fetch_prices"`
   but not `"weather.forecast"` or `"marketplace.something"`.

4. **Past max_ttl with pushback**: Raises `CacheRefreshSuppressed` with correct
   fields.

5. **Pushback expiry**: After duration elapses, refresh attempts resume normally.

6. **Pushback replacement**: New `CacheRefreshPushback` replaces existing pushback
   (single row).

7. **Cache miss ignores pushback**: No cached entry means method is called
   regardless of pushback.

8. **Default scope**: `CacheRefreshPushback(days=1)` with no prefix scopes to the
   raising method's `namespace_path` only.
