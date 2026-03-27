# Cache Wrapper for lythonic.compose

## Overview

A YAML-configured caching layer that wraps Python methods (sync or async) returning
JSON dicts or Pydantic BaseModels. Cached data is stored in SQLite via the existing
`lythonic.state` infrastructure. Methods are exposed through a nested namespace object
with attribute access.

## Config Model

```python
class CacheRule(BaseModel):
    gref: GRef                        # GlobalRef to original method
    namespace_path: str | None = None  # dot-separated path in namespace; None = root with original name
    min_ttl: float                     # days, fresh period (no refresh attempted)
    max_ttl: float                     # days, hard expiry (must re-fetch)

class CacheConfig(BaseModel):
    namespace: str = "lythonic.compose.cached"  # target namespace module path
    cache_db: str | None = None        # override SQLite DB path, relative to config dir
    rules: list[CacheRule]
```

### YAML Example

File: `./data/compose/cached/cache.yaml`

```yaml
namespace: lythonic.compose.cached
rules:
  - gref: "myapp.downloads:fetch_prices"
    namespace_path: "market.fetch_prices"
    min_ttl: 0.5
    max_ttl: 2.0

  - gref: "myapp.downloads:get_exchange_rate"
    min_ttl: 0.25
    max_ttl: 1.0
```

## File & Directory Conventions

```
./data/compose/cached/
  cache.yaml          # config (source of truth)
  cache.db            # SQLite DB (all tables, one file)
```

- Default config path: `./data/compose/cached/cache.yaml`
- Default DB path: `./data/compose/cached/cache.db` (same dir as config)
- `cache_db` in config overrides DB path (relative to config dir)

## `simple_type` Flag on KnownType

Add `simple_type: bool = False` to `KnownTypeArgs` and `simple_type: bool` to `KnownType`.

Auto-marked `True` during `KNOWN_TYPES.register()` for:
- `is_primitive()` types: `int`, `float`, `bool`, `str`
- `date`, `datetime`, `Path`

Meaning: the type's string representation is a plain, URL-safe scalar suitable for
cache key segments and URL paths. No serialized JSON objects or encoded blobs.

## Validation

Validation happens at config load time, not at call time.

For each `CacheRule`, the referenced method's signature is introspected via `Method`.
All non-optional parameters must have type annotations whose `KnownType` has
`simple_type=True`. If any parameter fails this check, config loading raises an error.

Validation logic lives on `Method` (or a dedicated method on it) in
`lythonic.compose.__init__`.

## Cache Storage

SQLite via `lythonic.state`. Since table schemas are derived dynamically from method
signatures (not known at class definition time), cache tables use direct DDL generation
rather than static `DbModel` subclasses. The `DbFile` and `open_sqlite_db` utilities
are reused for connection management.

### Per-Rule Tables

Each `CacheRule` gets its own table. Table name derived from namespace path:
- `xyz.fetch_prices` -> table `xyz__fetch_prices`
- Root-level `get_data` -> table `get_data`

### Table Schema

Columns:
- One column per non-optional parameter, typed via `KnownType.db` map pair
- `value_json: TEXT` - JSON-serialized return value
- `fetched_at: REAL` - UTC timestamp (seconds since epoch)

Primary key: composite of all parameter columns.

### Example

Rule for `fetch_prices(ticker: str)` at `market.fetch_prices`:

```sql
CREATE TABLE market__fetch_prices (
    ticker TEXT NOT NULL,
    value_json TEXT NOT NULL,
    fetched_at REAL NOT NULL,
    PRIMARY KEY (ticker)
);
```

Rule for `get_exchange_rate(from_currency: str, to_currency: str)`:

```sql
CREATE TABLE get_exchange_rate (
    from_currency TEXT NOT NULL,
    to_currency TEXT NOT NULL,
    value_json TEXT NOT NULL,
    fetched_at REAL NOT NULL,
    PRIMARY KEY (from_currency, to_currency)
);
```

## Wrapper Logic

The wrapper mirrors the original method's sync/async nature (detected via
`GlobalRef.is_async()`). No background refresh - the wrapper does inline fetch
when needed. The caller controls orchestration.

### Call Flow

1. Build column values from args via `KnownType.db.map_to()`
2. Query table by parameter column values
3. **Fresh** (age < `min_ttl`): return cached value
4. **Stale** (`min_ttl` <= age < `max_ttl`): probabilistic refresh.
   `p = (age - min_ttl) / (max_ttl - min_ttl)`. With probability `p`, call original
   and update cache; otherwise return stale value. If refresh attempt fails, return
   stale value.
5. **Expired** (age >= `max_ttl`) or **miss** (no entry): call original (sync or
   `await` for async), store result, return it. If fetch fails, raise the exception.

### Return Value Serialization

- Pydantic `BaseModel`: `model_dump_json()` to store, `model_validate_json()` to load
- `dict`: `json.dumps()` to store, `json.loads()` to load

Detection: check method's return type annotation.

## Namespace Object

A simple object supporting nested attribute access. Dot-separated `namespace_path`
values in config create intermediate namespace objects automatically.

Example: config path `market.fetch_prices` on default namespace yields
`cached.market.fetch_prices(ticker="AAPL")`.

If `namespace_path` is `None`, the method is published at root level with its
original function name: `cached.get_exchange_rate(from_currency="USD", to_currency="EUR")`.

## CacheRegistry

The entry point that ties everything together.

### Initialization

```python
registry = CacheRegistry(config_path=Path("./data/compose/cached/cache.yaml"))
```

Steps on init:
1. Load and validate `CacheConfig` from YAML
2. For each rule:
   a. Resolve `GlobalRef` to callable
   b. Wrap in `Method`, validate all non-optional args have `simple_type=True`
   c. Generate table DDL from method signature (parameter columns + `value_json` + `fetched_at`)
   d. Create/ensure table in SQLite DB
   e. Build wrapper callable (sync or async matching original)
   f. Install wrapper on namespace at configured path (or root with original name)
3. Expose namespace as `registry.cached`

### Usage

```python
from lythonic.compose.cached import cached

# After registry is initialized:
result = cached.market.fetch_prices(ticker="AAPL")
rate = await cached.get_exchange_rate(from_currency="USD", to_currency="EUR")
```

## Module Layout

### Modified Files

- `src/lythonic/types.py`: Add `simple_type: bool` to `KnownTypeArgs` and `KnownType`.
  Auto-set during `KNOWN_TYPES.register()` for primitives, date, datetime, Path.
- `src/lythonic/compose/__init__.py`: Add arg validation method to `Method` for
  `simple_type` checking.

### New Files

- `src/lythonic/compose/cached.py`: `CacheRule`, `CacheConfig`, `Namespace`,
  `CacheRegistry`, wrapper factory, module-level `cached` namespace object.
