# NamespaceFragment Design Spec

## Goal

Reduce boilerplate when registering groups of related callables into a
`Namespace`. A `NamespaceFragment` (class or module) groups methods that
share a namespace prefix, constructor dependencies, and per-method config
(cache TTLs, triggers) into a single YAML entry.

## Decorators

Three decorators control discovery within a fragment:

### `@nsnode(tags=[])`

Marks a method (or module-level function) for namespace discovery.
`tags` are user-defined strings (e.g., `"api"` for downstream exposure).
Methods without `@nsnode` or `@dag_factory` are ignored.

```python
@nsnode(tags=["api"])
async def fetch_prices(self, ticker: str) -> dict:
    ...
```

### `@require_cache`

Marks a callable as requiring cache configuration. Works both inside
fragments and on standalone functions:

- **Inside a fragment**: at registration time, if no matching entry with
  `min_ttl`/`max_ttl` exists in the fragment's `configs`, a `ValueError`
  is raised.
- **Standalone registration**: when `ns.register()` is called on a
  `@require_cache` function without a `NsCacheConfig`, a `ValueError`
  is raised. The caller must pass `config=NsCacheConfig(...)` or
  register via `from_dict` with `type: cache`.

Can be combined with `@nsnode`:

```python
@require_cache
@nsnode(tags=["api"])
async def fetch_volume(self, ticker: str) -> dict:
    ...
```

Standalone usage:

```python
@require_cache
async def fetch_prices(ticker: str) -> dict:
    ...

# This raises ValueError — no cache config
ns.register(fetch_prices, nsref="market:fetch_prices")

# This works
ns.register(fetch_prices, nsref="market:fetch_prices",
            config=NsCacheConfig(nsref="market:fetch_prices",
                                 min_ttl=0.5, max_ttl=2.0))
```

### `@dag_factory` (existing)

Already exists in the codebase. Auto-discovered within fragments without
needing `@nsnode`. Can optionally have `@nsnode` for tags.

## NamespaceFragment base class

A marker base class. Subclasses define constructor dependencies and
decorated methods:

```python
class DownloadFragment(NamespaceFragment):
    def __init__(self, api_key: str, base_url: str = "https://api.example.com"):
        self.api_key = api_key
        self.base_url = base_url

    @require_cache
    @nsnode(tags=["api"])
    async def fetch_prices(self, ticker: str) -> dict:
        return await self._call_api(f"/prices/{ticker}")

    @nsnode()
    async def transform(self, data: dict) -> dict:
        return {"processed": data}

    @dag_factory
    def pipeline(self):
        with Dag() as dag:
            dag.node(self.fetch_prices) >> dag.node(self.transform)
        return dag

    def _call_api(self, path: str) -> dict:
        # Not decorated - ignored during discovery
        ...
```

## Module-as-fragment

Module-level functions decorated with `@nsnode` or `@dag_factory` are
discovered the same way as class methods. A module fragment is identified
by a `gref` without a colon (e.g., `"myapp.downloads"`).

Modules are not instantiated, so `init` is not allowed (raises
`ValueError` at config validation time).

```python
# myapp/transforms.py
from lythonic.compose.namespace import nsnode, require_cache

@require_cache
@nsnode(tags=["api"])
def normalize(data: dict) -> dict:
    ...

@nsnode()
def flatten(data: list) -> list:
    ...
```

## YAML configuration

### Fragment config entry

```yaml
namespace:
  # Class fragment
  - type: fragment
    gref: "myapp.downloads:DownloadFragment"
    nsref: "downloads:"
    init:
      api_key: "abc123"
    configs:
      fetch_prices:
        min_ttl: 0.5
        max_ttl: 2.0
      pipeline:
        triggers:
          - name: "pipeline_repeat"
            schedule: "*/30 * * * * *"

  # Module fragment
  - type: fragment
    gref: "myapp.transforms"
    nsref: "transforms:"
    configs:
      normalize:
        min_ttl: 1.0
        max_ttl: 5.0

  # Existing single-method entries still work unchanged
  - gref: "myapp.standalone:task1"
    nsref: "misc:task1"
```

### Config model

```python
class NsFragmentConfig(NsNodeConfig):
    type: str = "fragment"
    init: dict[str, Any] = {}
    configs: dict[str, dict[str, Any]] = {}
```

The `nsref` field on a fragment ends with `:` (a prefix). Individual
method nsrefs are derived as `"{prefix}{method_name}"` — e.g.,
`"downloads:fetch_prices"`.

`NsFragmentConfig` is registered in `_CONFIG_TYPES` so `from_dict`
dispatches on `type: "fragment"`.

## Registration flow

When `Namespace.from_dict` encounters `type: fragment`:

1. **Resolve gref**: If the gref resolves to a class that is a subclass
   of `NamespaceFragment`, instantiate it with `init` kwargs. If it
   resolves to a module, use the module directly.

2. **Discover methods**: Scan the instance (or module) for attributes
   with `_is_nsnode` or `_is_dag_factory` flags (set by the decorators).

3. **Register each method**: For each discovered method, call
   `ns.register()` with:
   - `nsref = "{prefix}{method_name}"` (e.g., `"downloads:fetch_prices"`)
   - `tags` from the `@nsnode` decorator
   - If `configs` has a matching entry with `min_ttl`/`max_ttl`, wrap in
     `NsCacheConfig`
   - If `configs` has a matching entry with `triggers`, attach triggers
   - For `@dag_factory` methods, the existing registration path handles
     DAG expansion

4. **Validate**:
   - `@require_cache` method with no matching `configs` entry (or
     missing `min_ttl`/`max_ttl`) raises `ValueError`
   - `configs` entry naming a method that doesn't exist on the fragment
     logs a warning
   - Module fragment with non-empty `init` raises `ValueError`

## `@require_cache` enforcement

`Namespace.register()` checks `_require_cache` on the callable being
registered. If the flag is set and the provided `config` is not a
`NsCacheConfig` (or is `None`), registration raises `ValueError`.

This applies uniformly to all callable types:
- Standalone functions registered via `ns.register()`
- `@dag_factory` functions (the factory callable carries the flag;
  checked before DAG expansion)
- Fragment methods (checked during fragment registration)

For `@dag_factory`, stacking looks like:

```python
@require_cache
@dag_factory
def expensive_pipeline():
    with Dag() as dag:
        ...
    return dag
```

The DAG's wrapper callable gets cached — subsequent calls with the
same inputs return the cached `DagRunResult` without re-executing.

## Discovery mechanics

The `@nsnode` decorator sets `fn._is_nsnode = True` and
`fn._nsnode_tags = tags`. The `@require_cache` decorator sets
`fn._require_cache = True`.

Discovery scans for these flags:

- **Class**: Iterate `dir(instance)`, check each attribute for
  `_is_nsnode` or `_is_dag_factory`. Use bound methods (already bound
  to the instantiated fragment).
- **Module**: Iterate module attributes, check each callable for the
  same flags.

## Interaction with existing decorators

`@mountable` and `@mount_required` continue to work as before — they
can be stacked with `@nsnode`:

```python
@mount_required
@require_cache
@nsnode(tags=["api"])
async def fetch_prices(self, ticker: str) -> dict:
    ...
```

## File placement

- `NamespaceFragment` base class: in `namespace.py`
- `@nsnode` decorator: in `namespace.py` (alongside `@mountable`,
  `@mount_required`, `@dag_factory`)
- `@require_cache` decorator: in `namespace.py`
- `NsFragmentConfig`: in `namespace.py` (alongside `NsNodeConfig`,
  `NsCacheConfig`)
- Discovery logic: private function in `namespace.py` used by
  `from_dict`

## Testing

### `@require_cache` enforcement

- `ns.register()` on a `@require_cache` function without `NsCacheConfig`
  raises `ValueError`
- `ns.register()` on a `@require_cache` function with `NsCacheConfig`
  succeeds
- `ns.register()` on a `@require_cache` `@dag_factory` without
  `NsCacheConfig` raises `ValueError`
- `ns.register()` on a `@require_cache` `@dag_factory` with
  `NsCacheConfig` succeeds and the DAG wrapper is cached
- `ns.register()` on a non-`@require_cache` function without config
  continues to work as before (no regression)

### Fragment registration

- Register a class fragment with `@nsnode` and `@require_cache` methods;
  verify all methods appear under the correct nsref prefix
- Register a class fragment with `@dag_factory`; verify DAG is expanded
- Fragment `@require_cache` method without matching `configs` entry
  raises `ValueError`
- Verify module-as-fragment discovers decorated functions
- Verify module fragment with `init` raises `ValueError`
- Verify `configs` entry for nonexistent method logs warning
- Verify constructor args are passed from `init`
- Roundtrip: `to_dict` / `from_dict` preserves fragment config
