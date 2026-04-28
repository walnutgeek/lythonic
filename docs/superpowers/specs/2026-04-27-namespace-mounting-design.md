# Namespace Mounting and Declarative Cache Config

## Problem

`register_cached_callable()` is an imperative, narrow pathway for setting up
cached callables. It conflates resolution, DDL creation, wrapper building, and
namespace registration into a single function call that requires a `db_path`
at call time.

Meanwhile, `Namespace.from_dict()` and YAML configs provide a clean declarative
way to define namespace structure — but they cannot represent cache configs
because `from_dict` always deserializes as `NsNodeConfig` (ignoring the `type`
discriminator), and there is no mechanism to activate persistence features after
loading.

DAGs similarly work without persistence (`NullProvenance`) but benefit from
real provenance tracking when storage is available.

## Solution: Namespace Mounting

Introduce a **mount** operation that separates namespace definition from
persistence activation:

1. **Define** — load namespace from config (YAML/dict) with cache, trigger,
   and DAG declarations
2. **Mount** — `ns.mount(storage)` activates persistence: wraps cache nodes,
   creates provenance, makes triggers activatable

Pure namespaces (just methods, no cache/triggers/DAGs) don't need mounting
and work as-is.

## Design

### 1. Decorators: `@mountable` and `@mount_required`

Lightweight markers in `namespace.py`, following the `@inline` / `@dag_factory`
pattern:

```python
def mountable(fn: _F) -> _F:
    """Mark a callable as benefiting from a mounted namespace."""
    fn._is_mountable = True
    return fn

def mount_required(fn: _F) -> _F:
    """Mark a callable as requiring a mounted namespace."""
    fn._is_mount_required = True
    return fn
```

No runtime behavior. Decorated functions are returned as-is. If a
`@mount_required` method is called without mounting, it fails in its own
natural way (e.g. missing DB path). The markers exist for namespace
introspection only.

### 2. Discriminated Config Deserialization

`from_dict` dispatches on `type` field to create the right config subclass:

```python
_CONFIG_TYPES: dict[str, type[NsNodeConfig]] = {
    "auto": NsNodeConfig,
    "cache": NsCacheConfig,
}
```

`from_dict` uses `entry.get("type", "auto")` to pick the config class before
calling `model_validate`. This means YAML like this now works:

```yaml
namespace:
  - nsref: "market:fetch_prices"
    gref: "myapp.downloads:fetch_prices"
    type: cache
    min_ttl: 0.5
    max_ttl: 2.0
```

`EngineConfig.namespace` stays `list[NsNodeConfig]` for Pydantic schema, but
`_build_namespace` in `lyth.py` passes raw dicts through `from_dict` so the
discriminator is honored.

### 3. Mount-Awareness Properties on Namespace

```python
@property
def is_mounted(self) -> bool:
    """True if mount() has been called."""
    return self._storage is not None

@property
def requires_mount(self) -> bool:
    """True if any node has cache config, triggers, or @mount_required."""
    for node in self._nodes.values():
        if isinstance(node.config, NsCacheConfig):
            return True
        if node.config.triggers:
            return True
        if getattr(node.method.o, '_is_mount_required', False):
            return True
    return False

@property
def has_mountable(self) -> bool:
    """True if any node would alter behavior when mounted."""
    if self.requires_mount:
        return True
    for node in self._nodes.values():
        if node.dag is not None:
            return True
        if getattr(node.method.o, '_is_mountable', False):
            return True
    return False
```

`Namespace.__init__` gets:
- `self._storage: StorageConfig | None = None`
- `self._provenance: DagProvenance | NullProvenance | None = None`

### 4. `mount()` Method

```python
def mount(self, storage: StorageConfig) -> None:
    """Activate persistence features for all declared nodes."""
    self._storage = storage

    # Set up DAG provenance if dag_db is configured
    if storage.dag_db is not None:
        from lythonic.compose.dag_provenance import DagProvenance
        self._provenance = DagProvenance(storage.dag_db)

    # Wrap cache-declared nodes
    if storage.cache_db is not None:
        for node in self._nodes.values():
            if isinstance(node.config, NsCacheConfig):
                _mount_cached_node(node, storage.cache_db)
```

`_mount_cached_node` is a module-level function in `cached.py` (extracted from
the guts of `register_cached_callable`). It:

1. Validates method args are simple types
2. Derives table name from nsref
3. Creates cache table DDL + pushback table
4. Builds sync or async wrapper
5. Sets `node._decorated = wrapper`

### 5. DAG Provenance on Mount

DAGs registered in a namespace are inherently **mountable** — they work without
mounting (using `NullProvenance`) but gain real provenance tracking when
mounted.

`_register_dag` changes: the `dag_wrapper` closure captures the namespace
reference and checks mount state at call time:

```python
def _register_dag(self, dag, nsref, ...):
    ns_ref = self  # capture namespace

    async def dag_wrapper(**kwargs):
        provenance = ns_ref._provenance
        runner = DagRunner(dag, provenance=provenance)
        ...
        return await runner.run(source_inputs=..., dag_nsref=nsref)
```

When unmounted, `self._provenance` is `None`, so `DagRunner` defaults to
`NullProvenance`. When mounted, it picks up the real `DagProvenance`.

### 6. Remove `register_cached_callable`

Fully removed. No shim, no deprecation. This is a greenfield project.

The internal helpers it used (`_build_sync_wrapper`, `_build_async_wrapper`,
`generate_cache_table_ddl`, etc.) stay in `cached.py` — they are now called
by `_mount_cached_node`.

The module docstring and public exports are updated accordingly.

### 7. `_build_namespace` in `lyth.py`

Simplifies to:

```python
def _build_namespace(engine_config: EngineConfig) -> Namespace:
    ns = Namespace.from_dict(
        [e.model_dump(exclude_none=True) for e in engine_config.namespace]
    )
    ns.mount(engine_config.storage)
    return ns
```

Two clear steps: load declaratively, then mount.

### 8. Test Changes

- **`test_cached.py`**: All tests that use `register_cached_callable` migrate
  to: create `NsCacheConfig`, register via namespace, call `ns.mount(storage)`.
- **`test_dag_runner.py`**: Test with `register_cached_callable` migrates to
  the new flow.
- **New test**: `test_mount_awareness_properties` — verify `requires_mount`,
  `has_mountable`, `is_mounted` for various namespace configurations.
- **New test**: `test_dag_provenance_on_mount` — verify that a DAG in a
  mounted namespace uses real provenance, and unmounted uses NullProvenance.

## Files to Modify

1. `src/lythonic/compose/namespace.py` — decorators, `from_dict` discriminator,
   mount properties, `mount()`, `_register_dag` provenance lookup
2. `src/lythonic/compose/cached.py` — extract `_mount_cached_node`, remove
   `register_cached_callable`, update module docstring
3. `src/lythonic/compose/lyth.py` — simplify `_build_namespace`
4. `src/lythonic/compose/engine.py` — no changes (StorageConfig already has
   what we need)
5. `tests/test_cached.py` — migrate all tests from imperative to declarative
6. `tests/test_dag_runner.py` — migrate cache test
7. `docs/tutorials/compose-pipeline.md` — update cache section
8. `docs/reference/compose-cached.md` — update exports

## Verification

```bash
make lint
make test
```
