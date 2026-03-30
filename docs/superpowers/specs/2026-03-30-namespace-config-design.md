# Namespace Config — Persistent Structure Design

## Problem

The Namespace tree (callables, cached callables, DAGs) is built
programmatically at runtime. There is no way to declare the structure in a
config file, serialize a running Namespace for snapshot/restore, or validate
a configuration without loading it.

The existing `CacheConfig`/`CacheRule`/`CacheRegistry` in `cached.py`
handles only cache rules and has its own YAML format. A unified config
format is needed.

## Solution

A set of Pydantic models (`NamespaceConfig`) that describe the full
Namespace structure: plain callables, cached callables, and DAGs. The
models are format-agnostic — callers serialize/deserialize via
`model_dump()`/`model_validate()` with any format (YAML, JSON, TOML).

Three functions operate on these models:
- `load_namespace()` — two-pass loader that builds a live Namespace
- `dump_namespace()` — serializer that exports a live Namespace back to
  config models
- `validate_config()` — config-only validation without building a Namespace

`CacheConfig`, `CacheRule`, and `CacheRegistry` are removed. Cache
utilities stay in `cached.py` as functions.

## Config Models

```python
class StorageConfig(BaseModel):
    cache_db: str | None = None
    dag_db: str | None = None

class CacheEntryConfig(BaseModel):
    min_ttl: float
    max_ttl: float

class DagNodeConfig(BaseModel):
    label: str
    nsref: str             # must reference an entry in the namespace
    after: list[str] = []  # labels of upstream nodes in this DAG

class DagEntryConfig(BaseModel):
    nodes: list[DagNodeConfig]

class NamespaceEntryConfig(BaseModel):
    nsref: str
    gref: str | None = None
    cache: CacheEntryConfig | None = None
    dag: DagEntryConfig | None = None

class NamespaceConfig(BaseModel):
    storage: StorageConfig = StorageConfig()
    entries: list[NamespaceEntryConfig]
```

### Entry Discrimination

Determined by which fields are present:
- `gref` set, no `cache`, no `dag` → plain callable
- `gref` set, `cache` set → cached callable
- `dag` set, no `gref` → DAG entry
- `dag` + `gref` together → validation error
- Neither `gref` nor `dag` → validation error

### Serialization Order

Entries sorted alphabetically by `nsref`. DAG nodes sorted by `label`.
This ensures deterministic output for snapshot/restore and clean diffs in
version control.

## Config Example

```yaml
storage:
  cache_db: "cache.db"
  dag_db: "runs.db"

entries:
  - nsref: "analysis:compute_returns"
    gref: "myapp.analysis:compute_returns"

  - nsref: "market:fetch_prices"
    gref: "myapp.downloads:fetch_prices"
    cache:
      min_ttl: 0.5
      max_ttl: 2.0

  - nsref: "pipelines:daily"
    dag:
      nodes:
        - label: fetch
          nsref: "market:fetch_prices"
        - label: compute
          nsref: "analysis:compute_returns"
          after: [fetch]
```

## Loading — Two-Pass Deserialization

`load_namespace(config: NamespaceConfig, config_dir: Path) -> Namespace`

**Pass 1: Register all callable entries** (those with `gref`)

For each entry with `gref`:
- If `cache` is present: register with cache decorator using
  `storage.cache_db` (resolved relative to `config_dir`). Generates DDL,
  builds sync/async wrapper, installs via `ns.register()` with `decorate`.
  Stores cache config in `node.metadata["cache"]`.
- If no `cache`: plain `ns.register(gref, nsref=nsref)`.

**Pass 2: Build DAGs** (those with `dag`)

For each entry with `dag`:
- Create a `Dag`
- For each node in `dag.nodes`: `ns.get(node.nsref)` to get the
  `NamespaceNode`, `dag.node(ns_node, label=node.label)`
- For each node with `after`: add edges from each upstream label
- Validate the DAG (`dag.validate()`)
- Set `dag.db_path` from `storage.dag_db` (resolved relative to
  `config_dir`) if present
- Register: `ns.register(dag, nsref=entry.nsref)`

Two-pass means declaration order in the config doesn't matter. DAGs can
reference callables declared after them. This supports human-edited files
and clean SCM merges.

## Serialization — Namespace to NamespaceConfig

`dump_namespace(ns: Namespace, storage: StorageConfig | None = None) -> NamespaceConfig`

Walks the Namespace tree, collects entries:
- **Callable nodes:** extract `gref` from `node.method.gref`, `nsref` from
  `node.nsref`
- **Cached nodes:** extract cache config from `node.metadata["cache"]`
  (set during loading)
- **DAG nodes:** reconstruct `DagEntryConfig` from `Dag.nodes` and
  `Dag.edges` (convert edges to `after` lists)

Entries sorted alphabetically by `nsref`. DAG nodes sorted by `label`.

### NamespaceNode.metadata

A `metadata: dict[str, Any]` field on `NamespaceNode` stores config-
relevant data that doesn't belong in `Method`. Set at registration time,
read back during serialization. For cached entries:

```python
node.metadata = {"cache": {"min_ttl": 0.5, "max_ttl": 2.0}}
```

## Validation

### After Loading

`load_namespace` validates the fully-built Namespace at the end of both
passes. Failures raise `ValueError`.

**Structural checks:**
- All `gref` strings resolve to real callables (GlobalRef import succeeds)
- All DAG node `nsref` values reference entries that exist in the namespace
- All `after` labels reference nodes that exist within the same DAG
- No duplicate `nsref` across entries
- No duplicate `label` within a DAG
- DAG entries don't have `gref`, callable entries don't have `dag`
- Each entry has either `gref` or `dag` (not both, not neither)

**Type compatibility checks:**
- Cached entries: all parameters are `simple_type` (via
  `Method.validate_simple_type_args()`)
- DAG edges: upstream return types match downstream input types (via
  `Dag.validate()`)

### Config-Only Validation

`validate_config(config: NamespaceConfig) -> list[str]`

Validates the config without building a Namespace. Returns a list of error
messages (empty if valid). Checks:
- All `gref` strings resolve (imports succeed)
- All DAG node `nsref` values reference entries with `gref` in the config
- All `after` labels reference nodes within the same DAG
- No duplicate `nsref`
- No duplicate `label` within a DAG
- Entry discrimination rules (gref/dag mutual exclusivity)
- Cached entry parameters are `simple_type`

## CacheConfig Subsumption

### What Is Removed

- `CacheConfig` Pydantic model
- `CacheRule` Pydantic model
- `CacheRegistry` class
- `CacheRegistry.from_yaml()` class method

### What Stays in cached.py

Cache utility functions, now called by `load_namespace`:
- `generate_cache_table_ddl()`
- `table_name_from_path()`
- `_build_sync_wrapper()` / `_build_async_wrapper()`
- `_cache_lookup()` / `_cache_upsert()`
- `_pushback_set()` / `_pushback_check()`
- `CacheRefreshPushback` / `CacheRefreshSuppressed` exceptions
- `_namespace_path_to_nsref()`
- `_serialize_return_value()` / `_deserialize_return_value()`
- `_resolve_return_type()`

### New Helper in cached.py

```python
def register_cached_callable(
    ns: Namespace,
    gref: str,
    nsref: str,
    min_ttl: float,
    max_ttl: float,
    db_path: Path,
) -> NamespaceNode:
    """
    Register a callable with cache wrapping. Handles DDL generation,
    wrapper building, pushback table creation, and namespace registration.
    """
```

This encapsulates what `CacheRegistry._register_rule` does today, as a
standalone function callable from `load_namespace`.

### Breaking Change

Existing YAML configs in `CacheConfig` format must be converted to
`NamespaceConfig` format. No backward-compatibility shim.

## File Structure

**New file:** `src/lythonic/compose/namespace_config.py`
- `StorageConfig`, `CacheEntryConfig`, `DagNodeConfig`, `DagEntryConfig`,
  `NamespaceEntryConfig`, `NamespaceConfig`
- `load_namespace(config, config_dir)`
- `dump_namespace(ns, storage)`
- `validate_config(config)`

**Modified:** `src/lythonic/compose/namespace.py`
- `NamespaceNode.metadata: dict[str, Any]` field

**Modified:** `src/lythonic/compose/cached.py`
- Remove `CacheConfig`, `CacheRule`, `CacheRegistry`
- Add `register_cached_callable()` helper

**Modified:** `tests/test_cached.py`
- Update tests to use `register_cached_callable` or `load_namespace`

**New tests:** `tests/test_namespace_config.py`

**Modified docs:** Update reference pages, `mkdocs.yml`

## Testing

Tests in `tests/test_namespace_config.py`:

1. Config model parsing — valid YAML round-trip
2. Entry discrimination — gref-only, gref+cache, dag-only, invalid combos
3. Load callable entries into Namespace
4. Load cached entries with DDL and wrapper
5. Load DAG entries with nsref references
6. Two-pass order independence — DAG before callable in config
7. Validation: unresolvable gref raises
8. Validation: DAG nsref not in namespace raises
9. Validation: duplicate nsref raises
10. Validation: duplicate DAG label raises
11. Validation: cached entry with non-simple-type raises
12. dump_namespace round-trip — load, dump, compare
13. validate_config without loading — catches errors without imports
14. Serialization order — entries sorted by nsref, dag nodes by label
15. Existing cache behavior tests pass with register_cached_callable
