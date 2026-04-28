# Logging Configuration in StorageConfig

## Problem

`StorageConfig.log_file` only controls where logs go. Level and per-category
overrides are hardcoded in `_setup_file_logging()` in `lyth.py`. Logging setup
is also only triggered by the CLI `start` command, not by `mount()`.

## Solution

Add `log_level` and `loggers` fields to `StorageConfig`. Move logging setup
into `Namespace.mount()` so any mounted namespace with a `log_file` gets
file logging configured automatically.

## Design

### New fields on `StorageConfig`

```python
class StorageConfig(BaseModel):
    cache_db: Path | None = None
    dag_db: Path | None = None
    trigger_db: Path | None = None
    log_file: Path | None = None
    log_level: str = "DEBUG"
    loggers: dict[str, str] = {}
```

- `log_level`: Root logger level. Accepts standard Python level names
  (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`).
- `loggers`: Per-category level overrides. Keys are logger names (hierarchical),
  values are level names. Example: `{"lythonic.compose.cached": "INFO"}`.

### `mount()` sets up logging

When `storage.log_file` is not None, `mount()` calls a `_setup_logging(storage)`
helper that:

1. Creates parent directories for `log_file`
2. Creates a `FileHandler` with `NodeRunLogFilter` and the existing format:
   `"%(asctime)s %(levelname)-8s [%(name)s] run=%(run_id)s node=%(node_label)s %(message)s"`
3. Adds the handler to the root logger
4. Sets root logger level to `storage.log_level`
5. Iterates `storage.loggers` and sets each category's level

### Remove `_setup_file_logging` from `lyth.py`

The `start` command currently calls `_setup_file_logging(storage.log_file)`.
This is removed — `mount()` now handles it. The `start` command's
`ctx.run_result.print(f"Logging to {storage.log_file}")` line stays (it reads
from `storage.log_file` directly).

### When `log_file` is None

No handler is attached, no levels are changed. This is the default for tests
and programmatic use where callers pass `StorageConfig(cache_db=...)` without
a `log_file`.

### YAML config example

```yaml
storage:
  cache_db: cache.db
  dag_db: dags.db
  log_file: lyth.log
  log_level: INFO
  loggers:
    lythonic.compose.cached: DEBUG
    lythonic.compose.dag_runner: WARNING
```

## Files to Modify

1. `src/lythonic/compose/engine.py` — add `log_level`, `loggers` fields
2. `src/lythonic/compose/namespace.py` — `mount()` calls logging setup
3. `src/lythonic/compose/lyth.py` — remove `_setup_file_logging`, remove its
   call from `start`

## Verification

```bash
make lint
make test
```
