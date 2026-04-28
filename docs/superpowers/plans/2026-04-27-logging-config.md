# Logging Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `log_level` and `loggers` fields to `StorageConfig`, move logging setup into `mount()`.

**Architecture:** `StorageConfig` gains two fields. `mount()` calls a `_setup_logging` helper when `log_file` is set. `_setup_file_logging` in `lyth.py` is removed.

**Tech Stack:** Python 3.11+, Pydantic, stdlib logging

---

### Task 1: Add fields to `StorageConfig` and move logging into `mount()`

**Files:**
- Modify: `src/lythonic/compose/engine.py`
- Modify: `src/lythonic/compose/namespace.py`
- Modify: `src/lythonic/compose/lyth.py`
- Test: `tests/test_cached.py` (append new test)

- [ ] **Step 1: Write the test**

Append to `tests/test_cached.py`:

```python
def test_mount_configures_logging_when_log_file_set():
    """mount() sets up file logging with configured level and per-category overrides."""
    import logging

    from lythonic.compose.engine import StorageConfig
    from lythonic.compose.namespace import Namespace

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        log_file = Path(tmp) / "test.log"
        storage = StorageConfig(
            log_file=log_file,
            log_level="WARNING",
            loggers={"lythonic.compose.cached": "DEBUG"},
        )

        ns = Namespace()
        ns.register(lambda: "ok", nsref="t:plain")
        ns.mount(storage)

        # Root logger level set to WARNING
        root = logging.getLogger()
        assert root.level == logging.WARNING

        # Per-category override applied
        cached_logger = logging.getLogger("lythonic.compose.cached")
        assert cached_logger.level == logging.DEBUG

        # Log file created
        assert log_file.exists()

        # Clean up: remove the handler we added (avoid polluting other tests)
        for h in root.handlers[:]:
            if isinstance(h, logging.FileHandler) and str(log_file) in str(h.baseFilename):
                root.removeHandler(h)
                h.close()

        # Reset logger levels
        root.setLevel(logging.WARNING)
        cached_logger.setLevel(logging.NOTSET)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cached.py::test_mount_configures_logging_when_log_file_set -v`
Expected: FAIL — `StorageConfig` doesn't have `log_level` or `loggers` fields yet

- [ ] **Step 3: Add fields to `StorageConfig`**

In `src/lythonic/compose/engine.py`, update `StorageConfig`:

```python
class StorageConfig(BaseModel):
    """Storage paths for cache DB, DAG provenance DB, and trigger DB."""

    cache_db: Path | None = None
    dag_db: Path | None = None
    trigger_db: Path | None = None
    log_file: Path | None = None
    log_level: str = "DEBUG"
    loggers: dict[str, str] = {}
```

- [ ] **Step 4: Add logging setup to `mount()`**

In `src/lythonic/compose/namespace.py`, update the `mount()` method. Add logging
setup at the end (after cache wrapping):

```python
def mount(self, storage: Any) -> None:
    """Activate persistence features for all declared nodes."""
    self._storage = storage

    if storage.dag_db is not None:
        from lythonic.compose.dag_provenance import DagProvenance

        self._provenance = DagProvenance(storage.dag_db)

    if storage.cache_db is not None:
        from lythonic.compose.cached import mount_cached_node

        for node in self._nodes.values():
            if isinstance(node.config, NsCacheConfig):
                mount_cached_node(node, storage.cache_db)

    if storage.log_file is not None:
        self._setup_logging(storage)
```

Add the `_setup_logging` private method to `Namespace`:

```python
def _setup_logging(self, storage: Any) -> None:
    """Configure file logging from storage config."""
    import logging

    from lythonic.compose.log_context import NodeRunLogFilter

    log_file = storage.log_file
    log_file.parent.mkdir(parents=True, exist_ok=True)

    handler = logging.FileHandler(log_file)
    handler.addFilter(NodeRunLogFilter())
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)-8s [%(name)s] run=%(run_id)s node=%(node_label)s %(message)s"
        )
    )

    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(getattr(logging, storage.log_level.upper(), logging.DEBUG))

    for logger_name, level_name in storage.loggers.items():
        logging.getLogger(logger_name).setLevel(
            getattr(logging, level_name.upper(), logging.DEBUG)
        )
```

- [ ] **Step 5: Remove `_setup_file_logging` from `lyth.py`**

In `src/lythonic/compose/lyth.py`:

1. Delete the entire `_setup_file_logging` function (lines 72-86).
2. In the `start` function, remove the call to `_setup_file_logging`:
   - Remove: `_setup_file_logging(storage.log_file)`
   - Keep: `ctx.run_result.print(f"Logging to {storage.log_file}")`
3. Remove the `import logging` at the top (line 16) if it's now unused.
   Actually check — `logging` is not used elsewhere in the file after removing
   `_setup_file_logging`, so remove it.

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/test_cached.py::test_mount_configures_logging_when_log_file_set -v`
Expected: PASS

- [ ] **Step 7: Run full lint and test suite**

Run: `make lint && make test`
Expected: All pass

- [ ] **Step 8: Commit**

```bash
git add src/lythonic/compose/engine.py src/lythonic/compose/namespace.py src/lythonic/compose/lyth.py tests/test_cached.py
git commit -m "feat: move logging config into StorageConfig and mount()"
```

---

### Task 2: Final verification

**Depends on:** Task 1

- [ ] **Step 1: Run full lint and test suite**

Run: `make lint && make test`
Expected: 0 lint errors, all tests pass

- [ ] **Step 2: Verify the e2e test still works**

Run: `uv run pytest tests/test_lyth_e2e.py -v -k "not slow"`
If there's no non-slow test, just confirm the test file doesn't import
`_setup_file_logging`.

- [ ] **Step 3: Verify YAML config with new fields parses**

Run:
```bash
uv run python -c "
from lythonic.compose.engine import StorageConfig
s = StorageConfig(
    log_file='/tmp/test.log',
    log_level='INFO',
    loggers={'lythonic.compose.cached': 'DEBUG'},
)
print(s.model_dump())
print('OK')
"
```
Expected: prints the dict with all fields, then `OK`
