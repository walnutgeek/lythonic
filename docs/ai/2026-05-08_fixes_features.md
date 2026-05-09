# Fixes and Features — 2026-05-08

Post-NamespaceFragment implementation fixes and small features, applied
incrementally while integrating fragments into the `do_sleep_repeat` example
and validating with `lyth start`.

## 1. `--verbose` flag for CLI (`cli.py`)

Added `verbose: bool` field to `Main` and `is_verbose()` to `RunContext`.
When `--verbose` is passed, unhandled exceptions in `ActionTree._run_args`
print the full `traceback.format_exc()` instead of just the error message.

**Files:** `src/lythonic/compose/cli.py`, `tests/test_compose.py`

## 2. GlobalRef bound method support (`__init__.py`)

`GlobalRef.__init__` only checked `isfunction(s)`, so passing a bound method
(e.g., `self.quick_task` inside a fragment) hit the string-parsing branch and
failed with `'function' object has no attribute 'split'`.

Fix: added `ismethod` to the callable check.

**Files:** `src/lythonic/__init__.py`

## 3. EngineConfig namespace as raw dicts (`engine.py`, `lyth.py`)

`EngineConfig.namespace` was `list[NsNodeConfig]`. Pydantic validated each
entry as the base class, silently dropping fragment-specific fields (`init`,
`configs`). Changed to `list[dict[str, Any]]` so raw dicts pass through to
`Namespace.from_dict()` which handles type discrimination itself.

**Files:** `src/lythonic/compose/engine.py`, `src/lythonic/compose/lyth.py`

## 4. Trigger shorthand — bare cron strings (`namespace.py`)

Added `model_validator(mode="before")` to `TriggerConfig` that expands a bare
string like `"*/13 * * * * *"` into `{type: "poll", schedule: "..."}`. Made
`name` optional (`str | None = None`) and `type` default to `"poll"`.

Added `_auto_fill_trigger_names()` that derives trigger names from the node's
nsref leaf (`"{leaf}_{type}"`, with `_{i}` suffix for multiple triggers).
Called from both `from_dict` and `_register_fragment`.

**Files:** `src/lythonic/compose/namespace.py`

## 5. DagContext namespace access (`namespace.py`, `dag_runner.py`)

Added `namespace` field to `DagContext` (excluded from serialization via
`PydanticField(exclude=True)`) and `ns_call()` convenience method. Updated
`DagRunner._call_node` to pass `namespace=self.dag.parent_namespace`.

Required `ConfigDict(arbitrary_types_allowed=True)` on `DagContext` since
`Namespace` is not a Pydantic model.

**Files:** `src/lythonic/compose/namespace.py`, `src/lythonic/compose/dag_runner.py`

## 6. `get_as_dag` parent namespace (`namespace.py`)

`Namespace.get_as_dag()` wraps plain callables in an ad-hoc single-node `Dag`
but never set `parent_namespace`. Any callable expecting `DagContext.namespace`
(like `play_with_ctx`) got `None` and raised
`"DagContext has no namespace (not mounted)"`.

Fix: `dag.parent_namespace = self` on the ad-hoc DAG.

**Files:** `src/lythonic/compose/namespace.py`

## 7. Cache DDL and queries for zero-arg functions (`cached.py`)

`generate_cache_table_ddl` produced `PRIMARY KEY ()` for functions with no
parameters, causing `sqlite3.OperationalError`.

Three fixes:
- **DDL**: only emit `PRIMARY KEY (...)` when there are parameter columns.
- **`_cache_lookup`**: omit `WHERE` clause for zero-arg (select the single row).
- **`_cache_upsert`**: `DELETE` all rows before `INSERT` for zero-arg tables,
  since `INSERT OR REPLACE` has no key to match without a PRIMARY KEY.

**Files:** `src/lythonic/compose/cached.py`

## 8. Node failure traceback logging and DB storage (`dag_runner.py`)

Both `except Exception` blocks in `DagRunner.run()` previously stored only
`str(e)` and did no logging. Node failures were invisible in the log and
hard to diagnose from the DB.

Fix: capture `traceback.format_exc()`, log it via `_log.error(...)`, and
store the full traceback in the `node_executions.error` column.
`DagRunResult.error` keeps the short `str(e)` for console output.

**Files:** `src/lythonic/compose/dag_runner.py`

## 9. Fragment examples (`do_sleep_repeat.py`, `transforms.py`)

Added working examples for both fragment patterns:

- **Class fragment**: `SleepFragment(NamespaceFragment)` with `@nsnode`
  `quick_task` and `@dag_factory` `frag_dag`.
- **Module fragment**: `transforms.py` with `@nsnode` `double`/`negate` and
  `@dag_factory` `double_dag` (wraps `random_value >> double`).
- **Cached callable**: `get_timestamp()` with `@require_cache`, called from
  `play_with_ctx` via `ctx.ns_call("examples:get_timestamp")`.
- YAML config updated with fragment entries, cache entry, and trigger shorthand.

**Files:** `src/lythonic/examples/do_sleep_repeat.py`,
`src/lythonic/examples/transforms.py`, `tests/test_lyth_e2e.py`

## 10. `ns_acall` — async counterpart to `ns_call` (`namespace.py`)

`ns_call` is sync-only and raises `TypeError` if the target is async.
Added `ns_acall` for async contexts. It handles all callable types:

- **async** → `await fn(...)`
- **sync + `@inline`** → `fn(...)` directly on the event loop
- **sync (blocking)** → dispatched to `loop.run_in_executor()`

Extracted `_resolve_node()` as shared helper for both methods.

**Files:** `src/lythonic/compose/namespace.py`, `tests/test_namespace.py`

## 11. `always_failing_task` example (`do_sleep_repeat.py`)

Added an `always_failing_task` function that raises `ValueError("Failing")`
on every call. Registered with a `*/37s` poll trigger in the YAML config.
Validates that failure tracebacks are captured in both the log and the DB.

**Files:** `src/lythonic/examples/do_sleep_repeat.py`

## 12. E2E test coverage expansion (`test_lyth_e2e.py`)

Extended the e2e test with new assertions:

- **Failure tracebacks**: `always_failing_task` runs are all `failed`, each
  failed node has `error` containing `"Traceback"` and `"ValueError"`.
- **Cache refresh**: log shows `get_timestamp called (fresh)` at least twice
  over 120s, with differing timestamps (proves TTL-based refresh works).
- **`ns_call` works**: `play_with_ctx` has completed runs (proves namespace
  access from `DagContext` works end-to-end).
- **Log tracebacks**: `"Traceback"`, `"ValueError"`, `"Failing"` in log file.
- **Failed run structure**: failed runs have `finished_at` set.
- **Cache DB**: `examples__get_timestamp` table exists with valid cached float.
- Summary output now includes failed count and cache refresh count.

**Files:** `tests/test_lyth_e2e.py`

## 13. `ns_call`/`ns_acall` unit tests (`test_namespace.py`)

Seven new tests covering:

- `ns_call` with sync, `@inline`, and error on async targets
- `ns_call` without namespace raises `RuntimeError`
- `ns_acall` with async, sync-via-executor, and `@inline` callables

**Files:** `tests/test_namespace.py`
