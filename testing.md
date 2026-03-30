# Testing Strategy

This is the primary source of truth for all testing conventions in Lythonic.
`CLAUDE.md` contains a condensed summary; this document has the full rationale
and examples.

## Stack

- **pytest** with `--import-mode=importlib`
- **pytest-asyncio** with `mode=Mode.AUTO` (async tests auto-detected)
- **pytest-cov** for coverage (`--cov=src`)
- **`--doctest-modules`** enabled globally — all `>>>` examples in docstrings
  are executed as tests

## Three Testing Tiers

Testing in Lythonic uses three tiers, each with a clear purpose. Choose the
tier that matches the scope of what you're testing.

### Tier 1: Doctests (in docstrings)

**Purpose:** Document behavior with executable examples. Doctests serve double
duty: they are API documentation *and* tests.

**When to use:**
- Pure functions with simple inputs and outputs
- Parsing, formatting, conversion logic
- Any function where showing input/output examples clarifies usage

**When NOT to use:**
- Tests that need setup/teardown (temp files, databases, fixtures)
- Tests that verify error conditions or exceptions
- Tests that need mutable module state or side effects
- Tests with non-deterministic output (timestamps, UUIDs, random)
- Tests that need to import from `tests/` modules

**Where they live:** In the docstring of the function or class being tested,
inside the source module.

**Style rules:**
- Keep examples short — 1-5 lines of `>>>` per example
- Show the most common and illuminating cases
- Use realistic but minimal inputs
- Each example should be self-contained (no dependencies on prior examples
  unless they are in the same docstring block)
- Order examples from simple to complex

**Example (good):**

```python
def _parse_nsref(nsref: str) -> tuple[list[str], str]:
    """
    Parse nsref into (branch_parts, leaf_name).

    >>> _parse_nsref('market.data:fetch_prices')
    (['market', 'data'], 'fetch_prices')
    >>> _parse_nsref('market:fetch_prices')
    (['market'], 'fetch_prices')
    >>> _parse_nsref('fetch_prices')
    ([], 'fetch_prices')
    >>> _parse_nsref(':fetch_prices')
    ([], 'fetch_prices')

    """
```

**Example (bad — too complex for doctest):**

```python
def create_cache(db_path: Path) -> Cache:
    """
    # Don't do this — needs temp files, setup, cleanup:
    >>> import tempfile
    >>> from pathlib import Path
    >>> tmp = tempfile.mkdtemp()
    >>> cache = create_cache(Path(tmp) / "test.db")
    >>> cache.put("key", "value")
    >>> cache.get("key")
    'value'
    """
```

That test belongs in Tier 2 or 3 instead.

**Converting inline tests to doctests:** When a simple inline test (Tier 2)
only verifies input/output on a pure function, consider converting it to a
doctest. This improves documentation while keeping the test. Remove the
standalone test function after adding the doctest — don't duplicate tests
across tiers.

### Tier 2: Inline Test Functions (in source files)

**Purpose:** Test behavior that is closely related to the code, simple enough
to not need fixtures, and benefits from living near the implementation.

**When to use:**
- Simple validation or transformation logic
- Tests that need only stdlib (no pytest fixtures)
- Code where co-locating the test with the implementation makes maintenance
  easier

**When NOT to use:**
- Tests that need `pytest.raises`, parametrize, or fixtures
- Tests that need temp files, databases, or other external resources
- Tests for integration scenarios spanning multiple modules
- Large test suites that would clutter the source file

**Where they live:** At the bottom of the source file, below a `## Tests`
comment. They are discovered by pytest because `testpaths` includes `src`.

**Style rules:**
- Do NOT import `pytest` — inline tests should have no runtime dependency
  on pytest
- Use plain assertions. Use `raise AssertionError("explanation")` instead of
  `assert False` (assertions can be stripped by optimization)
- Define any helper functions (fixtures, stubs) inside the test function
  to keep them scoped and avoid polluting the module namespace
- Name tests `test_<what_is_tested>`

**Example:**

```python
## Tests


def test_validate_simple_type_args_passes():
    def fetch(ticker: str, year: int) -> dict[str, Any]:
        return {}

    m = Method(fetch)
    m.validate_simple_type_args()


def test_validate_simple_type_args_fails_on_non_simple():
    def fetch(data: bytes) -> dict[str, Any]:
        return {}

    m = Method(fetch)
    try:
        m.validate_simple_type_args()
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "data" in str(e)
        assert "simple_type" in str(e)
```

### Tier 3: Separate Test Files (in `tests/`)

**Purpose:** Integration tests, complex scenarios, tests that need fixtures,
temp files, databases, or multi-module coordination.

**When to use:**
- Tests that need `tempfile.TemporaryDirectory`, database connections, or
  other external resources
- Tests that exercise multiple modules working together
- Tests for CLI, HTTP endpoints, or other interface boundaries
- Async tests (all async tests go here)
- Tests that need `pytest.raises` or other pytest features
- Any test too large or complex for inline

**Where they live:** `tests/test_<module_name>.py` or
`tests/<subpackage>/test_<feature>.py`.

**Style rules:**
- Use local imports inside test functions (not at module top level) for the
  modules under test. This keeps test collection fast and avoids import
  side effects.
- Use `import tests.test_<module> as this_module` to reference module-level
  helpers via GlobalRef in tests that need it.
- Prefer simple assertions over pytest fixtures. Only use `pytest.raises`,
  parametrize, or fixtures when they meaningfully reduce repetition in
  complex test suites.
- For SQLite tests on Windows: use `contextlib.closing(sqlite3.connect(...))`
  instead of bare `with sqlite3.connect(...)` — the latter only
  commits/rollbacks but does NOT close the connection, leaving the file
  locked on Windows.

**Example:**

```python
from __future__ import annotations

import tempfile
from pathlib import Path

import tests.test_cached as this_module


def test_sync_wrapper_miss_fetches_and_caches():
    from lythonic.compose.cached import CacheConfig, CacheRegistry, CacheRule

    with tempfile.TemporaryDirectory() as tmp:
        config = CacheConfig(
            rules=[
                CacheRule(
                    gref="tests.test_cached:_fake_fetch",
                    namespace_path="market.fetch",
                    min_ttl=1.0,
                    max_ttl=2.0,
                )
            ],
            cache_db="cache.db",
        )
        registry = CacheRegistry(config, config_dir=Path(tmp))

        result = registry.cached.market.fetch(ticker="AAPL")
        assert result == {"price": 100.0, "ticker": "AAPL"}
```

## Choosing the Right Tier

```
Is it a pure function with simple input/output?
├── Yes → Can the example serve as documentation?
│   ├── Yes → Tier 1 (doctest)
│   └── No  → Tier 2 (inline test)
└── No → Does it need external resources (files, DB, network)?
    ├── Yes → Tier 3 (separate test file)
    └── No  → Is it simple enough for inline?
        ├── Yes → Tier 2 (inline test)
        └── No  → Tier 3 (separate test file)
```

## What NOT to Test

- Pydantic model instantiation with known fields (Pydantic already tests this)
- Constants or configuration values
- Trivial delegation methods
- Third-party library behavior
- Anything obvious directly from the code

## Writing Assertions

- Plain `assert` statements. The assertion expression appears in the stack
  trace, which is usually sufficient.
- Do NOT add assertion messages that just restate the assertion:
  `assert x == 5, "x should be 5"` — redundant.
- DO add messages only when the assertion is non-obvious:
  `assert len(results) == 3, f"expected 3 results after merge, got {len(results)}"`
- Use `raise AssertionError("explanation")` instead of `assert False`.
  Assertions can be removed by Python's `-O` flag.

## Async Tests

All async tests live in Tier 3 (separate test files). pytest-asyncio with
`mode=Mode.AUTO` automatically detects async test functions — no decorator
needed.

```python
async def test_dag_execution():
    runner = DagRunner(dag, db_path)
    result = await runner.run(source_inputs={"fetch": {"ticker": "X"}})
    assert result.status == "completed"
```

## Platform Considerations

### Windows / SQLite file locking

On Windows, `sqlite3.Connection` as a context manager does NOT close the
connection — it only commits or rollbacks. This leaves the `.db` file locked,
causing `PermissionError` when `TemporaryDirectory` tries to clean up.

Always use `contextlib.closing()`:

```python
from contextlib import closing

with closing(sqlite3.connect(str(db_path))) as conn:
    conn.execute("SELECT ...")
```

The project's `open_sqlite_db()` context manager already handles this
correctly.

### Timing-dependent tests

Avoid `asyncio.sleep()` for synchronization in tests. Use `asyncio.Event`
or other deterministic coordination primitives instead. Sleep-based tests
are flaky on slow CI runners (especially Windows).

```python
# Bad — timing-dependent
async def pause_after_delay():
    await asyncio.sleep(0.005)
    runner.pause()

# Good — event-based
step1_started = asyncio.Event()

async def step1():
    step1_started.set()
    await asyncio.sleep(0)  # yield control
    return 1.0

async def pause_when_ready():
    await step1_started.wait()
    runner.pause()
```

## Running Tests

```bash
# Run all tests (with coverage):
make test

# Run a specific test file:
uv run pytest tests/test_cached.py -v

# Run a specific test:
uv run pytest tests/test_cached.py::test_sync_wrapper_miss_fetches_and_caches -v

# Run with output visible (for debugging):
uv run pytest -s tests/test_cached.py

# Run only doctests:
uv run pytest --doctest-modules src/lythonic/compose/namespace.py

# Run inline tests in a source file:
uv run pytest src/lythonic/compose/__init__.py -v
```

## Configuration

Testing is configured in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["src", "tests"]
addopts = [
    "--import-mode=importlib",
    "--doctest-modules",
    "--cov=src",
    "--cov-report=term-missing",
    "--cov-report=xml:cov.xml",
]
```

Key settings:
- `testpaths` includes both `src` (for inline tests and doctests) and `tests`
- `--doctest-modules` enables doctest discovery in all Python files under
  `testpaths`
- `--import-mode=importlib` avoids `sys.path` manipulation
