# NamespaceFragment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `NamespaceFragment`, `@nsnode`, and `@require_cache` to reduce boilerplate when registering groups of related callables into a `Namespace`.

**Architecture:** Three decorators (`@nsnode`, `@require_cache`, existing `@dag_factory`) control discovery within a `NamespaceFragment` subclass or module. A new `NsFragmentConfig` model holds `init` kwargs and per-method `configs`. `Namespace.from_dict` dispatches on `type: "fragment"` to instantiate the fragment and auto-register all discovered methods under a shared nsref prefix.

**Tech Stack:** Python 3.11+, Pydantic v2, existing lythonic namespace infrastructure.

---

## File Structure

All changes are in two files:

- **Modify:** `src/lythonic/compose/namespace.py` — add decorators, `NamespaceFragment` base class, `NsFragmentConfig` model, discovery logic, and `@require_cache` enforcement in `register()`
- **Create:** `tests/test_namespace_fragment.py` — all fragment-related tests (separate file because these are integration tests with fixtures)

---

### Task 1: Add `@nsnode` and `@require_cache` decorators

**Files:**
- Modify: `src/lythonic/compose/namespace.py` (near existing `dag_factory`, `mountable`, `mount_required` decorators around lines 178-199)
- Test: `tests/test_namespace_fragment.py`

- [ ] **Step 1: Write failing tests for decorators**

Create `tests/test_namespace_fragment.py`:

```python
from __future__ import annotations

from lythonic.compose.namespace import dag_factory, nsnode, require_cache


def test_nsnode_sets_flag_and_tags():
    @nsnode(tags=["api", "market"])
    def fetch(ticker: str) -> dict:
        return {"ticker": ticker}

    assert getattr(fetch, "_is_nsnode", False) is True
    assert getattr(fetch, "_nsnode_tags", []) == ["api", "market"]
    assert fetch(ticker="X") == {"ticker": "X"}


def test_nsnode_empty_tags():
    @nsnode()
    def transform(data: dict) -> dict:
        return data

    assert getattr(transform, "_is_nsnode", False) is True
    assert getattr(transform, "_nsnode_tags", []) == []


def test_require_cache_sets_flag():
    @require_cache
    def expensive(key: str) -> dict:
        return {"key": key}

    assert getattr(expensive, "_require_cache", False) is True
    assert expensive(key="X") == {"key": "X"}


def test_require_cache_stacks_with_nsnode():
    @require_cache
    @nsnode(tags=["api"])
    def fetch(ticker: str) -> dict:
        return {"ticker": ticker}

    assert getattr(fetch, "_is_nsnode", False) is True
    assert getattr(fetch, "_require_cache", False) is True
    assert getattr(fetch, "_nsnode_tags", []) == ["api"]


def test_require_cache_stacks_with_dag_factory():
    from lythonic.compose.namespace import Dag

    @require_cache
    @dag_factory
    def my_pipeline():
        dag = Dag()
        dag.node(lambda: "x", label="src")
        return dag

    assert getattr(my_pipeline, "_is_dag_factory", False) is True
    assert getattr(my_pipeline, "_require_cache", False) is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace_fragment.py -v`
Expected: ImportError — `nsnode` and `require_cache` not yet defined.

- [ ] **Step 3: Implement the decorators**

In `src/lythonic/compose/namespace.py`, after the `mount_required` decorator (line ~199), add:

```python
def nsnode(tags: list[str] | None = None) -> Callable[[_F], _F]:
    """
    Mark a method or function for namespace discovery within a fragment.
    `tags` are user-defined strings (e.g., `"api"`) stored on the node.
    """
    def decorator(fn: _F) -> _F:
        fn._is_nsnode = True  # pyright: ignore[reportFunctionMemberAccess]
        fn._nsnode_tags = tags or []  # pyright: ignore[reportFunctionMemberAccess]
        return fn
    return decorator


def require_cache(fn: _F) -> _F:
    """
    Mark a callable as requiring cache configuration. At registration time,
    if the provided config is not a `NsCacheConfig`, raises `ValueError`.
    Works on standalone functions, fragment methods, and `@dag_factory`.
    """
    fn._require_cache = True  # pyright: ignore[reportFunctionMemberAccess]
    return fn
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace_fragment.py -v`
Expected: All 5 tests PASS.

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No new errors.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace_fragment.py
git commit -m "feat: add @nsnode and @require_cache decorators"
```

---

### Task 2: Enforce `@require_cache` in `Namespace.register()`

**Files:**
- Modify: `src/lythonic/compose/namespace.py` — `register()` method (line ~572)
- Test: `tests/test_namespace_fragment.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_namespace_fragment.py`:

```python
import tests.test_namespace_fragment as this_module

from lythonic.compose.namespace import Namespace, NsCacheConfig, require_cache


@require_cache
def _cached_fn(key: str) -> dict:  # pyright: ignore[reportUnusedFunction]
    return {"key": key}


def _plain_fn(key: str) -> dict:  # pyright: ignore[reportUnusedFunction]
    return {"key": key}


def test_register_require_cache_without_config_raises():
    ns = Namespace()
    try:
        ns.register(this_module._cached_fn, nsref="t:cached")  # pyright: ignore[reportPrivateUsage]
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "require_cache" in str(e).lower() or "cache" in str(e).lower()


def test_register_require_cache_with_cache_config_succeeds():
    ns = Namespace()
    cfg = NsCacheConfig(nsref="t:cached", min_ttl=0.5, max_ttl=2.0)
    node = ns.register(this_module._cached_fn, nsref="t:cached", config=cfg)  # pyright: ignore[reportPrivateUsage]
    assert node.nsref == "t:cached"


def test_register_plain_fn_without_config_still_works():
    ns = Namespace()
    node = ns.register(this_module._plain_fn, nsref="t:plain")  # pyright: ignore[reportPrivateUsage]
    assert node.nsref == "t:plain"


@require_cache
@dag_factory
def _cached_dag_factory():  # pyright: ignore[reportUnusedFunction]
    from lythonic.compose.namespace import Dag
    dag = Dag()
    dag.node(lambda: "x", label="src")
    return dag


def test_register_require_cache_dag_factory_without_config_raises():
    ns = Namespace()
    try:
        ns.register(this_module._cached_dag_factory)  # pyright: ignore[reportPrivateUsage]
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "cache" in str(e).lower()


def test_register_require_cache_dag_factory_with_config_succeeds():
    ns = Namespace()
    cfg = NsCacheConfig(nsref="t:cached_dag__", min_ttl=1.0, max_ttl=5.0)
    node = ns.register(this_module._cached_dag_factory, nsref="t:cached_dag__", config=cfg)  # pyright: ignore[reportPrivateUsage]
    assert node.nsref == "t:cached_dag__"
```

- [ ] **Step 2: Run tests to verify failures**

Run: `uv run pytest tests/test_namespace_fragment.py::test_register_require_cache_without_config_raises tests/test_namespace_fragment.py::test_register_require_cache_dag_factory_without_config_raises -v`
Expected: FAIL — no ValueError raised yet.

- [ ] **Step 3: Implement enforcement in `register()`**

In `src/lythonic/compose/namespace.py`, modify the `register()` method. Add a check at the top of the method, right after the `isinstance(c, Dag)` check (line ~588):

```python
def register(self, c, nsref=None, decorate=None, tags=None, config=None):
    if isinstance(c, Dag):
        return self._register_dag(c, nsref, tags=tags, config=config)

    # Enforce @require_cache — the callable (or underlying factory) must have NsCacheConfig
    if callable(c) and getattr(c, "_require_cache", False):
        if not isinstance(config, NsCacheConfig):
            name = getattr(c, "__name__", str(c))
            raise ValueError(
                f"'{name}' is decorated with @require_cache but no NsCacheConfig was provided"
            )

    # Handle @dag_factory decorated callables
    if callable(c) and getattr(c, "_is_dag_factory", False):
        ...  # existing code
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace_fragment.py -v`
Expected: All tests PASS.

- [ ] **Step 5: Run full test suite**

Run: `make test`
Expected: All existing tests still pass (no regression — existing code never puts `_require_cache` on functions).

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace_fragment.py
git commit -m "feat: enforce @require_cache in Namespace.register()"
```

---

### Task 3: Add `NamespaceFragment` base class and `NsFragmentConfig` model

**Files:**
- Modify: `src/lythonic/compose/namespace.py` — add class and model, register in `_CONFIG_TYPES`
- Test: `tests/test_namespace_fragment.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_namespace_fragment.py`:

```python
def test_ns_fragment_config_validates():
    from lythonic.compose.namespace import NsFragmentConfig

    cfg = NsFragmentConfig(
        gref="tests.test_namespace_fragment:SampleFragment",
        nsref="downloads:",
        init={"api_key": "abc123"},
        configs={
            "fetch_prices": {"min_ttl": 0.5, "max_ttl": 2.0},
        },
    )
    assert cfg.type == "fragment"
    assert cfg.init == {"api_key": "abc123"}
    assert cfg.configs["fetch_prices"]["min_ttl"] == 0.5


def test_ns_fragment_config_in_config_types():
    from lythonic.compose.namespace import _CONFIG_TYPES, NsFragmentConfig

    assert _CONFIG_TYPES["fragment"] is NsFragmentConfig


def test_ns_fragment_config_module_with_init_raises():
    """Module fragment with non-empty init should fail validation."""
    from lythonic.compose.namespace import NsFragmentConfig

    # Module grefs have no colon — e.g., "mymodule.transforms"
    # Validation happens at registration, not at model construction,
    # so we just verify the model accepts the data.
    cfg = NsFragmentConfig(
        gref="tests.test_namespace_fragment",
        nsref="transforms:",
        init={"key": "val"},
    )
    assert cfg.init == {"key": "val"}


def test_namespace_fragment_is_marker_class():
    from lythonic.compose.namespace import NamespaceFragment

    class MyFrag(NamespaceFragment):
        pass

    frag = MyFrag()
    assert isinstance(frag, NamespaceFragment)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace_fragment.py::test_ns_fragment_config_validates -v`
Expected: ImportError — `NsFragmentConfig` not yet defined.

- [ ] **Step 3: Implement the base class and config model**

In `src/lythonic/compose/namespace.py`:

After `NsCacheConfig` (line ~441), add:

```python
class NsFragmentConfig(NsNodeConfig):
    """
    Config for a fragment — a class or module that groups related
    callables under a shared nsref prefix. `init` provides constructor
    kwargs for class fragments. `configs` maps method names to per-method
    config (cache TTLs, triggers).
    """

    type: str = "fragment"  # pyright: ignore[reportIncompatibleVariableOverride]
    init: dict[str, Any] = {}
    configs: dict[str, dict[str, Any]] = {}
```

Update `_CONFIG_TYPES` (line ~443) to include the new type:

```python
_CONFIG_TYPES: dict[str, type[NsNodeConfig]] = {
    "auto": NsNodeConfig,
    "cache": NsCacheConfig,
    "fragment": NsFragmentConfig,
}
```

Before the `Namespace` class, add the marker base class:

```python
class NamespaceFragment:
    """Marker base class for namespace fragments."""
    pass
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace_fragment.py -v`
Expected: All tests PASS.

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: No new errors.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace_fragment.py
git commit -m "feat: add NamespaceFragment base class and NsFragmentConfig model"
```

---

### Task 4: Fragment discovery and registration logic

**Files:**
- Modify: `src/lythonic/compose/namespace.py` — add `_discover_fragment_methods()` and update `from_dict()`
- Test: `tests/test_namespace_fragment.py`

- [ ] **Step 1: Write failing tests for class fragment registration**

Append to `tests/test_namespace_fragment.py`:

```python
from lythonic.compose.namespace import (
    Dag,
    NamespaceFragment,
    dag_factory,
    nsnode,
    require_cache,
)


class SampleFragment(NamespaceFragment):
    def __init__(self, api_key: str, base_url: str = "https://api.example.com"):
        self.api_key = api_key
        self.base_url = base_url

    @require_cache
    @nsnode(tags=["api"])
    def fetch_prices(self, ticker: str) -> dict:
        return {"ticker": ticker, "key": self.api_key}

    @nsnode()
    def transform(self, data: dict) -> dict:
        return {"processed": data}

    @dag_factory
    def pipeline(self):
        dag = Dag()
        dag.node(self.fetch_prices) >> dag.node(self.transform)
        return dag

    def _private_helper(self):
        """Not decorated — should be ignored."""
        pass


def test_class_fragment_registers_all_methods():
    ns = Namespace.from_dict([
        {
            "type": "fragment",
            "gref": "tests.test_namespace_fragment:SampleFragment",
            "nsref": "downloads:",
            "init": {"api_key": "abc123"},
            "configs": {
                "fetch_prices": {"min_ttl": 0.5, "max_ttl": 2.0},
            },
        }
    ])

    # @nsnode methods registered under prefix
    fetch_node = ns.get("downloads:fetch_prices")
    assert fetch_node(ticker="AAPL") == {"ticker": "AAPL", "key": "abc123"}
    assert fetch_node.tags == frozenset({"api"})

    transform_node = ns.get("downloads:transform")
    assert transform_node(data={"x": 1}) == {"processed": {"x": 1}}

    # @dag_factory method registered
    pipeline_node = ns.get("downloads:pipeline__")
    assert pipeline_node.dag is not None

    # Private helper not registered
    try:
        ns.get("downloads:_private_helper")
        raise AssertionError("Should not find private helper")
    except KeyError:
        pass


def test_class_fragment_cache_config_applied():
    ns = Namespace.from_dict([
        {
            "type": "fragment",
            "gref": "tests.test_namespace_fragment:SampleFragment",
            "nsref": "dl:",
            "init": {"api_key": "key1"},
            "configs": {
                "fetch_prices": {"min_ttl": 0.5, "max_ttl": 2.0},
            },
        }
    ])

    fetch_node = ns.get("dl:fetch_prices")
    assert isinstance(fetch_node.config, NsCacheConfig)
    assert fetch_node.config.min_ttl == 0.5
    assert fetch_node.config.max_ttl == 2.0


def test_class_fragment_require_cache_missing_raises():
    """@require_cache method without matching configs entry raises ValueError."""
    try:
        Namespace.from_dict([
            {
                "type": "fragment",
                "gref": "tests.test_namespace_fragment:SampleFragment",
                "nsref": "dl:",
                "init": {"api_key": "key1"},
                "configs": {},  # No fetch_prices entry
            }
        ])
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "fetch_prices" in str(e) or "require_cache" in str(e).lower()


def test_class_fragment_constructor_args():
    ns = Namespace.from_dict([
        {
            "type": "fragment",
            "gref": "tests.test_namespace_fragment:SampleFragment",
            "nsref": "x:",
            "init": {"api_key": "secret", "base_url": "https://custom.api"},
            "configs": {
                "fetch_prices": {"min_ttl": 1.0, "max_ttl": 5.0},
            },
        }
    ])

    result = ns.get("x:fetch_prices")(ticker="MSFT")
    assert result == {"ticker": "MSFT", "key": "secret"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace_fragment.py::test_class_fragment_registers_all_methods -v`
Expected: FAIL — `from_dict` doesn't handle `type: "fragment"` yet.

- [ ] **Step 3: Implement fragment discovery and registration**

In `src/lythonic/compose/namespace.py`, add a private function before the `Namespace` class:

```python
def _discover_fragment_methods(
    target: Any,
) -> list[tuple[str, Any, bool, list[str], bool]]:
    """
    Scan a fragment instance or module for decorated methods.
    Returns list of (name, callable, is_dag_factory, tags, requires_cache).
    """
    results: list[tuple[str, Any, bool, list[str], bool]] = []
    for attr_name in dir(target):
        if attr_name.startswith("_") and not getattr(
            getattr(target, attr_name, None), "_is_nsnode", False
        ) and not getattr(
            getattr(target, attr_name, None), "_is_dag_factory", False
        ):
            continue
        try:
            attr = getattr(target, attr_name)
        except Exception:
            continue
        if not callable(attr):
            continue

        is_nsnode = getattr(attr, "_is_nsnode", False)
        is_factory = getattr(attr, "_is_dag_factory", False)
        if not is_nsnode and not is_factory:
            continue

        tags = getattr(attr, "_nsnode_tags", [])
        needs_cache = getattr(attr, "_require_cache", False)
        results.append((attr_name, attr, is_factory, tags, needs_cache))
    return results
```

Then modify `Namespace.from_dict()` to handle fragments. Replace the existing `from_dict` method:

```python
@classmethod
def from_dict(cls, entries: list[dict[str, Any]]) -> Namespace:
    """Build a Namespace from a list of serialized node configs."""
    ns = cls()
    for entry in entries:
        entry_type = entry.get("type", "auto")
        if entry_type == "fragment":
            ns._register_fragment(entry)
            continue
        config_cls = _CONFIG_TYPES.get(entry_type, NsNodeConfig)
        config = config_cls.model_validate(entry)
        if config.gref is not None:
            gref = GlobalRef(config.gref)
            instance = gref.get_instance()
            ns.register(instance, nsref=config.nsref, config=config)
        else:
            ns.register(lambda: None, nsref=config.nsref, config=config)
    return ns
```

Add the `_register_fragment` private method to `Namespace`:

```python
def _register_fragment(self, entry: dict[str, Any]) -> None:
    """Register all discovered methods from a fragment config entry."""
    import inspect as _insp

    config = NsFragmentConfig.model_validate(entry)
    if config.gref is None:
        raise ValueError("Fragment config must have a gref")

    gref = GlobalRef(config.gref)
    prefix = str(config.nsref) if config.nsref else ""
    # Prefix must end with ":"
    if not prefix.endswith(":"):
        raise ValueError(f"Fragment nsref must end with ':', got {prefix!r}")

    resolved = gref.get_instance()

    if _insp.ismodule(resolved):
        if config.init:
            raise ValueError(
                f"Module fragment '{gref}' cannot have 'init' (modules are not instantiated)"
            )
        target = resolved
    elif isinstance(resolved, type) and issubclass(resolved, NamespaceFragment):
        target = resolved(**config.init)
    elif isinstance(resolved, type):
        raise TypeError(
            f"Class '{gref}' must be a NamespaceFragment subclass"
        )
    else:
        raise TypeError(
            f"Fragment gref must resolve to a module or NamespaceFragment subclass, "
            f"got {type(resolved).__name__}"
        )

    methods = _discover_fragment_methods(target)

    # Warn about configs entries that don't match any method
    method_names = {name for name, *_ in methods}
    for cfg_name in config.configs:
        if cfg_name not in method_names:
            logging.getLogger(__name__).warning(
                "Fragment '%s' configs entry '%s' does not match any discovered method",
                gref, cfg_name,
            )

    for name, method_callable, is_factory, tags, needs_cache in methods:
        method_nsref = f"{prefix}{name}"
        method_config_dict = config.configs.get(name, {})

        # Build per-method config
        has_ttl = "min_ttl" in method_config_dict and "max_ttl" in method_config_dict
        if has_ttl:
            node_config: NsNodeConfig = NsCacheConfig(
                nsref=method_nsref,
                min_ttl=method_config_dict["min_ttl"],
                max_ttl=method_config_dict["max_ttl"],
            )
        else:
            node_config = NsNodeConfig(nsref=method_nsref)

        # Attach triggers if present
        if "triggers" in method_config_dict:
            triggers = [
                TriggerConfig.model_validate(t)
                for t in method_config_dict["triggers"]
            ]
            node_config.triggers = triggers

        # Enforce @require_cache
        if needs_cache and not isinstance(node_config, NsCacheConfig):
            raise ValueError(
                f"Fragment method '{name}' is decorated with @require_cache "
                f"but no min_ttl/max_ttl found in configs['{name}']"
            )

        if is_factory:
            # DAG factory — call it and register the resulting DAG
            dag_nsref = f"{prefix}{name}__"
            dag_result = method_callable()
            if not isinstance(dag_result, Dag):
                raise TypeError(
                    f"@dag_factory '{name}' must return a Dag, got {type(dag_result).__name__}"
                )
            # Update config nsref for the dag suffix
            if isinstance(node_config, NsCacheConfig):
                node_config = NsCacheConfig(
                    nsref=dag_nsref,
                    min_ttl=node_config.min_ttl,
                    max_ttl=node_config.max_ttl,
                    triggers=node_config.triggers,
                )
            else:
                node_config = NsNodeConfig(nsref=dag_nsref, triggers=node_config.triggers)
            self._register_dag(dag_result, dag_nsref, tags=tags, config=node_config)
        else:
            self.register(
                method_callable,
                nsref=method_nsref,
                tags=tags,
                config=node_config,
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace_fragment.py -v`
Expected: All tests PASS.

- [ ] **Step 5: Run full test suite and lint**

Run: `make lint && make test`
Expected: No errors, all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace_fragment.py
git commit -m "feat: fragment discovery and registration in from_dict"
```

---

### Task 5: Module-as-fragment support

**Files:**
- Modify: `src/lythonic/compose/namespace.py` — (already handled in Task 4's `_register_fragment`)
- Create: `tests/sample_module_fragment.py` — sample module with decorated functions
- Test: `tests/test_namespace_fragment.py`

- [ ] **Step 1: Create sample module fragment**

Create `tests/sample_module_fragment.py`:

```python
"""Sample module used as a namespace fragment in tests."""
from __future__ import annotations

from lythonic.compose.namespace import nsnode, require_cache


@require_cache
@nsnode(tags=["api"])
def normalize(data: dict) -> dict:
    return {"normalized": True, **data}


@nsnode()
def flatten(data: list) -> list:
    return [item for sublist in data for item in (sublist if isinstance(sublist, list) else [sublist])]
```

- [ ] **Step 2: Write failing tests**

Append to `tests/test_namespace_fragment.py`:

```python
def test_module_fragment_discovers_functions():
    ns = Namespace.from_dict([
        {
            "type": "fragment",
            "gref": "tests.sample_module_fragment",
            "nsref": "transforms:",
            "configs": {
                "normalize": {"min_ttl": 1.0, "max_ttl": 5.0},
            },
        }
    ])

    norm = ns.get("transforms:normalize")
    assert norm(data={"x": 1}) == {"normalized": True, "x": 1}
    assert norm.tags == frozenset({"api"})
    assert isinstance(norm.config, NsCacheConfig)

    flat = ns.get("transforms:flatten")
    assert flat(data=[[1, 2], [3]]) == [1, 2, 3]


def test_module_fragment_with_init_raises():
    try:
        Namespace.from_dict([
            {
                "type": "fragment",
                "gref": "tests.sample_module_fragment",
                "nsref": "t:",
                "init": {"key": "val"},
                "configs": {
                    "normalize": {"min_ttl": 1.0, "max_ttl": 5.0},
                },
            }
        ])
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "module" in str(e).lower() and "init" in str(e).lower()


def test_module_fragment_require_cache_missing_raises():
    try:
        Namespace.from_dict([
            {
                "type": "fragment",
                "gref": "tests.sample_module_fragment",
                "nsref": "t:",
                "configs": {},  # No normalize entry
            }
        ])
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "normalize" in str(e) or "require_cache" in str(e).lower()
```

- [ ] **Step 3: Run tests to verify**

Run: `uv run pytest tests/test_namespace_fragment.py::test_module_fragment_discovers_functions tests/test_namespace_fragment.py::test_module_fragment_with_init_raises tests/test_namespace_fragment.py::test_module_fragment_require_cache_missing_raises -v`
Expected: All PASS (the logic was already implemented in Task 4's `_register_fragment` — this task validates it).

If any test fails, debug and fix the `_register_fragment` method to handle modules correctly. The key issue may be that `GlobalRef.get_instance()` for a module path without `:` needs to return the module object. Check how `GlobalRef` resolves module-only paths and adjust if needed.

- [ ] **Step 4: Run full test suite and lint**

Run: `make lint && make test`
Expected: All pass.

- [ ] **Step 5: Commit**

```bash
git add tests/sample_module_fragment.py tests/test_namespace_fragment.py
git commit -m "feat: module-as-fragment support with tests"
```

---

### Task 6: Fragment config serialization roundtrip and edge cases

**Files:**
- Test: `tests/test_namespace_fragment.py`
- Possibly modify: `src/lythonic/compose/namespace.py` — `to_dict()` if needed

- [ ] **Step 1: Write tests for roundtrip and warnings**

Append to `tests/test_namespace_fragment.py`:

```python
import logging


def test_fragment_configs_nonexistent_method_warns(caplog):
    """configs entry for a method that doesn't exist logs a warning."""
    with caplog.at_level(logging.WARNING):
        Namespace.from_dict([
            {
                "type": "fragment",
                "gref": "tests.test_namespace_fragment:SampleFragment",
                "nsref": "w:",
                "init": {"api_key": "k"},
                "configs": {
                    "fetch_prices": {"min_ttl": 0.5, "max_ttl": 2.0},
                    "nonexistent_method": {"min_ttl": 1.0, "max_ttl": 3.0},
                },
            }
        ])
    assert any("nonexistent_method" in r.message for r in caplog.records)


def test_fragment_with_triggers():
    ns = Namespace.from_dict([
        {
            "type": "fragment",
            "gref": "tests.test_namespace_fragment:SampleFragment",
            "nsref": "trig:",
            "init": {"api_key": "k"},
            "configs": {
                "fetch_prices": {"min_ttl": 0.5, "max_ttl": 2.0},
                "pipeline": {
                    "triggers": [
                        {"name": "pipe_repeat", "type": "poll", "schedule": "*/30 * * * * *"}
                    ]
                },
            },
        }
    ])

    pipeline_node = ns.get("trig:pipeline__")
    assert len(pipeline_node.config.triggers) == 1
    assert pipeline_node.config.triggers[0].name == "pipe_repeat"


def test_fragment_nsref_must_end_with_colon():
    try:
        Namespace.from_dict([
            {
                "type": "fragment",
                "gref": "tests.test_namespace_fragment:SampleFragment",
                "nsref": "bad_prefix",
                "init": {"api_key": "k"},
                "configs": {
                    "fetch_prices": {"min_ttl": 0.5, "max_ttl": 2.0},
                },
            }
        ])
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert ":" in str(e)
```

- [ ] **Step 2: Run tests**

Run: `uv run pytest tests/test_namespace_fragment.py -v`
Expected: All PASS.

If the warning test or trigger test fails, fix the relevant logic in `_register_fragment`.

- [ ] **Step 3: Run full suite and lint**

Run: `make lint && make test`
Expected: All pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_namespace_fragment.py
git commit -m "test: fragment edge cases — triggers, warnings, nsref validation"
```

---

### Task 7: Update module docstring and public exports

**Files:**
- Modify: `src/lythonic/compose/namespace.py` — update module docstring to mention fragments

- [ ] **Step 1: Update the module docstring**

At the top of `src/lythonic/compose/namespace.py`, add a section about fragments to the existing module docstring. After the `## DAG Factories` section (around line 155), add:

```python
## Namespace Fragments

A `NamespaceFragment` groups related methods under a shared nsref prefix.
Subclass it and decorate methods with `@nsnode(tags=[...])` for discovery.
Use `@require_cache` to enforce cache configuration at registration time.

```yaml
namespace:
  - type: fragment
    gref: "myapp.downloads:DownloadFragment"
    nsref: "downloads:"
    init:
      api_key: "abc123"
    configs:
      fetch_prices:
        min_ttl: 0.5
        max_ttl: 2.0
```

Modules can also serve as fragments — decorate module-level functions with
`@nsnode` or `@dag_factory`, and reference the module via its import path
(no `:` separator):

```yaml
  - type: fragment
    gref: "myapp.transforms"
    nsref: "transforms:"
```
```

- [ ] **Step 2: Run lint**

Run: `make lint`
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add src/lythonic/compose/namespace.py
git commit -m "docs: add NamespaceFragment section to namespace module docstring"
```

---

## Self-Review Checklist

**1. Spec coverage:**
- `@nsnode(tags=[])` decorator — Task 1 ✔
- `@require_cache` decorator — Task 1 ✔
- `@require_cache` enforcement in `register()` — Task 2 ✔
- `@require_cache` on standalone functions — Task 2 ✔
- `@require_cache` on `@dag_factory` — Task 2 ✔
- `NamespaceFragment` base class — Task 3 ✔
- `NsFragmentConfig` model with `type: "fragment"` — Task 3 ✔
- Registration in `_CONFIG_TYPES` — Task 3 ✔
- Fragment discovery (`_is_nsnode`, `_is_dag_factory`) — Task 4 ✔
- Class fragment instantiation with `init` kwargs — Task 4 ✔
- `@dag_factory` in fragments with `__` suffix — Task 4 ✔
- `@require_cache` enforcement in fragments — Task 4 ✔
- `configs` entry for nonexistent method logs warning — Task 6 ✔
- Module-as-fragment discovery — Task 5 ✔
- Module fragment with `init` raises ValueError — Task 5 ✔
- Triggers in fragment configs — Task 6 ✔
- Fragment nsref must end with `:` — Task 6 ✔
- Interaction with `@mount_required`/`@mountable` — spec says they stack naturally; no code change needed ✔

**2. Placeholder scan:** None found.

**3. Type consistency:** `_discover_fragment_methods` returns `list[tuple[str, Any, bool, list[str], bool]]` consistently used in `_register_fragment`. `NsFragmentConfig` uses same field names as spec (`init`, `configs`). Decorator attributes match (`_is_nsnode`, `_nsnode_tags`, `_require_cache`).
