# Reference Type Hierarchy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `GlobalRef` into `_NsRefBase` (base) and `_GlobalRefBase` (subclass), expose `NsRef` and `GlobalRef` as annotated aliases, remove `GRef`, add `ModuleRef`.

**Architecture:** `_NsRefBase` handles `scope:name` parsing, string repr, equality/hashing, and Pydantic schema. `_GlobalRefBase` extends it with module import and object resolution. Public names `NsRef` and `GlobalRef` are `Annotated[...]` wrappers. `ModuleRef` is a standalone class for module-only references. `_parse_nsref()` in namespace.py is removed — replaced by `NsRef(s)`.

**Tech Stack:** Python 3.11+, Pydantic, stdlib importlib

---

### Task 1: Implement `_NsRefBase` class

**Files:**
- Modify: `src/lythonic/__init__.py`
- Test: `tests/test_gref.py` (append new tests)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_gref.py`:

```python
from lythonic import NsRef


def test_nsref_parse_scope_and_name():
    ref = NsRef("market.data:fetch_prices")
    assert ref.scope == ["market", "data"]
    assert ref.name == "fetch_prices"
    assert str(ref) == "market.data:fetch_prices"


def test_nsref_parse_simple_name():
    ref = NsRef("fetch_prices")
    assert ref.scope == []
    assert ref.name == "fetch_prices"
    assert str(ref) == "fetch_prices"


def test_nsref_parse_empty_scope():
    ref = NsRef(":fetch_prices")
    assert ref.scope == []
    assert ref.name == "fetch_prices"
    assert str(ref) == "fetch_prices"


def test_nsref_parse_empty_name():
    ref = NsRef("branch.path:")
    assert ref.scope == ["branch", "path"]
    assert ref.name == ""
    assert str(ref) == "branch.path:"


def test_nsref_equality_and_hash():
    a = NsRef("market:fetch")
    b = NsRef("market:fetch")
    c = NsRef("market:other")
    assert a == b
    assert a != c
    assert hash(a) == hash(b)
    d = {a: 1}
    assert d[b] == 1


def test_nsref_copy_constructor():
    original = NsRef("x.y:z")
    copy = NsRef(original)
    assert copy.scope == ["x", "y"]
    assert copy.name == "z"
    assert copy == original


def test_nsref_repr():
    ref = NsRef("market:fetch")
    assert repr(ref) == "NsRef('market:fetch')"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_gref.py::test_nsref_parse_scope_and_name -v`
Expected: FAIL — `NsRef` not importable from `lythonic`

- [ ] **Step 3: Implement `_NsRefBase` and `NsRef`**

In `src/lythonic/__init__.py`, add the `_NsRefBase` class BEFORE the existing `GlobalRef` class (after the `log = logging.getLogger(__name__)` line):

```python
class _NsRefBase:
    """
    Base reference type: parses "scope.path:name" format.

    >>> ref = _NsRefBase("market.data:fetch")
    >>> ref.scope
    ['market', 'data']
    >>> ref.name
    'fetch'
    >>> str(ref)
    'market.data:fetch'
    >>> _NsRefBase("leaf")
    _NsRefBase('leaf')
    >>> _NsRefBase(":leaf")
    _NsRefBase('leaf')
    """

    scope: list[str]
    name: str

    def __init__(self, s: Any) -> None:
        if isinstance(s, _NsRefBase):
            self.scope, self.name = list(s.scope), s.name
        elif isinstance(s, str):
            if ":" in s:
                scope_str, self.name = s.rsplit(":", 1)
                self.scope = scope_str.split(".") if scope_str else []
            else:
                self.scope = []
                self.name = s
        else:
            raise TypeError(f"Cannot create {type(self).__name__} from {type(s).__name__}")

    @override
    def __str__(self) -> str:
        if self.scope:
            return f"{'.'.join(self.scope)}:{self.name}"
        if self.name:
            return self.name
        return ":"

    @override
    def __repr__(self) -> str:
        # Use the public alias name for repr
        cls_name = "NsRef" if type(self) is _NsRefBase else type(self).__name__
        return f"{cls_name}({repr(str(self))})"

    @override
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, _NsRefBase):
            return self.scope == other.scope and self.name == other.name
        if isinstance(other, str):
            return str(self) == other
        return NotImplemented

    @override
    def __hash__(self) -> int:
        return hash((tuple(self.scope), self.name))

    @override
    def __ne__(self, other: Any) -> bool:
        return not self == other

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,  # pyright: ignore[reportUnusedParameter]
    ) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            lambda v: v if isinstance(v, cls) else cls(v),
            serialization=core_schema.plain_serializer_function_ser_schema(str),
        )


NsRef = Annotated[
    _NsRefBase,
    WithJsonSchema({"type": "string"}, mode="serialization"),
]
```

Also add `NsRef` to the module docstring's list and to any `__all__` if present.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_gref.py -k nsref -v`
Expected: All 7 new tests PASS

- [ ] **Step 5: Run full lint and test suite**

Run: `make lint && make test`
Expected: All pass (existing tests unaffected)

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/__init__.py tests/test_gref.py
git commit -m "feat: add _NsRefBase class and NsRef annotated alias"
```

---

### Task 2: Refactor `GlobalRef` to extend `_NsRefBase`

**Files:**
- Modify: `src/lythonic/__init__.py`
- Test: `tests/test_gref.py` (existing tests must still pass)

- [ ] **Step 1: Write a test for GlobalRef as NsRef subclass**

Append to `tests/test_gref.py`:

```python
def test_globalref_is_nsref_subclass():
    from lythonic import GlobalRef
    from lythonic import _NsRefBase

    gref = GlobalRef("lythonic.compose:Namespace")
    assert isinstance(gref, _NsRefBase)
    assert gref.scope == ["lythonic", "compose"]
    assert gref.name == "Namespace"
    assert str(gref) == "lythonic.compose:Namespace"


def test_globalref_from_module_uses_scope():
    import json

    from lythonic import GlobalRef
    from lythonic import _NsRefBase

    gref = GlobalRef(json)
    assert isinstance(gref, _NsRefBase)
    assert gref.scope == ["json"]
    assert gref.name == ""
    assert gref.is_module()


def test_globalref_from_class_uses_scope():
    from lythonic import GlobalRef

    gref = GlobalRef(GlobalRef)
    # GlobalRef is defined in "lythonic" module
    assert gref.scope == ["lythonic"]
    assert gref.name == "GlobalRef"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_gref.py::test_globalref_is_nsref_subclass -v`
Expected: FAIL — `GlobalRef` doesn't have `.scope` attribute yet

- [ ] **Step 3: Refactor `GlobalRef` to `_GlobalRefBase(_NsRefBase)`**

Replace the existing `GlobalRef` class and `GRef` alias in `src/lythonic/__init__.py` with:

```python
class _GlobalRefBase(_NsRefBase):
    """
    >>> ref = _GlobalRefBase('lythonic:GlobalRef')
    >>> ref
    GlobalRef('lythonic:GlobalRef')
    >>> ref.get_instance().__name__
    '_GlobalRefBase'
    >>> ref.is_module()
    False
    >>> ref.get_module().__name__
    'lythonic'
    >>> grgr = _GlobalRefBase(_GlobalRefBase)
    >>> grgr
    GlobalRef('lythonic:_GlobalRefBase')
    >>> grgr.get_instance()
    <class 'lythonic._GlobalRefBase'>
    >>> grgr.is_class()
    True
    >>> grgr.is_function()
    False
    >>> grgr.is_module()
    False
    >>> uref = _GlobalRefBase('lythonic:')
    >>> uref.is_module()
    True
    >>> uref.get_module().__name__
    'lythonic'
    >>> uref = _GlobalRefBase('lythonic')
    >>> uref.is_module()
    True
    >>> uref = _GlobalRefBase(uref)
    >>> uref.is_module()
    True
    >>> uref.get_module().__name__
    'lythonic'
    >>> uref = _GlobalRefBase(uref.get_module())
    >>> uref.is_module()
    True
    >>> uref.get_module().__name__
    'lythonic'
    """

    def __init__(self, s: Any) -> None:
        if isinstance(s, _NsRefBase):
            self.scope, self.name = list(s.scope), s.name
        elif ismodule(s):
            self.scope, self.name = s.__name__.split("."), ""
        elif isclass(s) or isfunction(s):
            self.scope, self.name = s.__module__.split("."), s.__name__
        elif isinstance(s, str):
            if ":" in s:
                scope_str, self.name = s.rsplit(":", 1)
                self.scope = scope_str.split(".") if scope_str else []
            else:
                # Module-only reference (no colon means entire string is module path)
                self.scope = s.split(".")
                self.name = ""
        else:
            raise TypeError(f"Cannot create GlobalRef from {type(s).__name__}")

    @override
    def __repr__(self) -> str:
        return f"GlobalRef({repr(str(self))})"

    def get_module(self) -> ModuleType:
        module_path = ".".join(self.scope)
        return __import__(module_path, fromlist=[""])

    def is_module(self) -> bool:
        return not self.name

    def is_class(self) -> bool:
        return not (self.is_module()) and isclass(self.get_instance())

    def is_function(self) -> bool:
        return not (self.is_module()) and isfunction(self.get_instance())

    def is_async(self) -> bool:
        if self.is_module():
            return False
        if self.is_class():
            return iscoroutinefunction(self.get_instance().__call__)
        return iscoroutinefunction(self.get_instance())

    def get_instance(self) -> Any:
        """
        Resolve the named attribute from the module. If the name ends
        with `__` and the attribute doesn't exist, strip the suffix and
        call the resulting factory function instead.

        The `__` suffix convention supports lazy initialization: define
        a factory function `xyz()` that returns an object, and reference
        it as `"module:xyz__"`. If the module defines `xyz__` directly
        (e.g., for caching an immutable result), that takes priority.
        For mutable objects, omit the cached variable so the factory is
        called fresh each time.
        """
        assert not self.is_module(), f"{repr(self)}.get_module() only"
        module = self.get_module()
        try:
            return getattr(module, self.name)
        except AttributeError:
            if self.name.endswith("__"):
                factory_name = self.name[:-2]
                factory = getattr(module, factory_name)
                return factory()
            raise


GlobalRef = Annotated[
    _GlobalRefBase,
    WithJsonSchema({"type": "string"}, mode="serialization"),
]
```

Remove the old `GRef` definition entirely.

**Important:** The old `GlobalRef` class had a `module: str` attribute. Code that accesses `.module` on a GlobalRef instance must now use `".".join(ref.scope)` or a compatibility property. Check if any code uses `.module` directly — if so, add a property:

```python
    @property
    def module(self) -> str:
        """Backward compat: returns dot-joined scope as module path."""
        return ".".join(self.scope)
```

- [ ] **Step 4: Run all existing GlobalRef tests**

Run: `uv run pytest tests/test_gref.py -v`
Expected: All pass (old and new tests)

- [ ] **Step 5: Run full lint and test suite**

Run: `make lint && make test`
Expected: All pass. If `.module` attribute access fails anywhere, add the
`module` property as noted above.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/__init__.py tests/test_gref.py
git commit -m "refactor: GlobalRef extends _NsRefBase, replace GRef with GlobalRef alias"
```

---

### Task 3: Replace `GRef` with `GlobalRef` across the codebase

**Files:**
- Modify: `src/lythonic/types.py`
- Modify: `src/lythonic/compose/namespace.py`
- Modify: `src/lythonic/__init__.py` (module docstring)
- Modify: `CLAUDE.md` (documentation reference)

- [ ] **Step 1: Update `src/lythonic/types.py`**

Change line 116:
```python
from lythonic import GlobalRef, GRef, str_or_none
```
to:
```python
from lythonic import GlobalRef, _GlobalRefBase, str_or_none
```

Change line 128:
```python
    type_gref: GRef | None = None
```
to:
```python
    type_gref: GlobalRef | None = None
```

- [ ] **Step 2: Update `src/lythonic/compose/namespace.py`**

Change the import (line 171):
```python
from lythonic import GlobalRef, GRef
```
to:
```python
from lythonic import GlobalRef, NsRef, _GlobalRefBase, _NsRefBase
```

Change `NsNodeConfig` docstring and field (lines 374, 379):
```python
    """
    Serializable configuration for a namespace node. The `type` field
    acts as a discriminator for Pydantic deserialization — subclasses
    set a literal `type` value so `model_validate` picks the right class.
    """

    type: str = "auto"
    nsref: str
    gref: GlobalRef | None = None
    tags: list[str] | None = None
    triggers: list[TriggerConfig] = []
```

Change `TriggerConfig.poll_fn` field (line 365):
```python
    poll_fn: GlobalRef | None = None
```

- [ ] **Step 3: Update module docstring in `__init__.py`**

Change line 6:
```python
- `GlobalRef` / `GRef`: Reference any Python object by its module path (e.g., `"mymodule:MyClass"`).
```
to:
```python
- `GlobalRef`: Reference any Python object by its module path (e.g., `"mymodule:MyClass"`).
- `NsRef`: Reference a namespace node by scope and name (e.g., `"market.data:fetch_prices"`).
```

- [ ] **Step 4: Run full lint and test suite**

Run: `make lint && make test`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git add src/lythonic/__init__.py src/lythonic/types.py src/lythonic/compose/namespace.py
git commit -m "refactor: replace GRef with GlobalRef throughout codebase"
```

---

### Task 4: Add `ModuleRef` class

**Files:**
- Modify: `src/lythonic/__init__.py`
- Test: `tests/test_gref.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_gref.py`:

```python
from lythonic import ModuleRef


def test_moduleref_from_string():
    ref = ModuleRef("lythonic.compose.namespace")
    assert ref.path == "lythonic.compose.namespace"
    assert str(ref) == "lythonic.compose.namespace"
    mod = ref.import_module()
    assert mod.__name__ == "lythonic.compose.namespace"


def test_moduleref_from_module():
    import json

    ref = ModuleRef(json)
    assert ref.path == "json"
    mod = ref.import_module()
    assert mod is json


def test_moduleref_from_globalref():
    from lythonic import _GlobalRefBase

    gref = _GlobalRefBase("lythonic.compose:Namespace")
    ref = ModuleRef(gref)
    assert ref.path == "lythonic.compose"


def test_moduleref_repr():
    ref = ModuleRef("json")
    assert repr(ref) == "ModuleRef('json')"


def test_moduleref_equality():
    a = ModuleRef("json")
    b = ModuleRef("json")
    c = ModuleRef("os")
    assert a == b
    assert a != c
    assert hash(a) == hash(b)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_gref.py::test_moduleref_from_string -v`
Expected: FAIL — `ModuleRef` not importable

- [ ] **Step 3: Implement `ModuleRef`**

In `src/lythonic/__init__.py`, add after the `GlobalRef` alias definition:

```python
class ModuleRef:
    """
    Reference to a Python module by its import path.

    >>> ref = ModuleRef("json")
    >>> ref.path
    'json'
    >>> ref.import_module().__name__
    'json'
    """

    path: str

    def __init__(self, s: Any) -> None:
        if isinstance(s, ModuleRef):
            self.path = s.path
        elif isinstance(s, _NsRefBase):
            self.path = ".".join(s.scope)
        elif ismodule(s):
            self.path = s.__name__
        elif isinstance(s, str):
            self.path = s
        else:
            raise TypeError(f"Cannot create ModuleRef from {type(s).__name__}")

    def import_module(self) -> ModuleType:
        return __import__(self.path, fromlist=[""])

    @override
    def __str__(self) -> str:
        return self.path

    @override
    def __repr__(self) -> str:
        return f"ModuleRef({repr(self.path)})"

    @override
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ModuleRef):
            return self.path == other.path
        if isinstance(other, str):
            return self.path == other
        return NotImplemented

    @override
    def __hash__(self) -> int:
        return hash(self.path)

    @override
    def __ne__(self, other: Any) -> bool:
        return not self == other

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,  # pyright: ignore[reportUnusedParameter]
    ) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            lambda v: v if isinstance(v, ModuleRef) else ModuleRef(v),
            serialization=core_schema.plain_serializer_function_ser_schema(str),
        )
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_gref.py -k moduleref -v`
Expected: All 5 new tests PASS

- [ ] **Step 5: Run full lint and test suite**

Run: `make lint && make test`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/__init__.py tests/test_gref.py
git commit -m "feat: add ModuleRef class for module-only references"
```

---

### Task 5: Use `NsRef` in namespace.py — remove `_parse_nsref()`

**Files:**
- Modify: `src/lythonic/compose/namespace.py`
- Test: `tests/test_namespace.py` (existing tests must pass)

- [ ] **Step 1: Write a test verifying NsRef in NsNodeConfig**

Append to `tests/test_namespace.py`:

```python
def test_nsnode_config_accepts_nsref_string():
    """NsNodeConfig.nsref field accepts string and stores it."""
    from lythonic.compose.namespace import NsNodeConfig

    config = NsNodeConfig(nsref="market.data:fetch")
    # Should be usable as NsRef
    from lythonic import NsRef, _NsRefBase

    ref = _NsRefBase(config.nsref)
    assert ref.scope == ["market", "data"]
    assert ref.name == "fetch"
```

- [ ] **Step 2: Remove `_parse_nsref()` and replace usages with `_NsRefBase`**

In `src/lythonic/compose/namespace.py`:

Delete the `_parse_nsref` function (lines 213-233 including doctest).

Replace all call sites. There are two usages:

1. Line ~698 (in `_all_leaves` or `_find_by_leaf`):
```python
            _, leaf = _parse_nsref(nsref)
```
Replace with:
```python
            leaf = _NsRefBase(nsref).name
```

2. Line ~890 (in `DagRunnerNamespace._source_args`):
```python
            _, leaf = _parse_nsref(ns_node.nsref)
```
Replace with:
```python
            leaf = _NsRefBase(ns_node.nsref).name
```

- [ ] **Step 3: Run full lint and test suite**

Run: `make lint && make test`
Expected: All pass (doctests for `_parse_nsref` are gone but new NsRef
doctests in `__init__.py` cover the same parsing)

- [ ] **Step 4: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace.py
git commit -m "refactor: remove _parse_nsref, use NsRef directly in namespace.py"
```

---

### Task 6: Update `DagContext.dag_nsref` type and verify backward compat

**Files:**
- Modify: `src/lythonic/compose/namespace.py` (DagContext class)
- Test: existing tests must pass

- [ ] **Step 1: Update DagContext**

In `src/lythonic/compose/namespace.py`, change the `DagContext` class (line 208):

```python
class DagContext(BaseModel):
    """
    Context injected into DAG node callables that declare it as first parameter.
    Subclass to add domain-specific fields.
    """

    dag_nsref: NsRef  # type: ignore[assignment]
    node_label: str
    run_id: str
```

Note: `NsRef` is the annotated alias (`Annotated[_NsRefBase, ...]`). Pydantic
will accept strings and serialize to strings. Add `# type: ignore[assignment]`
only if pyright complains about the annotation.

- [ ] **Step 2: Run full lint and test suite**

Run: `make lint && make test`
Expected: All pass. DagContext is constructed with string values throughout
the codebase — Pydantic's validator will coerce them to `_NsRefBase` instances.
Existing code that reads `ctx.dag_nsref` as a string should still work due to
`__eq__` supporting string comparison.

- [ ] **Step 3: Commit**

```bash
git add src/lythonic/compose/namespace.py
git commit -m "refactor: DagContext.dag_nsref typed as NsRef"
```

---

### Task 7: Final verification and cleanup

**Files:**
- Modify: `src/lythonic/__init__.py` (ensure exports)
- Modify: `CLAUDE.md` (update GRef reference)

- [ ] **Step 1: Verify exports**

Ensure `src/lythonic/__init__.py` has these names accessible at module level:
- `_NsRefBase` (class)
- `_GlobalRefBase` (class)
- `NsRef` (annotated alias)
- `GlobalRef` (annotated alias)
- `ModuleRef` (class)

The old `GRef` name must NOT exist.

- [ ] **Step 2: Update CLAUDE.md**

In `CLAUDE.md`, find the section about `GRef`:
```
- Use `GRef` (from `lythonic`) for `GlobalRef` fields in Pydantic models.
  `GRef` is `Annotated[GlobalRef, ...]` that accepts both `str` and `GlobalRef` as
  input and serializes to string. Use it instead of `str` for callable reference fields
  in config models (e.g., `NsNodeConfig.gref: GRef | None`).
```

Replace with:
```
- Use `GlobalRef` (from `lythonic`) for global reference fields in Pydantic models.
  Accepts both `str` and `GlobalRef` as input, serializes to string. Use for
  callable reference fields (e.g., `NsNodeConfig.gref: GlobalRef | None`).
- Use `NsRef` (from `lythonic`) for namespace reference fields in Pydantic models.
  Accepts both `str` and `NsRef` as input, serializes to string.
```

- [ ] **Step 3: Run full lint and test suite**

Run: `make lint && make test`
Expected: 0 errors, all tests pass

- [ ] **Step 4: Run doctests explicitly**

Run: `uv run pytest --doctest-modules src/lythonic/__init__.py -v`
Expected: All doctests pass (NsRefBase and GlobalRefBase doctests)

- [ ] **Step 5: Commit**

```bash
git add src/lythonic/__init__.py CLAUDE.md
git commit -m "chore: update exports and CLAUDE.md for NsRef/GlobalRef hierarchy"
```

---
