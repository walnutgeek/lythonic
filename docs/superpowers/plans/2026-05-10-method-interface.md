# Method Interface Refactoring — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Separate callable interface description (pure Pydantic data) from callable binding in `Method`, making `GlobalRef` optional and enabling serialization/structural type comparison.

**Architecture:** Introduce `ParamInfo` (Pydantic BaseModel) and `MethodInterface` (Pydantic BaseModel) as the pure-data interface description. Refactor `Method` to compose `MethodInterface` + optional `gref` binding. Move CLI-specific helpers from old `ArgInfo` to `cli.py`. Update all consumers.

**Tech Stack:** Python 3.11+, Pydantic v2, existing `lythonic.compose` module.

---

### Task 1: Add `ParamInfo` and `MethodInterface` to `compose/__init__.py`

**Files:**
- Modify: `src/lythonic/compose/__init__.py`

- [ ] **Step 1: Write tests for `ParamInfo` and `MethodInterface`**

Add after the existing tests at the bottom of `src/lythonic/compose/__init__.py`:

```python
def test_param_info_from_param():
    import inspect

    def sample(x: int, y: str = "hello") -> bool:
        pass

    sig = inspect.signature(sample)
    params = list(sig.parameters.values())

    p0 = ParamInfo.from_param(params[0], origin=sample)
    assert p0.name == "x"
    assert p0.type_str == "int"
    assert p0.annotation is int
    assert p0.is_optional is False
    assert p0.default is None

    p1 = ParamInfo.from_param(params[1], origin=sample)
    assert p1.name == "y"
    assert p1.type_str == "str"
    assert p1.annotation is str
    assert p1.is_optional is True
    assert p1.default == "hello"


def test_method_interface_from_callable():
    def sample(x: int, y: str = "hello") -> bool:
        """A sample function."""
        pass

    iface = MethodInterface.from_callable(sample)
    assert len(iface.params) == 2
    assert iface.params[0].name == "x"
    assert iface.params[1].name == "y"
    assert iface.return_type == "bool"
    assert iface.return_annotation is bool
    assert iface.doc == "A sample function."


def test_method_interface_serializable():
    def sample(x: int, y: str = "hello") -> bool:
        """A sample function."""
        pass

    iface = MethodInterface.from_callable(sample)
    data = iface.model_dump()
    # annotation and return_annotation are excluded
    assert "annotation" not in str(data.get("params", [{}])[0])
    assert "return_annotation" not in data
    # Can round-trip through model_validate
    iface2 = MethodInterface.model_validate(data)
    assert iface2.params[0].name == "x"
    assert iface2.return_type == "bool"
    # Excluded fields are None after deserialization
    assert iface2.return_annotation is None


def test_method_interface_from_pydantic_model():
    from pydantic import BaseModel as BM, Field as F

    class MyModel(BM):
        name: str = F(description="The name")
        count: int = F(default=0, description="How many")

    iface = MethodInterface.from_callable(MyModel)
    assert len(iface.params) == 2
    assert iface.params[0].name == "name"
    assert iface.params[0].description == "The name"
    assert iface.params[0].is_optional is False
    assert iface.params[1].name == "count"
    assert iface.params[1].description == "How many"
    assert iface.params[1].is_optional is True
    assert iface.params[1].default == 0


def test_validate_simple_type_args_on_interface():
    def fetch(ticker: str, year: int) -> dict:
        return {}

    iface = MethodInterface.from_callable(fetch)
    iface.validate_simple_type_args("test_func")  # Should not raise

    def bad(data: bytes) -> dict:
        return {}

    iface_bad = MethodInterface.from_callable(bad)
    try:
        iface_bad.validate_simple_type_args("bad_func")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "data" in str(e)
        assert "simple_type" in str(e)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/lythonic/compose/__init__.py -v -k "test_param_info or test_method_interface or test_validate_simple_type_args_on_interface" 2>&1 | tail -20`
Expected: FAIL — `ParamInfo` and `MethodInterface` not defined yet.

- [ ] **Step 3: Implement `ParamInfo`, `MethodInterface`, and `_type_to_str`**

In `src/lythonic/compose/__init__.py`, add these imports at the top (after existing imports):

```python
from pydantic import Field as PydanticField
```

Add a helper function before `ArgInfo`:

```python
def _type_to_str(annotation: Any) -> str:
    """Convert a type annotation to its canonical string representation."""
    if annotation is None or annotation is inspect.Parameter.empty:
        return "Any"
    if hasattr(annotation, "__name__"):
        return annotation.__name__
    return str(annotation)
```

Add `ParamInfo` class after `_type_to_str` (before `ArgInfo`):

```python
class ParamInfo(BaseModel):
    """Metadata about a function parameter, as a serializable Pydantic model."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    type_str: str = "Any"
    default: Any | None = None
    is_optional: bool = False
    description: str = ""
    annotation: Any = PydanticField(default=None, exclude=True)

    @classmethod
    def from_param(cls, param: inspect.Parameter, origin: Any) -> ParamInfo:
        description = ""
        is_optional = param.default != inspect.Parameter.empty
        default = param.default if is_optional else None
        if isinstance(origin, type) and issubclass(origin, BaseModel):
            if param.name in origin.model_fields:
                field = origin.model_fields[param.name]
                if field.description is not None:
                    description = field.description
                is_optional = not field.is_required()
                default = field.default
        annotation = param.annotation if param.annotation != inspect.Parameter.empty else None
        return cls(
            name=param.name,
            type_str=_type_to_str(annotation),
            annotation=annotation,
            default=default,
            is_optional=is_optional,
            description=description,
        )
```

Add `MethodInterface` class after `ParamInfo`:

```python
class MethodInterface(BaseModel):
    """Pure-data description of a callable's inputs and outputs."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    params: list[ParamInfo]
    return_type: str | None = None
    doc: str | None = None
    return_annotation: Any = PydanticField(default=None, exclude=True)

    @classmethod
    def from_callable(cls, o: Callable[..., Any]) -> MethodInterface:
        sig = inspect.signature(o)
        params = [ParamInfo.from_param(p, origin=o) for p in sig.parameters.values()]
        ret = sig.return_annotation
        return cls(
            params=params,
            return_type=_type_to_str(ret) if ret is not inspect.Parameter.empty else None,
            return_annotation=ret if ret is not inspect.Parameter.empty else None,
            doc=o.__doc__,
        )

    def validate_simple_type_args(self, func_name: str = "<unknown>") -> None:
        """Validate all params have KnownType with simple_type=True."""
        for param in self.params:
            if param.annotation is None:
                raise ValueError(
                    f"Parameter `{param.name}` on `{func_name}` has no type annotation, "
                    f"required for simple_type validation"
                )
            try:
                kt = KNOWN_TYPES.resolve_type(param.annotation)
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"Parameter `{param.name}` on `{func_name}` has type "
                    f"`{getattr(param.annotation, '__name__', str(param.annotation))}` "
                    f"which is not a registered KnownType"
                ) from e
            if not kt.simple_type:
                raise ValueError(
                    f"Parameter `{param.name}` on `{func_name}` has type "
                    f"`{getattr(param.annotation, '__name__', str(param.annotation))}` "
                    f"which is not a simple_type (required for cache key)"
                )
```

Also add `ConfigDict` to imports: `from pydantic import BaseModel, ConfigDict, Field as PydanticField`

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/lythonic/compose/__init__.py -v -k "test_param_info or test_method_interface or test_validate_simple_type_args_on_interface" 2>&1 | tail -20`
Expected: PASS

- [ ] **Step 5: Run full lint and test**

Run: `make lint && make test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/__init__.py
git commit -m "feat: add ParamInfo and MethodInterface Pydantic models"
```

---

### Task 2: Refactor `Method` to use `MethodInterface` with optional `gref`

**Files:**
- Modify: `src/lythonic/compose/__init__.py`

- [ ] **Step 1: Refactor Method class**

Replace the `Method` class (lines 97-191) with:

```python
class Method:
    """
    Wrapper around a callable that provides introspection of its arguments.

    Composes `MethodInterface` (pure data) with an optional callable binding.
    `gref` is `None` for closures, lambdas, and bound methods that aren't
    importable by path.
    """

    interface: MethodInterface
    name: str
    gref: GlobalRef | None
    _callable: Callable[..., Any] | None

    def __init__(self, o: Callable[..., Any] | GlobalRef, *, name: str | None = None):
        if isinstance(o, GlobalRef):
            self.gref = o
            self._callable = None
            self.name = name or o.name
        else:
            try:
                self.gref = GlobalRef(o)
            except (ValueError, TypeError):
                self.gref = None
            self._callable = o
            self.name = name or getattr(o, "__name__", str(o))
        self.interface = MethodInterface.from_callable(self.o)

    @property
    def o(self) -> Callable[..., Any]:
        if self._callable is None:
            assert self.gref is not None
            self._callable = self.gref.get_instance()
        assert self._callable is not None
        return self._callable

    @property
    def args(self) -> list[ParamInfo]:
        return self.interface.params

    @property
    def args_by_name(self) -> dict[str, ParamInfo]:
        return {p.name: p for p in self.interface.params}

    @property
    def return_annotation(self) -> Any | None:
        return self.interface.return_annotation

    @property
    def doc(self) -> str | None:
        return self.interface.doc

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.o(*args, **kwargs)

    def validate_simple_type_args(self) -> None:
        """Validate all params have KnownType with simple_type=True."""
        self.interface.validate_simple_type_args(
            str(self.gref) if self.gref else self.name
        )
```

- [ ] **Step 2: Update the old `ArgInfo` — keep it temporarily but unused**

The old `ArgInfo` NamedTuple is still there. Don't delete it yet — `cli.py` still references its methods. It will be removed in Task 4.

- [ ] **Step 3: Update existing tests in `__init__.py`**

The three existing tests (`test_validate_simple_type_args_passes`, `test_validate_simple_type_args_fails_on_non_simple`, `test_validate_simple_type_args_skips_optional`) call `m.validate_simple_type_args()` on `Method` — this still works via delegation. No changes needed.

- [ ] **Step 4: Add test for Method with closure (no gref)**

Add at the bottom of `src/lythonic/compose/__init__.py`:

```python
def test_method_with_closure():
    def make_adder(n: int):
        def adder(x: int) -> int:
            return x + n
        return adder

    add5 = make_adder(5)
    m = Method(add5)
    assert m.gref is None
    assert m.name == "adder"
    assert len(m.args) == 1
    assert m.args[0].name == "x"
    assert m.args[0].annotation is int
    assert m(3) == 8
```

- [ ] **Step 5: Run all tests**

Run: `uv run pytest src/lythonic/compose/__init__.py -v 2>&1 | tail -20`
Expected: PASS

- [ ] **Step 6: Run full lint and test**

Run: `make lint && make test`
Expected: PASS — all downstream consumers still work because `Method.args` returns `ParamInfo` objects which have the same field names (`name`, `annotation`, `default`, `is_optional`, `description`) as `ArgInfo`.

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/compose/__init__.py
git commit -m "feat: refactor Method to compose MethodInterface, optional gref"
```

---

### Task 3: Move CLI helpers from `ArgInfo` to `cli.py`

**Files:**
- Modify: `src/lythonic/compose/cli.py`
- Modify: `src/lythonic/compose/__init__.py`
- Modify: `tests/test_compose.py`

- [ ] **Step 1: Add CLI helper functions to `cli.py`**

Add these functions near the top of `src/lythonic/compose/cli.py`, after the imports and before `RunResult`:

```python
from lythonic.compose import ParamInfo


def param_to_value(param: ParamInfo, v: str) -> Any:
    """Parse a CLI string value using the param's live annotation."""
    if param.annotation is None:
        return v
    if param.annotation is bool:
        return v.lower() in ("true", "1", "yes", "y")
    if issubclass(param.annotation, BaseModel):
        return param.annotation.model_validate_json(v)
    return param.annotation(v)


def is_turn_on_flag(param: ParamInfo) -> bool:
    """True if bool param with default=False (becomes --flag with no value)."""
    return param.annotation is bool and param.default is False


def param_arg_help(param: ParamInfo, indent: int) -> str:
    """Format as positional argument help line."""
    return f"{' ' * indent}<{param.name}> - {param.type_str}: {param.description}"


def param_opt_help(param: ParamInfo, indent: int) -> str:
    """Format as option help line."""
    flag = f"--{param.name}{'=value' if not is_turn_on_flag(param) else ''}"
    return f"{' ' * indent}[{flag}] - {param.type_str}: {param.description}. Default: {param.default!r}"
```

Note: `issubclass` call needs a guard for non-class annotations. Update the function:

```python
def param_to_value(param: ParamInfo, v: str) -> Any:
    """Parse a CLI string value using the param's live annotation."""
    if param.annotation is None:
        return v
    if param.annotation is bool:
        return v.lower() in ("true", "1", "yes", "y")
    if isinstance(param.annotation, type) and issubclass(param.annotation, BaseModel):
        return param.annotation.model_validate_json(v)
    return param.annotation(v)
```

- [ ] **Step 2: Update `ActionTree._run_args` to use new helpers**

In `cli.py`, replace usages of `arg.to_value(...)`, `arg.is_turn_on_option()` in `_run_args`:

At line 149: `arg_values[arg.name] = arg.to_value(arg_str)` -> `arg_values[arg.name] = param_to_value(arg, arg_str)`

At line 167: `if arg.is_turn_on_option():` -> `if is_turn_on_flag(arg):`

At line 174: `arg_values[k] = arg.to_value(v)` -> `arg_values[k] = param_to_value(arg, v)`

- [ ] **Step 3: Update `RunContext._recursive_help` to use new helpers**

At line 326: `self.print(arg.arg_help(indent))` -> `self.print(param_arg_help(arg, indent))`

At line 328: `self.print(opt.opt_help(indent))` -> `self.print(param_opt_help(opt, indent))`

- [ ] **Step 4: Update `test_compose.py` to use new helpers for direct assertions**

In `tests/test_compose.py`, the test at line 32 calls `z.args[0].arg_help(0)` and line 41 calls `main.args[0].opt_help(0)`. These need to use the new functions:

Add import: `from lythonic.compose.cli import param_arg_help, param_opt_help`

Change line 32: `assert z.args[0].arg_help(0) == ...` -> `assert param_arg_help(z.args[0], 0) == ...`

Change line 41: `assert main.args[0].opt_help(0) == ...` -> `assert param_opt_help(main.args[0], 0) == ...`

Also update the expected string for `param_arg_help` since `type_str` is `"Config"` not `type` property — should produce identical output: `"<config> - Config: "`. And for opt_help: `"[--help] - bool: Show help. Default: False"` — same output since `type_str` for `bool` is `"bool"`.

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_compose.py -v 2>&1 | tail -20`
Expected: PASS

- [ ] **Step 6: Run full lint and test**

Run: `make lint && make test`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/compose/cli.py tests/test_compose.py
git commit -m "refactor: move CLI helpers from ArgInfo to cli.py free functions"
```

---

### Task 4: Delete old `ArgInfo` NamedTuple

**Files:**
- Modify: `src/lythonic/compose/__init__.py`
- Modify: `tests/test_compose.py`

- [ ] **Step 1: Check for remaining ArgInfo imports**

Run: `uv run python -c "from lythonic.compose import ArgInfo; print('still imported')"` to verify it's still there.

Search for all `ArgInfo` references: `rg "ArgInfo" src/ tests/`

- [ ] **Step 2: Remove `ArgInfo` class from `compose/__init__.py`**

Delete the entire `ArgInfo` NamedTuple class (lines 36-94 in the original file). This includes `from_param`, `to_value`, `is_turn_on_option`, `type` property, `arg_help`, and `opt_help`.

- [ ] **Step 3: Update exports**

If `ArgInfo` is referenced in any `__all__` or re-exported, remove those references. Check for `from lythonic.compose import ArgInfo` in test files and update to `from lythonic.compose import ParamInfo`.

In `tests/test_compose.py`, if `ArgInfo` is imported, change to `ParamInfo`. Looking at the current imports, line 7 imports `Method` but not `ArgInfo` directly — the test accesses `z.args[0].name` etc which are now `ParamInfo` fields. No import change needed unless `ArgInfo` is explicitly imported.

- [ ] **Step 4: Run full lint and test**

Run: `make lint && make test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lythonic/compose/__init__.py tests/test_compose.py
git commit -m "refactor: remove old ArgInfo NamedTuple, replaced by ParamInfo"
```

---

### Task 5: Update `cached.py` — table naming and validate call

**Files:**
- Modify: `src/lythonic/compose/cached.py`

- [ ] **Step 1: Update `mount_cached_node` — gref.is_async() and validate call**

In `src/lythonic/compose/cached.py`, at line 421:
`method.validate_simple_type_args()` — this still works via `Method.validate_simple_type_args()` delegation. No change needed.

At lines 443-444:
```python
    gref = node.method.gref
    if gref.is_async():
```
Change to:
```python
    import asyncio as _asyncio
    if _asyncio.iscoroutinefunction(node.method.o):
```

(Add `import asyncio` at the top of the file if not already present, then use it directly instead of the inline import.)

- [ ] **Step 2: Run tests**

Run: `make lint && make test`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/lythonic/compose/cached.py
git commit -m "fix: use iscoroutinefunction instead of gref.is_async in cached.py"
```

---

### Task 6: Update `namespace.py` — gref guards and fragment registration

**Files:**
- Modify: `src/lythonic/compose/namespace.py`

- [ ] **Step 1: Fix `DagContext.ns_call` — gref.is_async()**

At line 372:
```python
        if node.method.gref.is_async():
```
Change to:
```python
        import asyncio as _asyncio
        if _asyncio.iscoroutinefunction(node.method.o):
```

(Or add `import asyncio` at the top if not already there and use it.)

- [ ] **Step 2: Fix `NamespaceNode.__init__` — guard optional gref**

At line 633:
```python
            gref=method.gref if method.gref else None,
```
This already handles `None` correctly. No change needed — `method.gref` can be `None` and the conditional already produces `None`.

- [ ] **Step 3: Fix fragment registration — remove fake GlobalRef hack**

At lines 1049-1050:
```python
                method_obj = Method(GlobalRef(f"__fragment__:{name}"))
                method_obj._o = method_callable  # pyright: ignore[reportPrivateUsage]
```
Replace with:
```python
                method_obj = Method(method_callable, name=name)
```

This uses the new `Method` constructor that accepts any callable and sets `gref=None` when it can't create a `GlobalRef`.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_namespace.py -v 2>&1 | tail -30`
Expected: PASS

- [ ] **Step 5: Run full lint and test**

Run: `make lint && make test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py
git commit -m "fix: remove gref.is_async, fix fragment registration to use Method(callable)"
```

---

### Task 7: Update module docstrings and public exports

**Files:**
- Modify: `src/lythonic/compose/__init__.py`

- [ ] **Step 1: Update module docstring**

Update the module docstring at the top of `src/lythonic/compose/__init__.py` to reflect the new types:

```python
"""
Compose: Build typed callable compositions from annotated functions.

This module provides introspection and composition primitives for building
higher-level structures (CLIs, pipelines, workflows) from type-annotated
callables.

## Core Concepts

- `ParamInfo`: Pydantic model describing a single function parameter.
- `MethodInterface`: Pydantic model describing a callable's full signature
  (parameters, return type, docstring). Serializable and comparable.
- `Method`: Wrapper that composes `MethodInterface` with an optional callable
  binding (`GlobalRef` or direct callable reference).
- `MethodDict`: Dictionary of methods indexed by name.
"""
```

- [ ] **Step 2: Run lint**

Run: `make lint`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/lythonic/compose/__init__.py
git commit -m "docs: update compose module docstring for ParamInfo/MethodInterface"
```

---

### Task 8: Final verification

**Files:** None (verification only)

- [ ] **Step 1: Run full lint**

Run: `make lint`
Expected: PASS with zero warnings/errors.

- [ ] **Step 2: Run full test suite**

Run: `make test`
Expected: PASS with zero failures.

- [ ] **Step 3: Verify closure support works**

Run: `uv run python -c "
from lythonic.compose import Method
def make():
    def inner(x: int) -> str: return str(x)
    return inner
m = Method(make())
print('gref:', m.gref)
print('name:', m.name)
print('args:', m.args)
print('call:', m(42))
print('interface json:', m.interface.model_dump_json(indent=2))
"`
Expected: `gref: None`, correct args, call returns `"42"`, JSON serialization works.

- [ ] **Step 4: Verify serialization round-trip**

Run: `uv run python -c "
from lythonic.compose import MethodInterface
def sample(x: int, y: str = 'hi') -> bool: pass
iface = MethodInterface.from_callable(sample)
j = iface.model_dump_json()
print('JSON:', j)
iface2 = MethodInterface.model_validate_json(j)
print('Params:', [(p.name, p.type_str) for p in iface2.params])
print('Return:', iface2.return_type)
"`
Expected: JSON output, round-trip preserves param names/types and return type.
