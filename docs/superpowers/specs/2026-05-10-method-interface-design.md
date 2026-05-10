# Method Interface Refactoring

## Problem

`Method` in `lythonic.compose` conflates two concerns:

1. **Interface description** -- what a callable looks like (parameters, return
   type, docstring). This is pure data that should be serializable and
   comparable without a live callable.

2. **Binding** -- the actual callable, its name, and optionally a `GlobalRef`
   for lazy loading by import path.

`Method.__init__` always creates a `GlobalRef`, which fails for closures,
lambdas, and inner functions that aren't importable. The workaround in
woodglue's `system_api.py` was to build parallel Pydantic models (`ArgInfo`,
`MethodInfo`) to get a serializable interface description -- essentially
re-extracting what `Method` already knows but can't expose as data.

Current `ArgInfo` is a `NamedTuple` that stores live `annotation` objects
and embeds CLI-specific methods (`to_value`, `arg_help`, `opt_help`). It
mixes introspection data with presentation logic.

## Goals

- Separate interface description (pure Pydantic data) from callable binding.
- Make `GlobalRef` optional on `Method` so closures and bound methods work.
- Enable structural type comparison for DAG edge validation.
- Enable serialization for API documentation / OpenAPI-style schemas.
- Clean break -- no backward-compatibility shims or deprecation periods.

## Non-Goals

- JSON Schema generation for complex type annotations (use string repr).
- Nominal/protocol-based type matching (structural only).
- Changes to `Namespace`, `NamespaceNode`, or `Dag` beyond updating
  their access patterns.

## Design

### New Types

#### `ParamInfo` (Pydantic BaseModel)

Replaces the current `ArgInfo` NamedTuple. Describes one parameter.

```python
class ParamInfo(BaseModel):
    name: str
    type_str: str = "Any"
    default: Any | None = None
    is_optional: bool = False
    description: str = ""

    # Live annotation -- populated from inspect, not serialized.
    annotation: Any = PydanticField(default=None, exclude=True)
```

Fields:

- `name` -- parameter name.
- `type_str` -- canonical string representation of the type annotation
  (e.g. `"int"`, `"dict[str, int]"`, `"str | None"`). Used for
  serialization and structural comparison.
- `default` -- default value, or `None` if required.
- `is_optional` -- whether the parameter has a default.
- `description` -- from Pydantic field metadata if the callable origin
  is a BaseModel, otherwise empty.
- `annotation` -- live Python type object. Excluded from serialization
  but available at runtime for structural type comparison and value
  coercion.

`ParamInfo.from_param(param, origin)` classmethod extracts metadata from
an `inspect.Parameter`, identical logic to current `ArgInfo.from_param`.

CLI-specific methods (`to_value`, `is_turn_on_option`, `arg_help`,
`opt_help`) do NOT live here. They move to `cli.py`.

#### `MethodInterface` (Pydantic BaseModel)

Describes a callable's full signature as pure data.

```python
class MethodInterface(BaseModel):
    params: list[ParamInfo]
    return_type: str | None = None
    doc: str | None = None

    # Live return annotation -- not serialized.
    return_annotation: Any = PydanticField(default=None, exclude=True)
```

Key methods:

- `from_callable(cls, o: Callable) -> MethodInterface` -- classmethod.
  Calls `inspect.signature(o)`, builds `ParamInfo` list, extracts return
  type string and live annotation, captures docstring.

- `validate_simple_type_args(self) -> None` -- moved from `Method`.
  Validates all params have `KnownType` with `simple_type=True`.

Two `MethodInterface` instances with the same params and return type are
equal via Pydantic's `__eq__`. This means two functions with the same
signature share an equal interface object.

#### Helper: `_type_to_str(annotation) -> str`

Converts a live annotation to its canonical string:

- `None` / `Parameter.empty` -> `"Any"`
- Has `__name__` -> `annotation.__name__`
- Otherwise -> `str(annotation)`

This is intentionally simple. Complex generics produce their `repr`
string, which is human-readable and sufficient for structural comparison
and API docs. No attempt at normalization beyond what Python's type
system already provides.

### Refactored `Method`

```python
class Method:
    interface: MethodInterface
    name: str
    gref: GlobalRef | None
    _callable: Callable | None

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
            self.name = name or getattr(o, '__name__', str(o))
        self.interface = MethodInterface.from_callable(self.o)
```

Key changes:

- `gref` is `GlobalRef | None`. `None` when the callable isn't importable
  (closures, lambdas, dynamically created functions).
- `interface` is eagerly populated. No lazy `_update_from_signature`.
  The callable must be loadable at `Method` construction time.
- `name` is a simple string attribute, not derived from `gref`.

Properties for convenience (delegation to `interface`):

- `args -> list[ParamInfo]`: delegates to `interface.params`
- `args_by_name -> dict[str, ParamInfo]`: built from `interface.params`
- `return_annotation -> Any | None`: delegates to `interface.return_annotation`
- `doc -> str | None`: delegates to `interface.doc`
- `o -> Callable`: lazy-loads via `gref` if `_callable` is None

`__call__` delegates to `self.o(...)`.

### Consumer Updates

#### `compose/cli.py`

CLI-specific logic moves here as free functions operating on `ParamInfo`:

```python
def param_to_value(param: ParamInfo, v: str) -> Any:
    """Parse a CLI string value using the param's live annotation."""

def is_turn_on_flag(param: ParamInfo) -> bool:
    """True if bool param with default=False (becomes --flag)."""

def param_arg_help(param: ParamInfo, indent: int) -> str:
    """Format as positional argument help line."""

def param_opt_help(param: ParamInfo, indent: int) -> str:
    """Format as option help line."""
```

`ActionTree._split_ctx_args_opts` and `_run_args` use these functions
and access `self.args` / `self.args_by_name` (delegation properties).

#### `compose/cached.py`

- `generate_cache_table_ddl`, `_cache_lookup`, `_cache_upsert` iterate
  `method.args` -- works via delegation.
- Cache table naming currently uses `method.gref` string. Changes to
  `method.name` (since `gref` may be None).
- `method.validate_simple_type_args()` -> `method.interface.validate_simple_type_args()`.
- `method.o` for calling -- unchanged.

#### `compose/namespace.py`

- `node.method.gref.is_async()` at line 372 -> `asyncio.iscoroutinefunction(node.method.o)`.
- `method.gref` in `NamespaceNode.__init__` config construction: guard
  with `if method.gref else None`.
- `_check_type_compatibility` in `Dag`: accesses `method.return_annotation`
  and `method.args[i].annotation` via delegation -- no changes needed.
- Fragment registration's placeholder `Method(GlobalRef(f"__fragment__:{name}"))` at
  line 1049: replace with `Method(callable, name=name)` using the actual callable.

#### `compose/dag_runner.py`

No changes. Uses `method.args` and `method.o` -- both work via delegation.

#### `compose/trigger.py`

No changes. Uses `node.ns_node.method.args` -- works via delegation.

#### woodglue `system_api.py`

Simplifies substantially:

- `_register_closure` becomes unnecessary -- `Method(closure_fn)` works
  directly since `gref` is now optional.
- `ArgInfo` / `MethodInfo` Pydantic models can be replaced by or derived
  from `ParamInfo` / `MethodInterface`.

### Structural Type Comparison

`_check_type_compatibility` in `Dag` currently compares live `annotation`
objects. This continues to work since `ParamInfo.annotation` carries the
live type (excluded from serialization but available at runtime).

For serialized comparison (e.g., across processes), `type_str` provides
string-based matching. The `_type_to_str` helper ensures consistent
string representation.

### File Layout

All changes in `src/lythonic/compose/`:

| File | Changes |
|------|---------|
| `__init__.py` | Add `ParamInfo`, `MethodInterface`. Refactor `Method`. Delete `ArgInfo` NamedTuple. |
| `cli.py` | Add CLI helper functions for `ParamInfo`. Update `ActionTree`. |
| `cached.py` | `gref` -> `name` for table naming. `validate_simple_type_args` call path. |
| `namespace.py` | `gref.is_async()` -> `iscoroutinefunction()`. Guard optional `gref`. Fix fragment placeholder. |
| `dag_runner.py` | No changes. |
| `trigger.py` | No changes. |

Tests updated in-place (inline tests in `__init__.py`, `test_compose.py`,
`test_namespace.py`).

### Migration Order

1. Add `ParamInfo` and `MethodInterface` to `compose/__init__.py`.
2. Refactor `Method` to compose `MethodInterface`, optional `gref`.
3. Update `cached.py` (table naming, validate call).
4. Update `namespace.py` (async check, gref guards, fragment placeholder).
5. Move CLI helpers from deleted `ArgInfo` to `cli.py`.
6. Delete old `ArgInfo` NamedTuple.
7. Update all tests.
8. Update woodglue `system_api.py` (separate repo, noted for reference).
