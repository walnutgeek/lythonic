# Reference Type Hierarchy: NsRef, GlobalRef, ModuleRef

## Problem

`GlobalRef` combines two concerns: module importing and name-within-scope
resolution. Namespace references aren't even a class — just a string format
parsed by `_parse_nsref()` in namespace.py. Both share the `"scope:name"`
pattern with dot-hierarchical scope, but there's no shared abstraction.

## Solution

Split into three types in `lythonic/__init__.py`:

- **`_NsRefBase`** — internal base class: parses `"scope.path:name"`,
  string repr, Pydantic schema, equality/hashing
- **`NsRef`** — annotated alias of `_NsRefBase` for use in Pydantic models
- **`_GlobalRefBase(_NsRefBase)`** — internal class adding module import
  and object resolution
- **`GlobalRef`** — annotated alias of `_GlobalRefBase` (replaces `GRef`)
- **`ModuleRef`** — standalone class for module-only references

## Design

### `_NsRefBase` (internal base class)

```python
class _NsRefBase:
    scope: list[str]   # dot-hierarchical path (left of colon)
    name: str          # leaf identifier (right of colon)
```

**Parsing from string:**
- `"branch.path:leaf"` → scope=`["branch", "path"]`, name=`"leaf"`
- `"leaf"` (no colon) → scope=`[]`, name=`"leaf"`
- `":leaf"` → scope=`[]`, name=`"leaf"`
- `"branch.path:"` → scope=`["branch", "path"]`, name=`""`

**Constructor accepts:** `str`, another `_NsRefBase` (or subclass).

**`__str__`:** returns `"scope.parts:name"` (or just `"name"` if scope is empty).

**Pydantic integration:** `__get_pydantic_core_schema__` accepts `str` or
instance, serializes to string.

**Equality/hashing:** based on `(tuple(scope), name)` so usable as dict keys.

### `_GlobalRefBase(_NsRefBase)` (internal subclass)

Extends `_NsRefBase`. The `scope` is interpreted as a Python module path.

```python
class _GlobalRefBase(_NsRefBase):
    def get_module(self) -> ModuleType: ...
    def get_instance(self) -> Any: ...
    def is_module(self) -> bool: ...
    def is_class(self) -> bool: ...
    def is_function(self) -> bool: ...
    def is_async(self) -> bool: ...
```

**Constructor additionally accepts:** modules, classes, functions (extracts
`__module__` and `__name__`). This is existing behavior.

**`get_module()`:** imports `".".join(self.scope)` via `importlib.import_module`.

**`get_instance()`:** calls `get_module()` then `getattr(mod, name)`. Retains
the `__` factory convention (if name ends with `__`, call the resolved object).

**`is_module()`:** returns `True` when `name == ""`.

### ModuleRef (standalone)

For module-only references. Replaces today's `GlobalRef("module:")` pattern
where the name is empty.

```python
class ModuleRef:
    path: str  # "lythonic.compose.namespace"

    def import_module(self) -> ModuleType: ...
```

**Constructor accepts:** `str` (module path), a module object, or a
`GlobalRef` (takes its scope as the path).

**Pydantic integration:** accepts `str`, serializes to string.

**Relationship to GlobalRef:** `_GlobalRefBase.get_module()` can internally
create a `ModuleRef` from its scope, or just inline the import. No
inheritance relationship — `ModuleRef` is a sibling, not parent/child.

### Public Annotated Aliases

```python
NsRef = Annotated[
    _NsRefBase,
    WithJsonSchema({"type": "string"}, mode="serialization"),
]

GlobalRef = Annotated[
    _GlobalRefBase,
    WithJsonSchema({"type": "string"}, mode="serialization"),
]
```

`NsRef` and `GlobalRef` are the public names used everywhere: type
annotations, Pydantic model fields, isinstance checks, constructors.
The `_Base` classes are internal implementation details.

`GRef` is removed — all usages replaced with `GlobalRef`.

### Migration in namespace.py

- `_parse_nsref()` removed — `NsRef(s)` replaces it everywhere.
- `NsNodeConfig.nsref` typed as `NsRef`.
- `Namespace._nodes` dict key: `NsRef`. Lookup methods accept `str | NsRef`.
- `DagContext.dag_nsref` typed as `NsRef`.
- All internal code that previously called `_parse_nsref(s)` uses
  `NsRef(s).scope` / `NsRef(s).name` directly.

### File Changes

1. **`src/lythonic/__init__.py`** — define `_NsRefBase`, refactor
   `_GlobalRefBase` to extend it, add `ModuleRef`, export `NsRef` and
   `GlobalRef` as annotated aliases. Remove `GRef`.
2. **`src/lythonic/compose/namespace.py`** — remove `_parse_nsref()`, use
   `NsRef` throughout. Change `_nodes` key type.
3. **All files using `GRef`** — replace with `GlobalRef`.
4. **Tests** — existing `GlobalRef` tests should continue passing (backward
   compat). Add tests for `NsRef` parsing and `ModuleRef`.

## Verification

```bash
make lint
make test
```
