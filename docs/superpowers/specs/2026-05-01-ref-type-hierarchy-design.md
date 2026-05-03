# Reference Type Hierarchy: NSRef, GlobalRef, ModuleRef

## Problem

`GlobalRef` combines two concerns: module importing and name-within-scope
resolution. `NSRef` isn't even a class — it's a string format parsed by
`_parse_nsref()` in namespace.py. Both share the `"scope:name"` pattern
with dot-hierarchical scope, but there's no shared abstraction.

## Solution

Split into three types in `lythonic/__init__.py`:

- **NSRef** — base class: parses `"scope.path:name"`, provides string repr
  and Pydantic schema
- **GlobalRef(NSRef)** — adds Python module import and object resolution
- **ModuleRef** — standalone class for module-only references

## Design

### NSRef (base class)

```python
class NSRef:
    scope: list[str]   # dot-hierarchical path (left of colon)
    name: str          # leaf identifier (right of colon)
```

**Parsing from string:**
- `"branch.path:leaf"` → scope=`["branch", "path"]`, name=`"leaf"`
- `"leaf"` (no colon) → scope=`[]`, name=`"leaf"`
- `":leaf"` → scope=`[]`, name=`"leaf"`
- `"branch.path:"` → scope=`["branch", "path"]`, name=`""`

**Constructor accepts:** `str`, another `NSRef`, or `GlobalRef` (copy).

**`__str__`:** returns `"scope.parts:name"` (or just `"name"` if scope is empty).

**Pydantic integration:** accepts `str` or `NSRef`, serializes to string.
Same `__get_pydantic_core_schema__` pattern as current `GlobalRef`.

**Equality/hashing:** based on `(tuple(scope), name)` so usable as dict keys.

### GlobalRef(NSRef)

Extends NSRef. The `scope` is interpreted as a Python module path.

```python
class GlobalRef(NSRef):
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

**Relationship to GlobalRef:** `GlobalRef.get_module()` can internally create
a `ModuleRef` from its scope, or just inline the import. No inheritance
relationship — `ModuleRef` is a sibling, not parent/child.

### Pydantic Annotations

`NSRef` gets Pydantic schema support (`__get_pydantic_core_schema__`) so it
can be used directly in model fields. Accepts `str` or `NSRef` input,
serializes to string.

```python
NsRef = Annotated[
    NSRef,
    WithJsonSchema({"type": "string"}, mode="serialization"),
]

GRef = Annotated[
    GlobalRef,
    WithJsonSchema({"type": "string"}, mode="serialization"),
]
```

### Migration in namespace.py

- `_parse_nsref()` removed — `NSRef` replaces it everywhere.
- `NsNodeConfig.nsref` typed as `NsRef` (accepts str input, stores NSRef).
- `Namespace._nodes` dict key: `NSRef`. Lookup methods accept `str | NSRef`.
- `DagContext.dag_nsref` typed as `NsRef`.
- All internal code that previously called `_parse_nsref(s)` uses
  `NSRef(s).scope` / `NSRef(s).name` directly.

### File Changes

1. **`src/lythonic/__init__.py`** — define `NSRef`, refactor `GlobalRef` to
   extend it, add `ModuleRef`, keep `GRef` annotation
2. **`src/lythonic/compose/namespace.py`** — remove `_parse_nsref()`, use
   `NSRef` directly where branch parsing is needed
3. **Tests** — existing `GlobalRef` tests should continue passing (backward
   compat). Add tests for `NSRef` parsing and `ModuleRef`.

## Verification

```bash
make lint
make test
```
