"""
Core utilities for the Lythonic library.

This module provides foundational types and utilities used throughout Lythonic:

- `GlobalRef`: Reference any Python object by its module path (e.g., `"mymodule:MyClass"`).
  Useful for configuration files and lazy loading.
- `Result[TOk, TErr]`: A Rust-inspired Result type for explicit error handling without exceptions.
- `utc_now()`: Get the current UTC datetime.
- `get_module()`: Import a module by name.

## GlobalRef Usage

```python
from lythonic import GlobalRef

# Reference a class by string
ref = GlobalRef("json:dumps")
dumps_func = ref.get_instance()

# Reference from an object
ref = GlobalRef(MyClass)
print(ref)  # "mymodule:MyClass"
```

## Result Usage

```python
from lythonic import Result

def divide(a: int, b: int) -> Result[float, str]:
    if b == 0:
        return Result.Err("division by zero")
    return Result.Ok(a / b)

result = divide(10, 2)
if result.is_ok():
    print(result.unwrap())  # 5.0
```
"""

import logging
import sys
from datetime import UTC, datetime
from inspect import isclass, iscoroutinefunction, isfunction, ismodule
from types import ModuleType
from typing import Any, Generic, TypeVar, final

from pydantic import (
    GetCoreSchemaHandler,
)
from pydantic_core import CoreSchema, core_schema
from typing_extensions import override

log = logging.getLogger(__name__)


class _NsRefBase:
    """
    Base class for "scope:name" references with dot-separated scope paths.

    Parses strings of the form `"branch.path:leaf"` into a scope list and a
    name. The colon is the separator between scope and name; if absent, scope
    is empty.

    >>> ref = _NsRefBase("market.data:fetch_prices")
    >>> ref.scope
    ['market', 'data']
    >>> ref.name
    'fetch_prices'
    >>> str(ref)
    'market.data:fetch_prices'
    >>> repr(ref)
    "NsRef('market.data:fetch_prices')"
    >>> _NsRefBase("fetch_prices").scope
    []
    >>> str(_NsRefBase("fetch_prices"))
    'fetch_prices'
    >>> str(_NsRefBase(":fetch_prices"))
    'fetch_prices'
    >>> _NsRefBase("branch.path:") == _NsRefBase("branch.path:")
    True
    >>> str(_NsRefBase("branch.path:"))
    'branch.path:'
    """

    scope: list[str]
    name: str

    def __init__(self, s: "_NsRefBase | str") -> None:
        if isinstance(s, _NsRefBase):
            self.scope, self.name = list(s.scope), s.name
        else:
            if ":" in s:
                scope_part, self.name = s.split(":", 1)
                self.scope = scope_part.split(".") if scope_part else []
            else:
                self.scope = []
                self.name = s

    @override
    def __str__(self) -> str:
        if not self.scope:
            return self.name
        scope_str = ".".join(self.scope)
        return f"{scope_str}:{self.name}"

    @override
    def __repr__(self) -> str:
        return f"NsRef({repr(str(self))})"

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
            lambda v: v if isinstance(v, _NsRefBase) else _NsRefBase(v),
            serialization=core_schema.plain_serializer_function_ser_schema(str),
        )


NsRef = _NsRefBase


class _GlobalRefBase(_NsRefBase):
    """
    A reference to a Python object identified by module path and name.

    Accepts a string `"module.path:name"`, a module, a class, a function,
    or another `_GlobalRefBase` instance. Module-only references use an empty name.

    The string representation is always `"module.path:name"` (or `"module.path:"`
    for module-only refs), matching `_NsRefBase` format where scope is the module
    path parts.

    >>> ref = _GlobalRefBase('lythonic:_GlobalRefBase')
    >>> ref
    GlobalRef('lythonic:_GlobalRefBase')
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

    scope: list[str]
    name: str

    def __init__(self, s: Any) -> None:  # pyright: ignore[reportMissingSuperCall]
        if isinstance(s, _NsRefBase):
            self.scope, self.name = list(s.scope), s.name
        elif ismodule(s):
            self.scope = s.__name__.split(".")
            self.name = ""
        elif isclass(s) or isfunction(s):
            self.scope = s.__module__.split(".")
            self.name = s.__name__
        else:
            split = s.split(":")
            if len(split) == 1:
                assert bool(split[0]), f"is {repr(s)} empty?"
                # Module-only string: split on "." for scope, no name
                self.scope = split[0].split(".")
                self.name = ""
            else:
                assert len(split) == 2, f"too many ':' in: {repr(s)}"
                scope_str, self.name = split
                self.scope = scope_str.split(".") if scope_str else []

    @property
    def module(self) -> str:
        """Backward compat: dot-joined scope as module path."""
        return ".".join(self.scope)

    @override
    def __repr__(self) -> str:
        return f"GlobalRef({repr(str(self))})"

    def get_module(self) -> ModuleType:
        return __import__(self.module, fromlist=[""])

    def is_module(self) -> bool:
        return not (self.name)

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

    @override
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,  # pyright: ignore[reportUnusedParameter]
    ) -> CoreSchema:
        # Accept both str and _GlobalRefBase, normalize to _GlobalRefBase
        return core_schema.no_info_plain_validator_function(
            lambda v: v if isinstance(v, _GlobalRefBase) else _GlobalRefBase(v),
            serialization=core_schema.plain_serializer_function_ser_schema(str),
        )


GlobalRef = _GlobalRefBase


def get_module(name: str) -> ModuleType:
    """
    >>> type(get_module('lythonic'))
    <class 'module'>
    >>> get_module('lythonic.c99')
    Traceback (most recent call last):
    ...
    ModuleNotFoundError: No module named 'lythonic.c99'
    """
    if name in sys.modules:
        return sys.modules[name]
    return __import__(name, fromlist=[""])


TOk = TypeVar("TOk")
TErr = TypeVar("TErr")


@final
class Result(Generic[TOk, TErr]):
    """
    A generic Result type inspired by Rust, representing either success (Ok) or failure (Err).
    """

    _ok: TOk | None
    _err: TErr | None

    __slots__ = ("_ok", "_err")

    def __init__(self, ok: TOk | None = None, err: TErr | None = None) -> None:
        assert (ok is None and err is not None) or (ok is not None and err is None), (
            "Result can only have one of ok or err set."
        )
        self._ok = ok
        self._err = err

    @classmethod
    def Ok(cls, value: TOk) -> "Result[TOk, TErr]":
        return cls(ok=value)

    @classmethod
    def Err(cls, error: TErr) -> "Result[TOk, TErr]":
        return cls(err=error)

    def is_ok(self) -> bool:
        return self._ok is not None

    def is_err(self) -> bool:
        return self._err is not None

    def ok(self) -> TOk | None:
        return self._ok

    def err(self) -> TErr | None:
        return self._err

    def unwrap(self) -> TOk:
        if self._ok is not None:
            return self._ok
        raise ValueError(f"Called unwrap on Err: {self._err}")

    def unwrap_err(self) -> TErr:
        if self._err is not None:
            return self._err
        raise ValueError(f"Called unwrap_err on Ok: {self._ok}")

    @override
    def __repr__(self):
        if self.is_ok():
            return f"Ok({self._ok!r})"
        else:
            return f"Err({self._err!r})"


def utc_now() -> datetime:
    """return the current time in UTC
    >>> utc_now().tzinfo
    datetime.timezone.utc
    """
    return datetime.now(UTC)


def str_or_none(s: Any) -> str | None:
    """
    >>> str_or_none(None)
    >>> str_or_none(5)
    '5'
    >>> str_or_none('')
    ''
    """
    return str(s) if s is not None else None
