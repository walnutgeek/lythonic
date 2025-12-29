"""
Core utilities for the Lythonic library.

This module provides foundational types and utilities used throughout Lythonic:

- `GlobalRef` / `GRef`: Reference any Python object by its module path (e.g., `"mymodule:MyClass"`).
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
from typing import Annotated, Any, Generic, TypeVar, final

from pydantic import (
    AfterValidator,
    GetCoreSchemaHandler,
    PlainSerializer,
    WithJsonSchema,
)
from pydantic_core import CoreSchema, core_schema
from typing_extensions import override

log = logging.getLogger(__name__)


class GlobalRef:
    """
    >>> ref = GlobalRef('lythonic:GlobalRef')
    >>> ref
    GlobalRef('lythonic:GlobalRef')
    >>> ref.get_instance().__name__
    'GlobalRef'
    >>> ref.is_module()
    False
    >>> ref.get_module().__name__
    'lythonic'
    >>> grgr = GlobalRef(GlobalRef)
    >>> grgr
    GlobalRef('lythonic:GlobalRef')
    >>> grgr.get_instance()
    <class 'lythonic.GlobalRef'>
    >>> grgr.is_class()
    True
    >>> grgr.is_function()
    False
    >>> grgr.is_module()
    False
    >>> uref = GlobalRef('lythonic:')
    >>> uref.is_module()
    True
    >>> uref.get_module().__name__
    'lythonic'
    >>> uref = GlobalRef('lythonic')
    >>> uref.is_module()
    True
    >>> uref = GlobalRef(uref)
    >>> uref.is_module()
    True
    >>> uref.get_module().__name__
    'lythonic'
    >>> uref = GlobalRef(uref.get_module())
    >>> uref.is_module()
    True
    >>> uref.get_module().__name__
    'lythonic'
    """

    module: str
    name: str

    def __init__(self, s: Any) -> None:
        if isinstance(s, GlobalRef):
            self.module, self.name = s.module, s.name
        elif ismodule(s):
            self.module, self.name = s.__name__, ""
        elif isclass(s) or isfunction(s):
            self.module, self.name = s.__module__, s.__name__
        else:
            split = s.split(":")
            if len(split) == 1:
                assert bool(split[0]), f"is {repr(s)} empty?"
                split.append("")
            else:
                assert len(split) == 2, f"too many ':' in: {repr(s)}"
            self.module, self.name = split

    @override
    def __str__(self):
        return f"{self.module}:{self.name}"

    @override
    def __repr__(self):
        return f"{self.__class__.__name__}({repr(str(self))})"

    @override
    def __eq__(self, other: Any) -> bool:
        return str(self) == str(other)

    @override
    def __hash__(self) -> int:
        return hash(str(self))

    @override
    def __ne__(self, other: Any) -> bool:
        return not self == other

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
        assert not self.is_module(), f"{repr(self)}.get_module() only"
        attr = getattr(self.get_module(), self.name)
        return attr

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(str))


GRef = Annotated[
    GlobalRef,
    PlainSerializer(str, return_type=str),
    AfterValidator(GlobalRef),
    WithJsonSchema({"type": "string"}, mode="serialization"),
]


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
