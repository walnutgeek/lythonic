"""
Compose: Build typed callable compositions from annotated functions.

This module provides introspection and composition primitives for building
higher-level structures (CLIs, pipelines, workflows) from type-annotated
callables.

## Core Concepts

- `Method`: Wrapper around a callable with signature introspection
- `MethodDict`: Dictionary of methods indexed by name
- `ArgInfo`: Metadata about function arguments

## CLI Support

For building command-line interfaces, see `lythonic.compose.cli`:

```python
from lythonic.compose.cli import ActionTree, Main, RunContext
```
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, Generic, NamedTuple, TypeVar

from pydantic import BaseModel

from lythonic import GlobalRef


class ArgInfo(NamedTuple):
    """
    Metadata about a function argument, extracted from signature and Pydantic fields.

    Used to generate CLI argument/option parsing and help text.
    """

    name: str
    annotation: Any | None
    default: Any | None
    is_optional: bool
    description: str

    @classmethod
    def from_param(cls, param: inspect.Parameter, origin: Any):
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
        return cls(
            name=param.name,
            annotation=param.annotation if param.annotation != inspect.Parameter.empty else None,
            default=default,
            is_optional=is_optional,
            description=description,
        )

    def to_value(self, v: str):
        if self.annotation is None:
            return v
        if self.annotation is bool:
            return v.lower() in ("true", "1", "yes", "y")
        if issubclass(self.annotation, BaseModel):
            return self.annotation.model_validate_json(v)
        return self.annotation(v)

    def is_turn_on_option(self) -> bool:
        return self.annotation is bool and self.default is False

    @property
    def type(self) -> str:
        if self.annotation is not None:
            return self.annotation.__name__
        return "str"

    def arg_help(self, indent: int):
        return f"{' ' * indent}<{self.name}> - {self.type}: {self.description}"

    def opt_help(self, indent: int):
        return f"{' ' * indent}[--{self.name}{'=value' if not self.is_turn_on_option() else ''}] - {self.type}: {self.description}. Default: {self.default!r}"


class Method:
    """
    Wrapper around a callable that provides introspection of its arguments.

    Lazily loads the callable via GlobalRef and extracts argument metadata
    from the function signature. Supports both regular functions and Pydantic
    BaseModel classes (using their `__init__` signature).
    """

    gref: GlobalRef
    _o: Callable[..., Any] | None
    _args: list[ArgInfo] | None
    _args_by_name: dict[str, ArgInfo] | None
    _return_annotation: Any | None

    def __init__(self, o: Callable[..., Any] | GlobalRef):
        if isinstance(o, GlobalRef):
            self.gref = o
            self._o = None
        else:
            self.gref = GlobalRef(o)
            assert isinstance(o, Callable), "method instance must be a callable"
            self._o = o
        self._args = None
        self._args_by_name = None
        self._return_annotation = None

    def _update_from_signature(self):
        o = self.o
        sig = inspect.signature(o)
        self._args = [ArgInfo.from_param(param, origin=o) for param in sig.parameters.values()]
        self._args_by_name = {arg.name: arg for arg in self._args}
        self._return_annotation = sig.return_annotation

    @property
    def o(self) -> Callable[..., Any]:
        if self._o is None:
            self._o = self.gref.get_instance()
        assert self._o is not None
        return self._o

    @property
    def args(self) -> list[ArgInfo]:
        if self._args is None:
            self._update_from_signature()
        assert self._args is not None
        return self._args

    @property
    def args_by_name(self) -> dict[str, ArgInfo]:
        if self._args_by_name is None:
            self._update_from_signature()
        assert self._args_by_name is not None
        return self._args_by_name

    @property
    def return_annotation(self) -> Any | None:
        if self._args is None:
            self._update_from_signature()
        return self._return_annotation

    @property
    def name(self):
        return self.gref.name

    @property
    def doc(self):
        return self.o.__doc__

    def __call__(self, *args: Any, **kwargs: Any):
        return self.o(*args, **kwargs)


T = TypeVar("T", bound=Method)


class MethodDict(Generic[T], dict[str, T]):
    """
    Dictionary mapping lowercased method names to Method instances.

    Use `add()` to register a callable, or `wrap()` as a decorator.
    """

    method_type: type[T]

    def __init__(self, method_type: type[T]):
        super().__init__()
        self.method_type = method_type

    def add(self, o: Callable[..., Any]) -> T:
        m = self.method_type(o)
        self[m.name.lower()] = m
        return m

    def wrap(self, o: Callable[..., Any]) -> T:
        return self.add(o)
