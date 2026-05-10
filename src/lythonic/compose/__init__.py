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
import typing
from collections.abc import Callable
from typing import Any, ClassVar, Generic, NamedTuple, TypeVar

from pydantic import BaseModel, ConfigDict
from pydantic import Field as PydanticField

from lythonic import GlobalRef
from lythonic.compose._inline import inline as inline
from lythonic.types import KNOWN_TYPES


def _type_to_str(annotation: Any) -> str:
    """Convert a type annotation to its canonical string representation."""
    if annotation is None or annotation is inspect.Parameter.empty:
        return "Any"
    if hasattr(annotation, "__name__"):
        return annotation.__name__
    return str(annotation)


class ParamInfo(BaseModel):
    """Metadata about a function parameter, as a serializable Pydantic model."""

    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    name: str
    type_str: str = "Any"
    default: Any | None = None
    is_optional: bool = False
    description: str = ""
    annotation: Any = PydanticField(default=None, exclude=True)

    @classmethod
    def from_param(
        cls, param: inspect.Parameter, origin: Any, hints: dict[str, Any] | None = None
    ) -> ParamInfo:
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
        # Prefer resolved type hints over raw (possibly stringified) annotations.
        if hints and param.name in hints:
            annotation = hints[param.name]
        elif param.annotation != inspect.Parameter.empty:
            annotation = param.annotation
        else:
            annotation = None
        return cls(
            name=param.name,
            type_str=_type_to_str(annotation),
            annotation=annotation,
            default=default,
            is_optional=is_optional,
            description=description,
        )


class MethodInterface(BaseModel):
    """Pure-data description of a callable's inputs and outputs."""

    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    params: list[ParamInfo]
    return_type: str | None = None
    doc: str | None = None
    return_annotation: Any = PydanticField(default=None, exclude=True)

    @classmethod
    def from_callable(cls, o: Callable[..., Any]) -> MethodInterface:
        sig = inspect.signature(o)
        # Resolve string annotations (from `from __future__ import annotations`) to real types.
        try:
            hints = typing.get_type_hints(o)
        except Exception:
            hints = {}
        params = [ParamInfo.from_param(p, origin=o, hints=hints) for p in sig.parameters.values()]
        ret = hints.get("return", sig.return_annotation)
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
            if isinstance(self.annotation, str):
                return self.annotation
            else:
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

    def validate_simple_type_args(self) -> None:
        """
        Validate that all parameters have type annotations whose `KnownType`
        has `simple_type=True`. Raises `ValueError` if any parameter fails.
        """
        for arg in self.args:
            if arg.annotation is None:
                raise ValueError(
                    f"Parameter `{arg.name}` on `{self.gref}` has no type annotation, "
                    f"required for simple_type validation"
                )
            try:
                kt = KNOWN_TYPES.resolve_type(arg.annotation)
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"Parameter `{arg.name}` on `{self.gref}` has type `{getattr(arg.annotation, '__name__', str(arg.annotation))}` "
                    f"which is not a registered KnownType"
                ) from e
            if not kt.simple_type:
                raise ValueError(
                    f"Parameter `{arg.name}` on `{self.gref}` has type `{getattr(arg.annotation, '__name__', str(arg.annotation))}` "
                    f"which is not a simple_type (required for cache key)"
                )


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


## Tests


def test_validate_simple_type_args_passes():
    def fetch(ticker: str, year: int) -> dict[str, Any]:  # pyright: ignore[reportUnusedParameter]
        return {}

    m = Method(fetch)
    m.validate_simple_type_args()


def test_validate_simple_type_args_fails_on_non_simple():
    def fetch(data: bytes) -> dict[str, Any]:  # pyright: ignore[reportUnusedParameter]
        return {}

    m = Method(fetch)
    try:
        m.validate_simple_type_args()
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "data" in str(e)
        assert "simple_type" in str(e)


def test_validate_simple_type_args_skips_optional():
    def fetch(ticker: str, limit: int = 10) -> dict[str, Any]:  # pyright: ignore[reportUnusedParameter]
        return {}

    m = Method(fetch)
    m.validate_simple_type_args()


def test_param_info_from_param():
    def sample(x: int, y: str = "hello") -> None:  # pyright: ignore[reportUnusedParameter]
        pass

    sig = inspect.signature(sample)
    hints = typing.get_type_hints(sample)
    params = list(sig.parameters.values())

    p0 = ParamInfo.from_param(params[0], origin=sample, hints=hints)
    assert p0.name == "x"
    assert p0.type_str == "int"
    assert p0.annotation is int
    assert p0.is_optional is False
    assert p0.default is None

    p1 = ParamInfo.from_param(params[1], origin=sample, hints=hints)
    assert p1.name == "y"
    assert p1.type_str == "str"
    assert p1.annotation is str
    assert p1.is_optional is True
    assert p1.default == "hello"


def test_method_interface_from_callable():
    def sample(x: int, y: str = "hello") -> bool:  # pyright: ignore[reportUnusedParameter]
        """A sample function."""
        return True

    iface = MethodInterface.from_callable(sample)
    assert len(iface.params) == 2
    assert iface.params[0].name == "x"
    assert iface.params[1].name == "y"
    assert iface.return_type == "bool"
    assert iface.return_annotation is bool
    assert iface.doc == "A sample function."


def test_method_interface_serializable():
    def sample(x: int, y: str = "hello") -> bool:  # pyright: ignore[reportUnusedParameter]
        """A sample function."""
        return True

    iface = MethodInterface.from_callable(sample)
    data = iface.model_dump()
    # annotation and return_annotation are excluded
    for p in data["params"]:
        assert "annotation" not in p
    assert "return_annotation" not in data
    # Can round-trip through model_validate
    iface2 = MethodInterface.model_validate(data)
    assert iface2.params[0].name == "x"
    assert iface2.return_type == "bool"
    # Excluded fields are None after deserialization
    assert iface2.return_annotation is None


def test_method_interface_from_pydantic_model():
    from pydantic import Field as F

    class MyModel(BaseModel):
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
    def fetch(ticker: str, year: int) -> dict[str, Any]:  # pyright: ignore[reportUnusedParameter]
        return {}

    iface = MethodInterface.from_callable(fetch)
    iface.validate_simple_type_args("test_func")  # Should not raise

    def bad(data: bytes) -> dict[str, Any]:  # pyright: ignore[reportUnusedParameter]
        return {}

    iface_bad = MethodInterface.from_callable(bad)
    try:
        iface_bad.validate_simple_type_args("bad_func")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "data" in str(e)
        assert "simple_type" in str(e)
