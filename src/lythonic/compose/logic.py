"""
Logic: Build typed logic graphs from annotated functions.

This module provides primitives for composing typed functions into logic graphs
where nodes are functions that accept and return Pydantic BaseModels.

## Core Concepts

- `LogicNode`: Wrapper around a function with typed BaseModel inputs/outputs
- `LogicGraph`: A graph of LogicNodes (start/end nodes + named nodes)
- `PossibleTypes`: Union type tracking for error types

## Usage

```python
from pydantic import BaseModel
from lythonic.compose.logic import LogicNode

class Input(BaseModel):
    value: int

class Output(BaseModel):
    result: int

def double(input: Input) -> Output:
    return Output(result=input.value * 2)

node = LogicNode(double)
# node.input_types == [Input]
# node.ok_output_types == [Output]
```

Functions can also return `Result[T, E]` for explicit error handling:

```python
from lythonic import Result

def safe_divide(input: Input) -> Result[Output, ZeroDivisionError]:
    if input.value == 0:
        return Result.Err(ZeroDivisionError())
    return Result.Ok(Output(result=100 // input.value))
```
"""

from __future__ import annotations

from collections.abc import Callable
from types import UnionType
from typing import Any, get_args, get_origin

from pydantic import BaseModel
from typing_extensions import override

from lythonic import Result
from lythonic.compose import Method


def _append_base_model(_type: Any, base_models: list[type[BaseModel]]) -> None:
    assert isinstance(_type, type) and issubclass(_type, BaseModel), (
        f"Only BaseModel, Tuple[BaseModel, ...], or BaseModel, Any] allowed as but got {_type}"
    )
    base_models.append(_type)


def _unpack_base_model_tuple(_type: Any) -> list[type[BaseModel]]:
    assert _type is not None, "Type cannot be None"
    origin = get_origin(_type)
    base_models: list[type[BaseModel]] = []
    if origin is None:
        # Plain BaseModel type
        _append_base_model(_type, base_models)
    elif origin is tuple:
        # Tuple[BaseModel, ...] case
        for arg in get_args(_type):
            _append_base_model(arg, base_models)
    else:
        raise AssertionError(f"Only BaseModel, or Tuple[BaseModel, ...] allowed as but got {_type}")
    return base_models


class PossibleTypes:
    """
    Represents a union of types, tracking which are BaseModels vs exceptions.

    Used to analyze error return types in `Result[T, E]` signatures.

    >>> x = PossibleTypes(BaseModel|BaseException)
    >>> x.which_are_base_models()
    [True, False]
    >>> str(x)
    'BaseModel|BaseException'
    >>> x = PossibleTypes(BaseModel,include_all_exceptions=True)
    >>> x
    PossibleTypes(BaseModel|BaseException)
    >>> all(x.which_are_base_models())
    False
    >>> x = PossibleTypes(BaseModel)
    >>> all(x.which_are_base_models())
    True

    """

    types: list[Any]

    def __init__(self, annotation: Any, include_all_exceptions: bool = False) -> None:
        self.types = []
        origin = get_origin(annotation)
        if origin is UnionType:
            for arg in get_args(annotation):
                self.types.append(arg)
        else:
            self.types.append(annotation)
        if include_all_exceptions:
            self.types.append(BaseException)

    def which_are_base_models(self) -> list[bool]:
        return [isinstance(t, type) and issubclass(t, BaseModel) for t in self.types]

    @override
    def __str__(self):
        return "|".join(t.__name__ if isinstance(t, type) else str(t) for t in self.types)

    @override
    def __repr__(self):
        return f"PossibleTypes({str(self)})"


class LogicNode:
    """
    Wrapper around a typed function for use in logic graphs.

    Analyzes the function signature to extract:
    - `input_types`: List of BaseModel types accepted as arguments
    - `ok_output_types`: List of BaseModel types returned on success
    - `err_output_type`: PossibleTypes for error cases (if using Result)

    The function must accept zero or more BaseModel arguments and return
    either a BaseModel, tuple of BaseModels, or Result[BaseModel, E].
    """

    logic: Callable[[Any], Any]
    input_types: list[type[BaseModel]]
    ok_output_types: list[type[BaseModel]]
    err_output_type: PossibleTypes | None

    def __init__(self, logic: Callable[[Any], Any]) -> None:
        method = Method(logic)
        self.input_types = []
        self.ok_output_types = []
        self.err_output_type = None
        for ai in method.args:
            assert isinstance(ai.annotation, type) and issubclass(ai.annotation, BaseModel), (
                f"Only BaseModel is allowed as input type but got argument {ai}"
            )
            self.input_types.append(ai.annotation)

        return_type = method.return_annotation
        if get_origin(return_type) is Result:
            # Result[BaseModel|tuple[BaseModel, ...], Any] case
            args = get_args(return_type)
            ok_type = args[0] if args else None
            err_type = args[1] if len(args) > 1 else None
            assert ok_type is not None, (
                f"Result[TOk, TErr] requires BaseModel as TOk but got {ok_type}"
            )
            self.ok_output_types.extend(_unpack_base_model_tuple(ok_type))
            # TODO: unpack union type for err_type, and validate it is not a BaseModels, it is probably bare exceptions.
            if err_type is not None:
                self.err_output_type = PossibleTypes(err_type)
        else:
            self.ok_output_types.extend(_unpack_base_model_tuple(return_type))

        self.logic = logic


class LogicGraph:
    """
    A directed graph of LogicNodes representing a workflow.

    Currently a placeholder for future workflow composition features.
    """

    start_node: LogicNode | None
    end_node: LogicNode | None
    nodes: dict[str, LogicNode]

    def __init__(self) -> None:
        self.start_node = None
        self.end_node = None
        self.nodes = {}
