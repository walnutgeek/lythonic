from collections.abc import Callable
from typing import Any, Generic, TypeVar, get_args, get_origin

from pydantic import BaseModel
from typing_extensions import override

from lythonic import Result
from lythonic.annotated import Method
from lythonic.state import Schema

ConfigType = TypeVar("ConfigType", bound=BaseModel)


class Configurable(Generic[ConfigType]):
    """
    A configurable class that takes a BaseModel config in constructor
    """

    config: ConfigType

    def __init__(self, config: ConfigType) -> None:
        self.config = config


def _unpack_base_model_tuple(_type: Any) -> list[type[BaseModel]]:
    assert _type is not None, "Type cannot be None"
    origin = get_origin(_type)
    base_models: list[type[BaseModel]] = []
    if origin is None:
        # Plain BaseModel type
        assert isinstance(_type, type) and issubclass(_type, BaseModel), (
            f"Only BaseModel, Tuple[BaseModel, ...], or BaseModel, Any] allowed as but got {_type}"
        )
        base_models.append(_type)
    elif origin is tuple:
        # Tuple[BaseModel, ...] case
        args = get_args(_type)
        for p, arg in enumerate(args):
            assert isinstance(arg, type) and issubclass(arg, BaseModel), (
                f"Tuple elements must be BaseModel but got {arg} at position {p}"
            )
            base_models.append(arg)
    else:
        raise AssertionError(f"Only BaseModel, or Tuple[BaseModel, ...] allowed as but got {_type}")
    return base_models


class EdgeMethod:
    """
    A edge method is a wrapper around a function that is called when an edge is triggered,
    function  accepts zero or more BaseModel as input and
    returns a BaseModel or Tuple[BaseModel, ...] or Result[BaseModel, Any] as output

    """

    logic: Callable[[Any], Any]
    input_types: list[type[BaseModel]]
    ok_output_types: list[type[BaseModel]]
    err_output_type: Any | None

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
                self.err_output_type = err_type
        else:
            self.ok_output_types.extend(_unpack_base_model_tuple(return_type))

        self.logic = logic


class Kit:
    name: str
    states: dict[str, Schema]
    configurables: dict[str, Configurable[Any]]
    edges: dict[str, EdgeMethod]

    def __init__(self, name: str):
        self.name = name
        self.states = {}
        self.configurables = {}
        self.edges = {}

    @override
    def __str__(self):
        return self.name

    @override
    def __repr__(self):
        return f"Kit({self.name})"
