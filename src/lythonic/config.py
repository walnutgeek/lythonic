from typing import Generic, TypeVar

from pydantic import BaseModel

ConfigType = TypeVar("ConfigType", bound=BaseModel)


class Configurable(Generic[ConfigType]):
    """
    A configurable class that takes a BaseModel config in constructor
    """

    config: ConfigType

    def __init__(self, config: ConfigType) -> None:
        self.config = config


# class Kit:
#     name: str
#     states: dict[str, Schema]
#     configurables: dict[str, Configurable[Any]]
#     edges: dict[str, LogicNode]

#     def __init__(self, name: str):
#         self.name = name
#         self.states = {}
#         self.configurables = {}
#         self.edges = {}

#     @override
#     def __str__(self):
#         return self.name

#     @override
#     def __repr__(self):
#         return f"Kit({self.name})"
