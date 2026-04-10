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
