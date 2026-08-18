"""
Ordered, duplicate-free sequences of string keys naming one axis of a
keyed matrix.

A `Universe` is immutable and hashable, so checking whether two matrices
share an axis is cheap. It accepts a plain `list[str]` anywhere a
`Universe` is expected and serializes back to a JSON array of strings.

>>> u = Universe(["USD", "EUR"])
>>> len(u), u[0], u.index("EUR")
(2, 'USD', 1)
>>> "JPY" in u
False
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema
from typing_extensions import override


class Universe:
    """
    Ordered, duplicate-free sequence of `str` keys naming one axis.

    Equality and hashing are over the ordered keys, so two universes with
    the same keys in different orders are different universes.
    """

    keys: tuple[str, ...]

    def __init__(self, keys: Iterable[str] | Universe) -> None:
        if isinstance(keys, Universe):
            self.keys = keys.keys
            self._positions = keys._positions
            return
        self.keys = tuple(keys)
        self._positions: dict[str, int] = {k: i for i, k in enumerate(self.keys)}
        if len(self._positions) != len(self.keys):
            dupes = sorted({k for k in self.keys if self.keys.count(k) > 1})
            raise ValueError(f"duplicate keys in universe: {dupes}")

    def __len__(self) -> int:
        return len(self.keys)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys)

    def __contains__(self, key: object) -> bool:
        return key in self._positions

    def __getitem__(self, position: int) -> str:
        return self.keys[position]

    def index(self, key: str) -> int:
        """Position of `key`, raising `KeyError` if it is not in the universe."""
        try:
            return self._positions[key]
        except KeyError:
            raise KeyError(f"{key!r} not in universe") from None

    @override
    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Universe) and self.keys == other.keys

    @override
    def __hash__(self) -> int:
        return hash(self.keys)

    @override
    def __repr__(self) -> str:
        return f"Universe({list(self.keys)!r})"

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,  # pyright: ignore[reportUnusedParameter]
        handler: GetCoreSchemaHandler,  # pyright: ignore[reportUnusedParameter]
    ) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            cls,
            serialization=core_schema.plain_serializer_function_ser_schema(lambda u: list(u.keys)),
        )
