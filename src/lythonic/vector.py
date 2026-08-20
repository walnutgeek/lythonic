"""
A universe plus one value per key.

`KeyedVector` is the one-dimensional companion to the keyed matrix types:
keyed like a dict, aligned like an array, and serializable with no optional
dependency installed. Every key in the universe has an entry - there is no
absent entry, no fill value, and no sparse form.

>>> v = KeyedVector.from_mapping({"USD": 0.4, "EUR": 0.6})
>>> v.value("EUR")
0.6
>>> v.to_dict()
{'USD': 0.4, 'EUR': 0.6}

`cast` projects onto another universe, which is how a vector is aligned to a
matrix axis or to another vector. Keys outside the new universe are dropped;
keys it introduces are filled with NaN unless another fill is given.

>>> v.cast(["EUR", "JPY"], fill=0.0).to_dict()
{'EUR': 0.6, 'JPY': 0.0}

Unlike `ExposureMatrix` and `SymmetricMatrix`, this type stores NaN and the
infinities. It is a far more general object - whatever a caller has one value
per key of - and computed results legitimately contain non-finite values.
Rejecting them would not prevent them, only move the failure to a later
boundary. See `docs/adr/0005-keyed-vector-admits-non-finite-values.md`, which
also covers the two consequences: non-finite values serialize as `"NaN"`,
`"Infinity"` and `"-Infinity"` strings, and equality is defined here rather
than inherited.

The inherited equality is not merely strict but incoherent - a list of floats
compares elementwise by identity first, so a NaN-carrying vector equals itself
but not an identical copy, and a vector loaded from JSON compares unequal to
the one that was saved. `__eq__` therefore compares universes and then values
positionally with NaN matching NaN, the rule `numpy.array_equal(...,
equal_nan=True)` uses. `SymmetricMatrix` needs none of this, since it admits
no NaN and draws its guarantee from canonical storage.

There is no keyed access by subscript and no iteration over keys or values.
`Universe` already implements the container protocol and is a public field, so
re-exposing it would be trivial delegation - and `v[0]` against
`v.universe[0]` would mean different things. Omitting them also sidesteps the
keys-versus-values coin flip that container protocols on keyed types force on
readers. `BaseModel` does supply an `__iter__` over fields, inherited by every
model here; it yields neither keys nor values, so it raises no ambiguity, but
it cannot be removed.

Conversions live behind one facade per library, as in `lythonic.frame`:

```python
v = KeyedVector.np.from_array(arr, universe)   # numpy array in
arr = v.np.array()                             # numpy array out

v = KeyedVector.pd.from_series(s)              # pandas Series in
s = v.pd.series()                              # pandas Series out
```

Polars and pyarrow are deliberately absent: neither has an index, so any
mapping would be a lossy drop or an invented two-column convention. Callers
who want either can route through `FrameData`.
"""

from __future__ import annotations

from collections.abc import Mapping
from math import nan
from types import ModuleType
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, ConfigDict, model_validator
from typing_extensions import override

from lythonic.facade import LibAccess, require
from lythonic.universe import Universe

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd  # pyright: ignore[reportMissingImports]
    from numpy.typing import NDArray


def _same_value(a: float, b: float) -> bool:
    """Positional equality treating NaN as matching NaN."""
    return a == b or (a != a and b != b)


class VectorNpIn:
    """Class-access numpy facade: constructors that take a dense array."""

    _owner: type[KeyedVector]
    _numpy: ModuleType

    def __init__(self, owner: type[KeyedVector]) -> None:
        self._owner = owner
        self._numpy = require("numpy")

    def from_array(self, arr: NDArray[np.float64], universe: Universe | list[str]) -> KeyedVector:
        """Pair a one-dimensional array with a universe of the same length."""
        universe = Universe(universe)
        if arr.shape != (len(universe),):
            raise ValueError(f"array shape {arr.shape} does not match {len(universe)} keys")
        return self._owner(universe=universe, values=[float(v) for v in arr.tolist()])


class VectorNpOut:
    """Instance-access numpy facade: dense views of a vector."""

    _v: KeyedVector
    _numpy: ModuleType

    def __init__(self, owner: KeyedVector) -> None:
        self._v = owner
        self._numpy = require("numpy")

    def array(self) -> NDArray[np.float64]:
        """The values as a `float64` array, in universe order."""
        return self._numpy.array(self._v.values, dtype=self._numpy.float64)


class VectorPdIn:
    """Class-access pandas facade: constructors that take a pandas Series."""

    _owner: type[KeyedVector]
    _pandas: ModuleType

    def __init__(self, owner: type[KeyedVector]) -> None:
        self._owner = owner
        self._pandas = require("pandas")

    def from_series(self, series: pd.Series) -> KeyedVector:  # pyright: ignore[reportMissingTypeArgument, reportUnknownParameterType]
        """
        Take a Series' index as the universe, preserving its order.

        The index carries no uniqueness or type guarantee of its own, so both
        are checked here rather than left to produce a silently short universe
        or a non-string key downstream.
        """
        keys: list[Any] = list(series.index)  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
        if len(set(keys)) != len(keys):
            raise ValueError("series index has duplicate entries")
        string_keys = [k for k in keys if isinstance(k, str)]
        if len(string_keys) != len(keys):
            raise TypeError("series index must be all string keys")
        values = [float(v) for v in series.tolist()]  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType, reportUnknownArgumentType]
        return self._owner(universe=Universe(string_keys), values=values)  # pyright: ignore[reportUnknownArgumentType]


class VectorPdOut:
    """Instance-access pandas facade: Series views of a vector."""

    _v: KeyedVector
    _pandas: ModuleType

    def __init__(self, owner: KeyedVector) -> None:
        self._v = owner
        self._pandas = require("pandas")

    def series(self) -> pd.Series:  # pyright: ignore[reportMissingTypeArgument, reportUnknownParameterType]
        """The vector as a Series indexed by the universe."""
        v = self._v
        return self._pandas.Series(list(v.values), index=list(v.universe), dtype="float64")  # pyright: ignore[reportUnknownMemberType]


class KeyedVector(BaseModel):
    """
    One value per key of a universe, in universe order.

    Immutable. Reads by key raise `KeyError` for a key outside the universe;
    there is no "absent entry" state, so a key that is present always has a
    value.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, ser_json_inf_nan="strings")

    universe: Universe
    values: list[float]

    np: ClassVar[LibAccess[KeyedVector, VectorNpIn, VectorNpOut]] = LibAccess(
        VectorNpIn, VectorNpOut
    )
    """Numpy facade. Class access gives constructors, instance access conversions."""

    pd: ClassVar[LibAccess[KeyedVector, VectorPdIn, VectorPdOut]] = LibAccess(
        VectorPdIn, VectorPdOut
    )
    """Pandas facade. Class access gives constructors, instance access conversions."""

    @model_validator(mode="after")
    def _check_length(self) -> KeyedVector:
        if len(self.values) != len(self.universe):
            raise ValueError(f"{len(self.values)} values for {len(self.universe)} keys")
        return self

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, float]) -> KeyedVector:
        """
        Build from a mapping, taking the universe from its insertion order.

        This is the inverse of `to_dict`, and the bridge from the matrix
        types' dict accessors, which return an entry per key in universe
        order.
        """
        return cls(universe=Universe(mapping.keys()), values=list(mapping.values()))

    def value(self, key: str) -> float:
        """Value at one key."""
        return self.values[self.universe.index(key)]

    def to_dict(self) -> dict[str, float]:
        """The whole vector as a mapping, in universe order."""
        return dict(zip(self.universe, self.values, strict=True))

    def cast(self, universe: Universe | list[str], fill: float = nan) -> KeyedVector:
        """
        A new vector over `universe`, dropping keys outside it.

        Keys the cast introduces take `fill`. The NaN default is what makes
        silent extension acceptable: a fabricated NaN cannot pass for a
        measurement, because any arithmetic touching it yields NaN. Returns
        `self` when the universe is already equal, so a defensive alignment
        call is free.
        """
        target = Universe(universe)
        if target == self.universe:
            return self
        values = [
            self.values[self.universe.index(key)] if key in self.universe else fill
            for key in target
        ]
        return KeyedVector(universe=target, values=values)

    @override
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, KeyedVector):
            return False
        if self.universe != other.universe:
            return False
        return all(_same_value(a, b) for a, b in zip(self.values, other.values, strict=True))
