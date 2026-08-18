# pyright: reportUnknownParameterType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportReturnType=false
"""
Library-agnostic tabular data model.

`FrameData` is a Pydantic model that stores columnar data in a simple
row-oriented format (`columns` + `data`). It serializes to/from JSON
without any DataFrame library dependency.

Conversions live behind one facade per library, named for the library's
conventional import alias. Class access gives the inbound constructors and
instance access the outbound conversions:

```python
fd = FrameData.pd.from_frame(df)    # pandas DataFrame in
df = fd.pd.frame()                  # pandas DataFrame out

fd = FrameData.pl.from_frame(df)    # polars DataFrame in
df = fd.pl.frame()                  # polars DataFrame out

fd = FrameData.pa.from_table(tbl)   # pyarrow Table in
tbl = fd.pa.table()                 # pyarrow Table out
```

Each facade lazily imports its library on first access, so importing this
module costs nothing when no optional dependency is installed. Reaching for a
library that is not installed raises `ImportError` naming the package.

>>> fd = FrameData(columns=["a", "b"], data=[[1, 2], [3, 4]])
>>> fd.columns
['a', 'b']
>>> fd.data
[[1, 2], [3, 4]]
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel

from lythonic.facade import LibAccess, require

if TYPE_CHECKING:
    import pandas as pd  # pyright: ignore[reportMissingImports]
    import polars as pl  # pyright: ignore[reportMissingImports]
    import pyarrow as pa  # pyright: ignore[reportMissingImports]


class PdIn:
    """Class-access pandas facade: constructors that take a pandas DataFrame."""

    _owner: type[FrameData]
    _pandas: ModuleType

    def __init__(self, owner: type[FrameData]) -> None:
        self._owner = owner
        self._pandas = require("pandas")

    def from_frame(self, df: pd.DataFrame) -> FrameData:
        """Build `FrameData` from a pandas DataFrame."""
        if not isinstance(df, self._pandas.DataFrame):
            raise TypeError(f"Expected pandas.DataFrame, got {type(df).__name__}")
        t: dict[str, Any] = df.to_dict(orient="tight", index=False)
        return self._owner(columns=t["columns"], data=t["data"])


class PdOut:
    """Instance-access pandas facade: pandas views of the data."""

    _fd: FrameData
    _pandas: ModuleType

    def __init__(self, owner: FrameData) -> None:
        self._fd = owner
        self._pandas = require("pandas")

    def frame(self) -> pd.DataFrame:
        """The data as a pandas DataFrame."""
        fd = self._fd
        return self._pandas.DataFrame.from_dict(
            {
                **fd.model_dump(mode="json"),
                "index": list(range(len(fd.data))),
                "index_names": [""],
                "column_names": [None],
            },
            orient="tight",
        )


class PlIn:
    """Class-access polars facade: constructors that take a polars DataFrame."""

    _owner: type[FrameData]
    _polars: ModuleType

    def __init__(self, owner: type[FrameData]) -> None:
        self._owner = owner
        self._polars = require("polars")

    def from_frame(self, df: pl.DataFrame) -> FrameData:
        """Build `FrameData` from a polars DataFrame."""
        if not isinstance(df, self._polars.DataFrame):
            raise TypeError(f"Expected polars.DataFrame, got {type(df).__name__}")
        return self._owner(columns=df.columns, data=[list(row) for row in df.rows()])


class PlOut:
    """Instance-access polars facade: polars views of the data."""

    _fd: FrameData
    _polars: ModuleType

    def __init__(self, owner: FrameData) -> None:
        self._fd = owner
        self._polars = require("polars")

    def frame(self) -> pl.DataFrame:
        """The data as a polars DataFrame."""
        fd = self._fd
        return self._polars.DataFrame(fd.data, schema=fd.columns, orient="row")


class PaIn:
    """Class-access pyarrow facade: constructors that take a pyarrow Table."""

    _owner: type[FrameData]
    _pyarrow: ModuleType

    def __init__(self, owner: type[FrameData]) -> None:
        self._owner = owner
        self._pyarrow = require("pyarrow")

    def from_table(self, table: pa.Table) -> FrameData:
        """Build `FrameData` from a pyarrow Table."""
        if not isinstance(table, self._pyarrow.Table):
            raise TypeError(f"Expected pyarrow.Table, got {type(table).__name__}")
        col_dict = table.to_pydict()
        columns = table.column_names
        data = [[col_dict[c][i] for c in columns] for i in range(table.num_rows)]
        return self._owner(columns=columns, data=data)


class PaOut:
    """Instance-access pyarrow facade: Arrow views of the data."""

    _fd: FrameData
    _pyarrow: ModuleType

    def __init__(self, owner: FrameData) -> None:
        self._fd = owner
        self._pyarrow = require("pyarrow")

    def table(self) -> pa.Table:
        """The data as a pyarrow Table."""
        fd = self._fd
        col_dict = {c: [row[i] for row in fd.data] for i, c in enumerate(fd.columns)}
        return self._pyarrow.table(col_dict)


class FrameData(BaseModel):
    """
    Serializable tabular data container. Library-agnostic wire format
    for passing table data between components.

    One facade per DataFrame library: `pd`, `pl`, `pa`. Class access gives
    the inbound constructors (`FrameData.pd.from_frame(df)`), instance access
    the outbound conversions (`fd.pd.frame()`). Each facade lazily imports its
    library and raises `ImportError` if it is not installed.
    """

    columns: list[str]
    data: list[list[Any]]

    pd: ClassVar[LibAccess[FrameData, PdIn, PdOut]] = LibAccess(PdIn, PdOut)
    """Pandas facade. Class access gives constructors, instance access conversions."""

    pl: ClassVar[LibAccess[FrameData, PlIn, PlOut]] = LibAccess(PlIn, PlOut)
    """Polars facade. Class access gives constructors, instance access conversions."""

    pa: ClassVar[LibAccess[FrameData, PaIn, PaOut]] = LibAccess(PaIn, PaOut)
    """Pyarrow facade. Class access gives constructors, instance access conversions."""
