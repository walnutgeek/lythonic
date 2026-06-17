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

Conversion methods for pandas, polars, and pyarrow are available but
lazily import their respective libraries. If a library isn't installed,
calling its conversion method raises `ImportError`.

>>> fd = FrameData(columns=["a", "b"], data=[[1, 2], [3, 4]])
>>> fd.columns
['a', 'b']
>>> fd.data
[[1, 2], [3, 4]]
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

if TYPE_CHECKING:
    import pandas as pd  # pyright: ignore[reportMissingImports]
    import polars as pl  # pyright: ignore[reportMissingImports]
    import pyarrow as pa  # pyright: ignore[reportMissingImports]


class FrameData(BaseModel):
    """
    Serializable tabular data container. Library-agnostic wire format
    for passing table data between components.

    Use `from_pandas`/`to_pandas`, `from_polars`/`to_polars`, or
    `from_arrow`/`to_arrow` to convert to/from DataFrame libraries.
    Each method lazily imports its library and raises `ImportError`
    if it's not installed.
    """

    columns: list[str]
    data: list[list[Any]]

    # -- pandas --

    @staticmethod
    def from_pandas(df: pd.DataFrame) -> FrameData:
        """Convert a pandas DataFrame to FrameData."""
        import pandas  # pyright: ignore[reportMissingImports]  # noqa: F811

        if not isinstance(df, pandas.DataFrame):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError(f"Expected pandas.DataFrame, got {type(df).__name__}")
        t: dict[str, Any] = df.to_dict(orient="tight", index=False)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        return FrameData(columns=t["columns"], data=t["data"])

    def to_pandas(self) -> pd.DataFrame:
        """Convert FrameData to a pandas DataFrame."""
        import pandas  # pyright: ignore[reportMissingImports]  # noqa: F811

        return pandas.DataFrame.from_dict(
            {
                **self.model_dump(mode="json"),
                "index": list(range(len(self.data))),
                "index_names": [""],
                "column_names": [None],
            },
            orient="tight",
        )

    # -- polars --

    @staticmethod
    def from_polars(df: pl.DataFrame) -> FrameData:
        """Convert a polars DataFrame to FrameData."""
        import polars  # pyright: ignore[reportMissingImports]  # noqa: F811

        if not isinstance(df, polars.DataFrame):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError(f"Expected polars.DataFrame, got {type(df).__name__}")
        return FrameData(columns=df.columns, data=df.rows())  # pyright: ignore[reportArgumentType]

    def to_polars(self) -> pl.DataFrame:
        """Convert FrameData to a polars DataFrame."""
        import polars  # pyright: ignore[reportMissingImports]  # noqa: F811

        return polars.DataFrame(self.data, schema=self.columns, orient="row")

    # -- pyarrow --

    @staticmethod
    def from_arrow(table: pa.Table) -> FrameData:
        """Convert a pyarrow Table to FrameData."""
        import pyarrow  # pyright: ignore[reportMissingImports]  # noqa: F811

        if not isinstance(table, pyarrow.Table):
            raise TypeError(f"Expected pyarrow.Table, got {type(table).__name__}")
        col_dict = table.to_pydict()
        columns = table.column_names
        n_rows = table.num_rows
        data = [[col_dict[c][i] for c in columns] for i in range(n_rows)]
        return FrameData(columns=columns, data=data)

    def to_arrow(self) -> pa.Table:
        """Convert FrameData to a pyarrow Table."""
        import pyarrow  # pyright: ignore[reportMissingImports]  # noqa: F811

        col_dict = {c: [row[i] for row in self.data] for i, c in enumerate(self.columns)}
        return pyarrow.table(col_dict)
