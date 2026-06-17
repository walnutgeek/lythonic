# FrameData: Library-Agnostic Tabular Data Model

## Summary

Move `FrameData` from cointoss into `lythonic.frame` as a Pydantic model for
serializable tabular data. Provide library-specific conversion methods for
pandas, polars, and pyarrow via lazy imports — no hard dependency on any of
them.

## Data Model

```python
class FrameData(BaseModel):
    columns: list[str]
    data: list[list[Any]]
```

Pure Pydantic model. Serializable to/from JSON. This is the wire format for
tabular data.

## Conversion Methods

Each pair does a lazy import inside the method body. Type annotations use
`TYPE_CHECKING` guard. Calling a method when its library isn't installed
raises `ImportError`.

```python
# pandas
@staticmethod
def from_pandas(df: pd.DataFrame) -> FrameData: ...
def to_pandas(self) -> pd.DataFrame: ...

# polars
@staticmethod
def from_polars(df: pl.DataFrame) -> FrameData: ...
def to_polars(self) -> pl.DataFrame: ...

# pyarrow
@staticmethod
def from_arrow(table: pa.Table) -> FrameData: ...
def to_arrow(self) -> pa.Table: ...
```

### pandas conversion detail

`from_pandas` uses `df.to_dict(orient="tight", index=False)` and extracts
`columns` and `data`. `to_pandas` reconstructs via
`pd.DataFrame.from_dict(..., orient="tight")`.

### polars conversion detail

`from_polars` uses `df.columns` and `df.rows()`. `to_polars` uses
`pl.DataFrame(data, schema=columns, orient="row")`.

### pyarrow conversion detail

`from_arrow` uses `table.column_names` and iterates `table.to_pydict()` to
build row-oriented data. `to_arrow` builds column-oriented dict and calls
`pa.table(...)`.

## KnownType Registration

Register `FrameData` as a `KnownType` in `lythonic.types` with
`json_type=dict`, using `FrameData.model_validate` / `model_dump` for the
JSON conversions. This resolves the existing TODO at `types.py:664`.

## File Layout

- `src/lythonic/frame.py` — `FrameData` class with all conversion methods
- `src/lythonic/types.py` — `KnownType` registration for `FrameData`

## Testing

All tests in `tests/test_frame.py`:

- JSON roundtrip test (no library dependency)
- Library-specific `from_*/to_*` roundtrip tests guarded by
  `pytest.importorskip("pandas")` / `"polars"` / `"pyarrow"`
