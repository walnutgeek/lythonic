# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
from __future__ import annotations

import subprocess
import sys
from textwrap import dedent

import pytest

from lythonic.frame import FrameData


def test_json_roundtrip():
    fd = FrameData(columns=["a", "b"], data=[[1, 2.0], [3, 4.0]])
    fd2 = FrameData.model_validate_json(fd.model_dump_json())
    assert fd == fd2


def test_empty_frame():
    fd = FrameData(columns=["x", "y"], data=[])
    assert fd.model_dump() == {"columns": ["x", "y"], "data": []}
    fd2 = FrameData.model_validate_json(fd.model_dump_json())
    assert fd == fd2


def test_facades_are_not_fields():
    assert set(FrameData.model_fields) == {"columns", "data"}


def test_module_import_pulls_in_no_optional_libraries():
    # In-process this would pass trivially once another test has imported the
    # libraries, so check laziness in a fresh interpreter.
    script = dedent("""
        import sys
        import lythonic.frame
        leaked = [n for n in ("pandas", "polars", "pyarrow", "numpy") if n in sys.modules]
        assert not leaked, leaked
        """).strip()
    subprocess.run([sys.executable, "-c", script], check=True)


def test_pandas_roundtrip():
    pd = pytest.importorskip("pandas")

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
    fd = FrameData.pd.from_frame(df)
    assert fd.columns == ["a", "b"]
    assert fd.data == [[1, 4.0], [2, 5.0], [3, 6.0]]
    restored = fd.pd.frame()
    pd.testing.assert_frame_equal(restored, df, check_names=False)


def test_pandas_json_roundtrip():
    pd = pytest.importorskip("pandas")

    df = pd.DataFrame({"x": [10, 20], "y": ["foo", "bar"]})
    fd = FrameData.pd.from_frame(df)
    fd2 = FrameData.model_validate_json(fd.model_dump_json())
    pd.testing.assert_frame_equal(fd2.pd.frame(), df, check_names=False)
    assert fd == fd2


def test_pandas_empty_frame_roundtrip():
    pytest.importorskip("pandas")

    fd = FrameData(columns=["x", "y"], data=[])
    restored = fd.pd.frame()
    assert list(restored.columns) == ["x", "y"]
    assert len(restored) == 0
    assert FrameData.pd.from_frame(restored) == fd


def test_pandas_wrong_type_raises():
    pytest.importorskip("pandas")

    with pytest.raises(TypeError, match="pandas.DataFrame"):
        FrameData.pd.from_frame([[1, 2]])  # pyright: ignore[reportArgumentType]


def test_polars_roundtrip():
    pl = pytest.importorskip("polars")

    df = pl.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
    fd = FrameData.pl.from_frame(df)
    assert fd.columns == ["a", "b"]
    assert fd.data == [[1, 4.0], [2, 5.0], [3, 6.0]]
    restored = fd.pl.frame()
    assert restored.columns == ["a", "b"]
    assert restored.rows() == [(1, 4.0), (2, 5.0), (3, 6.0)]


def test_polars_json_roundtrip():
    pl = pytest.importorskip("polars")

    df = pl.DataFrame({"x": [10, 20], "y": ["foo", "bar"]})
    fd = FrameData.pl.from_frame(df)
    fd2 = FrameData.model_validate_json(fd.model_dump_json())
    restored = fd2.pl.frame()
    assert restored.columns == df.columns
    assert restored.rows() == df.rows()
    assert fd == fd2


def test_polars_empty_frame_roundtrip():
    pytest.importorskip("polars")

    fd = FrameData(columns=["x", "y"], data=[])
    restored = fd.pl.frame()
    assert restored.columns == ["x", "y"]
    assert restored.height == 0
    assert FrameData.pl.from_frame(restored) == fd


def test_polars_wrong_type_raises():
    pytest.importorskip("polars")

    with pytest.raises(TypeError, match="polars.DataFrame"):
        FrameData.pl.from_frame([[1, 2]])  # pyright: ignore[reportArgumentType]


def test_arrow_roundtrip():
    pa = pytest.importorskip("pyarrow")

    table = pa.table({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
    fd = FrameData.pa.from_table(table)
    assert fd.columns == ["a", "b"]
    assert fd.data == [[1, 4.0], [2, 5.0], [3, 6.0]]
    restored = fd.pa.table()
    assert restored.column_names == ["a", "b"]
    assert restored.to_pydict() == {"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}


def test_arrow_json_roundtrip():
    pa = pytest.importorskip("pyarrow")

    table = pa.table({"x": [10, 20], "y": ["foo", "bar"]})
    fd = FrameData.pa.from_table(table)
    fd2 = FrameData.model_validate_json(fd.model_dump_json())
    restored = fd2.pa.table()
    assert restored.column_names == table.column_names
    assert restored.to_pydict() == table.to_pydict()
    assert fd == fd2


def test_arrow_empty_table_roundtrip():
    pytest.importorskip("pyarrow")

    fd = FrameData(columns=["x", "y"], data=[])
    restored = fd.pa.table()
    assert restored.column_names == ["x", "y"]
    assert restored.num_rows == 0
    assert FrameData.pa.from_table(restored) == fd


def test_arrow_wrong_type_raises():
    pytest.importorskip("pyarrow")

    with pytest.raises(TypeError, match="pyarrow.Table"):
        FrameData.pa.from_table([[1, 2]])  # pyright: ignore[reportArgumentType]
