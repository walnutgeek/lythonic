# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
from __future__ import annotations

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


def test_pandas_roundtrip():
    pd = __import__("pytest").importorskip("pandas")

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
    fd = FrameData.from_pandas(df)
    assert fd.columns == ["a", "b"]
    assert fd.data == [[1, 4.0], [2, 5.0], [3, 6.0]]
    restored = fd.to_pandas()
    pd.testing.assert_frame_equal(restored, df, check_names=False)


def test_pandas_json_roundtrip():
    pd = __import__("pytest").importorskip("pandas")

    df = pd.DataFrame({"x": [10, 20], "y": ["foo", "bar"]})
    fd = FrameData.from_pandas(df)
    fd2 = FrameData.model_validate_json(fd.model_dump_json())
    pd.testing.assert_frame_equal(fd2.to_pandas(), df, check_names=False)
    assert fd == fd2


def test_polars_roundtrip():
    pl = __import__("pytest").importorskip("polars")

    df = pl.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
    fd = FrameData.from_polars(df)
    assert fd.columns == ["a", "b"]
    assert fd.data == [[1, 4.0], [2, 5.0], [3, 6.0]]
    restored = fd.to_polars()
    assert restored.columns == ["a", "b"]
    assert restored.rows() == [(1, 4.0), (2, 5.0), (3, 6.0)]


def test_polars_json_roundtrip():
    pl = __import__("pytest").importorskip("polars")

    df = pl.DataFrame({"x": [10, 20], "y": ["foo", "bar"]})
    fd = FrameData.from_polars(df)
    fd2 = FrameData.model_validate_json(fd.model_dump_json())
    restored = fd2.to_polars()
    assert restored.columns == df.columns
    assert restored.rows() == df.rows()


def test_arrow_roundtrip():
    pa = __import__("pytest").importorskip("pyarrow")

    table = pa.table({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
    fd = FrameData.from_arrow(table)
    assert fd.columns == ["a", "b"]
    assert fd.data == [[1, 4.0], [2, 5.0], [3, 6.0]]
    restored = fd.to_arrow()
    assert restored.column_names == ["a", "b"]
    assert restored.to_pydict() == {"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}


def test_arrow_json_roundtrip():
    pa = __import__("pytest").importorskip("pyarrow")

    table = pa.table({"x": [10, 20], "y": ["foo", "bar"]})
    fd = FrameData.from_arrow(table)
    fd2 = FrameData.model_validate_json(fd.model_dump_json())
    restored = fd2.to_arrow()
    assert restored.column_names == table.column_names
    assert restored.to_pydict() == table.to_pydict()
