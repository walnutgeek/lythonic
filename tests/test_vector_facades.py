# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false
from __future__ import annotations

import pytest

from lythonic.universe import Universe


def test_array_out_is_float64_in_universe_order():
    numpy = pytest.importorskip("numpy")
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a", "b"]), values=[1.0, 2.0])
    arr = v.np.array()
    assert arr.dtype == numpy.float64
    assert arr.tolist() == [1.0, 2.0]


def test_array_in_pairs_with_a_universe():
    numpy = pytest.importorskip("numpy")
    from lythonic.vector import KeyedVector

    v = KeyedVector.np.from_array(numpy.array([1.0, 2.0]), ["a", "b"])
    assert v == KeyedVector(universe=Universe(["a", "b"]), values=[1.0, 2.0])


def test_array_in_rejects_a_mismatched_shape():
    numpy = pytest.importorskip("numpy")
    from lythonic.vector import KeyedVector

    with pytest.raises(ValueError, match="does not match"):
        KeyedVector.np.from_array(numpy.array([1.0, 2.0, 3.0]), ["a", "b"])


def test_array_in_rejects_a_two_dimensional_array():
    numpy = pytest.importorskip("numpy")
    from lythonic.vector import KeyedVector

    with pytest.raises(ValueError, match="does not match"):
        KeyedVector.np.from_array(numpy.array([[1.0], [2.0]]), ["a", "b"])


def test_array_round_trip_preserves_non_finite_values():
    pytest.importorskip("numpy")
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a", "b"]), values=[float("nan"), 2.0])
    assert KeyedVector.np.from_array(v.np.array(), v.universe) == v


def test_series_out_indexes_by_the_universe():
    pytest.importorskip("pandas")
    from lythonic.vector import KeyedVector

    s = KeyedVector(universe=Universe(["a", "b"]), values=[1.0, 2.0]).pd.series()
    assert list(s.index) == ["a", "b"]
    assert s.tolist() == [1.0, 2.0]
    assert s["b"] == 2.0


def test_series_in_preserves_index_order():
    pandas = pytest.importorskip("pandas")
    from lythonic.vector import KeyedVector

    v = KeyedVector.pd.from_series(pandas.Series([2.0, 1.0], index=["b", "a"]))
    assert list(v.universe) == ["b", "a"]
    assert v.values == [2.0, 1.0]


def test_series_in_rejects_a_duplicated_index():
    pandas = pytest.importorskip("pandas")
    from lythonic.vector import KeyedVector

    with pytest.raises(ValueError, match="duplicate"):
        KeyedVector.pd.from_series(pandas.Series([1.0, 2.0], index=["a", "a"]))


def test_series_in_rejects_a_non_string_index():
    pandas = pytest.importorskip("pandas")
    from lythonic.vector import KeyedVector

    with pytest.raises(TypeError, match="string"):
        KeyedVector.pd.from_series(pandas.Series([1.0, 2.0], index=[1, 2]))


def test_series_round_trip():
    pytest.importorskip("pandas")
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a", "b"]), values=[1.0, 2.0])
    assert KeyedVector.pd.from_series(v.pd.series()) == v
