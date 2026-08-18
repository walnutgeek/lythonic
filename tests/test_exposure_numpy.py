# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from lythonic.exposure import ExposureMatrix

numpy = pytest.importorskip("numpy")


def _sample() -> ExposureMatrix:
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposures("acct1", {"USD": 0.4, "EUR": 0.6})
    b.set_exposures("acct2", {"EUR": 0.9})
    return b.build()


def test_dense_matrix_is_subjects_by_targets():
    arr = _sample().np.matrix()
    assert arr.shape == (2, 2)
    assert arr.dtype == numpy.float64
    numpy.testing.assert_array_equal(arr, [[0.4, 0.6], [0.0, 0.9]])


def test_absent_cells_materialize_as_cell_fill():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder(cell_fill=1.5)
    b.set_exposure("acct1", "USD", 0.4)
    b.set_exposure("acct2", "EUR", 0.9)
    numpy.testing.assert_array_equal(b.build().np.matrix(), [[0.4, 1.5], [1.5, 0.9]])


def test_row_and_column_are_aligned_to_the_universes():
    m = _sample()
    numpy.testing.assert_array_equal(m.np.row("acct1"), [0.4, 0.6])
    numpy.testing.assert_array_equal(m.np.col("EUR"), [0.6, 0.9])


def test_from_matrix_round_trips():
    from lythonic.exposure import ExposureMatrix

    m = _sample()
    restored = ExposureMatrix.np.from_matrix(m.np.matrix(), m.subjects, m.targets)
    assert restored == m


def test_from_matrix_drops_fill_valued_cells():
    from lythonic.exposure import ExposureMatrix

    arr = numpy.array([[0.4, 0.0], [0.0, 0.9]])
    m = ExposureMatrix.np.from_matrix(arr, ["acct1", "acct2"], ["USD", "EUR"])
    assert m.records == [(0, 0, 0.4), (1, 1, 0.9)]


def test_a_product_comes_home_as_a_matrix():
    from lythonic.exposure import ExposureMatrix

    m = _sample()
    product = m.np.matrix() @ numpy.eye(2)
    assert ExposureMatrix.np.from_matrix(product, m.subjects, m.targets) == m


def test_from_matrix_rejects_a_shape_mismatch():
    from lythonic.exposure import ExposureMatrix

    with pytest.raises(ValueError, match="shape"):
        ExposureMatrix.np.from_matrix(numpy.zeros((2, 3)), ["acct1", "acct2"], ["USD"])


def test_from_matrix_rejects_nan():
    from lythonic.exposure import ExposureMatrix

    with pytest.raises(ValueError, match="NaN"):
        ExposureMatrix.np.from_matrix(numpy.array([[float("nan")]]), ["acct1"], ["USD"])


def test_builder_array_row_requires_a_frozen_target_axis():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    with pytest.raises(ValueError, match="frozen"):
        b.np.set_exposures("acct1", numpy.array([0.4]))


def test_builder_array_writers_reject_nan():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder(subjects=["acct1"], targets=["USD"])
    with pytest.raises(ValueError, match="NaN"):
        b.np.set_exposures("acct1", numpy.array([float("nan")]))
    with pytest.raises(ValueError, match="NaN"):
        b.np.set_matrix(numpy.array([[float("nan")]]))


def test_builder_array_row_writes_by_position():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder(targets=["USD", "EUR"])
    b.np.set_exposures("acct1", numpy.array([0.4, 0.6]))
    assert b.build().exposures_of("acct1") == {"USD": 0.4, "EUR": 0.6}


def test_builder_set_matrix_requires_both_axes_frozen():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder(targets=["USD"])
    with pytest.raises(ValueError, match="frozen"):
        b.np.set_matrix(numpy.array([[0.4]]))


def test_builder_set_matrix_populates_the_whole_matrix():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder(subjects=["acct1", "acct2"], targets=["USD", "EUR"])
    b.np.set_matrix(numpy.array([[0.4, 0.6], [0.0, 0.9]]))
    m = b.build()
    assert m.exposures_of("acct1") == {"USD": 0.4, "EUR": 0.6}
    assert m.exposures_of("acct2") == {"EUR": 0.9}
