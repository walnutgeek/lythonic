# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from lythonic.universe import Universe

if TYPE_CHECKING:
    from lythonic.symmetric import SymmetricMatrix

numpy = pytest.importorskip("numpy")


def _sample() -> SymmetricMatrix:
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder(universe=Universe(["a", "b", "c"]))
    b.set_diagonal({"a": 1.0, "b": 1.0, "c": 1.0})
    b.set_value("a", "b", 0.5)
    return b.build()


def test_dense_matrix_materializes_symmetry():
    arr = _sample().np.matrix()
    assert arr.shape == (3, 3)
    assert arr.tolist() == [
        [1.0, 0.5, 0.0],
        [0.5, 1.0, 0.0],
        [0.0, 0.0, 1.0],
    ]


def test_diagonal_and_vector_are_arrays():
    m = _sample()
    assert m.np.diagonal().tolist() == [1.0, 1.0, 1.0]
    assert m.np.vector("a").tolist() == [1.0, 0.5, 0.0]


def test_from_matrix_reads_the_lower_triangle():
    from lythonic.symmetric import SymmetricMatrix

    arr = numpy.array([[1.0, 0.5, 0.0], [0.5, 1.0, 0.0], [0.0, 0.0, 1.0]])
    assert SymmetricMatrix.np.from_matrix(arr, Universe(["a", "b", "c"])) == _sample()


def test_from_matrix_ignores_the_upper_triangle():
    from lythonic.symmetric import SymmetricMatrix

    # Deliberately asymmetric: the upper half disagrees and is discarded.
    arr = numpy.array([[1.0, 9.9, 9.9], [0.5, 1.0, 9.9], [0.0, 0.0, 1.0]])
    assert SymmetricMatrix.np.from_matrix(arr, Universe(["a", "b", "c"])) == _sample()


def test_from_matrix_rejects_a_mismatched_shape():
    from lythonic.symmetric import SymmetricMatrix

    with pytest.raises(ValueError, match="does not match"):
        SymmetricMatrix.np.from_matrix(numpy.eye(2), Universe(["a", "b", "c"]))


def test_from_matrix_rejects_a_non_square_array():
    from lythonic.symmetric import SymmetricMatrix

    with pytest.raises(ValueError, match="does not match"):
        SymmetricMatrix.np.from_matrix(numpy.zeros((2, 3)), Universe(["a", "b"]))


def test_matrix_round_trip():
    from lythonic.symmetric import SymmetricMatrix

    m = _sample()
    assert SymmetricMatrix.np.from_matrix(m.np.matrix(), m.universe) == m


def test_set_matrix_requires_a_frozen_universe():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    with pytest.raises(ValueError, match="frozen"):
        b.np.set_matrix(numpy.eye(2))


def test_set_matrix_writes_the_lower_triangle():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder(universe=Universe(["a", "b", "c"]))
    b.np.set_matrix(numpy.array([[1.0, 0.0, 0.0], [0.5, 1.0, 0.0], [0.0, 0.0, 1.0]]))
    assert b.build() == _sample()


def test_identity_is_positive_semi_definite():
    from lythonic.symmetric import SymmetricMatrix

    m = SymmetricMatrix.np.from_matrix(numpy.eye(3), Universe(["a", "b", "c"]))
    assert m.np.is_psd()
    assert m.np.min_eigenvalue() == pytest.approx(1.0)
    assert m.np.eigenvalues().tolist() == pytest.approx([1.0, 1.0, 1.0])


def test_a_negative_eigenvalue_fails_the_check():
    from lythonic.symmetric import SymmetricMatrix

    # Off-diagonal correlation above 1 makes the matrix indefinite.
    arr = numpy.array([[1.0, 2.0], [2.0, 1.0]])
    m = SymmetricMatrix.np.from_matrix(arr, Universe(["a", "b"]))
    assert not m.np.is_psd()
    assert m.np.min_eigenvalue() < 0


def test_rank_deficient_covariance_passes_under_the_default_tolerance():
    from lythonic.symmetric import SymmetricMatrix

    # Three variables from two observations: mathematically PSD, but the
    # smallest eigenvalue lands just below zero after a float round trip, so a
    # bare non-negativity test would reject it.
    observations = numpy.array([[1.0, 2.0, 3.0], [2.0, 1.0, 0.0]])
    cov = numpy.cov(observations.T)
    m = SymmetricMatrix.np.from_matrix(cov, Universe(["a", "b", "c"]))
    assert m.np.min_eigenvalue() < 0
    assert m.np.is_psd()
    assert not m.np.is_psd(tol=0.0)


def test_tolerance_can_be_overridden():
    from lythonic.symmetric import SymmetricMatrix

    arr = numpy.array([[1.0, 1.1], [1.1, 1.0]])
    m = SymmetricMatrix.np.from_matrix(arr, Universe(["a", "b"]))
    assert not m.np.is_psd()
    assert m.np.is_psd(tol=1.0)


def test_eigenvalues_are_ascending_and_agree_with_the_minimum():
    from lythonic.symmetric import SymmetricMatrix

    arr = numpy.array([[2.0, 0.3], [0.3, 1.0]])
    m = SymmetricMatrix.np.from_matrix(arr, Universe(["a", "b"]))
    eigs = m.np.eigenvalues()
    assert list(eigs) == sorted(eigs)
    assert m.np.min_eigenvalue() == pytest.approx(float(eigs[0]))


def test_empty_matrix_is_positive_semi_definite():
    from lythonic.symmetric import SymmetricMatrixBuilder

    assert SymmetricMatrixBuilder().build().np.is_psd()


def test_set_matrix_rejects_a_mismatched_shape():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder(universe=Universe(["a", "b", "c"]))
    with pytest.raises(ValueError, match="does not match"):
        b.np.set_matrix(numpy.eye(2))
