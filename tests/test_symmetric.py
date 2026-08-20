from __future__ import annotations

import json
from math import inf, nan
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from lythonic.universe import Universe

if TYPE_CHECKING:
    from lythonic.symmetric import SymmetricMatrix

# With n keys and k stored off-diagonal values, sparse is canonical below
# k = n(n-1)/6. At n=4 that threshold is 2, so k<=1 is sparse and k>=2 dense.


def _sparse_side() -> SymmetricMatrix:
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder(universe=Universe(["a", "b", "c", "d"]))
    b.set_diagonal({"a": 1.0, "b": 1.0, "c": 1.0, "d": 1.0})
    b.set_value("b", "a", 0.5)
    return b.build()


def _dense_side() -> SymmetricMatrix:
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder(universe=Universe(["a", "b", "c", "d"]))
    b.set_diagonal({"a": 1.0, "b": 1.0, "c": 1.0, "d": 1.0})
    b.set_value("b", "a", 0.5)
    b.set_value("c", "a", 0.25)
    b.set_value("d", "c", 0.75)
    return b.build()


def test_universe_grows_in_first_mention_order():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    b.set_value("b", "a", 0.5)
    b.set_diagonal({"a": 1.0, "b": 1.0})
    m = b.build()
    assert list(m.universe) == ["b", "a"]


def test_declaring_a_universe_freezes_it():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder(universe=Universe(["a", "b"]))
    assert b.universe_frozen
    with pytest.raises(KeyError, match="frozen"):
        b.set_value("a", "zz", 1.0)


def test_freeze_and_thaw():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    b.set_diagonal({"a": 1.0})
    b.freeze()
    with pytest.raises(KeyError, match="frozen"):
        b.set_value("b", "b", 1.0)
    b.thaw()
    b.set_diagonal({"b": 1.0})
    assert list(b.build().universe) == ["a", "b"]


def test_growth_appends_and_never_permutes():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    b.set_diagonal({"a": 1.0, "b": 2.0})
    b.set_value("b", "a", 0.5)
    b.set_diagonal({"c": 3.0})
    m = b.build()
    assert list(m.universe) == ["a", "b", "c"]
    assert m.value("a", "b") == 0.5
    assert m.diagonal() == {"a": 1.0, "b": 2.0, "c": 3.0}


def test_build_raises_naming_keys_without_a_diagonal():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    b.set_value("b", "a", 0.5)
    with pytest.raises(ValueError, match="'b', 'a'"):
        b.build()


def test_setting_the_diagonals_fixes_a_missing_diagonal():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    b.set_value("b", "a", 0.5)
    b.set_diagonal(dict.fromkeys(b.universe, 1.0))
    assert b.build().diagonal() == {"b": 1.0, "a": 1.0}


def test_argument_order_is_irrelevant():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    b.set_diagonal({"a": 1.0, "b": 1.0})
    b.set_value("a", "b", 0.5)
    m = b.build()
    assert m.value("a", "b") == m.value("b", "a") == 0.5
    assert b.value("a", "b") == b.value("b", "a") == 0.5


def test_zero_off_diagonal_write_removes_the_value():
    from lythonic.symmetric import SymmetricMatrixBuilder

    def built(with_zero: bool):
        b = SymmetricMatrixBuilder()
        b.set_diagonal({"a": 1.0, "b": 1.0})
        if with_zero:
            b.set_value("a", "b", 0.0)
        return b.build()

    assert built(True) == built(False)
    assert built(True).model_dump_json() == built(False).model_dump_json()


def test_zero_on_the_diagonal_is_kept():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    b.set_diagonal({"a": 0.0})
    assert b.build().diagonal() == {"a": 0.0}


def test_build_snapshots_and_leaves_the_builder_usable():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    b.set_diagonal({"a": 1.0, "b": 1.0})
    b.set_value("a", "b", 0.5)
    first = b.build()

    b.set_value("a", "b", 0.9)
    b.set_diagonal({"c": 1.0})
    second = b.build()

    assert first.value("a", "b") == 0.5
    assert list(first.universe) == ["a", "b"]
    assert second.value("a", "b") == 0.9
    assert list(second.universe) == ["a", "b", "c"]


def test_absent_off_diagonal_reads_as_zero():
    m = _sparse_side()
    assert m.value("c", "d") == 0.0


def test_unknown_key_raises():
    m = _sparse_side()
    with pytest.raises(KeyError):
        m.value("a", "zz")


def test_values_of_returns_an_entry_for_every_key():
    m = _sparse_side()
    assert m.values_of("a") == {"a": 1.0, "b": 0.5, "c": 0.0, "d": 0.0}


def test_pairs_yields_every_pair_once():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    b.set_diagonal({"a": 1.0, "b": 2.0})
    b.set_value("a", "b", 0.5)
    assert list(b.build().pairs()) == [("a", "a", 1.0), ("a", "b", 0.5), ("b", "b", 2.0)]


def test_reads_are_identical_either_side_of_the_density_threshold():
    from lythonic.symmetric import SymmetricMatrixBuilder

    def rebuild(source: SymmetricMatrix) -> SymmetricMatrix:
        b = SymmetricMatrixBuilder(universe=source.universe)
        for x, y, v in source.pairs():
            b.set_value(x, y, v)
        return b.build()

    for original in (_sparse_side(), _dense_side()):
        copy = rebuild(original)
        assert copy == original
        assert copy.diagonal() == original.diagonal()
        assert copy.values_of("a") == original.values_of("a")
        assert list(copy.pairs()) == list(original.pairs())


def test_the_density_rule_picks_the_smaller_encoding():
    sparse_kind = json.loads(_sparse_side().model_dump_json())["storage"]["kind"]
    dense_kind = json.loads(_dense_side().model_dump_json())["storage"]["kind"]
    assert (sparse_kind, dense_kind) == ("sparse", "dense")


def test_same_content_reached_five_ways_is_equal_and_serializes_identically():
    from lythonic.symmetric import SymmetricMatrix, SymmetricMatrixBuilder

    for reference in (_sparse_side(), _dense_side()):
        via_builder = reference
        via_to_builder = reference.to_builder().build()
        via_json = SymmetricMatrix.model_validate_json(reference.model_dump_json())
        via_cast = reference.cast(list(reference.universe))

        wide = SymmetricMatrixBuilder()
        wide.set_diagonal({"extra": 9.0})
        for x, y, v in reference.pairs():
            wide.set_value(x, y, v)
        wide.set_diagonal({k: reference.value(k, k) for k in reference.universe})
        via_narrowing_cast = wide.build().cast(list(reference.universe))

        for other in (via_builder, via_to_builder, via_json, via_cast, via_narrowing_cast):
            assert other == reference
            assert other.model_dump_json() == reference.model_dump_json()


def test_json_round_trip():
    from lythonic.symmetric import SymmetricMatrix

    for m in (_sparse_side(), _dense_side()):
        assert SymmetricMatrix.model_validate_json(m.model_dump_json()) == m


def test_non_canonical_storage_is_normalized_on_load():
    from lythonic.symmetric import DenseTriangle, SparseTriangle, SymmetricMatrix

    canonical = _sparse_side()
    # The same content in the other encoding, with pairs out of order.
    as_dense = SymmetricMatrix(
        universe=Universe(["a", "b", "c", "d"]),
        storage=DenseTriangle(values=[1.0, 0.5, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0]),
    )
    assert as_dense == canonical
    assert as_dense.model_dump_json() == canonical.model_dump_json()

    dense = _dense_side()
    as_sparse = SymmetricMatrix(
        universe=Universe(["a", "b", "c", "d"]),
        storage=SparseTriangle(
            diagonal=[1.0, 1.0, 1.0, 1.0],
            records=[(3, 2, 0.75), (1, 0, 0.5), (2, 0, 0.25), (3, 0, 0.0)],
        ),
    )
    assert as_sparse == dense
    assert as_sparse.model_dump_json() == dense.model_dump_json()


def test_wrong_length_triangle_rejected():
    from lythonic.symmetric import DenseTriangle, SymmetricMatrix

    with pytest.raises(ValidationError, match="expected 3"):
        SymmetricMatrix(universe=Universe(["a", "b"]), storage=DenseTriangle(values=[1.0]))


def test_wrong_length_diagonal_rejected():
    from lythonic.symmetric import SparseTriangle, SymmetricMatrix

    with pytest.raises(ValidationError, match="diagonal"):
        SymmetricMatrix(
            universe=Universe(["a", "b"]),
            storage=SparseTriangle(diagonal=[1.0], records=[]),
        )


def test_out_of_range_index_rejected():
    from lythonic.symmetric import SparseTriangle, SymmetricMatrix

    with pytest.raises(ValidationError, match="out of range"):
        SymmetricMatrix(
            universe=Universe(["a", "b"]),
            storage=SparseTriangle(diagonal=[1.0, 1.0], records=[(5, 0, 1.0)]),
        )


def test_upper_triangle_pair_rejected():
    from lythonic.symmetric import SparseTriangle, SymmetricMatrix

    with pytest.raises(ValidationError, match="out of range"):
        SymmetricMatrix(
            universe=Universe(["a", "b"]),
            storage=SparseTriangle(diagonal=[1.0, 1.0], records=[(0, 1, 1.0)]),
        )


def test_duplicate_pair_rejected():
    from lythonic.symmetric import SparseTriangle, SymmetricMatrix

    with pytest.raises(ValidationError, match="duplicate"):
        SymmetricMatrix(
            universe=Universe(["a", "b"]),
            storage=SparseTriangle(diagonal=[1.0, 1.0], records=[(1, 0, 1.0), (1, 0, 2.0)]),
        )


@pytest.mark.parametrize("bad", [nan, inf, -inf])
def test_non_finite_values_rejected(bad: float):
    from lythonic.symmetric import DenseTriangle, SymmetricMatrix

    with pytest.raises(ValidationError, match="finite"):
        SymmetricMatrix(universe=Universe(["a"]), storage=DenseTriangle(values=[bad]))


def test_builder_rejects_non_finite_values():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    with pytest.raises(ValueError, match="finite"):
        b.set_value("a", "a", nan)


def test_cast_narrows_silently():
    m = _dense_side()
    narrowed = m.cast(["a", "b"])
    assert list(narrowed.universe) == ["a", "b"]
    assert narrowed.value("a", "b") == 0.5
    assert narrowed.diagonal() == {"a": 1.0, "b": 1.0}


def test_cast_reorders():
    m = _sparse_side()
    reordered = m.cast(["d", "c", "b", "a"])
    assert list(reordered.universe) == ["d", "c", "b", "a"]
    assert reordered.value("a", "b") == 0.5


def test_cast_raises_naming_an_introduced_key():
    m = _sparse_side()
    with pytest.raises(KeyError, match="zz"):
        m.cast(["a", "zz"])


def test_cast_onto_equal_universe_returns_self():
    m = _sparse_side()
    assert m.cast(m.universe) is m


def test_to_builder_arrives_frozen():
    m = _sparse_side()
    b = m.to_builder()
    assert b.universe_frozen
    with pytest.raises(KeyError, match="frozen"):
        b.set_value("zz", "zz", 1.0)
    b.thaw()
    b.set_diagonal({"zz": 1.0})
    assert list(b.build().universe) == ["a", "b", "c", "d", "zz"]


def test_matrix_is_immutable():
    m = _sparse_side()
    with pytest.raises(ValidationError):
        m.universe = Universe(["x"])


def test_empty_matrix():
    from lythonic.symmetric import SymmetricMatrix, SymmetricMatrixBuilder

    m = SymmetricMatrixBuilder().build()
    assert len(m.universe) == 0
    assert m.diagonal() == {}
    assert list(m.pairs()) == []
    assert SymmetricMatrix.model_validate_json(m.model_dump_json()) == m


def test_builder_reads_what_has_been_set_so_far():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    b.set_diagonal({"a": 1.0, "b": 2.0})
    b.set_value("a", "b", 0.5)
    assert b.value("a", "a") == 1.0
    assert b.value("a", "b") == 0.5
    assert b.diagonal() == {"a": 1.0, "b": 2.0}


def test_builder_reads_reject_an_unknown_key():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    b.set_diagonal({"a": 1.0})
    with pytest.raises(KeyError, match="not in universe"):
        b.value("a", "zz")


def test_builder_diagonal_omits_keys_without_one():
    from lythonic.symmetric import SymmetricMatrixBuilder

    b = SymmetricMatrixBuilder()
    b.set_value("a", "b", 0.5)
    b.set_diagonal({"a": 1.0})
    assert b.diagonal() == {"a": 1.0}


def _square(n: int, off_diagonal: int) -> SymmetricMatrix:
    """A matrix over `n` keys with exactly `off_diagonal` non-zero pairs."""
    from lythonic.symmetric import SymmetricMatrixBuilder

    keys = [f"k{i}" for i in range(n)]
    b = SymmetricMatrixBuilder(universe=Universe(keys))
    b.set_diagonal(dict.fromkeys(keys, 1.0))
    placed = 0
    for row in range(n):
        for column in range(row):
            if placed == off_diagonal:
                break
            b.set_value(keys[row], keys[column], 0.5)
            placed += 1
    return b.build()


@pytest.mark.parametrize(
    ("n", "off_diagonal"),
    [
        (4, 1),  # threshold exactly 2
        (4, 2),
        (5, 3),  # threshold 3.33: truncating it would pick the larger encoding
        (5, 4),
        (8, 9),  # threshold 9.33
        (8, 10),
    ],
)
def test_the_canonical_encoding_is_always_the_smaller_one(n: int, off_diagonal: int):
    m = _square(n, off_diagonal)
    kind = json.loads(m.model_dump_json())["storage"]["kind"]
    dense_cost = n * (n + 1) // 2
    sparse_cost = n + 3 * off_diagonal
    assert kind == ("sparse" if sparse_cost < dense_cost else "dense")


def test_construction_and_reads_need_no_numpy():
    from unittest.mock import patch

    from lythonic.symmetric import SymmetricMatrix

    # Nothing outside the facade may import numpy: a stored matrix must load
    # and be readable with no optional dependency installed.
    with patch("importlib.import_module", side_effect=ImportError("no numpy")):
        m = _dense_side()
        assert m.value("a", "b") == 0.5
        assert m.diagonal()["a"] == 1.0
        assert m.values_of("a")["b"] == 0.5
        assert list(m.pairs())
        assert m.cast(["a", "b"]).value("a", "b") == 0.5
        assert SymmetricMatrix.model_validate_json(m.model_dump_json()) == m


def test_the_facade_names_the_extra_when_numpy_is_missing():
    from unittest.mock import patch

    from lythonic.symmetric import SymmetricMatrix

    m = _dense_side()
    with patch("importlib.import_module", side_effect=ImportError("no numpy")):
        with pytest.raises(ImportError, match=r"lythonic\[numpy\]"):
            m.np.matrix()
        with pytest.raises(ImportError, match=r"lythonic\[numpy\]"):
            SymmetricMatrix.np.from_matrix(None, ["a"])  # pyright: ignore[reportArgumentType]
