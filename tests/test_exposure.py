from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from lythonic.exposure import ExposureMatrix
from pydantic import ValidationError


def test_universes_grow_in_first_mention_order():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposure("acct2", "EUR", 0.5)
    b.set_exposure("acct1", "USD", 0.4)
    m = b.build()
    assert list(m.subjects) == ["acct2", "acct1"]
    assert list(m.targets) == ["EUR", "USD"]
    assert m.exposure("acct1", "USD") == 0.4
    assert m.exposure("acct2", "EUR") == 0.5


def test_known_key_with_no_record_returns_cell_fill():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposure("acct1", "USD", 0.4)
    b.set_exposure("acct2", "EUR", 0.5)
    assert b.build().exposure("acct1", "EUR") == 0.0


def test_unknown_key_raises():
    from lythonic.exposure import ExposureMatrixBuilder

    m = ExposureMatrixBuilder().build()
    with pytest.raises(KeyError, match="acct1"):
        m.exposure("acct1", "USD")


def test_row_and_column_reads_return_stored_entries_only():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposure("acct1", "USD", 0.4)
    b.set_exposure("acct1", "EUR", 0.6)
    b.set_exposure("acct2", "USD", 0.9)
    m = b.build()
    assert m.exposures_of("acct1") == {"USD": 0.4, "EUR": 0.6}
    assert m.exposures_to("USD") == {"acct1": 0.4, "acct2": 0.9}
    assert m.exposures_to("EUR") == {"acct1": 0.6}


def test_matrix_is_immutable():
    from lythonic.exposure import ExposureMatrixBuilder

    m = ExposureMatrixBuilder().build()
    with pytest.raises(ValidationError):
        m.cell_fill = 1.0


def test_declared_axis_is_frozen_and_rejects_unknown_keys():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder(targets=["USD", "EUR"])
    b.set_exposure("acct1", "USD", 0.4)
    with pytest.raises(KeyError, match="USDD"):
        b.set_exposure("acct1", "USDD", 0.4)
    assert list(b.build().targets) == ["USD", "EUR"]


def test_undeclared_axis_stays_open():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder(targets=["USD"])
    b.set_exposure("acct1", "USD", 0.4)
    b.set_exposure("acct2", "USD", 0.5)
    assert list(b.build().subjects) == ["acct1", "acct2"]


def test_thaw_reopens_an_axis_without_reordering_it():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder(targets=["USD", "EUR"])
    b.thaw_targets()
    b.set_exposure("acct1", "JPY", 0.1)
    assert list(b.build().targets) == ["USD", "EUR", "JPY"]


def test_freeze_closes_an_axis_after_ingest():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposure("acct1", "USD", 0.4)
    b.freeze_subjects()
    with pytest.raises(KeyError, match="acct2"):
        b.set_exposure("acct2", "USD", 0.5)
    b.set_exposure("acct1", "EUR", 0.6)


def test_declared_universe_positions_are_preserved_not_first_mention():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder(targets=["USD", "EUR", "JPY"])
    b.set_exposure("acct1", "JPY", 0.1)
    assert list(b.build().targets) == ["USD", "EUR", "JPY"]


def test_setting_a_row_replaces_it():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposures("acct1", {"USD": 0.4, "EUR": 0.6})
    b.set_exposures("acct1", {"JPY": 1.0})
    assert b.build().exposures_of("acct1") == {"JPY": 1.0}


def test_empty_row_registers_the_subject_without_records():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposures("acct1", {})
    m = b.build()
    assert list(m.subjects) == ["acct1"]
    assert m.exposures_of("acct1") == {}


def test_none_applies_the_configured_default_row():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder(default_row={"USD": 1.0})
    b.set_exposures("acct1", None)
    assert b.build().exposures_of("acct1") == {"USD": 1.0}


def test_none_without_a_default_row_raises():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    with pytest.raises(ValueError, match="no default row"):
        b.set_exposures("acct1", None)


def test_default_row_validated_against_a_frozen_target_axis_when_set():
    from lythonic.exposure import ExposureMatrixBuilder

    with pytest.raises(KeyError, match="GBP"):
        ExposureMatrixBuilder(targets=["USD"], default_row={"GBP": 1.0})


def test_default_row_does_not_pre_seed_an_open_target_axis():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder(default_row={"EUR": 0.5})
    assert list(b.targets) == []
    b.set_exposure("acct1", "USD", 1.0)
    assert list(b.targets) == ["USD"]
    b.set_exposures("acct2", None)
    assert list(b.targets) == ["USD", "EUR"]


def test_builder_rejects_a_non_finite_cell_fill_on_construction():
    from lythonic.exposure import ExposureMatrixBuilder

    with pytest.raises(ValueError, match="finite"):
        ExposureMatrixBuilder(cell_fill=float("nan"))


def test_fill_valued_writes_leave_no_record():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposure("acct1", "USD", 0.4)
    b.set_exposure("acct1", "USD", 0.0)
    m = b.build()
    assert m.records == []
    assert list(m.subjects) == ["acct1"]
    assert m.exposure("acct1", "USD") == 0.0


def test_fill_normalization_follows_a_non_zero_cell_fill():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder(cell_fill=1.0)
    b.set_exposure("acct1", "USD", 1.0)
    b.set_exposure("acct1", "EUR", 0.0)
    m = b.build()
    assert m.exposures_of("acct1") == {"EUR": 0.0}
    assert m.exposure("acct1", "USD") == 1.0


def test_builder_reads_expose_work_in_progress():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposures("acct1", {"USD": 0.4})
    assert b.exposure("acct1", "USD") == 0.4
    assert b.exposures_of("acct1") == {"USD": 0.4}
    assert list(b.subjects) == ["acct1"]
    assert list(b.targets) == ["USD"]


def test_build_snapshots_and_the_builder_stays_usable():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposure("acct1", "USD", 0.4)
    first = b.build()
    b.set_exposure("acct2", "EUR", 0.5)
    second = b.build()

    assert list(first.subjects) == ["acct1"]
    assert first.records == [(0, 0, 0.4)]
    assert list(second.subjects) == ["acct1", "acct2"]
    assert second.exposure("acct2", "EUR") == 0.5


def test_built_matrix_does_not_alias_builder_state():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposure("acct1", "USD", 0.4)
    m = b.build()
    b.set_exposure("acct1", "USD", 99.0)
    b.set_exposure("acct2", "EUR", 1.0)

    assert m.exposure("acct1", "USD") == 0.4
    assert m.records == [(0, 0, 0.4)]
    assert list(m.subjects) == ["acct1"]


def test_to_builder_round_trips_and_freezes_both_axes():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposure("acct1", "USD", 0.4)
    m = b.build()

    b2 = m.to_builder()
    b2.set_exposure("acct1", "USD", 0.7)
    assert b2.build().exposure("acct1", "USD") == 0.7
    with pytest.raises(KeyError, match="acct2"):
        b2.set_exposure("acct2", "USD", 0.1)
    with pytest.raises(KeyError, match="EUR"):
        b2.set_exposure("acct1", "EUR", 0.1)


def test_to_builder_preserves_cell_fill_and_accepts_a_default_row():
    from lythonic.exposure import ExposureMatrixBuilder

    m = ExposureMatrixBuilder(cell_fill=1.0).build()
    b = m.to_builder(default_row={})
    assert b.build().cell_fill == 1.0


def test_builder_read_supports_merging_a_row_by_hand():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposures("acct1", {"USD": 0.4})
    b.set_exposures("acct1", {**b.exposures_of("acct1"), "EUR": 0.6})
    assert b.build().exposures_of("acct1") == {"USD": 0.4, "EUR": 0.6}


def test_serializes_to_universes_and_index_triples():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposure("acct1", "USD", 0.4)
    b.set_exposure("acct2", "EUR", 0.9)
    assert b.build().model_dump() == {
        "subjects": ["acct1", "acct2"],
        "targets": ["USD", "EUR"],
        "cell_fill": 0.0,
        "records": [(0, 0, 0.4), (1, 1, 0.9)],
    }


def test_json_round_trip():
    from lythonic.exposure import ExposureMatrix, ExposureMatrixBuilder

    b = ExposureMatrixBuilder(cell_fill=1.5)
    b.set_exposure("acct1", "USD", 0.4)
    m = b.build()
    restored = ExposureMatrix.model_validate_json(m.model_dump_json())
    assert restored == m
    assert restored.exposure("acct1", "USD") == 0.4


def test_same_exposures_serialize_identically_regardless_of_write_order():
    from lythonic.exposure import ExposureMatrixBuilder

    forward = ExposureMatrixBuilder(subjects=["acct1", "acct2"], targets=["USD", "EUR"])
    forward.set_exposure("acct1", "USD", 0.4)
    forward.set_exposure("acct2", "EUR", 0.9)

    backward = ExposureMatrixBuilder(subjects=["acct1", "acct2"], targets=["USD", "EUR"])
    backward.set_exposure("acct2", "EUR", 0.9)
    backward.set_exposure("acct1", "USD", 0.4)

    assert forward.build().model_dump_json() == backward.build().model_dump_json()
    assert forward.build() == backward.build()


def test_equality_includes_cell_fill():
    from lythonic.exposure import ExposureMatrixBuilder

    a = ExposureMatrixBuilder(subjects=["acct1"], targets=["USD"], cell_fill=0.0).build()
    b = ExposureMatrixBuilder(subjects=["acct1"], targets=["USD"], cell_fill=1.0).build()
    assert a != b


def test_deserialization_canonicalizes_unsorted_and_fill_valued_records():
    from lythonic.exposure import ExposureMatrix

    m = ExposureMatrix.model_validate(
        {
            "subjects": ["acct1", "acct2"],
            "targets": ["USD", "EUR"],
            "cell_fill": 0.0,
            "records": [(1, 1, 0.9), (0, 1, 0.0), (0, 0, 0.4)],
        }
    )
    assert m.records == [(0, 0, 0.4), (1, 1, 0.9)]


def test_deserialization_rejects_out_of_range_indexes():
    from lythonic.exposure import ExposureMatrix

    with pytest.raises(ValidationError, match="out of range"):
        ExposureMatrix.model_validate(
            {"subjects": ["acct1"], "targets": ["USD"], "records": [(0, 3, 0.4)]}
        )


def test_deserialization_rejects_duplicate_cells():
    from lythonic.exposure import ExposureMatrix

    with pytest.raises(ValidationError, match="duplicate"):
        ExposureMatrix.model_validate(
            {
                "subjects": ["acct1"],
                "targets": ["USD"],
                "records": [(0, 0, 0.4), (0, 0, 0.5)],
            }
        )


def _sample() -> ExposureMatrix:
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder()
    b.set_exposures("acct1", {"USD": 0.4, "EUR": 0.6})
    b.set_exposures("acct2", {"USD": 0.9})
    return b.build()


def test_cast_reorders_without_touching_values():
    m = _sample().cast(targets=["EUR", "USD"])
    assert list(m.targets) == ["EUR", "USD"]
    assert m.exposures_of("acct1") == {"EUR": 0.6, "USD": 0.4}
    assert m.records == [(0, 0, 0.6), (0, 1, 0.4), (1, 1, 0.9)]


def test_cast_drops_keys_outside_the_new_universes():
    m = _sample().cast(subjects=["acct1"], targets=["USD"])
    assert list(m.subjects) == ["acct1"]
    assert m.exposures_of("acct1") == {"USD": 0.4}
    assert m.records == [(0, 0, 0.4)]


def test_cast_leaves_the_other_axis_alone():
    m = _sample().cast(targets=["USD"])
    assert list(m.subjects) == ["acct1", "acct2"]
    assert list(m.targets) == ["USD"]


def test_cast_gives_new_subjects_the_default_row():
    m = _sample().cast(subjects=["acct1", "acct3"], default_row={"USD": 1.0})
    assert m.exposures_of("acct3") == {"USD": 1.0}
    assert m.exposures_of("acct1") == {"USD": 0.4, "EUR": 0.6}


def test_cast_gives_new_subjects_cell_fill_when_no_default_row():
    m = _sample().cast(subjects=["acct1", "acct3"])
    assert m.exposures_of("acct3") == {}
    assert m.exposure("acct3", "USD") == 0.0


def test_cast_preserves_cell_fill():
    from lythonic.exposure import ExposureMatrixBuilder

    b = ExposureMatrixBuilder(cell_fill=1.0)
    b.set_exposure("acct1", "USD", 0.4)
    assert b.build().cast(subjects=["acct1"]).cell_fill == 1.0


def test_cast_default_row_is_restricted_to_the_new_target_universe():
    m = _sample().cast(subjects=["acct3"], targets=["USD"], default_row={"USD": 1.0, "EUR": 2.0})
    assert m.exposures_of("acct3") == {"USD": 1.0}


def test_cast_has_no_default_column():
    m = _sample().cast(targets=["USD", "GBP"])
    assert m.exposures_to("GBP") == {}


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_cell_fill_rejected(bad: float):
    from lythonic.exposure import ExposureMatrix

    with pytest.raises(ValidationError, match="finite"):
        ExposureMatrix.model_validate(
            {"subjects": [], "targets": [], "cell_fill": bad, "records": []}
        )
