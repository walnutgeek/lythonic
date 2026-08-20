from __future__ import annotations

from math import inf, nan

import pytest
from pydantic import ValidationError

from lythonic.universe import Universe


def test_reads_by_key_in_universe_order():
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["USD", "EUR", "JPY"]), values=[1.0, 2.0, 3.0])
    assert list(v.universe) == ["USD", "EUR", "JPY"]
    assert v.value("EUR") == 2.0
    assert v.to_dict() == {"USD": 1.0, "EUR": 2.0, "JPY": 3.0}


def test_from_mapping_takes_universe_from_insertion_order():
    from lythonic.vector import KeyedVector

    v = KeyedVector.from_mapping({"b": 2.0, "a": 1.0})
    assert list(v.universe) == ["b", "a"]
    assert v.values == [2.0, 1.0]


def test_length_mismatch_rejected():
    from lythonic.vector import KeyedVector

    with pytest.raises(ValidationError, match="3 values"):
        KeyedVector(universe=Universe(["a", "b"]), values=[1.0, 2.0, 3.0])


def test_unknown_key_raises():
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a"]), values=[1.0])
    with pytest.raises(KeyError):
        v.value("nope")


def test_no_key_indexing():
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a"]), values=[1.0])
    with pytest.raises(TypeError):
        v["a"]  # pyright: ignore[reportIndexIssue]


def test_iteration_is_pydantic_field_iteration_not_keys_or_values():
    from lythonic.vector import KeyedVector

    # BaseModel supplies __iter__ over fields, inherited by every model in the
    # library. It yields neither keys nor values, so the ambiguity the design
    # avoids does not arise, but the protocol cannot be removed.
    v = KeyedVector(universe=Universe(["a"]), values=[1.0])
    assert dict(v).keys() == {"universe", "values"}


def test_immutable():
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a"]), values=[1.0])
    with pytest.raises(ValidationError):
        v.values = [2.0]


def test_non_finite_values_are_stored():
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a", "b", "c"]), values=[nan, inf, -inf])
    assert v.value("a") != v.value("a")
    assert v.value("b") == inf
    assert v.value("c") == -inf


def test_cast_narrows_silently():
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a", "b", "c"]), values=[1.0, 2.0, 3.0])
    assert v.cast(["a", "c"]).to_dict() == {"a": 1.0, "c": 3.0}


def test_cast_reorders():
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a", "b"]), values=[1.0, 2.0])
    cast = v.cast(["b", "a"])
    assert list(cast.universe) == ["b", "a"]
    assert cast.values == [2.0, 1.0]


def test_cast_extends_with_nan_by_default():
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a"]), values=[1.0])
    cast = v.cast(["a", "b"])
    assert cast.value("a") == 1.0
    assert cast.value("b") != cast.value("b")


def test_cast_extends_with_given_fill():
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a"]), values=[1.0])
    assert v.cast(["a", "b"], fill=0.0).to_dict() == {"a": 1.0, "b": 0.0}


def test_cast_onto_equal_universe_returns_self():
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a", "b"]), values=[1.0, 2.0])
    assert v.cast(["a", "b"]) is v
    assert v.cast(v.universe) is v


def test_equal_content_compares_equal():
    from lythonic.vector import KeyedVector

    a = KeyedVector(universe=Universe(["a", "b"]), values=[1.0, 2.0])
    b = KeyedVector(universe=Universe(["a", "b"]), values=[1.0, 2.0])
    assert a == b


def test_nan_matches_nan_positionally():
    from lythonic.vector import KeyedVector

    a = KeyedVector(universe=Universe(["a", "b"]), values=[nan, 2.0])
    b = KeyedVector(universe=Universe(["a", "b"]), values=[nan, 2.0])
    assert a == b
    assert a == a


def test_inequality_by_universe_order_and_value():
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a", "b"]), values=[1.0, 2.0])
    assert v != KeyedVector(universe=Universe(["a", "c"]), values=[1.0, 2.0])
    assert v != KeyedVector(universe=Universe(["b", "a"]), values=[1.0, 2.0])
    assert v != KeyedVector(universe=Universe(["a", "b"]), values=[1.0, 3.0])
    assert v != KeyedVector(universe=Universe(["a"]), values=[1.0])
    assert v != "not a vector"


def test_nan_in_only_one_operand_is_unequal():
    from lythonic.vector import KeyedVector

    a = KeyedVector(universe=Universe(["a"]), values=[nan])
    b = KeyedVector(universe=Universe(["a"]), values=[1.0])
    assert a != b
    assert b != a


def test_json_round_trip_of_finite_values():
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a", "b"]), values=[1.5, -2.5])
    assert KeyedVector.model_validate_json(v.model_dump_json()) == v


def test_json_round_trip_of_non_finite_values():
    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a", "b", "c"]), values=[nan, inf, -inf])
    assert KeyedVector.model_validate_json(v.model_dump_json()) == v


def test_non_finite_values_serialize_as_strings():
    import json

    from lythonic.vector import KeyedVector

    text = KeyedVector(
        universe=Universe(["a", "b", "c"]), values=[nan, inf, -inf]
    ).model_dump_json()
    assert json.loads(text) == {
        "universe": ["a", "b", "c"],
        "values": ["NaN", "Infinity", "-Infinity"],
    }


def test_universe_serializes_as_a_list_of_strings():
    import json

    from lythonic.vector import KeyedVector

    text = KeyedVector(universe=Universe(["a"]), values=[1.0]).model_dump_json()
    assert json.loads(text) == {"universe": ["a"], "values": [1.0]}


def test_bridges_from_a_dict_accessor_losslessly():
    from lythonic.exposure import ExposureMatrixBuilder
    from lythonic.vector import KeyedVector

    b = ExposureMatrixBuilder()
    b.set_exposures("acct1", {"USD": 0.4, "EUR": 0.6})
    m = b.build()
    v = KeyedVector.from_mapping(m.exposures_of("acct1"))
    assert v.to_dict() == {"USD": 0.4, "EUR": 0.6}


def test_construction_and_reads_need_no_optional_dependency():
    from unittest.mock import patch

    from lythonic.vector import KeyedVector

    with patch("importlib.import_module", side_effect=ImportError("no library")):
        v = KeyedVector(universe=Universe(["a", "b"]), values=[1.0, nan])
        assert v.value("a") == 1.0
        assert v.cast(["a"]).to_dict() == {"a": 1.0}
        assert KeyedVector.model_validate_json(v.model_dump_json()) == v


@pytest.mark.parametrize(("facade", "extra"), [("np", "numpy"), ("pd", "pandas")])
def test_the_facades_name_the_extra_when_the_library_is_missing(facade: str, extra: str):
    from unittest.mock import patch

    from lythonic.vector import KeyedVector

    v = KeyedVector(universe=Universe(["a"]), values=[1.0])
    with patch("importlib.import_module", side_effect=ImportError("missing")):
        with pytest.raises(ImportError, match=rf"lythonic\[{extra}\]"):
            getattr(v, facade)
        with pytest.raises(ImportError, match=rf"lythonic\[{extra}\]"):
            getattr(KeyedVector, facade)
