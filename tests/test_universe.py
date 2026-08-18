from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError


def test_ordered_access_by_position_and_key():
    from lythonic.universe import Universe

    u = Universe(["acct1", "acct2", "acct3"])
    assert len(u) == 3
    assert list(u) == ["acct1", "acct2", "acct3"]
    assert u[1] == "acct2"
    assert u.index("acct3") == 2
    assert "acct2" in u
    assert "nope" not in u


def test_duplicate_keys_rejected():
    from lythonic.universe import Universe

    with pytest.raises(ValueError, match="duplicate"):
        Universe(["USD", "EUR", "USD"])


def test_equality_and_hashing_are_order_sensitive():
    from lythonic.universe import Universe

    assert Universe(["USD", "EUR"]) == Universe(["USD", "EUR"])
    assert Universe(["USD", "EUR"]) != Universe(["EUR", "USD"])
    assert hash(Universe(["USD", "EUR"])) == hash(Universe(["USD", "EUR"]))
    assert len({Universe(["USD", "EUR"]), Universe(["USD", "EUR"])}) == 1


def test_unknown_key_lookup_raises():
    from lythonic.universe import Universe

    with pytest.raises(KeyError, match="JPY"):
        Universe(["USD", "EUR"]).index("JPY")


def test_coerces_from_list_and_serializes_back_to_list():
    from lythonic.universe import Universe

    class Holder(BaseModel):
        axis: Universe

    h = Holder(axis=["USD", "EUR"])  # pyright: ignore[reportArgumentType]
    assert h.axis == Universe(["USD", "EUR"])
    assert h.model_dump() == {"axis": ["USD", "EUR"]}
    assert h.model_dump_json() == '{"axis":["USD","EUR"]}'
    assert Holder.model_validate_json(h.model_dump_json()) == h


def test_accepts_an_existing_universe():
    from lythonic.universe import Universe

    class Holder(BaseModel):
        axis: Universe

    u = Universe(["USD"])
    assert Holder(axis=u).axis == u
    assert Universe(u) == u


def test_duplicates_rejected_through_pydantic():
    from lythonic.universe import Universe

    class Holder(BaseModel):
        axis: Universe

    with pytest.raises(ValidationError, match="duplicate"):
        Holder.model_validate({"axis": ["USD", "USD"]})
