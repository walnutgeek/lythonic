from __future__ import annotations

import pytest
from pydantic import Field

from lythonic.state import DbModel


class Region(DbModel["Region"]):
    id: int = Field(default=-1, description="(PK)")
    code: str = Field(description="(AK) Region code like 'us-west'")
    name: str = Field(description="Display name")


class Team(DbModel["Team"]):
    id: int = Field(default=-1, description="(PK)")
    region_id: int = Field(description="(FK:Region.id)(AK)")
    name: str = Field(description="(AK) Team name, unique within region")
    founded: int | None = None


class Player(DbModel["Player"]):
    id: int = Field(default=-1, description="(PK)")
    team_id: int = Field(description="(FK:Team.id)(AK)")
    jersey_number: int = Field(description="(AK) Jersey number, unique within team")
    player_name: str = Field(description="Player display name")


class Stat(DbModel["Stat"]):
    id: int = Field(default=-1, description="(PK)")
    player_id: int = Field(description="(FK:Player.id)")
    points: int


def test_fieldinfo_ak_parsing():
    """(AK) marker is parsed from field descriptions."""
    fi_map = Region.get_field_map()
    assert fi_map["code"].alt_key is True
    assert fi_map["code"].description == "Region code like 'us-west'"
    assert fi_map["name"].alt_key is False
    assert fi_map["id"].alt_key is False


def test_fieldinfo_fk_ak_combined():
    """(FK:...)(AK) parses both markers."""
    fi_map = Team.get_field_map()
    assert fi_map["region_id"].foreign_key == ("Region", "id")
    assert fi_map["region_id"].alt_key is True
    assert fi_map["name"].alt_key is True
    assert fi_map["name"].foreign_key is None
    assert fi_map["founded"].alt_key is False


def test_fieldinfo_nullable_ak_rejected():
    """Nullable field with (AK) raises AssertionError."""

    class Bad(DbModel["Bad"]):
        id: int = Field(default=-1, description="(PK)")
        slug: str | None = Field(default=None, description="(AK) Nullable AK")

    with pytest.raises(AssertionError, match="nullable"):
        Bad.get_field_map()


def test_ddl_single_ak():
    """Single AK field generates UNIQUE constraint."""
    ddl = Region.create_ddl()
    assert ddl == (
        "CREATE TABLE Region ("
        "id INTEGER PRIMARY KEY, "
        "code TEXT NOT NULL, "
        "name TEXT NOT NULL, "
        "UNIQUE (code))"
    )


def test_ddl_composite_ak():
    """Composite AK fields generate multi-column UNIQUE constraint."""
    ddl = Team.create_ddl()
    assert ddl == (
        "CREATE TABLE Team ("
        "id INTEGER PRIMARY KEY, "
        "region_id INTEGER NOT NULL REFERENCES Region(id), "
        "name TEXT NOT NULL, "
        "founded INTEGER, "
        "UNIQUE (region_id, name))"
    )


def test_ddl_no_ak():
    """Table without AK has no UNIQUE constraint."""
    ddl = Stat.create_ddl()
    assert "UNIQUE" not in ddl


from lythonic.state.alt_key import AltKey, AltKeyField


def test_altkey_from_model_local_only():
    """AltKey built from a model with only local (non-FK) AK fields."""
    ak = AltKey.from_model(Region)
    assert ak is not None
    assert ak.fields == [AltKeyField(name="code", foreign_key=None)]


def test_altkey_from_model_with_fk():
    """AltKey built from a model with FK + local AK fields."""
    ak = AltKey.from_model(Team)
    assert ak is not None
    assert ak.fields == [
        AltKeyField(name="region_id", foreign_key=("Region", "id")),
        AltKeyField(name="name", foreign_key=None),
    ]


def test_altkey_from_model_none():
    """Model without AK fields returns None."""
    ak = AltKey.from_model(Stat)
    assert ak is None
