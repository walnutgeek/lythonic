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
