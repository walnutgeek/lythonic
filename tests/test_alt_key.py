# pyright: reportPrivateUsage=false
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from pydantic import Field

from lythonic.misc import tabula_rasa_path
from lythonic.state import DbModel, open_sqlite_db


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


from lythonic.state import Schema


def test_schema_table_map():
    """Schema builds a table_map keyed by table name."""
    schema = Schema([Region, Team, Player, Stat])
    assert "Region" in schema.table_map
    assert "Team" in schema.table_map
    assert schema.table_map["Region"] is Region


def test_schema_validates_fk_ak_references():
    """Schema rejects FK-AK that references a table without its own AK."""

    class Orphan(DbModel["Orphan"]):
        id: int = Field(default=-1, description="(PK)")
        stat_id: int = Field(description="(FK:Stat.id)(AK)")
        label: str = Field(description="(AK)")

    with pytest.raises(AssertionError, match="has no alternative key"):
        Schema([Stat, Orphan])


def test_schema_validates_self_ref_fk_ak():
    """Schema rejects self-referential FK in AK."""

    class SelfRef(DbModel["SelfRef"]):
        id: int = Field(default=-1, description="(PK)")
        parent_id: int = Field(description="(FK:SelfRef.id)(AK)")
        code: str = Field(description="(AK)")

    with pytest.raises(AssertionError, match="[Ss]elf-referential"):
        Schema([SelfRef])


def test_schema_validates_missing_table():
    """Schema rejects FK-AK referencing a table not in the schema."""

    class Dangling(DbModel["Dangling"]):
        id: int = Field(default=-1, description="(PK)")
        region_id: int = Field(description="(FK:Region.id)(AK)")

    with pytest.raises(AssertionError, match="not in schema"):
        Schema([Dangling])


SCHEMA = Schema([Region, Team, Player, Stat])


def test_altkey_chain_local_only():
    """Region AK has no joins — all fields are local."""
    ak = AltKey.from_model(Region)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)
    assert ak._join_chain == []
    assert ak._local_ak_columns == ["code"]


def test_altkey_chain_one_level():
    """Team AK cascades region_id through one JOIN to Region."""
    ak = AltKey.from_model(Team)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)
    assert len(ak._join_chain) == 1
    step = ak._join_chain[0]
    assert step.local_field == "region_id"
    assert step.remote_table == "Region"
    assert step.remote_field == "id"
    assert step.leaf_columns == ["code"]


def test_altkey_chain_two_levels():
    """Player AK cascades team_id through Team, which cascades to Region."""
    ak = AltKey.from_model(Player)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)
    assert len(ak._join_chain) == 2
    # First join: Player.team_id -> Team.id
    assert ak._join_chain[0].source_alias == "_t"
    assert ak._join_chain[0].local_field == "team_id"
    assert ak._join_chain[0].remote_table == "Team"
    # Second join: Team.region_id -> Region.id (chained from first)
    assert ak._join_chain[1].source_alias == "_j1"
    assert ak._join_chain[1].local_field == "region_id"
    assert ak._join_chain[1].remote_table == "Region"
    assert ak._join_chain[1].leaf_columns == ["code"]


ak_db_path = tabula_rasa_path(Path("build/tests/ak.db"))


def _seed_db(conn: sqlite3.Connection) -> tuple[Region, Region, Team, Team, Player]:
    """Insert test data and return the records with their assigned PKs."""
    r1 = Region(code="us-west", name="US West").save(conn)
    r2 = Region(code="eu-north", name="EU North").save(conn)
    t1 = Team(region_id=r1.id, name="Eagles", founded=2010).save(conn)
    t2 = Team(region_id=r2.id, name="Vikings", founded=2015).save(conn)
    p1 = Player(team_id=t1.id, jersey_number=7, player_name="Alice").save(conn)
    conn.commit()
    return r1, r2, t1, t2, p1


def test_resolve_ak_single():
    """Resolve a single-field AK (Region.code) to integer PK."""
    SCHEMA.create_schema(ak_db_path)
    ak = AltKey.from_model(Region)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)

    with open_sqlite_db(ak_db_path) as conn:
        r1, r2, _t1, _t2, _p1 = _seed_db(conn)
        assert ak.resolve(conn, code="us-west") == r1.id
        assert ak.resolve(conn, code="eu-north") == r2.id
        assert ak.resolve(conn, code="nonexistent") is None


def test_resolve_ak_composite_one_level():
    """Resolve composite AK (Team: region code + name) with one FK cascade."""
    ak = AltKey.from_model(Team)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)

    with open_sqlite_db(ak_db_path) as conn:
        assert ak.resolve(conn, code="us-west", name="Eagles") is not None
        assert ak.resolve(conn, code="eu-north", name="Vikings") is not None
        assert ak.resolve(conn, code="us-west", name="Vikings") is None


def test_resolve_ak_two_levels():
    """Resolve 2-level cascade AK. All leaf values are flattened into kwargs."""
    ak = AltKey.from_model(Player)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)

    with open_sqlite_db(ak_db_path) as conn:
        pk = ak.resolve(conn, code="us-west", name="Eagles", jersey_number=7)
        assert pk is not None
        assert ak.resolve(conn, code="us-west", name="Eagles", jersey_number=99) is None


def test_to_ak_dict_local_only():
    """Serialize Region (local-only AK) — no query needed."""
    ak = AltKey.from_model(Region)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)

    with open_sqlite_db(ak_db_path) as conn:
        r1 = Region.select(conn, code="us-west")[0]
        result = ak.to_ak_dict(conn, r1)
        assert result == {"code": "us-west"}


def test_to_ak_dict_one_level():
    """Serialize Team — resolves region_id FK to Region.code."""
    ak = AltKey.from_model(Team)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)

    with open_sqlite_db(ak_db_path) as conn:
        t1 = Team.select(conn, name="Eagles")[0]
        result = ak.to_ak_dict(conn, t1)
        assert result == {"region_id": "us-west", "name": "Eagles"}


def test_to_ak_dict_two_levels():
    """Serialize Player — cascades through Team to Region."""
    ak = AltKey.from_model(Player)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)

    with open_sqlite_db(ak_db_path) as conn:
        p1 = Player.select(conn, jersey_number=7)[0]
        result = ak.to_ak_dict(conn, p1)
        assert result == {
            "team_id": {"region_id": "us-west", "name": "Eagles"},
            "jersey_number": 7,
        }


def test_to_ak_dicts_batch():
    """Batch serialize multiple records in one query."""
    ak = AltKey.from_model(Team)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)

    with open_sqlite_db(ak_db_path) as conn:
        teams = Team.select(conn)
        results = ak.to_ak_dicts(conn, teams)
        assert len(results) == 2
        codes = {r["region_id"] for r in results}
        assert codes == {"us-west", "eu-north"}


def test_dbmodel_resolve_ak():
    """DbModel.resolve_ak delegates to AltKey.resolve."""
    with open_sqlite_db(ak_db_path) as conn:
        pk = Region.resolve_ak(conn, code="us-west")
        assert pk is not None
        region = Region.load_by_id(conn, pk)
        assert region is not None
        assert region.code == "us-west"


def test_dbmodel_load_by_ak():
    """DbModel.load_by_ak resolves AK and loads in one call."""
    with open_sqlite_db(ak_db_path) as conn:
        team = Team.load_by_ak(conn, code="us-west", name="Eagles")
        assert team is not None
        assert team.name == "Eagles"
        assert team.founded == 2010

        missing = Team.load_by_ak(conn, code="us-west", name="Nonexistent")
        assert missing is None


def test_dbmodel_to_ak_dict():
    """DbModel instance.to_ak_dict() serializes AK values."""
    with open_sqlite_db(ak_db_path) as conn:
        team = Team.select(conn, name="Eagles")[0]
        ak_dict = team.to_ak_dict(conn)
        assert ak_dict == {"region_id": "us-west", "name": "Eagles"}


def test_dbmodel_to_ak_dicts():
    """DbModel.to_ak_dicts() batch serializes."""
    with open_sqlite_db(ak_db_path) as conn:
        teams = Team.select(conn)
        ak_dicts = Team.to_ak_dicts(conn, teams)
        assert len(ak_dicts) == 2


def test_dbmodel_no_ak_raises():
    """AK methods raise on models without AK."""
    with open_sqlite_db(ak_db_path) as conn:
        with pytest.raises(AssertionError, match="no alternative key"):
            Stat.resolve_ak(conn, id=1)
