# Alternative Key Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add alternative key (AK) support to `DbModel` — annotation-based declaration, UNIQUE DDL generation, JOIN-based cascade resolution, and bidirectional AK-to-PK mapping.

**Architecture:** `FieldInfo` gains `alt_key: bool` parsed from `(AK)` annotations. A new `AltKey` class (in `src/lythonic/state/alt_key.py`) encapsulates cascade chain metadata and builds JOIN queries for resolution and serialization. `Schema` provides the table registry and validates AK-FK consistency. `DbModel` delegates to `AltKey` via `resolve_ak()`, `load_by_ak()`, `to_ak_dict()`, `to_ak_dicts()`.

**Tech Stack:** Python 3.11+, Pydantic, SQLite, pytest

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/lythonic/state/__init__.py` | Modify | `FieldInfo`: add `alt_key` field + parse `(AK)`. `DbModel`: add delegate methods, modify `create_ddl()`. `Schema`: add `table_map`, validation. |
| `src/lythonic/state/alt_key.py` | Create | `AltKeyField`, `JoinStep`, `AltKey` classes — cascade chain, JOIN query building, resolve + serialize. |
| `tests/test_alt_key.py` | Create | All AK tests: FieldInfo parsing, DDL, resolution, serialization, validation, edge cases. |
| `tests/test_sqlite.py` | Modify | Update `test_type_info` tuple assertion to include new `alt_key` field. |

---

## Test Models

All tasks use these three test models (defined in `tests/test_alt_key.py`). They exercise single AK, composite AK with FK cascade, and 2-level deep cascade:

```python
from pydantic import Field
from lythonic.state import DbModel, Schema

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
```

- `Region` AK: `(code)` — single local field
- `Team` AK: `(region_id, name)` — composite, `region_id` cascades to `Region.code`
- `Player` AK: `(team_id, jersey_number)` — `team_id` cascades through `Team` → `Region` (2-level)

Also a model without AK for negative tests:

```python
class Stat(DbModel["Stat"]):
    id: int = Field(default=-1, description="(PK)")
    player_id: int = Field(description="(FK:Player.id)")
    points: int
```

Schema: `SCHEMA = Schema([Region, Team, Player, Stat])`

---

### Task 1: FieldInfo — add `alt_key` field and parse `(AK)`

**Files:**
- Modify: `src/lythonic/state/__init__.py` (lines 305-372, `FieldInfo` class)
- Modify: `tests/test_sqlite.py` (line 19-27, `test_type_info`)
- Test: `tests/test_alt_key.py` (create)

- [ ] **Step 1: Write failing tests for FieldInfo (AK) parsing**

Create `tests/test_alt_key.py`:

```python
from __future__ import annotations

import sqlite3
from pathlib import Path

from pydantic import Field

from lythonic.misc import tabula_rasa_path
from lythonic.state import DbModel, FieldInfo, Schema, open_sqlite_db


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
    import pytest

    class Bad(DbModel["Bad"]):
        id: int = Field(default=-1, description="(PK)")
        slug: str | None = Field(default=None, description="(AK) Nullable AK")

    with pytest.raises(AssertionError, match="nullable"):
        Bad.get_field_map()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_alt_key.py::test_fieldinfo_ak_parsing -v`
Expected: FAIL — `FieldInfo` has no `alt_key` attribute.

- [ ] **Step 3: Add `alt_key` field to `FieldInfo` and parse `(AK)` in `build()`**

In `src/lythonic/state/__init__.py`, modify `FieldInfo`:

```python
class FieldInfo(NamedTuple):
    name: str
    ktype: KnownType
    description: str
    nullable: bool
    primary_key: bool
    foreign_key: tuple[str, str] | None
    fixed_choices: list[Any] | None  # For enum types and literal types
    alt_key: bool
```

In `FieldInfo.build()`, after the FK parsing block (after line 362), add `(AK)` parsing:

```python
        is_alt_key = description.startswith("(AK)")
        if is_alt_key:
            description = description[4:].strip()
        assert not (is_alt_key and is_nullable), (
            f"Field {name} cannot be both an alternative key and nullable"
        )
```

Update the return statement to include `alt_key=is_alt_key`:

```python
        return cls(
            name=name,
            ktype=KnownType.ensure(type_),
            description=description,
            nullable=is_nullable,
            primary_key=is_primary_key,
            foreign_key=foreign_key,
            fixed_choices=fixed_choices,
            alt_key=is_alt_key,
        )
```

- [ ] **Step 4: Fix existing test_type_info in test_sqlite.py**

The tuple assertion needs the new `alt_key=False` at the end:

```python
def test_type_info():
    assert FieldInfo.build("timestamp", rag.RagAction.model_fields["timestamp"]) == (
        "timestamp",
        KnownType.ensure("datetime"),
        "When the attempt was made",
        False,
        False,
        None,
        None,
        False,
    )
```

- [ ] **Step 5: Run all tests to verify they pass**

Run: `uv run pytest tests/test_alt_key.py tests/test_sqlite.py -v`
Expected: All PASS.

- [ ] **Step 6: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero errors.

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/state/__init__.py tests/test_alt_key.py tests/test_sqlite.py
git commit -m "feat: add alt_key field to FieldInfo, parse (AK) annotation"
```

---

### Task 2: DDL — emit UNIQUE constraint for AK fields

**Files:**
- Modify: `src/lythonic/state/__init__.py` (lines 417-433, `create_ddl`)
- Test: `tests/test_alt_key.py`

- [ ] **Step 1: Write failing tests for UNIQUE DDL generation**

Add to `tests/test_alt_key.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_alt_key.py::test_ddl_single_ak -v`
Expected: FAIL — DDL doesn't include UNIQUE.

- [ ] **Step 3: Modify `create_ddl()` to emit UNIQUE constraint**

In `src/lythonic/state/__init__.py`, modify `DbModel.create_ddl()`:

```python
    @classmethod
    def create_ddl(cls) -> str:
        fields: list[str] = []
        ak_fields: list[str] = []
        for fi in cls.get_field_infos():
            type_name = (
                f"{fi.ktype.db_type_info.name}{'' if fi.primary_key or fi.nullable else ' NOT NULL'}"
                + f"{' PRIMARY KEY' if fi.primary_key else ''}"
                + (
                    f" REFERENCES {fi.foreign_key[0]}({fi.foreign_key[1]})"
                    if fi.foreign_key
                    else ""
                )
                + fi.check_constraint_ddl()
            )
            fields.append(f"{fi.name} {type_name}")
            if fi.alt_key:
                ak_fields.append(fi.name)

        if ak_fields:
            fields.append(f"UNIQUE ({', '.join(ak_fields)})")

        return f"CREATE TABLE {cls.get_table_name()} (" + ", ".join(fields) + ")"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_alt_key.py::test_ddl_single_ak tests/test_alt_key.py::test_ddl_composite_ak tests/test_alt_key.py::test_ddl_no_ak -v`
Expected: All PASS.

- [ ] **Step 5: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero errors. Existing DDL tests in `test_sqlite.py` still pass (those models have no AK fields).

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/state/__init__.py tests/test_alt_key.py
git commit -m "feat: create_ddl emits UNIQUE constraint for AK fields"
```

---

### Task 3: AltKey class — basic structure and local-only AK

**Files:**
- Create: `src/lythonic/state/alt_key.py`
- Test: `tests/test_alt_key.py`

- [ ] **Step 1: Write failing tests for AltKey construction**

Add to `tests/test_alt_key.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_alt_key.py::test_altkey_from_model_local_only -v`
Expected: FAIL — module `lythonic.state.alt_key` does not exist.

- [ ] **Step 3: Create `src/lythonic/state/alt_key.py`**

```python
"""
Alternative key support for DbModel.

`AltKey` encapsulates a table's alternative key metadata: which fields
compose it, which cascade through foreign keys, and the precomputed
JOIN chain for resolution and serialization queries.

Constructed via `AltKey.from_model()`. Returns `None` if the model has
no `(AK)`-annotated fields.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING, Any, NamedTuple

if TYPE_CHECKING:
    from lythonic.state import DbModel, FieldInfo

from lythonic.state import execute_sql


class AltKeyField(NamedTuple):
    """One component of a composite alternative key."""

    name: str
    foreign_key: tuple[str, str] | None  # (TableName, field_name) if FK


class JoinStep(NamedTuple):
    """One JOIN in the cascade chain.

    `local_field` on the current table joins to `remote_field` on
    `remote_table` (aliased as `alias`). `leaf_columns` are the AK
    column names on the remote table to SELECT (using the alias).
    """

    local_field: str
    remote_table: str
    remote_field: str
    alias: str
    leaf_columns: list[str]


class AltKey:
    """
    Describes a table's alternative key and handles bidirectional
    resolution between external AK values and internal integer PKs.

    FK fields in the AK cascade to the referenced table's own AK
    recursively. The cascade chain is precomputed as a list of
    `JoinStep`s used to build JOIN queries at runtime.
    """

    model_cls: type[DbModel[Any]]
    fields: list[AltKeyField]
    _join_chain: list[JoinStep]
    _local_ak_columns: list[str]

    def __init__(
        self,
        model_cls: type[DbModel[Any]],
        fields: list[AltKeyField],
    ) -> None:
        self.model_cls = model_cls
        self.fields = fields
        self._join_chain = []
        self._local_ak_columns = [f.name for f in fields if f.foreign_key is None]

    @classmethod
    def from_model(cls, model_cls: type[DbModel[Any]]) -> AltKey | None:
        """Build an AltKey from a model's (AK)-annotated fields, or None."""
        ak_fields: list[AltKeyField] = []
        for fi in model_cls.get_field_infos():
            if fi.alt_key:
                ak_fields.append(AltKeyField(name=fi.name, foreign_key=fi.foreign_key))
        if not ak_fields:
            return None
        return cls(model_cls, ak_fields)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_alt_key.py::test_altkey_from_model_local_only tests/test_alt_key.py::test_altkey_from_model_with_fk tests/test_alt_key.py::test_altkey_from_model_none -v`
Expected: All PASS.

- [ ] **Step 5: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero errors.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/state/alt_key.py tests/test_alt_key.py
git commit -m "feat: add AltKey class with from_model construction"
```

---

### Task 4: Schema — table registry and AK validation

**Files:**
- Modify: `src/lythonic/state/__init__.py` (lines 642-662, `Schema` class)
- Test: `tests/test_alt_key.py`

- [ ] **Step 1: Write failing tests for Schema validation**

Add to `tests/test_alt_key.py`:

```python
def test_schema_table_map():
    """Schema builds a table_map keyed by table name."""
    schema = Schema([Region, Team, Player, Stat])
    assert "Region" in schema.table_map
    assert "Team" in schema.table_map
    assert schema.table_map["Region"] is Region


def test_schema_validates_fk_ak_references():
    """Schema rejects FK-AK that references a table without its own AK."""
    import pytest

    class Orphan(DbModel["Orphan"]):
        id: int = Field(default=-1, description="(PK)")
        stat_id: int = Field(description="(FK:Stat.id)(AK)")
        label: str = Field(description="(AK)")

    with pytest.raises(AssertionError, match="has no alternative key"):
        Schema([Stat, Orphan])


def test_schema_validates_self_ref_fk_ak():
    """Schema rejects self-referential FK in AK."""
    import pytest

    class SelfRef(DbModel["SelfRef"]):
        id: int = Field(default=-1, description="(PK)")
        parent_id: int = Field(description="(FK:SelfRef.id)(AK)")
        code: str = Field(description="(AK)")

    with pytest.raises(AssertionError, match="[Ss]elf-referential"):
        Schema([SelfRef])


def test_schema_validates_missing_table():
    """Schema rejects FK-AK referencing a table not in the schema."""
    import pytest

    class Dangling(DbModel["Dangling"]):
        id: int = Field(default=-1, description="(PK)")
        region_id: int = Field(description="(FK:Region.id)(AK)")

    with pytest.raises(AssertionError, match="not in schema"):
        Schema([Dangling])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_alt_key.py::test_schema_table_map -v`
Expected: FAIL — `Schema` has no `table_map` attribute.

- [ ] **Step 3: Add `table_map` and AK validation to `Schema`**

In `src/lythonic/state/__init__.py`, modify `Schema.__init__()`:

```python
class Schema:
    tables: list[type[DbModel[Any]]]
    table_map: dict[str, type[DbModel[Any]]]

    def __init__(self, tables: list[type[DbModel[Any]]]):
        self.tables = tables
        self.table_map = {t.get_table_name(): t for t in tables}
        self._validate_alt_keys()

    def _validate_alt_keys(self) -> None:
        """Validate AK-FK consistency across all tables."""
        from lythonic.state.alt_key import AltKey

        for table in self.tables:
            ak = AltKey.from_model(table)
            if ak is None:
                continue
            table_name = table.get_table_name()
            for field in ak.fields:
                if field.foreign_key is None:
                    continue
                ref_table_name, _ref_field = field.foreign_key
                assert ref_table_name != table_name, (
                    f"{table_name}.{field.name}: self-referential FK in AK is disallowed"
                )
                assert ref_table_name in self.table_map, (
                    f"{table_name}.{field.name}: FK references '{ref_table_name}' "
                    f"which is not in schema"
                )
                ref_ak = AltKey.from_model(self.table_map[ref_table_name])
                assert ref_ak is not None, (
                    f"{table_name}.{field.name}: FK references '{ref_table_name}' "
                    f"which has no alternative key"
                )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_alt_key.py::test_schema_table_map tests/test_alt_key.py::test_schema_validates_fk_ak_references tests/test_alt_key.py::test_schema_validates_self_ref_fk_ak tests/test_alt_key.py::test_schema_validates_missing_table -v`
Expected: All PASS.

- [ ] **Step 5: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero errors. Existing Schema usages (in `rag_schema.py`, `cashflow_tracking.py`) have no AK fields so validation passes trivially.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/state/__init__.py tests/test_alt_key.py
git commit -m "feat: Schema validates AK-FK references and builds table_map"
```

---

### Task 5: AltKey — cascade chain computation

**Files:**
- Modify: `src/lythonic/state/alt_key.py`
- Test: `tests/test_alt_key.py`

- [ ] **Step 1: Write failing tests for cascade chain**

Add to `tests/test_alt_key.py`:

```python
from lythonic.state.alt_key import JoinStep


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
    assert ak._join_chain[0].local_field == "team_id"
    assert ak._join_chain[0].remote_table == "Team"
    # Second join: Team.region_id -> Region.id (chained from first)
    assert ak._join_chain[1].local_field == "region_id"
    assert ak._join_chain[1].remote_table == "Region"
    assert ak._join_chain[1].leaf_columns == ["code"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_alt_key.py::test_altkey_chain_local_only -v`
Expected: FAIL — `AltKey` has no `build_chain` method.

- [ ] **Step 3: Implement `build_chain()` on `AltKey`**

Add to `AltKey` in `src/lythonic/state/alt_key.py`:

```python
    def build_chain(self, table_map: dict[str, type[DbModel[Any]]]) -> None:
        """Precompute the JOIN chain by walking FK references recursively.

        Must be called after Schema construction provides the table_map.
        """
        self._join_chain = []
        alias_counter = 0

        def walk_fk_fields(
            model_cls: type[DbModel[Any]],
            fk_ak_fields: list[AltKeyField],
            parent_alias: str | None,
        ) -> None:
            nonlocal alias_counter
            for field in fk_ak_fields:
                if field.foreign_key is None:
                    continue
                ref_table_name, ref_field_name = field.foreign_key
                ref_model = table_map[ref_table_name]
                ref_ak = AltKey.from_model(ref_model)
                assert ref_ak is not None

                alias_counter += 1
                alias = f"_j{alias_counter}"

                # Leaf columns: non-FK AK fields on the referenced table
                leaf_cols = [f.name for f in ref_ak.fields if f.foreign_key is None]

                self._join_chain.append(
                    JoinStep(
                        local_field=field.name,
                        remote_table=ref_table_name,
                        remote_field=ref_field_name,
                        alias=alias,
                        leaf_columns=leaf_cols,
                    )
                )

                # Recurse for FK fields in the referenced table's AK
                ref_fk_fields = [f for f in ref_ak.fields if f.foreign_key is not None]
                if ref_fk_fields:
                    walk_fk_fields(ref_model, ref_fk_fields, alias)

        fk_fields = [f for f in self.fields if f.foreign_key is not None]
        walk_fk_fields(self.model_cls, fk_fields, None)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_alt_key.py::test_altkey_chain_local_only tests/test_alt_key.py::test_altkey_chain_one_level tests/test_alt_key.py::test_altkey_chain_two_levels -v`
Expected: All PASS.

- [ ] **Step 5: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero errors.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/state/alt_key.py tests/test_alt_key.py
git commit -m "feat: AltKey.build_chain computes cascade JOIN chain"
```

---

### Task 6: AltKey — resolve_ak (AK values to integer PK)

**Files:**
- Modify: `src/lythonic/state/alt_key.py`
- Test: `tests/test_alt_key.py`

- [ ] **Step 1: Write failing tests for resolve_ak**

Add to `tests/test_alt_key.py`:

```python
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
        r1, r2, t1, t2, p1 = _seed_db(conn)
        assert ak.resolve(conn, code="us-west") == r1.id
        assert ak.resolve(conn, code="eu-north") == r2.id
        assert ak.resolve(conn, code="nonexistent") is None


def test_resolve_ak_composite_one_level():
    """Resolve composite AK (Team: region code + name) with one FK cascade."""
    ak = AltKey.from_model(Team)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)

    with open_sqlite_db(ak_db_path) as conn:
        assert ak.resolve(conn, region_id="us-west", name="Eagles") is not None
        assert ak.resolve(conn, region_id="eu-north", name="Vikings") is not None
        assert ak.resolve(conn, region_id="us-west", name="Vikings") is None


def test_resolve_ak_two_levels():
    """Resolve 2-level cascade AK (Player: team AK + jersey)."""
    ak = AltKey.from_model(Player)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)

    with open_sqlite_db(ak_db_path) as conn:
        pk = ak.resolve(conn, team_id={"region_id": "us-west", "name": "Eagles"}, jersey_number=7)
        assert pk is not None
```

- [ ] **Step 2: Reconsider the 2-level cascade API**

Wait — the spec says kwargs use local field names: `resolve_ak(conn, team_id=..., jersey_number=7)`. But for a 2-level cascade, what value does `team_id` take? The cascaded AK of Team is `(region_id, name)`, which itself cascades `region_id` to Region's `code`.

The simplest approach: flatten all leaf AK values. Player's external AK is `(code, name, jersey_number)` — flattened across the full cascade. The kwargs are the **leaf column names** from the entire chain.

But this introduces a naming problem: `name` could collide between tables. Use the **local field name** as the kwarg key for each AK component on the model being resolved. For FK fields, the value is what the referenced table's AK would accept — recursively a dict if the referenced AK itself has FK fields. But that's nested dicts, which is ugly.

Simpler: flatten with the convention that FK-AK fields contribute their referenced table's leaf AK column names. If names collide, prefix with table name. For the common case, names don't collide.

Actually, re-reading the spec: "kwargs use the local field names. `run_id="abc-123"` means the AK-resolved value for the `run_id` FK field."

So for Player: `resolve(conn, team_id=..., jersey_number=7)`. The `team_id` value needs to identify a Team — but Team's AK is composite (`region_id`, `name`). So `team_id` can't be a single scalar.

The cleanest API that matches the spec: **flatten the cascade into a single dict of leaf values**. Player's `resolve()` takes `region_id="us-west", name="Eagles", jersey_number=7`. The `AltKey` knows from the chain which WHERE clauses to build.

Update the test:

```python
def test_resolve_ak_two_levels():
    """Resolve 2-level cascade AK (Player: region code + team name + jersey)."""
    ak = AltKey.from_model(Player)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)

    with open_sqlite_db(ak_db_path) as conn:
        pk = ak.resolve(conn, code="us-west", name="Eagles", jersey_number=7)
        assert pk is not None
        # Nonexistent combination
        assert ak.resolve(conn, code="us-west", name="Eagles", jersey_number=99) is None
```

Hmm, but this loses the "local field names" principle from the spec. Let me re-read...

The spec says: "kwargs use the local field names". For `NodeExecutionRecord`, the local field is `run_id` and the value `"abc-123"` is the text AK of the referenced `DagRunRecord`. That works because `DagRunRecord`'s AK is a single field. For a composite referenced AK, we need a different approach.

Since the user said "one AK per table" and the common case is single-field AK references, let's keep it simple: **kwargs use the local AK field name, and the value is the leaf AK value of the referenced table**. For FK fields referencing a table with a single-field AK, this is just the scalar value. For FK fields referencing a table with a composite AK, the value is a tuple of the leaf values.

But actually, the 2-level case (Player → Team → Region) means Team's AK when flattened is `(region_code, team_name)`. Player's `team_id` would need to accept a tuple `("us-west", "Eagles")`.

**Simplest viable API:** Flatten all leaf AK values into kwargs. Use leaf column names. If there's a collision, it's a schema design error (detected at chain-build time).

Let me go with flattened kwargs for now. This is the most natural API.

Replace the 2-level test:

```python
def test_resolve_ak_two_levels():
    """Resolve 2-level cascade AK. All leaf values are flattened into kwargs."""
    ak = AltKey.from_model(Player)
    assert ak is not None
    ak.build_chain(SCHEMA.table_map)

    with open_sqlite_db(ak_db_path) as conn:
        pk = ak.resolve(conn, code="us-west", name="Eagles", jersey_number=7)
        assert pk is not None
        assert ak.resolve(conn, code="us-west", name="Eagles", jersey_number=99) is None
```

- [ ] **Step 3: Implement `resolve()` on `AltKey`**

Add to `AltKey` in `src/lythonic/state/alt_key.py`:

```python
    def get_leaf_ak_names(self) -> list[str]:
        """Return the flattened list of leaf AK column names for the resolve/serialize API.

        Local non-FK AK fields contribute their own name. FK AK fields contribute
        the leaf names from the referenced table's AK (recursively).
        """
        names: list[str] = []
        for step in self._join_chain:
            names.extend(step.leaf_columns)
        names.extend(self._local_ak_columns)
        return names

    def resolve(self, conn: sqlite3.Connection, **ak_values: Any) -> int | None:
        """Resolve AK values to the integer PK via JOINs.

        kwargs are the flattened leaf AK column names and their values.
        Returns the integer PK or None if not found.
        """
        table_name = self.model_cls.get_table_name()
        pk_field = self.model_cls._ensure_pk()  # pyright: ignore[reportPrivateUsage]
        base_alias = "_t"

        joins: list[str] = []
        where_clauses: list[str] = []
        args: list[Any] = []

        # Track which alias each join step uses
        # The base table is aliased as _t
        # For chained joins, we need to join from the previous step's alias
        prev_alias_for_field: dict[str, str] = {}
        for field in self.fields:
            prev_alias_for_field[field.name] = base_alias

        for step in self._join_chain:
            source_alias = prev_alias_for_field.get(step.local_field, base_alias)
            joins.append(
                f"JOIN {step.remote_table} {step.alias} "
                f"ON {source_alias}.{step.local_field} = {step.alias}.{step.remote_field}"
            )
            for col in step.leaf_columns:
                assert col in ak_values, (
                    f"Missing AK value for '{col}' (from {step.remote_table})"
                )
                where_clauses.append(f"{step.alias}.{col} = ?")
                args.append(ak_values[col])

        # Local (non-FK) AK fields
        for col in self._local_ak_columns:
            assert col in ak_values, f"Missing AK value for local field '{col}'"
            where_clauses.append(f"{base_alias}.{col} = ?")
            args.append(ak_values[col])

        sql = (
            f"SELECT {base_alias}.{pk_field.name} FROM {table_name} {base_alias} "
            + " ".join(joins)
            + " WHERE " + " AND ".join(where_clauses)
        )
        cursor = conn.cursor()
        execute_sql(cursor, sql, tuple(args))
        row = cursor.fetchone()
        return row[0] if row else None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_alt_key.py::test_resolve_ak_single tests/test_alt_key.py::test_resolve_ak_composite_one_level tests/test_alt_key.py::test_resolve_ak_two_levels -v`
Expected: All PASS.

- [ ] **Step 5: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero errors.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/state/alt_key.py tests/test_alt_key.py
git commit -m "feat: AltKey.resolve() — JOIN-based AK to PK resolution"
```

---

### Task 7: AltKey — to_ak_dict and to_ak_dicts (serialization)

**Files:**
- Modify: `src/lythonic/state/alt_key.py`
- Test: `tests/test_alt_key.py`

- [ ] **Step 1: Write failing tests for serialization**

Add to `tests/test_alt_key.py`:

```python
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
```

- [ ] **Step 2: Reconsider serialization dict structure**

Wait — for `to_ak_dict` the spec says the dict keys are the local AK field names. For Team's `region_id` FK field, the key is `"region_id"` and the value is `"us-west"` (the Region's AK value). This is a flat dict: `{"region_id": "us-west", "name": "Eagles"}`.

For Player's `team_id` FK field — Team's AK is composite (region_id + name), so what goes in `"team_id"`? A nested dict: `{"region_id": "us-west", "name": "Eagles"}`.

This matches `test_to_ak_dict_two_levels` above. The serialization is **structured** (nested for composite FK AKs), while `resolve()` is **flat** (all leaf values as kwargs).

This is a reasonable asymmetry: resolve needs flat kwargs for ergonomic API calls, serialization preserves the hierarchical structure so consumers know which values belong to which FK.

- [ ] **Step 3: Implement `to_ak_dict()` and `to_ak_dicts()`**

Add to `AltKey` in `src/lythonic/state/alt_key.py`:

```python
    def to_ak_dict(self, conn: sqlite3.Connection, record: DbModel[Any]) -> dict[str, Any]:
        """Serialize a record's AK fields, resolving FK values to their referenced AK.

        Returns a dict keyed by local AK field names. FK fields are replaced with
        the referenced table's AK value (scalar for single-field AK, nested dict
        for composite AK). Local fields use the instance value directly.
        """
        if not self._join_chain:
            # All AK fields are local — no query needed
            return {col: getattr(record, col) for col in self._local_ak_columns}

        return self._serialize_records(conn, [record])[0]

    def to_ak_dicts(
        self, conn: sqlite3.Connection, records: list[DbModel[Any]]
    ) -> list[dict[str, Any]]:
        """Batch serialize multiple records' AK fields in one query."""
        if not records:
            return []
        if not self._join_chain:
            return [{col: getattr(r, col) for col in self._local_ak_columns} for r in records]
        return self._serialize_records(conn, records)

    def _serialize_records(
        self, conn: sqlite3.Connection, records: list[DbModel[Any]]
    ) -> list[dict[str, Any]]:
        """Build a JOIN query to fetch leaf AK values for one or more records."""
        table_name = self.model_cls.get_table_name()
        pk_field = self.model_cls._ensure_pk()  # pyright: ignore[reportPrivateUsage]
        base_alias = "_t"

        # Build SELECT columns: pk (for matching back), then leaf columns per join, then local columns
        select_parts: list[str] = [f"{base_alias}.{pk_field.name}"]
        joins: list[str] = []

        prev_alias_for_field: dict[str, str] = {}
        for field in self.fields:
            prev_alias_for_field[field.name] = base_alias

        # Track which join step corresponds to which AK field
        field_to_join: dict[str, JoinStep] = {}
        for step in self._join_chain:
            source_alias = prev_alias_for_field.get(step.local_field, base_alias)
            joins.append(
                f"JOIN {step.remote_table} {step.alias} "
                f"ON {source_alias}.{step.local_field} = {step.alias}.{step.remote_field}"
            )
            for col in step.leaf_columns:
                select_parts.append(f"{step.alias}.{col}")
            field_to_join[step.local_field] = step

        for col in self._local_ak_columns:
            select_parts.append(f"{base_alias}.{col}")

        # WHERE clause: filter by PKs
        pk_values = [getattr(r, pk_field.name) for r in records]
        placeholders = ",".join("?" * len(pk_values))
        where = f"WHERE {base_alias}.{pk_field.name} IN ({placeholders})"

        sql = (
            f"SELECT {', '.join(select_parts)} FROM {table_name} {base_alias} "
            + " ".join(joins)
            + f" {where}"
        )
        cursor = conn.cursor()
        execute_sql(cursor, sql, tuple(pk_values))
        rows = cursor.fetchall()

        # Map PK -> row for matching back to input records
        row_by_pk: dict[int, tuple[Any, ...]] = {}
        for row in rows:
            row_by_pk[row[0]] = row

        # Build result dicts
        results: list[dict[str, Any]] = []
        for record in records:
            pk_val = getattr(record, pk_field.name)
            row = row_by_pk[pk_val]
            result: dict[str, Any] = {}
            col_idx = 1  # skip pk at index 0

            for field in self.fields:
                if field.foreign_key is not None and field.name in field_to_join:
                    step = field_to_join[field.name]
                    if len(step.leaf_columns) == 1:
                        result[field.name] = row[col_idx]
                        col_idx += 1
                    else:
                        nested: dict[str, Any] = {}
                        for lc in step.leaf_columns:
                            nested[lc] = row[col_idx]
                            col_idx += 1
                        result[field.name] = nested
                else:
                    # Must be a local AK column — picked up at the end
                    pass

            # Local columns are at the end of the SELECT
            for col in self._local_ak_columns:
                result[col] = row[col_idx]
                col_idx += 1

            results.append(result)
        return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_alt_key.py::test_to_ak_dict_local_only tests/test_alt_key.py::test_to_ak_dict_one_level tests/test_alt_key.py::test_to_ak_dict_two_levels tests/test_alt_key.py::test_to_ak_dicts_batch -v`
Expected: All PASS.

- [ ] **Step 5: Handle the 2-level serialization case**

The 2-level case (Player → Team → Region) needs the Team join's leaf columns AND the Region join's leaf columns grouped properly under the `team_id` key. The current implementation handles direct joins but needs the recursive grouping for deeper cascades.

Verify by running: `uv run pytest tests/test_alt_key.py::test_to_ak_dict_two_levels -v`

If it fails, the serialization logic needs adjustment to recursively group cascaded join results under their parent FK field name. The fix: when building the result dict, for each FK AK field, collect all leaf columns from its direct join AND any chained joins beneath it into a nested dict.

- [ ] **Step 6: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero errors.

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/state/alt_key.py tests/test_alt_key.py
git commit -m "feat: AltKey.to_ak_dict/to_ak_dicts — serialize records to AK values"
```

---

### Task 8: DbModel — delegate methods and Schema-triggered chain init

**Files:**
- Modify: `src/lythonic/state/__init__.py` (`DbModel` class, `Schema` class)
- Test: `tests/test_alt_key.py`

- [ ] **Step 1: Write failing tests for DbModel delegate methods**

Add to `tests/test_alt_key.py`:

```python
def test_dbmodel_resolve_ak():
    """DbModel.resolve_ak delegates to AltKey.resolve."""
    SCHEMA.create_schema(ak_db_path)
    with open_sqlite_db(ak_db_path) as conn:
        _seed_db(conn)
        pk = Region.resolve_ak(conn, code="us-west")
        assert pk is not None
        region = Region.load_by_id(conn, pk)
        assert region is not None
        assert region.code == "us-west"


def test_dbmodel_load_by_ak():
    """DbModel.load_by_ak resolves AK and loads in one call."""
    with open_sqlite_db(ak_db_path) as conn:
        team = Team.load_by_ak(conn, region_id="us-west", name="Eagles")
        assert team is not None
        assert team.name == "Eagles"
        assert team.founded == 2010

        missing = Team.load_by_ak(conn, region_id="us-west", name="Nonexistent")
        assert missing is None


def test_dbmodel_to_ak_dict():
    """DbModel instance.to_ak_dict() serializes AK values."""
    with open_sqlite_db(ak_db_path) as conn:
        team = Team.load_by_ak(conn, region_id="us-west", name="Eagles")
        assert team is not None
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
    import pytest

    with open_sqlite_db(ak_db_path) as conn:
        with pytest.raises(AssertionError, match="no alternative key"):
            Stat.resolve_ak(conn, id=1)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_alt_key.py::test_dbmodel_resolve_ak -v`
Expected: FAIL — `DbModel` has no `resolve_ak` method.

- [ ] **Step 3: Add `_alt_key` caching and delegate methods to `DbModel`**

In `src/lythonic/state/__init__.py`, add to `DbModel` (after `load_by_id`):

```python
    # Alternative key support

    _alt_key_cache: ClassVar[AltKey | None] = None
    _alt_key_initialized: ClassVar[bool] = False

    @classmethod
    def _get_alt_key(cls) -> AltKey | None:
        if not cls._alt_key_initialized:
            from lythonic.state.alt_key import AltKey
            cls._alt_key_cache = AltKey.from_model(cls)
            cls._alt_key_initialized = True
        return cls._alt_key_cache

    @classmethod
    def _ensure_alt_key(cls) -> AltKey:
        from lythonic.state.alt_key import AltKey
        ak = cls._get_alt_key()
        assert ak is not None, f"{cls.get_table_name()} has no alternative key defined"
        return ak

    @classmethod
    def resolve_ak(cls, conn: sqlite3.Connection, **ak_values: Any) -> int | None:
        """Resolve alternative key values to the integer PK."""
        return cls._ensure_alt_key().resolve(conn, **ak_values)

    @classmethod
    def load_by_ak(cls: type[T], conn: sqlite3.Connection, **ak_values: Any) -> T | None:
        """Resolve AK values and load the record."""
        pk = cls.resolve_ak(conn, **ak_values)
        if pk is None:
            return None
        return cls.load_by_id(conn, pk)

    def to_ak_dict(self, conn: sqlite3.Connection) -> dict[str, Any]:
        """Serialize this record's alternative key values."""
        return self.__class__._ensure_alt_key().to_ak_dict(conn, self)

    @classmethod
    def to_ak_dicts(cls, conn: sqlite3.Connection, records: list[T]) -> list[dict[str, Any]]:
        """Batch serialize alternative key values for multiple records."""
        return cls._ensure_alt_key().to_ak_dicts(conn, records)
```

Add the import at the top of the file:

```python
from typing import Any, ClassVar, Generic, Literal, NamedTuple, Self, TypeVar, cast, get_args, get_origin
```

- [ ] **Step 4: Wire Schema to trigger `build_chain()` on all AltKeys**

In `Schema._validate_alt_keys()`, after validation passes, build the chains:

```python
    def _validate_alt_keys(self) -> None:
        """Validate AK-FK consistency and build cascade chains."""
        from lythonic.state.alt_key import AltKey

        for table in self.tables:
            ak = AltKey.from_model(table)
            if ak is None:
                continue
            table_name = table.get_table_name()
            for field in ak.fields:
                if field.foreign_key is None:
                    continue
                ref_table_name, _ref_field = field.foreign_key
                assert ref_table_name != table_name, (
                    f"{table_name}.{field.name}: self-referential FK in AK is disallowed"
                )
                assert ref_table_name in self.table_map, (
                    f"{table_name}.{field.name}: FK references '{ref_table_name}' "
                    f"which is not in schema"
                )
                ref_ak = AltKey.from_model(self.table_map[ref_table_name])
                assert ref_ak is not None, (
                    f"{table_name}.{field.name}: FK references '{ref_table_name}' "
                    f"which has no alternative key"
                )

        # Build cascade chains now that validation passed
        for table in self.tables:
            ak = AltKey.from_model(table)
            if ak is not None:
                ak.build_chain(self.table_map)
                # Cache on the model class
                table._alt_key_cache = ak  # pyright: ignore[reportPrivateUsage]
                table._alt_key_initialized = True  # pyright: ignore[reportPrivateUsage]
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_alt_key.py::test_dbmodel_resolve_ak tests/test_alt_key.py::test_dbmodel_load_by_ak tests/test_alt_key.py::test_dbmodel_to_ak_dict tests/test_alt_key.py::test_dbmodel_to_ak_dicts tests/test_alt_key.py::test_dbmodel_no_ak_raises -v`
Expected: All PASS.

- [ ] **Step 6: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero errors.

- [ ] **Step 7: Commit**

```bash
git add src/lythonic/state/__init__.py src/lythonic/state/alt_key.py tests/test_alt_key.py
git commit -m "feat: DbModel delegate methods for AK resolve/load/serialize"
```

---

### Task 9: Integration test — full lifecycle with seed data

**Files:**
- Test: `tests/test_alt_key.py`

- [ ] **Step 1: Write a full lifecycle integration test**

Add to `tests/test_alt_key.py`:

```python
def test_full_lifecycle():
    """End-to-end: create schema, insert via integer PKs, query via AKs, serialize."""
    db_path = tabula_rasa_path(Path("build/tests/ak_lifecycle.db"))
    schema = Schema([Region, Team, Player, Stat])
    schema.create_schema(db_path)

    with open_sqlite_db(db_path) as conn:
        # Insert via integer PKs (standard DbModel flow)
        r1 = Region(code="us-west", name="US West").save(conn)
        r2 = Region(code="eu-north", name="EU North").save(conn)
        t1 = Team(region_id=r1.id, name="Eagles", founded=2010).save(conn)
        t2 = Team(region_id=r2.id, name="Vikings").save(conn)
        p1 = Player(team_id=t1.id, jersey_number=7, player_name="Alice").save(conn)
        p2 = Player(team_id=t1.id, jersey_number=10, player_name="Bob").save(conn)
        p3 = Player(team_id=t2.id, jersey_number=7, player_name="Charlie").save(conn)
        conn.commit()

        # UNIQUE constraint enforced: duplicate AK raises IntegrityError
        import sqlite3 as sqlite_mod

        with pytest.raises(sqlite_mod.IntegrityError):
            Player(team_id=t1.id, jersey_number=7, player_name="Duplicate").insert(conn)
        conn.rollback()

        # Resolve AK → PK
        assert Region.resolve_ak(conn, code="us-west") == r1.id
        assert Team.resolve_ak(conn, region_id="us-west", name="Eagles") == t1.id
        assert Player.resolve_ak(conn, code="us-west", name="Eagles", jersey_number=7) == p1.id

        # Load by AK
        loaded_team = Team.load_by_ak(conn, region_id="eu-north", name="Vikings")
        assert loaded_team is not None
        assert loaded_team.id == t2.id

        # Serialize single
        ak = p1.to_ak_dict(conn)
        assert ak["jersey_number"] == 7

        # Serialize batch
        all_players = Player.select(conn)
        ak_dicts = Player.to_ak_dicts(conn, all_players)
        assert len(ak_dicts) == 3

        # Roundtrip: serialize then resolve
        for player, ak_dict in zip(all_players, ak_dicts, strict=True):
            # Flatten nested AK dict for resolve
            flat: dict[str, Any] = {}
            for k, v in ak_dict.items():
                if isinstance(v, dict):
                    flat.update(v)
                else:
                    flat[k] = v
            resolved_pk = Player.resolve_ak(conn, **flat)
            assert resolved_pk == player.id
```

- [ ] **Step 2: Run the integration test**

Run: `uv run pytest tests/test_alt_key.py::test_full_lifecycle -v -s`
Expected: PASS.

- [ ] **Step 3: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero errors.

- [ ] **Step 4: Commit**

```bash
git add tests/test_alt_key.py
git commit -m "test: full lifecycle integration test for alternative keys"
```

---

### Task 10: Final cleanup and documentation

**Files:**
- Modify: `src/lythonic/state/__init__.py` (module docstring)
- Modify: `src/lythonic/state/alt_key.py` (module docstring)

- [ ] **Step 1: Update module docstring in `__init__.py`**

Add an "Alternative Keys" section to the module docstring after the "Foreign Keys" section:

```python
### Alternative Keys

Mark fields as part of the alternative key with `(AK)`:

```python
class Region(DbModel["Region"]):
    id: int = Field(default=-1, description="(PK)")
    code: str = Field(description="(AK) Region code")
    name: str

class Team(DbModel["Team"]):
    id: int = Field(default=-1, description="(PK)")
    region_id: int = Field(description="(FK:Region.id)(AK)")
    name: str = Field(description="(AK) Team name")
```

AK fields generate a UNIQUE constraint. FK fields marked `(AK)` cascade through
to the referenced table's AK for external resolution:

```python
SCHEMA = Schema([Region, Team])  # Validates and builds cascade chains

with open_sqlite_db("db.sqlite") as conn:
    team = Team.load_by_ak(conn, region_id="us-west", name="Eagles")
    ak_dict = team.to_ak_dict(conn)  # {"region_id": "us-west", "name": "Eagles"}
```
```

- [ ] **Step 2: Add `AltKey` to module exports list in docstring**

Update the Exports section:

```
- `AltKey`: Alternative key metadata and resolution (from `lythonic.state.alt_key`)
```

- [ ] **Step 3: Run full lint and test suite**

Run: `make lint && make test`
Expected: Zero errors, all tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/lythonic/state/__init__.py src/lythonic/state/alt_key.py
git commit -m "docs: add alternative key documentation to module docstrings"
```
