# Alternative Key Support for `DbModel`

## Problem

`DbModel` uses integer auto-increment primary keys for all tables. This is efficient
for joins and referential integrity, but integer PKs are not practical as external
identifiers in APIs. Tables like `dag_provenance.py`'s `node_executions` already use
composite alternative keys (`run_id`, `node_label`) — but in raw SQL outside `DbModel`.

We need a way to declare alternative keys on `DbModel` that:

- Preserve integer PKs for internal storage, joins, and upserts
- Provide human/API-friendly identifiers for lookup and serialization
- Support composite alternative keys
- Cascade through FK references to build external identity from related tables

## Design Summary

- Every `DbModel` table has an integer auto-increment PK (always `id`).
- A table may declare one alternative key (AK) — single or composite — by marking
  fields with `(AK)` in their description annotation.
- FK fields marked `(AK)` cascade: the external AK representation replaces the integer
  FK value with the referenced table's own AK value(s), recursively to arbitrary depth.
- A new `AltKey` class encapsulates cascade metadata and JOIN-based resolution.
- `DbModel` gains delegate methods: `resolve_ak()`, `load_by_ak()`, `to_ak_dict()`,
  `to_ak_dicts()`.
- `create_ddl()` emits a `UNIQUE` constraint for AK fields.
- Existing insert/save/update/delete methods are unchanged — they operate on integer PKs.

## Concrete Example

How `dag_provenance` tables would look as DbModels:

```python
class DagRunRecord(DbModel["DagRunRecord"]):
    id: int = Field(default=-1, description="(PK)")
    run_id: str = Field(description="(AK) Unique run identifier")
    dag_nsref: str
    parent_run_id: int | None = Field(default=None, description="(FK:DagRunRecord.id)")
    status: str
    started_at: float
    finished_at: float | None = None

class NodeExecutionRecord(DbModel["NodeExecutionRecord"]):
    id: int = Field(default=-1, description="(PK)")
    run_id: int = Field(description="(FK:DagRunRecord.id)(AK)")
    node_label: str = Field(description="(AK)")
    status: str
    started_at: float | None = None
    finished_at: float | None = None
    error: str | None = None

class EdgeTraversalRecord(DbModel["EdgeTraversalRecord"]):
    id: int = Field(default=-1, description="(PK)")
    run_id: int = Field(description="(FK:DagRunRecord.id)(AK)")
    upstream_label: str = Field(description="(AK)")
    downstream_label: str = Field(description="(AK)")
    traversed_at: float
```

`NodeExecutionRecord`'s AK is `(run_id, node_label)`. Internally, `run_id` is an
integer FK. Externally, resolution cascades through to `DagRunRecord.run_id` (text):

- External AK: `{"run_id": "abc-123", "node_label": "my_node"}`
- Internal storage: `run_id=42, node_label="my_node"` (where 42 is the integer PK of
  the DagRunRecord with `run_id="abc-123"`)

## Annotation Syntax

Field descriptions are parsed left-to-right for markers:

```
"(PK)"               -> primary_key=True
"(FK:Table.field)"   -> foreign_key=(Table, field)
"(AK)"               -> alt_key=True
"remaining text"     -> description
```

All markers are optional and independent. A field can combine them:
`"(FK:DagRunRecord.id)(AK)"` means the field is both a FK and part of the AK.

## `FieldInfo` Changes

`FieldInfo` gains a new field:

```python
class FieldInfo(NamedTuple):
    name: str
    ktype: KnownType
    description: str
    nullable: bool
    primary_key: bool
    foreign_key: tuple[str, str] | None
    fixed_choices: list[Any] | None
    alt_key: bool  # NEW
```

`build()` parses `(AK)` after `(FK:...)`, before the remaining description text.

## `AltKey` Class

```python
class AltKeyField(NamedTuple):
    """One component of a composite alternative key."""
    name: str
    foreign_key: tuple[str, str] | None  # (TableName, field_name) if FK

class AltKey:
    """
    Describes a table's alternative key and handles bidirectional resolution.

    Computed once from (AK)-annotated FieldInfos. FK fields in the AK
    cascade to the referenced table's own AK recursively.
    """
    model_cls: type[DbModel]
    fields: list[AltKeyField]
    _join_chain: list[...]  # precomputed JOIN recipe
```

`AltKey` is constructed lazily on first access and cached at class level as
`cls._alt_key`. It is `None` for models without `(AK)` fields.

The `_join_chain` is computed once and describes the full cascade: which tables to
JOIN, on which columns, and which leaf columns to SELECT for external representation.

### Resolution: AK values to integer PK

`resolve_ak()` builds a single JOIN query from the precomputed chain:

```python
pk: int | None = NodeExecutionRecord.resolve_ak(
    conn, run_id="abc-123", node_label="my_node"
)
```

Generated SQL:

```sql
SELECT ne.id
FROM NodeExecutionRecord ne
JOIN DagRunRecord dr ON ne.run_id = dr.id
WHERE dr.run_id = ? AND ne.node_label = ?
```

For deeper cascades (C -> B -> A), more JOINs stack:

```sql
SELECT c.id
FROM TableC c
JOIN TableB b ON c.b_id = b.id
JOIN TableA a ON b.a_id = a.id
WHERE a.ext_key = ? AND b.local_field = ? AND c.local_field = ?
```

**Parameter naming:** kwargs use the local field names. `run_id="abc-123"` means
"the AK-resolved value for the `run_id` FK field." `AltKey` knows which fields are
FK-based and need cascade resolution vs. which are local.

**Convenience method:**

```python
record: NodeExecutionRecord | None = NodeExecutionRecord.load_by_ak(
    conn, run_id="abc-123", node_label="my_node"
)
```

Combines `resolve_ak()` + `load_by_id()`.

### Serialization: integer PK/FK to AK values

`to_ak_dict()` builds a SELECT with JOINs to fetch leaf AK values:

```python
record = NodeExecutionRecord.load_by_id(conn, 7)
ak_dict = record.to_ak_dict(conn)
# {"run_id": "abc-123", "node_label": "my_node"}
```

Generated SQL:

```sql
SELECT dr.run_id, ne.node_label
FROM NodeExecutionRecord ne
JOIN DagRunRecord dr ON ne.run_id = dr.id
WHERE ne.id = ?
```

**Simple case optimization:** For tables where the AK has no FK fields (e.g.,
`DagRunRecord` with AK = `run_id` text), `to_ak_dict()` returns values directly from
the instance — no query needed.

**Batch serialization:**

```python
records = NodeExecutionRecord.select(conn, status="completed")
ak_dicts = NodeExecutionRecord.to_ak_dicts(conn, records)
```

Single query with `WHERE ne.id IN (?, ?, ...)`.

## DDL Generation

`create_ddl()` emits a table-level `UNIQUE` constraint for AK fields. The constraint
is on the stored columns (integer FK values), not the cascaded leaf values.

```sql
CREATE TABLE NodeExecutionRecord (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES DagRunRecord(id),
    node_label TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at REAL,
    finished_at REAL,
    error TEXT,
    UNIQUE (run_id, node_label)
)
```

For single-field AKs:

```sql
CREATE TABLE DagRunRecord (
    id INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL,
    dag_nsref TEXT NOT NULL,
    ...
    UNIQUE (run_id)
)
```

## Schema Integration

`Schema` provides the table registry that `AltKey` needs for cascade resolution.

**Validation at schema construction time:**

- Every FK field marked `(AK)` must reference a table that has its own AK
- No circular AK cascades
- Self-referential FK in AK is disallowed (it requires nullability, which contradicts
  the NOT NULL constraint on AK fields)
- Referenced tables must be in the same `Schema`

**`AltKey` lifecycle:**

- `create_ddl()` works without the schema (only needs local field names for UNIQUE)
- `resolve_ak()` / `to_ak_dict()` require the schema to have been built (for cascade)
- `Schema.__init__()` triggers `AltKey` chain resolution for all tables

## Constraints and Edge Cases

| Rule | Behavior |
|------|----------|
| One AK per table | Multiple `(AK)` fields form a single composite AK |
| AK fields must be NOT NULL | `FieldInfo.build()` raises if nullable + `(AK)` |
| Self-referential FK in AK | Disallowed — `Schema` validation rejects |
| AK value not found | `resolve_ak()` returns `None` |
| Duplicate AK on insert | SQLite `IntegrityError` (standard UNIQUE violation) |
| Tables without AK | `_alt_key` is `None`, AK methods raise if called |
| AK field ordering | Order of fields in model definition |

## What Does NOT Change

- `insert()`, `save()`, `update()`, `delete()` — integer PK only, unchanged
- `load_by_id()`, `select()`, `get_pk_filter()` — unchanged
- `_ensure_pk()` — still asserts single integer PK
- `_prepare_where()` — unchanged

## Components Summary

| Component | Responsibility |
|-----------|---------------|
| `FieldInfo` | Parses `(AK)` marker, stores `alt_key: bool` |
| `AltKey` | Cascade chain, JOIN query building, resolve + serialize |
| `DbModel` | `_alt_key` cache, delegate methods |
| `Schema` | Cross-table AK validation, table registry for cascade |
| `create_ddl()` | Emits `UNIQUE (...)` for AK fields |
