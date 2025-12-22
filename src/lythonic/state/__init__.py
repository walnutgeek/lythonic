"""
Lightweight SQLite ORM with Pydantic-based schema definition.

This module provides a simple ORM-like interface for SQLite databases using
Pydantic models to define the schema. It supports automatic DDL generation,
CRUD operations, type mapping, and multi-tenant access patterns.

## Quick Start

Define your models by subclassing `DbModel`:

```python
from pydantic import Field
from lythonic.state import DbModel, Schema, open_sqlite_db

class Author(DbModel["Author"]):
    author_id: int = Field(default=-1, description="(PK)")
    name: str = Field(description="Author name")

class Book(DbModel["Book"]):
    book_id: int = Field(default=-1, description="(PK)")
    author_id: int = Field(description="(FK:Author.author_id)")
    title: str
    year: int | None = None

SCHEMA = Schema([Author, Book])
```

Create tables and use CRUD operations:

```python
SCHEMA.create_schema(Path("books.db"))

with open_sqlite_db("books.db") as conn:
    author = Author(name="Jane Austen")
    author.save(conn)  # Inserts with auto-increment, sets author_id

    book = Book(author_id=author.author_id, title="Pride and Prejudice", year=1813)
    book.save(conn)

    # Query
    books = Book.select(conn, author_id=author.author_id)
    loaded = Book.load_by_id(conn, book.book_id)
    conn.commit()
```

## Schema Definition

### Primary Keys

Mark a field as primary key by adding `(PK)` at the start of the description:

```python
user_id: int = Field(default=-1, description="(PK) Unique user identifier")
```

When `save()` is called on a model with `pk_field=-1`, it auto-increments.

### Foreign Keys

Mark foreign keys with `(FK:Table.field)`:

```python
author_id: int = Field(description="(FK:Author.author_id) Reference to author")
```

### Nullable Fields

Use `| None` union type:

```python
email: str | None = Field(default=None, description="Optional email")
```

### Enum and Literal Constraints

Enum and Literal types generate CHECK constraints:

```python
from typing import Literal

status: Literal["active", "inactive", "pending"]
```

Generates: `status TEXT NOT NULL CHECK (status IN ('active', 'inactive', 'pending'))`

## Supported Types

The following Python types are automatically mapped to SQLite:

| Python Type     | SQLite Type | Notes                           |
|-----------------|-------------|---------------------------------|
| int, bool       | INTEGER     | bool stored as 0/1              |
| float           | REAL        |                                 |
| str             | TEXT        |                                 |
| bytes           | BLOB        |                                 |
| datetime        | TEXT        | Stored as ISO format string     |
| date            | TEXT        | Stored as ISO format string     |
| Path            | TEXT        | Stored as string path           |
| Enum            | TEXT        | Stored as enum value            |
| IntEnum         | INTEGER     |                                 |
| BaseModel       | TEXT        | Stored as JSON string           |
| JsonBase        | TEXT        | Stored as JSON with type info   |

## CRUD Operations

### Insert

```python
record = MyModel(field1="value")
record.insert(conn, auto_increment=True)  # Sets PK from lastrowid
```

### Save (Upsert)

```python
record.save(conn)  # Inserts if pk=-1, else updates existing row
```

### Select

```python
# Select all
all_records = MyModel.select(conn)

# Filter by field value
filtered = MyModel.select(conn, status="active")

# Filter with operators: eq, ne, gt, lt, gte, lte
recent = MyModel.select(conn, gt__year=2020)

# IN clause (pass a list)
specific = MyModel.select(conn, status=["active", "pending"])
```

### Load by ID

```python
record = MyModel.load_by_id(conn, 42)  # Returns None if not found
```

### Update

```python
record.field = "new value"
n_updated = record.update(conn, pk_field=record.pk_field)
```

### Count and Exists

```python
count = MyModel.select_count(conn, status="active")
exists = MyModel.exists(conn, email="user@example.com")
```

## Multi-Model Queries

For joins, use `from_multi_model_row()`:

```python
from lythonic.state import execute_sql, from_multi_model_row

cursor = conn.cursor()
execute_sql(
    cursor,
    f"SELECT {Author.columns('a')}, {Book.columns('b')} "
    f"FROM {Author.alias('a')}, {Book.alias('b')} "
    "WHERE a.author_id = b.author_id"
)
for row in cursor.fetchall():
    author, book = from_multi_model_row(row, [Author, Book])
```

## Multi-Tenant Support (UserOwned)

For multi-tenant applications, use `UserOwned` base class (from `lythonic.state.user`):

```python
from lythonic.state.user import User, UserOwned, UserContext

class Task(UserOwned["Task"]):
    task_id: int = Field(default=-1, description="(PK)")
    title: str

# All operations require a UserContext
user_ctx = UserContext(user=current_user)
task = Task(title="My Task")
task.save_with_ctx(user_ctx, conn)  # Automatically sets user_id

# Queries are automatically scoped to user
tasks = Task.select(conn, user_ctx=user_ctx)
task = Task.load_by_id_with_ctx(conn, user_ctx, task_id=42)
```

## Schema Management

```python
from lythonic.state import Schema, DbFile

SCHEMA = Schema([User, Task, Event])

# Create tables directly
SCHEMA.create_schema(Path("app.db"))

# Or use DbFile for lifecycle management
db = DbFile("app.db", SCHEMA)
db.check(ensure=True)  # Creates tables if missing

with db.open() as conn:
    # Use connection
    pass
```

## Exports

- `DbModel`: Base class for database models
- `Schema`: Collection of DbModel classes
- `DbFile`: Database file manager
- `FieldInfo`: Field metadata extraction
- `open_sqlite_db`: Context manager for SQLite connections
- `execute_sql`: Execute SQL with logging
- `from_multi_model_row`: Parse multi-model query results
- `to_sql_datetime`: Convert datetime to SQL string
"""

import logging
import sqlite3
from collections.abc import Callable, Generator
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from pathlib import Path
from types import NoneType, UnionType
from typing import Any, Generic, Literal, NamedTuple, TypeVar, cast, get_args, get_origin

from pydantic import BaseModel
from typing_extensions import override

from lythonic.types import KnownType

logger = logging.getLogger("lythonic.state")


def execute_sql(cursor: sqlite3.Cursor, sql: str, *args: Any):
    logger.debug(f"execute: {sql}" + (f" -- with args: {args}" if len(args) > 0 else ""))
    cursor.execute(sql, *args)


T = TypeVar("T", bound="DbModel")  # pyright: ignore [reportMissingTypeArgument]

FilterOperator = Literal["eq", "ne", "gt", "lt", "gte", "lte"]
FILTER_OPERATOR_SQL: dict[FilterOperator, str] = {
    "eq": "=",
    "ne": "!=",
    "gt": ">",
    "lt": "<",
    "gte": ">=",
    "lte": "<=",
}


class FilterOp(NamedTuple):
    operator: FilterOperator
    name: str

    @classmethod
    def parse(cls, filter_key: str) -> "FilterOp":
        if "__" in filter_key:
            operator, name = filter_key.split("__", 1)
        else:
            operator = "eq"
            name = filter_key
        assert operator in FILTER_OPERATOR_SQL, f"Unknown filter operator: {operator}"
        return cls(operator=operator, name=name)

    @override
    def __str__(self) -> str:
        return self.name if self.operator == "eq" else f"{self.name}__{self.operator}"

    @override
    def __repr__(self) -> str:
        return self.__str__()


class FieldInfo(NamedTuple):
    name: str
    ktype: KnownType
    description: str
    nullable: bool
    primary_key: bool
    foreign_key: tuple[str, str] | None
    fixed_choices: list[Any] | None  # For enum types and literal types

    @classmethod
    def build(cls, name: str, field_info: Any) -> "FieldInfo":
        assert field_info.annotation is not None, f"Field {name} has no annotation"
        ann = field_info.annotation
        type_: type[Any]
        fixed_choices: list[Any] | None = None
        if isinstance(ann, type):
            type_ = ann
            is_nullable = False
        else:
            origin = get_origin(ann)
            if origin is UnionType:
                args = get_args(ann)
                nones = [arg for arg in args if arg is NoneType]
                valid_types = [arg for arg in args if arg is not NoneType]
                assert len(valid_types) == 1 and len(nones) == 1, (
                    f"Union {ann} must have exactly 2 types and one of them must be None"
                )
                type_ = valid_types[0]
                is_nullable = True
            elif origin is Literal:
                args = get_args(ann)
                nones = [arg is NoneType or arg is None for arg in args]
                fixed_choices = [a for a, n in zip(args, nones, strict=True) if not n]
                is_nullable = any(nones)
                types: set[type[Any]] = {type(c) for c in fixed_choices}
                assert len(types) == 1, f"Literal {ann} must have only one type"
                type_ = types.pop()
            else:
                raise AssertionError(f"Unknown field annotation type: {ann}")

        if issubclass(type_, Enum):
            fixed_choices = list(type_)

        description = field_info.description or ""
        is_primary_key = description.startswith("(PK)")
        if is_primary_key:
            description = description[4:].strip()
        assert not (is_primary_key and is_nullable), (
            "A field cannot be both a primary key and nullable"
        )

        is_foreign_key = description.startswith("(FK:")
        if is_foreign_key:
            x, description = description[4:].strip().split(")")
            table_name, field_name = x.split(".")
            foreign_key = table_name, field_name
        else:
            foreign_key = None

        return cls(
            name=name,
            ktype=KnownType.ensure(type_),
            description=description,
            nullable=is_nullable,
            primary_key=is_primary_key,
            foreign_key=foreign_key,
            fixed_choices=fixed_choices,
        )

    def check_constraint_ddl(self) -> str:
        if self.fixed_choices is not None:
            return f" CHECK ({self.name} IN ({', '.join([repr(self.ktype.db.map_to(c)) for c in self.fixed_choices])}))"
        return ""

    def to_sql_value(self, o: "DbModel[T]") -> Any:
        v = getattr(o, self.name)
        return self.ktype.db.map_to(v)

    def from_sql_value(self, v: Any) -> Any:
        return self.ktype.db.map_from(v)

    def set_value(self, o: "DbModel[T]", v: Any):
        setattr(o, self.name, self.from_sql_value(v))


class DbModel(BaseModel, Generic[T]):
    @classmethod
    def get_table_name(cls: type) -> str:
        return cls.__name__

    @classmethod
    def alias(cls, alias: str) -> str:
        return f"{cls.get_table_name()} as {alias}"

    @classmethod
    def columns(cls, alias: str | None = None) -> str:
        alias = "" if alias is None else f"{alias}."
        return ", ".join([f"{alias}{c.name}" for c in cls.get_field_infos()])

    @classmethod
    def get_field_infos(
        cls, filter: Callable[[FieldInfo], bool] = lambda _: True
    ) -> Generator[FieldInfo, None, None]:
        for field_name, field_info in cls.model_fields.items():
            fi = FieldInfo.build(field_name, field_info)
            if filter(fi):
                yield fi

    @classmethod
    def get_field_map(cls) -> dict[str, FieldInfo]:
        return {fi.name: fi for fi in cls.get_field_infos()}

    @classmethod
    def create_ddl(cls) -> str:
        fields: list[str] = []
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

        return f"CREATE TABLE {cls.get_table_name()} (" + ", ".join(fields) + ")"

    def insert(self, conn: sqlite3.Connection, auto_increment: bool = False):
        cursor = conn.cursor()
        cls = self.__class__
        fields = self._choose_fields(lambda fi: not auto_increment or not fi.primary_key)
        execute_sql(
            cursor,
            f"INSERT INTO {cls.get_table_name()} ({', '.join(map(lambda fi: fi.name, fields))}) "
            + f"VALUES ({', '.join(['?'] * len(fields))})",
            [fi.to_sql_value(self) for fi in fields],
        )
        if auto_increment:
            pks = list(cls.get_field_infos(lambda fi: fi.primary_key))
            assert len(pks) == 1
            pks[0].set_value(self, cursor.lastrowid)

    @classmethod
    def _choose_fields(cls, lambda_filter: Callable[[FieldInfo], bool]) -> list[FieldInfo]:
        return list(cls.get_field_infos(lambda_filter))

    @classmethod
    def _ensure_pk(cls) -> FieldInfo:
        pks = cls._choose_fields(lambda fi: fi.primary_key)
        assert len(pks) == 1
        return pks[0]

    def save(self, conn: sqlite3.Connection) -> None:
        cls = self.__class__
        pk = cls._ensure_pk()
        pk_val = getattr(self, pk.name)
        if pk_val == -1:
            self.insert(conn, auto_increment=True)
            return
        n_updated = self.update(conn, **{pk.name: pk_val})
        if n_updated == 0:
            self.insert(conn)
        else:
            assert n_updated == 1

    class _WhereBased(NamedTuple):
        cursor: sqlite3.Cursor
        table_name: str
        field_map: dict[str, FieldInfo]
        where_keys: set[str]
        where_clauses: list[str]
        args: list[Any]

        def fields(self) -> list[FieldInfo]:
            return list(self.field_map.values())

        def where_clause(self) -> str:
            return (
                f"WHERE {' AND '.join(self.where_clauses)}" if len(self.where_clauses) > 0 else ""
            )

        def execute_select(self, select_vars: str | None = None):
            if select_vars is None:
                select_vars = ", ".join([fi.name for fi in self.fields()])
            execute_sql(
                self.cursor,
                f"SELECT {select_vars} FROM {self.table_name} " + self.where_clause(),
                self.args,
            )

    @classmethod
    def _prepare_where(cls, conn: sqlite3.Connection, **filters: Any) -> _WhereBased:
        """Select all rows from the database that match the filters.

        Filters are given as keyword arguments, the keys are the field names
        and the values are the values to filter by.
        """
        field_map: dict[str, FieldInfo] = cls.get_field_map()
        filter_ops: dict[FilterOp, Any] = {FilterOp.parse(k): v for k, v in filters.items()}
        unknown_filters = {k: filter_ops[k] for k in filter_ops if k.name not in field_map}
        assert len(unknown_filters) == 0, (
            f"Known fields: {field_map.keys()}, but unknown filters: {unknown_filters}"
        )
        cursor = conn.cursor()
        args: list[Any] = []
        where_clauses: list[str] = []
        where_keys: set[str] = set()
        for f_op, v in filter_ops.items():
            fi = field_map[f_op.name]
            where_keys.add(f_op.name)
            if f_op.operator == "eq":
                if isinstance(v, list):
                    v_list: list[Any] = [fi.ktype.db.map_to(val) for val in cast(list[Any], v)]
                    where_clauses.append(f"{f_op.name} IN ({', '.join('?' * len(v_list))})")
                    args.extend(v_list)
                else:
                    where_clauses.append(f"{f_op.name} = ?")
                    args.append(fi.ktype.db.map_to(v))
            else:
                op_resolved = FILTER_OPERATOR_SQL[f_op.operator]
                where_clauses.append(f"{f_op.name} {op_resolved} ?")
                args.append(fi.ktype.db.map_to(v))

        return cls._WhereBased(
            cursor, cls.get_table_name(), field_map, where_keys, where_clauses, args
        )

    def update(self, conn: sqlite3.Connection, **filters: Any) -> int:
        cls = self.__class__
        sc = cls._prepare_where(conn, **filters)
        rest_of_the_fields = cls._choose_fields(lambda fi: fi.name not in sc.where_keys)
        execute_sql(
            sc.cursor,
            f"UPDATE {cls.get_table_name()} "
            + f"SET {', '.join([f'{fi.name} = ?' for fi in rest_of_the_fields])} "
            + sc.where_clause(),
            [fi.to_sql_value(self) for fi in rest_of_the_fields] + sc.args,
        )
        return sc.cursor.rowcount

    @classmethod
    def select(cls, conn: sqlite3.Connection, **filters: Any) -> list[T]:
        """Select all rows from the database that match the filters.

        Filters are given as keyword arguments, the keys are the field names
        and the values are the values to filter by.
        """
        sc = cls._prepare_where(conn, **filters)
        sc.execute_select()
        return [cls._from_row(row, sc.fields()) for row in sc.cursor.fetchall()]

    @classmethod
    def _from_row(cls, row: tuple[Any, ...], fields: list[FieldInfo]) -> T:
        assert len(row) == len(fields)
        return cast(
            T,
            cls.model_validate({fi.name: fi.from_sql_value(row[i]) for i, fi in enumerate(fields)}),
        )

    @classmethod
    def select_count(cls, conn: sqlite3.Connection, **filters: Any) -> int:
        sc = cls._prepare_where(conn, **filters)
        sc.execute_select("COUNT(*)")
        return sc.cursor.fetchone()[0]

    @classmethod
    def exists(cls, conn: sqlite3.Connection, **filters: Any) -> bool:
        return cls.select_count(conn, **filters) > 0

    @classmethod
    def load_by_id(cls: type[T], conn: sqlite3.Connection, id: int) -> T | None:
        rr: list[T] = cls.select(conn, **{cls._ensure_pk().name: id})
        assert len(rr) <= 1
        return rr[0] if rr else None


def from_multi_model_row(
    row: tuple[Any, ...], models: list[type[DbModel[Any]]]
) -> Generator[DbModel[Any], None, None]:
    start = 0
    for model in models:
        fields = list(model.get_field_infos())
        yield model._from_row(row[start : start + len(fields)], fields)  # pyright: ignore [reportPrivateUsage]
        start += len(fields)


def to_sql_datetime(dt: datetime) -> str:
    return dt.isoformat()


@contextmanager
def open_sqlite_db(db_name: str | Path):
    """Open or create SQLite database with given name.

    Args:
        db_name: Path or name of the SQLite database file

    Yields:
        sqlite3.Connection: Database connection object
    """
    logger.info(f"Opening database {db_name}")
    conn = sqlite3.connect(db_name)
    try:
        yield conn
    finally:
        conn.close()


class Schema:
    tables: list[type[DbModel[Any]]]

    def __init__(self, tables: list[type[DbModel[Any]]]):
        self.tables = tables

    def check_all_tables_exist(self, conn: sqlite3.Connection):
        cursor = conn.cursor()
        execute_sql(cursor, "SELECT name FROM sqlite_master WHERE type='table' ")
        all_tables = set(r[0] for r in cursor.fetchall())
        return all(t.get_table_name() in all_tables for t in self.tables)

    def create_tables(self, conn: sqlite3.Connection):
        cursor = conn.cursor()
        for table in self.tables:
            execute_sql(cursor, table.create_ddl())
        conn.commit()

    def create_schema(self, path: Path):
        with open_sqlite_db(path) as conn:
            self.create_tables(conn)


class DbFile:
    path: Path
    schema: Schema

    def __init__(self, db_path: Path | str, schema: Schema) -> None:
        self.path = Path(db_path)
        self.schema = schema

    def check(self, ensure: bool = False) -> bool:
        if not self.path.exists():
            if ensure:
                with self.open() as conn:
                    self.schema.create_tables(conn)
                return True
            return False
        else:
            # TODO : migrate db if necessary
            with self.open() as conn:
                if not self.schema.check_all_tables_exist(conn):
                    if not ensure:
                        return False
                    self.schema.create_tables(conn)
            return True
        return False

    def open(self):
        return open_sqlite_db(self.path)


class DbConfig(BaseModel):
    db_path: Path
