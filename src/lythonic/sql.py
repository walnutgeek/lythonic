import logging
import sqlite3
import time
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from inspect import Traceback
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel

from lythonic.types import KNOWN_TYPES, KnownType

log = logging.getLogger(__name__)


class HasDefault(BaseModel):
    default_value: Any


class ArgField(BaseModel):
    name: str
    ktype_name: str
    default: HasDefault | None
    is_key: bool

    @staticmethod
    def build_field_dict(fields: Iterable["ArgField"]) -> dict[str, "ArgField"]:
        return {f.name: f for f in fields}

    @property
    def ktype(self) -> KnownType:
        return KNOWN_TYPES.resolve_type(self.ktype_name)


class Table(BaseModel):
    name: str
    fields: dict[str, ArgField]


class SQLiteDbMap:
    auto_create: bool
    root: Path
    db_map: dict[str, "SQLiteDb"]

    def __init__(
        self, root: str | Path, auto_create: bool = False, db_names: None | list[str] = None
    ):
        self.auto_create = auto_create
        self.root = Path(root)
        self.db_map = {}
        if db_names:
            for n in db_names:
                self.add_with_the_same_db_name(n)

    def keys(self):
        return self.db_map.keys()

    def __getitem__(self, name: str) -> "SQLiteDb":
        if self.auto_create and name not in self.db_map:
            self.add_with_the_same_db_name(name)
        return self.db_map[name]

    def add_with_the_same_db_name(self, n: str):
        self.add(n, self.root / f"{n}.db")

    def add(self, name: str, db_file: str | Path) -> "SQLiteDbMap":
        assert name not in self.db_map
        self.db_map[name] = SQLiteDb(db_file)
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: Traceback | None):
        for db in self.db_map.values():
            db.close()


class SQLiteDb:
    conn: None | list[sqlite3.Connection]
    database: str

    def __init__(self, db_file: str | Path):
        if db_file == ":memory:":
            database = ":memory:"
        else:
            database = str(Path(db_file).absolute())
        self.database = database
        self.conn = None

    @contextmanager
    def connection(self, max_wait: float = 1.0) -> Generator[sqlite3.Connection, None, None]:
        if self.conn is None:
            self.conn = [sqlite3.connect(self.database, check_same_thread=False)]
        while True:
            try:
                connection = self.conn.pop()
                break
            except IndexError:
                if max_wait <= 0:
                    raise ValueError("Connection pool exhausted") from None
                time.sleep(0.1)
                max_wait -= 0.1
        try:
            yield connection
            connection.commit()
        except:
            connection.rollback()
            raise
        finally:
            self.conn.append(connection)

    def close(self):
        """try to close no matter what"""
        try:
            if self.conn and self.conn[0]:
                self.conn[0].close()
            self.conn = []
            self.conn = None
        except BaseException:
            pass  # pragma: no cover


def exec_sql(conn: sqlite3.Connection, sql: str, *args: Any) -> sqlite3.Cursor:
    log.info(f"exec_sql: sql='{sql}' args={args}")
    return conn.execute(sql, args)


class SQLiteTable:
    """
    Represents a SQLite table.

    Attributes:
        table (Table): The table object associated with this SQLiteTable.
        name (str): The name of the table.
        pkeys (str): The comma-separated string of primary key column names.

    Methods:
        create_table_sql(): Returns the SQL statement for creating the table.
        has_table(conn): Checks if the table exists in the database.
        ensure_table(conn): Ensures that the table exists in the database.
        insert(conn, *values): Inserts values into the table.

    """

    table: Table
    name: str
    pkeys: str
    _insert_sql: str

    def __init__(self, table: Table) -> None:
        self.table = table
        self.name = table.name
        self.pkeys = ", ".join(k.name for k in table.fields.values() if k.is_key)
        all_cols = ", ".join(k.name for k in table.fields.values())
        placeholders = ", ".join("?" for _ in range(len(table.fields)))
        self._insert_sql = f"insert into {self.name} ({all_cols}) values ({placeholders})"

    def create_table_sql(self):
        def field_ddl(f: ArgField) -> str:
            s = f"{f.name} {f.ktype.sqlite_type.name}"
            if f.default is not None and f.default.default_value is not None:
                s += f" DEFAULT {cast(str, f.default.default_value)!r}"
            return s

        all_defs = ", ".join(map(field_ddl, self.table.fields.values()))
        if_pkeys = f", primary key ({self.pkeys})" if self.pkeys else ""
        return f"create table {self.table.name} ({all_defs}{if_pkeys})"

    def has_table(self, conn: sqlite3.Connection) -> bool:
        """
        Checks if the table exists in the database.

        Args:
            conn: The SQLite connection object.

        Returns:
            bool: True if the table exists, False otherwise.

        """
        return bool(
            len(conn.execute(f"select 1 from sqlite_master where name = '{self.name}'").fetchall())
        )

    def ensure_table(self, conn: sqlite3.Connection):
        """
        Ensures that the table exists in the database.

        Args:
            conn: The SQLite connection object.

        """
        if not self.has_table(conn):
            exec_sql(conn, self.create_table_sql())

    def insert(self, conn: sqlite3.Connection, *values: Any):
        """
        Inserts values into the table.

        Args:
            conn: The SQLite connection object.
            *values: The values to be inserted into the table.

        """
        cur = exec_sql(conn, self._insert_sql, *values)
        assert cur.rowcount == 1
