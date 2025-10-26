import logging
from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lythonic.misc import tabula_rasa_path
from lythonic.state import (
    FieldInfo,
    open_sqlite_db,
)
from lythonic.types import KnownType
from tests.rag_schema import (
    RagAction,
    RagActionCollection,
    RagSource,
    create_schema,
    select_all_active_sources,
)


def test_type_info():
    assert FieldInfo.build("timestamp", RagAction.model_fields["timestamp"]) == (
        "timestamp",
        KnownType.ensure("datetime"),
        "When the attempt was made",
        False,
        False,
        None,
    )


def test_dll():
    assert (
        RagAction.create_ddl()
        == "CREATE TABLE RagAction (action_id INTEGER PRIMARY KEY, source_id INTEGER REFERENCES RagSource(source_id), timestamp TEXT, n_chunks INTEGER, error TEXT NULL, sha256 TEXT)"
    )
    assert (
        RagSource.create_ddl()
        == "CREATE TABLE RagSource (source_id INTEGER PRIMARY KEY, absolute_path TEXT)"
    )
    assert (
        RagActionCollection.create_ddl()
        == "CREATE TABLE RagActionCollection (action_id INTEGER REFERENCES RagAction(action_id), action TEXT, collection TEXT, timestamp TEXT)"
    )


# test db cleanup
test_db_path = tabula_rasa_path(Path("build/tests/test.db"))


def search_caplog(
    caplog: pytest.LogCaptureFixture, prefix: str, category: str | None = None
) -> Generator[str, None, None]:
    for r in caplog.records:
        m = r.getMessage()
        if category is None or r.name == category:
            if m.startswith(prefix):
                yield m[len(prefix) :]


def test_sqlite_db(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG)
    create_schema(test_db_path)
    extract = tuple(search_caplog(caplog, "execute: ", category="llore.state"))
    print(extract)
    assert extract == (
        "CREATE TABLE RagSource (source_id INTEGER PRIMARY KEY, absolute_path TEXT)",
        "CREATE TABLE RagAction (action_id INTEGER PRIMARY KEY, source_id INTEGER REFERENCES RagSource(source_id), timestamp TEXT, n_chunks INTEGER, error TEXT NULL, sha256 TEXT)",
        "CREATE TABLE RagActionCollection (action_id INTEGER REFERENCES RagAction(action_id), action TEXT, collection TEXT, timestamp TEXT)",
        "CREATE TABLE ConvoMessage (role TEXT, tool_call_id TEXT NULL, content TEXT, finish_reason TEXT, message_id INTEGER PRIMARY KEY, session_id INTEGER REFERENCES ConvoSession(session_id), captured TEXT)",
        "CREATE TABLE ConvoSession (session_id INTEGER PRIMARY KEY, created TEXT, updated TEXT, model TEXT, user_id TEXT NULL, session_type TEXT)",
    )


def test_add_attempt():
    with open_sqlite_db(test_db_path) as conn:
        select = select_all_active_sources(conn)
        assert len(select) == 0
        s = RagSource(absolute_path=Path("test.txt"))
        assert s.source_id == -1
        s.save(conn)
        assert s.source_id != -1
        s_loaded = RagSource.load_by_id(conn, s.source_id)
        assert s_loaded is not None
        assert s_loaded == s
        assert isinstance(s_loaded.absolute_path, Path)
        a1 = RagAction(
            source_id=s.source_id,
            n_chunks=1,
            error=None,
            sha256="1234567890",
        )
        assert a1.action_id == -1
        a1.save(conn)

        assert a1.action_id != -1
        c1 = RagActionCollection(
            action_id=a1.action_id,
            action="new",
            collection="test",
        )
        c1.insert(conn)

        a2 = RagAction(
            source_id=s.source_id,
            n_chunks=1,
            error=None,
            sha256="6789012345",
        )
        assert a2.action_id == -1
        a2.save(conn)

        assert a2.action_id != -1
        assert a2.action_id != a1.action_id
        c2 = RagActionCollection(
            action_id=a2.action_id,
            action="new",
            collection="test",
        )
        c2.insert(conn)

        a2.timestamp = datetime.now(tz=UTC)
        a2.sha256 = "6172839405"
        a2.save(conn)

        select = select_all_active_sources(conn)
        if len(select) != 1:
            print(f"{select=}")
            raise AssertionError(f"{select=}")

        assert len(select[0][2]) == 1

        a3 = RagAction(
            action_id=6,
            source_id=s.source_id,
            n_chunks=6,
            error=None,
            sha256="1122334455",
        )
        a3.save(conn)

        c3 = RagActionCollection(
            action_id=a3.action_id,
            action="update",
            collection="test",
        )
        c3.insert(conn)

        c4 = RagActionCollection(
            action_id=a3.action_id,
            action="new",
            collection="test2",
        )
        c4.insert(conn)

        a1_loaded = RagAction.load_by_id(conn, a1.action_id)
        a2_loaded = RagAction.load_by_id(conn, a2.action_id)
        a3_loaded = RagAction.load_by_id(conn, a3.action_id)
        nothing_loaded = RagAction.load_by_id(conn, 7)
        assert nothing_loaded is None
        assert a1_loaded is not None
        assert a2_loaded is not None
        assert a3_loaded is not None
        assert a1_loaded == a1, f"{a1_loaded=} != {a1=}"
        assert a2_loaded == a2, f"{a2_loaded=} != {a2=}"
        assert a3_loaded == a3, f"{a3_loaded=} != {a3=}"
        assert isinstance(a1_loaded.timestamp, datetime)
        assert isinstance(a2_loaded.timestamp, datetime)
        assert isinstance(a3_loaded.timestamp, datetime)
        select = RagAction.select(conn, source_id=s.source_id)
        assert len(select) == 3
        assert a1 in select
        assert a2 in select
        assert a3 in select
        select = select_all_active_sources(conn)
        assert len(select) == 1
        s_loaded2, a_loaded2, c_loaded2 = select[0]
        assert s_loaded2 == s
        assert a_loaded2 == a3
        assert isinstance(c_loaded2, list)
        assert len(c_loaded2) == 2
        conn.commit()
