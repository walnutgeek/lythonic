import logging
from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path

import pytest

import tests.rag_schema as rag
from lythonic.examples.cashflow_tracking import SCHEMA as acc
from lythonic.misc import tabula_rasa_path
from lythonic.state import (
    FieldInfo,
    open_sqlite_db,
)
from lythonic.types import KnownType


def test_type_info():
    assert FieldInfo.build("timestamp", rag.RagAction.model_fields["timestamp"]) == (
        "timestamp",
        KnownType.ensure("datetime"),
        "When the attempt was made",
        False,
        False,
        None,
        None,
    )


rag_ddls = (
    "CREATE TABLE RagSource (source_id INTEGER PRIMARY KEY, absolute_path TEXT NOT NULL)",
    "CREATE TABLE RagAction (action_id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL REFERENCES RagSource(source_id), timestamp TEXT NOT NULL, n_chunks INTEGER NOT NULL, error TEXT, sha256 TEXT NOT NULL)",
    "CREATE TABLE RagActionCollection (action_id INTEGER NOT NULL REFERENCES RagAction(action_id), action TEXT NOT NULL CHECK (action IN ('new', 'update', 'delete')), collection TEXT NOT NULL, timestamp TEXT NOT NULL)",
    "CREATE TABLE ConvoMessage (role TEXT NOT NULL CHECK (role IN ('system', 'user', 'assistant', 'tool')), tool_call_id TEXT, content TEXT NOT NULL, finish_reason TEXT NOT NULL CHECK (finish_reason IN ('stop', 'length', 'content_filter', 'null')), message_id INTEGER PRIMARY KEY, session_id INTEGER NOT NULL REFERENCES ConvoSession(session_id), captured TEXT NOT NULL)",
    "CREATE TABLE ConvoSession (session_id INTEGER PRIMARY KEY, created TEXT NOT NULL, updated TEXT NOT NULL, model TEXT NOT NULL, user_id TEXT, session_type TEXT NOT NULL CHECK (session_type IN ('active', 'completed', 'failed', 'archived')))",
)

acc_ddls = (
    "CREATE TABLE User (user_id INTEGER PRIMARY KEY, info TEXT NOT NULL, created_at TEXT NOT NULL)",
    "CREATE TABLE Organization (user_id INTEGER NOT NULL REFERENCES User(user_id), org_id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL, is_hidden INTEGER NOT NULL)",
    "CREATE TABLE Account (user_id INTEGER NOT NULL REFERENCES User(user_id), acc_id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL REFERENCES Organization(org_id), name TEXT NOT NULL, account_type TEXT NOT NULL CHECK (account_type IN ('cash', 'credit_card')), account_kind TEXT, start_date TEXT NOT NULL, end_date TEXT, linked_cash_acc_id INTEGER REFERENCES Account(acc_id), created_at TEXT NOT NULL)",
    "CREATE TABLE ScheduledEvent (user_id INTEGER NOT NULL REFERENCES User(user_id), sch_id INTEGER PRIMARY KEY, acc_id INTEGER NOT NULL REFERENCES Account(acc_id), description_template TEXT NOT NULL, event_type TEXT NOT NULL CHECK (event_type IN ('deposit', 'payment', 'set_balance')), amount_type TEXT NOT NULL CHECK (amount_type IN ('fixed', 'variable')), amount REAL, frequency TEXT NOT NULL CHECK (frequency IN ('weekly', 'monthly', 'quarterly', 'annually')), day_in_the_cycle INTEGER NOT NULL, reminder_days INTEGER, root_version_id INTEGER REFERENCES ScheduledEvent(sch_id), is_active INTEGER NOT NULL, start_date TEXT NOT NULL, end_date TEXT, modified_at TEXT NOT NULL)",
    "CREATE TABLE CashEvent (user_id INTEGER NOT NULL REFERENCES User(user_id), cash_id INTEGER PRIMARY KEY, cash_acc_id INTEGER NOT NULL REFERENCES Account(acc_id), related_acc_id INTEGER REFERENCES Account(acc_id), sch_id INTEGER REFERENCES ScheduledEvent(sch_id), event_type TEXT NOT NULL CHECK (event_type IN ('deposit', 'payment', 'set_balance')), event_date TEXT NOT NULL, amount REAL NOT NULL, description TEXT, modified_at TEXT NOT NULL)",
    "CREATE TABLE CashEventNotification (user_id INTEGER NOT NULL REFERENCES User(user_id), note_id INTEGER PRIMARY KEY, sch_id INTEGER NOT NULL REFERENCES ScheduledEvent(sch_id), note_date TEXT NOT NULL, muted INTEGER NOT NULL, event_date TEXT NOT NULL)",
    "CREATE TABLE CashAccountProjection (user_id INTEGER NOT NULL REFERENCES User(user_id), cash_acc_id INTEGER PRIMARY KEY REFERENCES Account(acc_id), projection TEXT, as_of TEXT NOT NULL, modified_at TEXT NOT NULL)",
)


def test_ddl():
    assert rag.RagSource.create_ddl() == rag_ddls[0]
    assert rag.RagAction.create_ddl() == rag_ddls[1]
    assert rag.RagActionCollection.create_ddl() == rag_ddls[2]
    assert rag.ConvoMessage.create_ddl() == rag_ddls[3]
    assert rag.ConvoSession.create_ddl() == rag_ddls[4]

    assert acc_ddls == tuple(t.create_ddl() for t in acc.tables)


# test db cleanup
rag_db_path = tabula_rasa_path(Path("build/tests/rag.db"))
acc_db_path = tabula_rasa_path(Path("build/tests/acc.db"))


def search_caplog(
    caplog: pytest.LogCaptureFixture,
    prefix: str,
    suffix: str | None = None,
    category: str | None = None,
) -> Generator[str, None, None]:
    for r in caplog.records:
        m = r.getMessage()
        if category is None or r.name == category:
            if m.startswith(prefix):
                s = m[len(prefix) :]
                if suffix is not None:
                    idx = s.index(suffix)
                    if idx != -1:
                        s = s[:idx]
                yield s


def test_rag_db(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG)
    rag.SCHEMA.create_schema(rag_db_path)
    extract = tuple(search_caplog(caplog, "execute: ", category="lythonic.state"))
    print(extract)
    assert extract == rag_ddls


def test_acc_db(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG)
    acc.create_schema(acc_db_path)
    extract = tuple(search_caplog(caplog, "execute: ", category="lythonic.state"))
    print(extract)
    assert extract == acc_ddls


def test_add_attempt():
    with open_sqlite_db(rag_db_path) as conn:
        select = rag.select_all_active_sources(conn)
        assert len(select) == 0
        s = rag.RagSource(absolute_path=Path("test.txt"))
        assert s.source_id == -1
        s.save(conn)
        assert s.source_id != -1
        s_loaded = rag.RagSource.load_by_id(conn, s.source_id)
        assert s_loaded is not None
        assert s_loaded == s
        assert isinstance(s_loaded.absolute_path, Path)
        a1 = rag.RagAction(
            source_id=s.source_id,
            n_chunks=1,
            error=None,
            sha256="1234567890",
        )
        assert a1.action_id == -1
        a1.save(conn)

        assert a1.action_id != -1
        c1 = rag.RagActionCollection(
            action_id=a1.action_id,
            action="new",
            collection="test",
        )
        c1.insert(conn)

        a2 = rag.RagAction(
            source_id=s.source_id,
            n_chunks=1,
            error=None,
            sha256="6789012345",
        )
        assert a2.action_id == -1
        a2.save(conn)

        assert a2.action_id != -1
        assert a2.action_id != a1.action_id
        c2 = rag.RagActionCollection(
            action_id=a2.action_id,
            action="new",
            collection="test",
        )
        c2.insert(conn)

        a2.timestamp = datetime.now(tz=UTC)
        a2.sha256 = "6172839405"
        a2.save(conn)

        select = rag.select_all_active_sources(conn)
        if len(select) != 1:
            print(f"{select=}")
            raise AssertionError(f"{select=}")

        assert len(select[0][2]) == 1

        a3 = rag.RagAction(
            action_id=6,
            source_id=s.source_id,
            n_chunks=6,
            error=None,
            sha256="1122334455",
        )
        a3.save(conn)

        c3 = rag.RagActionCollection(
            action_id=a3.action_id,
            action="update",
            collection="test",
        )
        c3.insert(conn)

        c4 = rag.RagActionCollection(
            action_id=a3.action_id,
            action="new",
            collection="test2",
        )
        c4.insert(conn)

        a1_loaded = rag.RagAction.load_by_id(conn, a1.action_id)
        a2_loaded = rag.RagAction.load_by_id(conn, a2.action_id)
        a3_loaded = rag.RagAction.load_by_id(conn, a3.action_id)
        nothing_loaded = rag.RagAction.load_by_id(conn, 7)
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
        select = rag.RagAction.select(conn, source_id=s.source_id)
        assert len(select) == 3
        assert a1 in select
        assert a2 in select
        assert a3 in select
        select = rag.select_all_active_sources(conn)
        assert len(select) == 1
        s_loaded2, a_loaded2, c_loaded2 = select[0]
        assert s_loaded2 == s
        assert a_loaded2 == a3
        assert isinstance(c_loaded2, list)
        assert len(c_loaded2) == 2
        conn.commit()
        sess = rag.ConvoSession(model="4o", user_id="alice", session_type="active").save(conn)
        rag.ConvoMessage(
            role="user", content="hello", finish_reason="stop", session_id=sess.session_id
        ).save(conn)
        rag.ConvoMessage(
            role="assistant",
            content="how are you",
            finish_reason="stop",
            session_id=sess.session_id,
        ).save(conn)
        assert rag.ConvoMessage.select_count(conn, session_id=sess.session_id) == 2
        pk_filter, pk_defined = sess.get_pk_filter()
        assert pk_defined
        assert rag.ConvoMessage.delete(conn, **pk_filter) == 2
        assert rag.ConvoMessage.select_count(conn, session_id=sess.session_id) == 0
        assert rag.ConvoSession.delete(conn, **pk_filter) == 1
        assert rag.ConvoSession.select_count(conn, session_id=sess.session_id) == 0

        conn.commit()
