import logging
import sqlite3
from collections.abc import Generator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import pytest
from pydantic import BaseModel, Field

import tests.rag_schema as rag
from lythonic import utc_now
from lythonic.misc import tabula_rasa_path
from lythonic.state import (
    FieldInfo,
    Schema,
    open_sqlite_db,
)
from lythonic.state.user import User, UserContext, UserOwned
from lythonic.types import JsonBase, KnownType

AccountType = Literal["cash", "credit_card"]
EventType = Literal["deposit", "payment", "set_balance"]
AmountType = Literal["fixed", "variable"]


class Organization(UserOwned["Organization"]):
    org_id: int = Field(default=-1, description="(PK) Unique identifier for the organization")
    name: str = Field(description="Name of the organization")
    created_at: datetime = Field(default_factory=utc_now)
    is_hidden: bool = Field(default=False)


class Account(UserOwned["Account"]):
    acc_id: int = Field(default=-1, description="(PK) Unique identifier for the account")
    org_id: int = Field(description="(FK:Organization.org_id) Reference to the organization")
    name: str = Field(description="Name of the account")
    account_type: AccountType = Field(description="Type of account")
    account_kind: str | None = Field(
        default=None,
        description="Account kind pointing to reusable implementation of account logic",
    )
    start_date: date = Field(description="Start date of the account")
    end_date: date | None = Field(default=None, description="End date of the account")
    linked_cash_acc_id: int | None = Field(
        default=None, description="(FK:Account.acc_id) Reference to linked cash account"
    )
    created_at: datetime = Field(default_factory=utc_now)

    @classmethod
    def filter_active(cls, accounts: list["Account"], as_of: date) -> list["Account"]:
        return [
            account
            for account in accounts
            if account.start_date <= as_of
            and (account.end_date is None or account.end_date > as_of)
        ]


class ScheduledEvent(UserOwned["ScheduledEvent"]):
    sch_id: int = Field(default=-1, description="(PK) Unique identifier for the scheduled event")
    acc_id: int = Field(description="(FK:Account.acc_id) Reference to the account")
    description_template: str = Field(description="Description template for the event")
    event_type: EventType = Field(description="Type of event")
    amount_type: AmountType = Field(description="Type of amount")
    amount: float | None = Field(default=None, description="Fixed amount or NULL for variable")
    frequency: str = Field(description="Frequency of the event")
    day_in_the_cycle: int = Field(
        description="Day of the cycle (day of week 0-6/month 0-30/quarter 0-3/year 0-365)"
    )
    reminder_days: int | None = Field(default=None, description="Days before due to remind user")

    # versioning fields
    root_version_id: int | None = Field(
        default=None,
        description="(FK:ScheduledEvent.sch_id) Reference to the root version",
    )
    is_active: bool = Field(default=True, description="Whether the event is active")
    start_date: date = Field(
        default=date(1900, 1, 1), description="Start date of the event"
    )  # default is not important as it is overridden by save_versioned()
    end_date: date | None = Field(default=None, description="End date of the event")
    modified_at: datetime = Field(default_factory=utc_now)

    def save_versioned(self, user_ctx: UserContext, conn: sqlite3.Connection, as_of: date) -> None:
        assert self.end_date is None, (
            f"use enddate_record() explicitly if you want logically remove this record: {self}"
        )
        assert self.root_version_id is None and self.start_date <= as_of and self.is_active, (
            f"sainity check failed: {self}"
        )
        if self.sch_id == -1:  # new record
            self.root_version_id = None
            self.start_date = as_of
            self.end_date = None
        else:
            rr = self.select(conn, user_ctx=user_ctx, sch_id=self.sch_id)
            if len(rr) != 1:
                raise ValueError(
                    "Cannot save prev version:",
                    " Record does not exist or belong other user then the one in context:",
                    user_ctx.user.user_id,
                )
            # save previous version
            prev = rr[0]
            assert prev.end_date is None, f"cannot modify enddated record: {prev}"
            assert prev.start_date <= as_of, f"sainity check failed: {prev}"
            if prev.start_date != as_of:  # save previous version
                prev.root_version_id = prev.sch_id
                prev.sch_id = -1
                prev.is_active = False
                prev.end_date = as_of
                prev.save_with_ctx(user_ctx, conn)
        self.start_date = as_of
        self.is_active = True
        self.root_version_id = None
        self.modified_at = utc_now()
        self.save_with_ctx(user_ctx, conn)

    def enddate_record(self, user_ctx: UserContext, conn: sqlite3.Connection, as_of: date) -> None:
        assert self.end_date is None, f"already ended: {self}"
        assert self.start_date <= as_of and self.is_active, f"sainity check failed: {self}"
        self.end_date = as_of
        self.is_active = False
        self.root_version_id = None
        self.modified_at = utc_now()
        self.save_with_ctx(user_ctx, conn)


class CashEvent(UserOwned["CashEvent"]):
    cash_id: int = Field(default=-1, description="(PK) Unique identifier for the cash event")
    cash_acc_id: int = Field(description="(FK:Account.acc_id) Reference to cash account")
    related_acc_id: int | None = Field(
        default=None,
        description="(FK:Account.acc_id) Reference to related account in case of credit card payment or cash transfer",
    )
    sch_id: int | None = Field(
        default=None, description="(FK:ScheduledEvent.sch_id) Reference to scheduled event"
    )
    event_type: EventType = Field(description="Type of event")
    event_date: date = Field(description="Date of the event")
    amount: float = Field(description="Amount of the event")
    description: str | None = Field(default=None, description="Description of the event")
    modified_at: datetime = Field(default_factory=utc_now)

    @classmethod
    def from_scheduled_event(
        cls, scheduled_event: ScheduledEvent, event_date: date, account: Account
    ) -> "CashEvent":
        assert scheduled_event.amount is not None and scheduled_event.amount_type == "fixed", (
            "Scheduled event amount cannot be None for fixed amount type"
        )
        cc_acc_id: int | None = None
        amount = scheduled_event.amount
        if account.account_type == "cash":
            cash_acc_id = account.acc_id
        else:
            assert (
                account.account_type == "credit_card" and account.linked_cash_acc_id is not None
            ), "Credit card account must be linked to a cash account"
            cash_acc_id = account.linked_cash_acc_id
            cc_acc_id = account.acc_id
        return cls(
            cash_acc_id=cash_acc_id,
            user_id=scheduled_event.user_id,
            related_acc_id=cc_acc_id,
            sch_id=scheduled_event.sch_id,
            event_type=scheduled_event.event_type,
            event_date=event_date,
            amount=amount,
            description=scheduled_event.description_template,
        )


class CashEventNotification(UserOwned["CashEventNotification"]):
    """
    Represents a notification that a cash event needs to be created but requires user input.
    """

    note_id: int = Field(default=-1, description="(PK) Unique identifier for the notification")
    sch_id: int = Field(description="(FK:ScheduledEvent.sch_id) Reference to scheduled event")
    note_date: date = Field(description="Date of the notification")
    muted: bool = Field(default=False, description="Whether the notification is muted")
    event_date: date = Field(description="Date of the event")


class FlowItem(BaseModel):
    event_type: EventType
    event_date: date
    amount: float
    description: str | None
    cash_id: int | None = Field(
        default=None,
        description="Indicate if this event is already materialized as CashEvent or it is future event",
    )
    note_id: int | None = Field(
        default=None, description="Indicate if this event has associated CashEventNotification"
    )
    balance: float | None = Field(default=None, description="Balance after this event")

    @classmethod
    def from_cash_event(cls, cash_event: CashEvent) -> "FlowItem":
        return cls(
            event_date=cash_event.event_date,
            event_type=cash_event.event_type,
            amount=cash_event.amount if cash_event.event_type != "payment" else -cash_event.amount,
            description=cash_event.description,
            cash_id=cash_event.cash_id,
        )


class FlowProjection(JsonBase):
    as_of: date = Field(description="Date of the projection typically today")
    events: list[FlowItem] = Field(description="List of events materialized and projected")
    alert: bool = Field(
        description="Indicate if if balance is negative or close to zero at any point in the projected future"
    )


class CashAccountProjection(UserOwned["CashAccountProjection"]):
    """
    Cash account projection for a given date and extending for month in future. All scheduled events
    that triggered prior to this date are materilaized as CashEvent and CashEventNotification to calculate the projection.
    """

    cash_acc_id: int = Field(description="(PK)(FK:Account.acc_id) Reference to cash account")
    projection: FlowProjection | None = Field(
        default=None,
        description="Projection for next month starting from last known balance (set_balance event)",
    )
    as_of: date = Field(description="Date of projection typically today")
    modified_at: datetime = Field(default_factory=utc_now)


acc = Schema(
    tables=[
        User,
        Organization,
        Account,
        ScheduledEvent,
        CashEvent,
        CashEventNotification,
        CashAccountProjection,
    ]
)


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
    "CREATE TABLE ScheduledEvent (user_id INTEGER NOT NULL REFERENCES User(user_id), sch_id INTEGER PRIMARY KEY, acc_id INTEGER NOT NULL REFERENCES Account(acc_id), description_template TEXT NOT NULL, event_type TEXT NOT NULL CHECK (event_type IN ('deposit', 'payment', 'set_balance')), amount_type TEXT NOT NULL CHECK (amount_type IN ('fixed', 'variable')), amount REAL, frequency TEXT NOT NULL, day_in_the_cycle INTEGER NOT NULL, reminder_days INTEGER, root_version_id INTEGER REFERENCES ScheduledEvent(sch_id), is_active INTEGER NOT NULL, start_date TEXT NOT NULL, end_date TEXT, modified_at TEXT NOT NULL)",
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
