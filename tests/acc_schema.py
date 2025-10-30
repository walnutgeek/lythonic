import logging
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field

from lythonic.periodic import utc_now
from lythonic.state import DbModel, execute_sql, open_sqlite_db

logger = logging.getLogger(__name__)


AccountType = Literal["cash", "credit_card"]
EventType = Literal["deposit", "payment", "set_balance"]
AmountType = Literal["fixed", "variable"]
Frequency = Literal["weekly", "monthly", "quarterly", "annually"]


class Organization(DbModel["Organization"]):
    org_id: int = Field(default=-1, description="(PK) Unique identifier for the organization")
    name: str = Field(description="Name of the organization")
    created_at: datetime = Field(default_factory=utc_now)
    is_hidden: bool = Field(default=False)


class Account(DbModel["Account"]):
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
    scheduled_activity_id: int | None = Field(
        default=None, description="(FK:ScheduledEvent.sch_id) Reference to scheduled event"
    )
    created_at: datetime = Field(default_factory=utc_now)


class ScheduledEvent(DbModel["ScheduledEvent"]):
    sch_id: int = Field(default=-1, description="(PK) Unique identifier for the scheduled event")
    is_active: bool = Field(default=False, description="Whether the event is active")
    acc_id: int = Field(description="(FK:Account.acc_id) Reference to the account")
    description_template: str = Field(description="Description template for the event")
    event_type: EventType = Field(description="Type of event")
    amount_type: AmountType = Field(description="Type of amount")
    amount: float | None = Field(default=None, description="Fixed amount or NULL for variable")
    frequency: Frequency = Field(description="Frequency of the event")
    day_in_the_cycle: int = Field(
        description="Day of the cycle (day of week 0-6/month 1-31/year 0-365)"
    )
    start_date: date = Field(description="Start date of the event")
    end_date: date | None = Field(default=None, description="End date of the event")
    reminder_days: int | None = Field(default=None, description="Days before due to remind user")
    created_at: datetime = Field(default_factory=utc_now)


class CashEvent(DbModel["CashEvent"]):
    cash_id: int = Field(default=-1, description="(PK) Unique identifier for the cash event")
    cash_acc_id: int = Field(description="(FK:Account.acc_id) Reference to cash account")
    cc_acc_id: int | None = Field(
        default=None, description="(FK:Account.acc_id) Reference to credit card account"
    )
    sch_id: int | None = Field(
        default=None, description="(FK:ScheduledEvent.sch_id) Reference to scheduled event"
    )
    event_date: date = Field(description="Date of the event")
    amount: float = Field(description="Amount of the event")
    description: str | None = Field(default=None, description="Description of the event")
    created_at: datetime = Field(default_factory=utc_now)


tables = [Organization, Account, ScheduledEvent, CashEvent]


def check_all_tables_exist(conn: sqlite3.Connection):
    cursor = conn.cursor()
    execute_sql(cursor, "SELECT name FROM sqlite_master WHERE type='table' ")
    all_tables = set(r[0] for r in cursor.fetchall())
    return all(t.get_table_name() in all_tables for t in tables)


def create_tables(conn: sqlite3.Connection):
    cursor = conn.cursor()
    for table in tables:
        execute_sql(cursor, table.create_ddl())
    conn.commit()


def create_schema(path: Path):
    with open_sqlite_db(path) as conn:
        create_tables(conn)
