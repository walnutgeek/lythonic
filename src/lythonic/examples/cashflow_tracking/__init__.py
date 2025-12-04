import logging
import sqlite3
from collections.abc import Generator
from datetime import date, datetime, timedelta
from typing import Literal

from pydantic import BaseModel, Field

from lythonic import utc_now
from lythonic.periodic import FrequencyOffset, FrequencyType
from lythonic.state import Schema
from lythonic.state.user import User, UserContext, UserOwned
from lythonic.types import JsonBase

logger = logging.getLogger(__name__)


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
    frequency: FrequencyType = Field(description="Frequency of the event")
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

    def get_event_dates_for_range(
        self, start_date: date, end_date: date
    ) -> Generator[date, None, None]:
        freq_offset = FrequencyOffset(self.frequency, self.day_in_the_cycle)
        boundaries = freq_offset.boundaries(start_date)
        while boundaries[0] < start_date:
            boundaries = freq_offset.boundaries(boundaries[1] + timedelta(days=1))
        while boundaries[0] <= end_date:
            yield boundaries[0]
            boundaries = freq_offset.boundaries(boundaries[1] + timedelta(days=1))

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

    @classmethod
    def from_scheduled_event(
        cls, scheduled_event: ScheduledEvent, event_date: date
    ) -> "CashEventNotification":
        assert scheduled_event.amount is not None and scheduled_event.amount_type == "variable", (
            "Scheduled event amount cannot be None for variable amount type"
        )
        return cls(
            sch_id=scheduled_event.sch_id,
            user_id=scheduled_event.user_id,
            note_date=event_date + timedelta(days=scheduled_event.reminder_days or 1),
            muted=False,
            event_date=event_date,
        )


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


SCHEMA = Schema(
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
