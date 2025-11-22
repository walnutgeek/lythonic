import sqlite3
from datetime import date, timedelta

from pydantic import BaseModel, Field

from lythonic.config import Configurable
from lythonic.examples.cashflow_tracking import (
    SCHEMA,
    Account,
    CashAccountProjection,
    CashEvent,
    CashEventNotification,
    Organization,
    ScheduledEvent,
)
from lythonic.state import DbConfig, DbFile
from lythonic.types import JsonBase

PROJECTION_RANGE = timedelta(days=62)  # 2 months


class ApiContext(JsonBase):
    org: Organization = Field(description="Current organization")
    as_of: date = Field(description="Date of the projection typically today")


class CashAccountProjectionCalculator(BaseModel):
    cash_account: Account = Field(description="Cash account to calculate the projection for")
    current_projection: CashAccountProjection | None = Field(
        default=None, description="Current projection for the cash account"
    )
    new_projection: CashAccountProjection | None = Field(
        default=None, description="New projection for the cash account"
    )
    new_events: list[CashEvent] = Field(
        default_factory=list,
        description="List of events materialized and projected for the cash account",
    )
    new_notifications: list[CashEventNotification] = Field(
        default_factory=list,
        description="List of notifications for the events for the cash account",
    )

    def calculate_projection(
        self, conn: sqlite3.Connection, as_of: date, force: bool = False
    ) -> bool:
        need_recalculate = (
            force or self.current_projection is None or self.current_projection.as_of < as_of
        )
        if need_recalculate:
            # Look for the latest 'set_balance' CashEvent, and keep only the events after it to be considered for projection
            starting_from_date = as_of - PROJECTION_RANGE
            cash_events: list[CashEvent] = sorted(
                CashEvent.select(
                    conn, cash_acc_id=self.cash_account.acc_id, event_date__gte=starting_from_date
                ),
                key=lambda x: x.event_date,
            )
            set_balance_events: list[int] = [
                i for i, e in enumerate(cash_events) if e.event_type == "set_balance"
            ]
            assert set_balance_events, (
                f"No 'set_balance' CashEvent found starting from the date {starting_from_date}"
            )

            relevant_cash_events = cash_events[set_balance_events[-1] :]
            start_projection_date = cash_events[0].event_date
            end_projection_date = as_of + PROJECTION_RANGE

            relevant_notifications = CashEventNotification.select(
                conn,
                cash_acc_id=self.cash_account.acc_id,
                notification_date__gte=start_projection_date,
            )

            class EventProjection:
                event_date: date
                schedule: ScheduledEvent
                matched_cash_event: CashEvent | None = None
                matched_notification: CashEventNotification | None = None

                def __init__(self, event_date: date, schedule: ScheduledEvent):
                    self.event_date = event_date
                    self.schedule = schedule
                    self.matched_cash_event = next(
                        (
                            e
                            for e in relevant_cash_events
                            if e.event_date == event_date and e.sch_id == schedule.sch_id
                        ),
                        None,
                    )
                    self.matched_notification = next(
                        (
                            n
                            for n in relevant_notifications
                            if n.note_date == event_date and n.sch_id == schedule.sch_id
                        ),
                        None,
                    )

            scheduled_events = ScheduledEvent.select(
                conn, is_active=True, account_id=self.cash_account.acc_id
            )

            event_projections: list[EventProjection] = []

            for scheduled_event in scheduled_events:
                for event_date in scheduled_event.get_event_dates_for_range(
                    start_projection_date, end_projection_date
                ):
                    event_projections.append(EventProjection(event_date, scheduled_event))

        return need_recalculate


class CashflowTrackingApi(Configurable[DbConfig]):
    db_file: DbFile

    def __init__(self, config: DbConfig) -> None:
        super().__init__(config)
        self.db_file = DbFile(config.db_path, SCHEMA)
        self.db_file.check(ensure=True)

    def save_organization(self, org: Organization) -> Organization:
        with self.db_file.open() as conn:
            org.save(conn)
            return org

    def list_organizations(self, is_hidden: bool = False) -> list[Organization]:
        with self.db_file.open() as conn:
            return Organization.select(conn, is_hidden=is_hidden)

    def get_organization(self, org_id: int) -> Organization:
        with self.db_file.open() as conn:
            org = Organization.load_by_id(conn, org_id)
            if org is None:
                raise ValueError(f"Organization with id {org_id} not found in database")
            return org

    def get_all_accounts(self, ctx: ApiContext) -> list[Account]:
        with self.db_file.open() as conn:
            return Account.select(conn, org_id=ctx.org.org_id)

    def save_account(self, ctx: ApiContext, account: Account) -> Account:
        assert ctx.org.org_id == account.org_id, "Organization ID mismatch"
        with self.db_file.open() as conn:
            account.save(conn)
            return account

    def delete_account(self, ctx: ApiContext, account: Account) -> None:
        assert ctx.org.org_id == account.org_id, "Organization ID mismatch"
        with self.db_file.open() as conn:
            account.end_date = ctx.as_of
            account.save(conn)

    def get_scheduled_events(self, ctx: ApiContext) -> list[tuple[ScheduledEvent, Account]]:
        with self.db_file.open() as conn:
            active_accounts = {
                a.acc_id: a
                for a in Account.filter_active(
                    Account.select(conn, org_id=ctx.org.org_id), ctx.as_of
                )
            }
            return [
                (se, active_accounts[se.acc_id])
                for se in ScheduledEvent.select(
                    conn, is_active=True, account_id=list(active_accounts.keys())
                )
            ]

    def calculate_cash_account_projections(
        self, ctx: ApiContext, force: bool = False
    ) -> tuple[bool, dict[int, CashAccountProjectionCalculator]]:
        with self.db_file.open() as conn:
            calculators: dict[int, CashAccountProjectionCalculator] = {
                ca.acc_id: CashAccountProjectionCalculator(cash_account=ca)
                for ca in Account.select(conn, account_type="cash", org_id=ctx.org.org_id)
            }
            for cap in CashAccountProjection.select(conn, cash_acc_id=list(calculators.keys())):
                calculators[cap.cash_acc_id].current_projection = cap
            return any(
                calculator.calculate_projection(conn, ctx.as_of, force)
                for calculator in calculators.values()
            ), calculators
