import sqlite3
from datetime import date, timedelta
from typing import Literal

from pydantic import BaseModel, Field

from lythonic.config import Configurable
from lythonic.examples.cashflow_tracking import (
    SCHEMA,
    Account,
    CashAccountProjection,
    CashEvent,
    CashEventNotification,
    FlowItem,
    FlowProjection,
    Organization,
    ScheduledEvent,
)
from lythonic.state import DbConfig, DbFile
from lythonic.state.user import UserContext

PROJECTION_RANGE = timedelta(days=62)  # 2 months


class ApiContext(UserContext):
    as_of: date = Field(description="Date of the projection typically today")


EventAction = Literal["create_event", "imagine_event", "create_notification", "do_nothing"]


class CashAccountProjectionCalculator(BaseModel):
    cash_account: Account = Field(description="Cash account to calculate the projection for")
    cash_projection: CashAccountProjection | None = Field(
        default=None, description="Current projection for the cash account"
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
    ) -> tuple[bool, CashAccountProjection]:
        need_recalculate = (
            force or self.cash_projection is None or self.cash_projection.as_of < as_of
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
                account: Account
                matched_cash_event: CashEvent | None = None
                matched_notification: CashEventNotification | None = None
                action: EventAction = "do_nothing"
                new_cash_event: CashEvent | None = None
                new_notification: CashEventNotification | None = None

                def __init__(self, event_date: date, schedule: ScheduledEvent, account: Account):
                    self.event_date = event_date
                    self.schedule = schedule
                    self.account = account
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
                    if self.schedule.amount_type == "fixed" and self.matched_cash_event is None:
                        if self.event_date <= as_of:
                            self.action = "create_event"
                        else:
                            self.action = "imagine_event"
                        self.new_cash_event = CashEvent.from_scheduled_event(
                            self.schedule, self.event_date, self.account
                        )
                    elif (
                        self.schedule.amount_type == "variable"
                        and self.matched_cash_event is None
                        and self.matched_notification is None
                        and self.event_date
                        <= as_of + timedelta(days=self.schedule.reminder_days or 1)
                    ):
                        self.action = "create_notification"
                        self.new_notification = CashEventNotification.from_scheduled_event(
                            self.schedule, self.event_date
                        )
                    else:
                        self.action = "do_nothing"

                def flow_item(self) -> FlowItem | None:
                    if self.matched_cash_event is not None:
                        return FlowItem.from_cash_event(self.matched_cash_event)
                    elif self.new_cash_event is not None:
                        return FlowItem.from_cash_event(self.new_cash_event)
                    else:
                        return None

            scheduled_events = ScheduledEvent.select(
                conn, is_active=True, account_id=self.cash_account.acc_id
            )

            event_projections: list[EventProjection] = []

            accounts = {
                a.acc_id: a
                for a in Account.select(
                    conn, acc_id=[scheduled_event.acc_id for scheduled_event in scheduled_events]
                )
            }

            for scheduled_event in scheduled_events:
                account = accounts[scheduled_event.acc_id]
                for event_date in scheduled_event.get_event_dates_for_range(
                    start_projection_date, end_projection_date
                ):
                    event_projections.append(EventProjection(event_date, scheduled_event, account))

            left_over_events = {e.cash_id: e for e in relevant_cash_events}
            for event_projection in event_projections:
                if event_projection.matched_cash_event is not None:
                    left_over_events.pop(event_projection.matched_cash_event.cash_id)

            flow_items: list[FlowItem] = []
            for event_projection in event_projections:
                if (
                    event_projection.action == "create_event"
                    and event_projection.new_cash_event is not None
                ):
                    event_projection.new_cash_event.save(conn)
                elif (
                    event_projection.action == "create_notification"
                    and event_projection.new_notification is not None
                ):
                    event_projection.new_notification.save(conn)

                flow_item = event_projection.flow_item()
                if flow_item is not None:
                    flow_items.append(flow_item)
            flow_items.extend(FlowItem.from_cash_event(e) for e in left_over_events.values())
            flow_items.sort(key=lambda x: (x.event_date, x.event_type != "set_balance", -x.amount))
            assert flow_items[0].event_type == "set_balance", (
                "First flow item must be the set_balance event"
            )
            balance = flow_items[0].amount
            alert = False
            for flow_item in flow_items:
                flow_item.balance = balance
                if flow_item.balance < 0:
                    alert = True
                balance += flow_item.amount

            projection = FlowProjection(as_of=as_of, events=flow_items, alert=alert)

            self.cash_projection = CashAccountProjection(
                cash_acc_id=self.cash_account.acc_id,
                user_id=self.cash_account.user_id,
                projection=projection,
                as_of=as_of,
            )
            self.cash_projection.save(conn)
        assert self.cash_projection is not None

        return need_recalculate, self.cash_projection


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

    def get_all_accounts(
        self,
        ctx: ApiContext,
        org_id: int | None = None,
    ) -> list[Account]:
        with self.db_file.open() as conn:
            if org_id is not None:
                return Account.select(conn, user_id=ctx.user.user_id, org_id=org_id)
            else:
                return Account.select(conn, user_id=ctx.user.user_id)

    def save_account(self, ctx: ApiContext, account: Account) -> Account:
        account.user_id = ctx.user.user_id
        with self.db_file.open() as conn:
            if account.acc_id != -1:
                assert (
                    len(account.select(conn, acc_id=account.acc_id, user_id=ctx.user.user_id)) == 1
                ), "Account not found"
            account.save(conn)
            return account

    def delete_account(self, ctx: ApiContext, account: Account) -> None:
        account.user_id = ctx.user.user_id
        assert account.acc_id != -1, "Account ID is -1"
        with self.db_file.open() as conn:
            account.end_date = ctx.as_of
            assert (
                len(account.select(conn, acc_id=account.acc_id, user_id=ctx.user.user_id)) == 1
            ), "Account not found"
            account.save(conn)

    def get_scheduled_events(
        self, ctx: ApiContext, org_id: int
    ) -> list[tuple[ScheduledEvent, Account]]:
        with self.db_file.open() as conn:
            active_accounts = {
                a.acc_id: a
                for a in Account.filter_active(
                    Account.select(conn, user_id=ctx.user.user_id, org_id=org_id), ctx.as_of
                )
            }
            return [
                (se, active_accounts[se.acc_id])
                for se in ScheduledEvent.select(
                    conn,
                    is_active=True,
                    user_id=ctx.user.user_id,
                    account_id=list(active_accounts.keys()),
                )
            ]

    def calculate_cash_account_projections(
        self, ctx: ApiContext, org_id: int, force: bool = False
    ) -> tuple[bool, dict[int, CashAccountProjection]]:
        with self.db_file.open() as conn:
            calculators: dict[int, CashAccountProjectionCalculator] = {
                ca.acc_id: CashAccountProjectionCalculator(cash_account=ca)
                for ca in Account.filter_active(
                    Account.select(
                        conn, account_type="cash", user_id=ctx.user.user_id, org_id=org_id
                    ),
                    ctx.as_of,
                )
            }
            for cap in CashAccountProjection.select(conn, cash_acc_id=list(calculators.keys())):
                calculators[cap.cash_acc_id].cash_projection = cap
            results: list[tuple[bool, CashAccountProjection]] = [
                calculator.calculate_projection(conn, ctx.as_of, force)
                for calculator in calculators.values()
            ]
            return any(map(lambda x: x[0], results)), {r[1].cash_acc_id: r[1] for r in results}
