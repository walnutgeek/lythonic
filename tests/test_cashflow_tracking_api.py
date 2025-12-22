import logging
from datetime import date
from pathlib import Path

import pytest

from lythonic.examples.cashflow_tracking import SCHEMA as CFT_SCHEMA
from lythonic.examples.cashflow_tracking import Account, Organization, ScheduledEvent
from lythonic.misc import tabula_rasa_path
from lythonic.state import open_sqlite_db
from lythonic.state.user import User, UserContext, UserInfo
from lythonic.types import base_model_to_json, json_dumps
from tests.test_sqlite import acc_ddls, search_caplog

acc_db_path = tabula_rasa_path(Path("build/tests/cft_api.db"))


def test_acc_db(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG)
    CFT_SCHEMA.create_schema(acc_db_path)
    extract = tuple(search_caplog(caplog, "execute: ", category="lythonic.state"))
    print(extract)
    assert extract == acc_ddls


def test_create_user(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG)
    with open_sqlite_db(acc_db_path) as conn:
        u = User(info=UserInfo())
        u.save(conn)
        loaded = User.load_by_id(conn, u.user_id)
        assert loaded == u
        conn.commit()
    extract = tuple(search_caplog(caplog, "execute: ", category="lythonic.state"))
    assert extract == (
        f"INSERT INTO User (info, created_at) VALUES (?, ?) -- with args: {([json_dumps(base_model_to_json(u.info)), u.created_at.isoformat()],)}",
        f"SELECT user_id, info, created_at FROM User WHERE user_id = ? -- with args: {([u.user_id],)}",
    )


def test_user_owned_objects(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG)
    with open_sqlite_db(acc_db_path) as conn:
        u2 = User(info=UserInfo())
        u2.save(conn)
        users: dict[int, User] = {u.user_id: u for u in User.select(conn)}
        assert len(users) == 2
        assert users.keys() == {1, 2}
        ctxs: dict[int, UserContext] = {u.user_id: UserContext(user=u) for u in users.values()}

        o = Organization(name="Personal")
        o.save_with_ctx(ctxs[2], conn)
        loaded = Organization.load_by_id_with_ctx(conn, ctxs[2], o.org_id)
        assert loaded == o

        bank1 = Account(
            name="BankOne Checking",
            account_type="cash",
            org_id=o.org_id,
            start_date=date(2025, 1, 1),
        )
        bank1.save_with_ctx(ctxs[2], conn)
        loaded = Account.load_by_id_with_ctx(conn, ctxs[2], bank1.acc_id)
        assert loaded == bank1

        sch1 = ScheduledEvent(
            acc_id=bank1.acc_id,
            description_template="Set balalnce at the beginning of the month",
            event_type="set_balance",
            amount_type="variable",
            amount=None,
            frequency="monthly",
            day_in_the_cycle=0,
            reminder_days=2,
        )
        with pytest.raises(NotImplementedError):
            sch1.save(conn)
        with pytest.raises(NotImplementedError):
            sch1.load_by_id(conn, sch1.sch_id)
        with pytest.raises(AssertionError):
            sch1.select(conn, sch_id=sch1.sch_id)
        sch1.save_versioned(ctxs[2], conn, date(2025, 1, 1))
        sch1.reminder_days = 3
        sch1.save_versioned(ctxs[2], conn, date(2025, 1, 2))
        sch1.reminder_days = 4
        sch1.save_versioned(ctxs[2], conn, date(2025, 1, 2))
        rr = ScheduledEvent.select(conn, user_ctx=ctxs[2], acc_id=bank1.acc_id)
        assert len(rr) == 2
        assert {sch.is_active: sch.reminder_days for sch in rr} == {True: 4, False: 2}

        sch2 = ScheduledEvent(
            acc_id=bank1.acc_id,
            description_template="Mid month salary deposit",
            event_type="deposit",
            amount_type="fixed",
            amount=2000,
            frequency="monthly",
            day_in_the_cycle=14,
        )
        sch2.save_versioned(ctxs[2], conn, date(2025, 1, 1))
        sch2.day_in_the_cycle = 15
        with pytest.raises(ValueError) as e:
            sch2.save_versioned(ctxs[1], conn, date(2025, 1, 2))
        assert str(e.value.args[0]) == "Cannot save prev version:"

        with pytest.raises(ValueError) as e:
            sch2.enddate_record(ctxs[1], conn, date(2025, 1, 2))
        assert str(e.value.args[0]) == "Possible Access violation:"

        rr = list(sch2.get_event_dates_for_range(date(2025, 1, 1), date(2025, 3, 31)))

        assert rr == [date(2025, 1, 16), date(2025, 2, 16), date(2025, 3, 16)]

        rr = list(sch1.get_event_dates_for_range(date(2025, 1, 1), date(2025, 3, 31)))

        assert rr == [date(2025, 1, 1), date(2025, 2, 1), date(2025, 3, 1)]

        conn.commit()
