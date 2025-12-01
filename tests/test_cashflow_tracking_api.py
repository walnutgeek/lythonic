import logging
from pathlib import Path

import pytest

from lythonic.examples.cashflow_tracking import SCHEMA as CFT_SCHEMA
from lythonic.examples.cashflow_tracking import (
    Organization,
    User,  # pyright: ignore[reportPrivateLocalImportUsage]
)
from lythonic.misc import tabula_rasa_path
from lythonic.state import open_sqlite_db
from lythonic.state.user import UserContext, UserInfo
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

        o = Organization(name="Test Organization")
        o.save_with_ctx(ctxs[2], conn)
        loaded = Organization.load_by_id_with_ctx(conn, ctxs[2], o.org_id)
        assert loaded == o
