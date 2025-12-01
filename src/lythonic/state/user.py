import sqlite3
from datetime import datetime
from typing import Any, TypeVar

from pydantic import BaseModel, Field
from typing_extensions import override

from lythonic import utc_now
from lythonic.state import DbModel, FieldInfo
from lythonic.types import JsonBase


class UserInfo(JsonBase):
    pass


class User(DbModel["User"]):
    user_id: int = Field(default=-1, description="(PK) Unique identifier for the user")
    info: UserInfo = Field(description="User information object")
    created_at: datetime = Field(
        default_factory=utc_now, description="Date and time when the user was created"
    )


class UserContext(BaseModel):
    user: User


UO = TypeVar("UO", bound="UserOwned")  # pyright: ignore [reportMissingTypeArgument]


class UserOwned(DbModel[UO]):
    user_id: int = Field(default=-1, description="(FK:User.user_id) Reference to the user")

    @override
    def save(self, conn: sqlite3.Connection):
        raise AssertionError("Use save_with_ctx instead")

    @classmethod
    def _user_id_field(cls) -> FieldInfo:
        return cls._choose_fields(lambda fi: fi.name == "user_id")[0]

    def save_with_ctx(self, ctx: UserContext, conn: sqlite3.Connection):
        self.user_id = ctx.user.user_id
        cls = self.__class__
        pks = cls._choose_fields(lambda fi: fi.primary_key)
        assert len(pks) == 1
        pk = pks[0]
        pk_val = getattr(self, pk.name)
        if pk_val == -1:
            self.insert(conn, auto_increment=True)
            return
        n_updated = self.update(
            conn, **{pk.name: pk_val, cls._user_id_field().name: ctx.user.user_id}
        )
        if n_updated == 0:
            if cls.exists(conn, **{pk.name: pk_val}):
                raise AssertionError(
                    f"Record already exists but belong other user {ctx.user.user_id}"
                )
            self.insert(conn)
        else:
            assert n_updated == 1

    @override
    @classmethod
    def _prepare_where(cls, conn: sqlite3.Connection, **filters: Any) -> DbModel._SelectCursor:
        assert "user_ctx" in filters, "user_ctx is required"
        user_ctx = filters.pop("user_ctx")
        assert isinstance(user_ctx, UserContext), "user_ctx must be a UserContext"
        filters["user_id"] = user_ctx.user.user_id
        return super()._prepare_where(conn, **filters)

    @override
    @classmethod
    def load_by_id(cls: type[UO], conn: sqlite3.Connection, id: int) -> UO | None:
        raise AssertionError("Use load_by_id_with_ctx instead")

    @classmethod
    def load_by_id_with_ctx(
        cls: type[UO], conn: sqlite3.Connection, user_ctx: UserContext, id: int
    ) -> UO | None:
        rr: list[UO] = cls.select(conn, user_ctx=user_ctx, **{cls._ensure_pk().name: id})
        assert len(rr) <= 1
        return rr[0] if rr else None
