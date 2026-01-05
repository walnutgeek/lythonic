"""
User: Multi-tenant user ownership patterns for database models.

This module provides base classes for building multi-tenant applications where
records are owned by users and access is controlled accordingly.

## Core Classes

- `User`: Base user model with `user_id`, `info`, and `created_at`
- `UserContext`: Context object passed to operations requiring user scope
- `UserOwned`: Base class for models that belong to a user

## Usage

```python
from lythonic.state.user import User, UserContext, UserOwned, UserInfo
from pydantic import Field

class MyUserInfo(UserInfo):
    name: str
    email: str

class Document(UserOwned["Document"]):
    doc_id: int = Field(default=-1, description="(PK)")
    title: str

# Create user context
user = User(user_id=1, info=MyUserInfo(name="Alice", email="alice@example.com"))
ctx = UserContext(user=user)

# Save with user context (enforces ownership)
doc = Document(title="My Doc")
doc.save_with_ctx(ctx, conn)

# Load with user context (only returns user's records)
doc = Document.load_by_id_with_ctx(conn, ctx, doc_id=1)
```

The `UserOwned` class overrides `save()` and `load_by_id()` to require
`UserContext`, ensuring all database operations are scoped to the current user.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Self, TypeVar

from pydantic import BaseModel, Field
from typing_extensions import override

from lythonic import GlobalRef, utc_now
from lythonic.state import DbModel
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
    def save(self, conn: sqlite3.Connection) -> Self:
        raise NotImplementedError("Use save_with_ctx instead")

    @override
    @classmethod
    def load_by_id(cls: type[UO], conn: sqlite3.Connection, id: int) -> UO | None:
        raise NotImplementedError("Use load_by_id_with_ctx instead")

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
        n_updated = self.update(conn, user_ctx=ctx, **{pk.name: pk_val})
        if n_updated == 0:
            raise ValueError(
                "Possible Access violation:",
                " Record does not exist or belong other user then the one in context:",
                ctx.user.user_id,
            )
        else:
            assert n_updated == 1

    @override
    @classmethod
    def _prepare_where(cls, conn: sqlite3.Connection, **filters: Any) -> DbModel._WhereBased:
        assert "user_ctx" in filters, f"user_ctx:{GlobalRef(UserContext)!s} is required"
        user_ctx = filters.pop("user_ctx")
        assert isinstance(user_ctx, UserContext), (
            f"user_ctx:{GlobalRef(UserContext)!s} must be a UserContext"
        )
        filters["user_id"] = user_ctx.user.user_id
        return super()._prepare_where(conn, **filters)

    @classmethod
    def load_by_id_with_ctx(
        cls: type[UO], conn: sqlite3.Connection, user_ctx: UserContext, id: int
    ) -> UO | None:
        rr: list[UO] = cls.select(conn, user_ctx=user_ctx, **{cls._ensure_pk().name: id})
        assert len(rr) <= 1
        return rr[0] if rr else None
