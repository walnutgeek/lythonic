# Cashflow Tracking Example

This tutorial walks through the cashflow tracking example in `lythonic.examples.cashflow_tracking`,
demonstrating a real-world multi-tenant application.

## Overview

The cashflow tracking example models:

- Organizations and Accounts
- Scheduled recurring events (deposits, payments)
- Cash events and projections
- Multi-tenant data isolation via `UserOwned`

## Schema Design

```python
from typing import Literal
from pydantic import Field
from lythonic.state import Schema
from lythonic.state.user import User, UserOwned

AccountType = Literal["cash", "credit_card"]
EventType = Literal["deposit", "payment", "set_balance"]

class Organization(UserOwned["Organization"]):
    org_id: int = Field(default=-1, description="(PK)")
    name: str
    is_hidden: bool = False

class Account(UserOwned["Account"]):
    acc_id: int = Field(default=-1, description="(PK)")
    org_id: int = Field(description="(FK:Organization.org_id)")
    name: str
    account_type: AccountType
    start_date: date
    end_date: date | None = None

SCHEMA = Schema([User, Organization, Account, ...])
```

## Multi-Tenant Pattern

All entities extend `UserOwned` instead of `DbModel`:

```python
class UserOwned(DbModel):
    user_id: int = Field(description="(FK:User.user_id)")
```

This automatically:

- Adds `user_id` foreign key to every table
- Requires `UserContext` for all operations
- Scopes queries to the current user

## Using UserContext

```python
from lythonic.state.user import User, UserContext

# Create or load a user
user = User(info=UserInfo())
user.save(conn)

# Create context
ctx = UserContext(user=user)

# All operations require context
org = Organization(name="My Company")
org.save_with_ctx(ctx, conn)

# Queries are automatically scoped
my_orgs = Organization.select(conn, user_ctx=ctx)
```

## Scheduled Events

The example includes versioned scheduled events for recurring transactions:

```python
class ScheduledEvent(UserOwned["ScheduledEvent"]):
    sch_id: int = Field(default=-1, description="(PK)")
    acc_id: int = Field(description="(FK:Account.acc_id)")
    event_type: EventType
    frequency: FrequencyType  # weekly, monthly, etc.
    amount: float | None
    is_active: bool = True
    start_date: date
    end_date: date | None = None
```

Versioning allows tracking changes over time while preserving history.

## Key Patterns

1. **User isolation** - All data is scoped to users via `UserOwned`
2. **Referential integrity** - Foreign keys maintain relationships
3. **Temporal modeling** - Start/end dates for time-bounded entities
4. **Literal constraints** - Type-safe enums for status fields

See the full source in `src/lythonic/examples/cashflow_tracking/`.
