# How to Build Multi-Tenant Apps

Use `UserOwned` base class for automatic user scoping.

## Define User-Owned Models

```python
from lythonic.state.user import User, UserOwned, UserContext
from pydantic import Field

class Project(UserOwned["Project"]):
    project_id: int = Field(default=-1, description="(PK)")
    name: str
```

`UserOwned` automatically adds:

```python
user_id: int = Field(description="(FK:User.user_id)")
```

## Create User and Context

```python
from lythonic.state.user import User, UserInfo, UserContext

user = User(info=UserInfo())
user.save(conn)

ctx = UserContext(user=user)
```

## Save with Context

```python
project = Project(name="My Project")
project.save_with_ctx(ctx, conn)  # Sets user_id automatically
```

## Query with Context

All queries require context and are scoped to that user:

```python
# Only returns projects for ctx.user
projects = Project.select(conn, user_ctx=ctx)

# Load by ID (returns None if wrong user)
project = Project.load_by_id_with_ctx(conn, ctx, project_id=42)
```

## Schema Setup

Include `User` table first:

```python
from lythonic.state import Schema
from lythonic.state.user import User

SCHEMA = Schema([User, Project, Task])
```

## Access Control

`UserOwned` prevents accessing other users' data:

- `save_with_ctx()` raises error if updating another user's record
- `select()` automatically filters by `user_id`
- `load_by_id_with_ctx()` returns `None` for other users' records
