# How to Define a Schema

## Basic Model

```python
from pydantic import Field
from lythonic.state import DbModel

class MyModel(DbModel["MyModel"]):
    id: int = Field(default=-1, description="(PK)")
    name: str
```

## Primary Key

Add `(PK)` to field description, use `default=-1` for auto-increment:

```python
user_id: int = Field(default=-1, description="(PK)")
```

## Foreign Key

Add `(FK:Table.field)` to field description:

```python
author_id: int = Field(description="(FK:Author.author_id)")
```

## Nullable Fields

Use union with `None`:

```python
email: str | None = Field(default=None)
```

## Constrained Values

Use `Literal` for enum-like constraints:

```python
from typing import Literal

status: Literal["draft", "published", "archived"]
```

## Complex Types

Nested Pydantic models are stored as JSON:

```python
from lythonic.types import JsonBase

class Settings(JsonBase):
    theme: str = "light"
    notifications: bool = True

class User(DbModel["User"]):
    user_id: int = Field(default=-1, description="(PK)")
    settings: Settings = Field(default_factory=Settings)
```

## Register Schema

```python
from lythonic.state import Schema

SCHEMA = Schema([Author, Book, Review])
SCHEMA.create_schema(Path("library.db"))
```

List tables in dependency order (referenced tables first).
