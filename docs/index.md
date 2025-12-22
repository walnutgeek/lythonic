# Lythonic

**Light-weight pythonic SQLite integration with Pydantic.**

Lythonic provides a simple ORM-like interface for SQLite databases using Pydantic models
to define the schema. It supports automatic DDL generation, CRUD operations, type mapping,
and multi-tenant access patterns.

## Features

- **Pydantic-based schema** - Define tables as Pydantic models
- **Automatic DDL** - Generate CREATE TABLE statements from models
- **Type mapping** - Automatic conversion between Python and SQLite types
- **CRUD operations** - Insert, select, update with filtering
- **Multi-tenant support** - Built-in user-scoped data access patterns

## Installation

```bash
pip install lythonic
```

Or with uv:

```bash
uv add lythonic
```

## Quick Example

```python
from pydantic import Field
from lythonic.state import DbModel, Schema, open_sqlite_db

class Author(DbModel["Author"]):
    author_id: int = Field(default=-1, description="(PK)")
    name: str

class Book(DbModel["Book"]):
    book_id: int = Field(default=-1, description="(PK)")
    author_id: int = Field(description="(FK:Author.author_id)")
    title: str
    year: int | None = None

SCHEMA = Schema([Author, Book])
SCHEMA.create_schema("books.db")

with open_sqlite_db("books.db") as conn:
    author = Author(name="Jane Austen")
    author.save(conn)

    book = Book(author_id=author.author_id, title="Pride and Prejudice", year=1813)
    book.save(conn)

    books = Book.select(conn, author_id=author.author_id)
    conn.commit()
```

## Next Steps

- [Getting Started](getting-started.md) - Installation and first steps
- [Tutorials](tutorials/first-schema.md) - Step-by-step guides
- [API Reference](reference/state.md) - Complete API documentation
