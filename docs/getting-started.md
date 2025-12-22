# Getting Started

This guide walks you through installing Lythonic and creating your first database schema.

## Installation

Install with pip:

```bash
pip install lythonic
```

Or with uv:

```bash
uv add lythonic
```

## Define Your First Model

Create a Python file with your schema definition:

```python
from pydantic import Field
from lythonic.state import DbModel, Schema

class Task(DbModel["Task"]):
    task_id: int = Field(default=-1, description="(PK)")
    title: str = Field(description="Task title")
    completed: bool = Field(default=False)

SCHEMA = Schema([Task])
```

Key points:

- Inherit from `DbModel["YourClassName"]` (the string must match the class name)
- Mark primary keys with `(PK)` in the field description
- Use `default=-1` for auto-increment primary keys

## Create the Database

```python
from pathlib import Path

SCHEMA.create_schema(Path("tasks.db"))
```

This generates and executes:

```sql
CREATE TABLE Task (
    task_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    completed INTEGER NOT NULL
)
```

## Insert and Query Data

```python
from lythonic.state import open_sqlite_db

with open_sqlite_db("tasks.db") as conn:
    # Create a task
    task = Task(title="Learn Lythonic")
    task.save(conn)  # Inserts and sets task_id

    print(f"Created task with id: {task.task_id}")

    # Query all tasks
    all_tasks = Task.select(conn)

    # Query with filter
    incomplete = Task.select(conn, completed=False)

    # Load by ID
    loaded = Task.load_by_id(conn, task.task_id)

    conn.commit()
```

## Next Steps

- [Your First Schema Tutorial](tutorials/first-schema.md) - Deeper dive into schema definition
- [CRUD Operations](tutorials/crud-operations.md) - All database operations explained
- [API Reference](reference/state.md) - Complete API documentation
