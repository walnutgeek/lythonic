# CRUD Operations

This tutorial covers all database operations: Create, Read, Update, Delete.

## Setup

```python
from lythonic.state import DbModel, Schema, open_sqlite_db
from pydantic import Field

class Task(DbModel["Task"]):
    task_id: int = Field(default=-1, description="(PK)")
    title: str
    priority: int = 0
    completed: bool = False

SCHEMA = Schema([Task])
SCHEMA.create_schema("tasks.db")
```

## Create (Insert)

### Using save()

The `save()` method handles both insert and update:

```python
with open_sqlite_db("tasks.db") as conn:
    task = Task(title="Write docs", priority=1)
    task.save(conn)  # Inserts, sets task_id
    print(task.task_id)  # Now has a real ID

    conn.commit()
```

### Using insert()

For explicit insert with auto-increment:

```python
task = Task(title="Review code")
task.insert(conn, auto_increment=True)
```

## Read (Select)

### Select All

```python
all_tasks = Task.select(conn)
```

### Filter by Field Value

```python
high_priority = Task.select(conn, priority=1)
incomplete = Task.select(conn, completed=False)
```

### Filter with Operators

Prefix field names with operator and double underscore:

```python
# Greater than
urgent = Task.select(conn, gt__priority=5)

# Less than or equal
low = Task.select(conn, lte__priority=2)

# Not equal
active = Task.select(conn, ne__completed=True)
```

Available operators: `eq`, `ne`, `gt`, `lt`, `gte`, `lte`

### IN Clause

Pass a list for IN queries:

```python
specific = Task.select(conn, priority=[1, 2, 3])
```

### Load by ID

```python
task = Task.load_by_id(conn, 42)  # Returns None if not found
```

### Count and Exists

```python
count = Task.select_count(conn, completed=False)
exists = Task.exists(conn, title="Write docs")
```

## Update

### Using save()

Modify and save:

```python
task = Task.load_by_id(conn, 1)
task.title = "Updated title"
task.save(conn)  # Updates existing row

conn.commit()
```

### Using update()

For explicit update with filters:

```python
task.priority = 10
n_updated = task.update(conn, task_id=task.task_id)
```

## Delete

Currently, delete operations should be done with raw SQL:

```python
from lythonic.state import execute_sql

cursor = conn.cursor()
execute_sql(cursor, "DELETE FROM Task WHERE task_id = ?", [task_id])
conn.commit()
```

## Transactions

Always commit after modifications:

```python
with open_sqlite_db("tasks.db") as conn:
    task1 = Task(title="Task 1")
    task1.save(conn)

    task2 = Task(title="Task 2")
    task2.save(conn)

    conn.commit()  # Both saved atomically
```

If an exception occurs before commit, changes are rolled back when the
connection closes.
