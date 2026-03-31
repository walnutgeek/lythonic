# Lythonic

**Lightweight pythonic toolkit for building complex workflows.**

Lythonic has two pillars: **compose** for building callable pipelines and
DAGs, and **state** for structured data persistence with SQLite and Pydantic.

## Compose: Callable Pipelines

Register functions in a namespace, wire them into a DAG, and execute:

```python
from lythonic.compose.namespace import Namespace, Dag
from lythonic.compose.dag_runner import DagRunner
import asyncio

def fetch(url: str) -> dict:
    return {"source": url, "raw": [1, 2, 3]}

def transform(data: dict) -> dict:
    return {"source": data["source"], "values": [v * 2 for v in data["raw"]]}

ns = Namespace()
ns.register(fetch, nsref="pipeline:fetch")
ns.register(transform, nsref="pipeline:transform")

with Dag() as dag:
    dag.node(ns.get("pipeline:fetch")) >> dag.node(ns.get("pipeline:transform"))

runner = DagRunner(dag)
result = asyncio.run(runner.run(source_inputs={"fetch": {"url": "https://example.com"}}))
print(result.status)   # "completed"
print(result.outputs)  # {"transform": {"source": "https://example.com", "values": [2, 4, 6]}}
```

## State: Structured Persistence

Define tables as Pydantic models with automatic DDL, CRUD, and multi-tenant
support:

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

SCHEMA = Schema([Author, Book])
SCHEMA.create_schema("library.db")

with open_sqlite_db("library.db") as conn:
    author = Author(name="Jane Austen")
    author.save(conn)
    Book(author_id=author.author_id, title="Pride and Prejudice").save(conn)
    conn.commit()
```

## Next Steps

- [Build a Pipeline](tutorials/compose-pipeline.md) — end-to-end compose tutorial
- [Your First Schema](tutorials/first-schema.md) — define tables with Pydantic models
- [CRUD Operations](tutorials/crud-operations.md) — insert, query, update, delete
- [API Reference](reference/compose-namespace.md) — complete API documentation
