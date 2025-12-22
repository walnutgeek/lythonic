# lythonic

[![PyPI version](https://img.shields.io/pypi/v/lythonic.svg?label=lythonic&color=blue)](https://pypi.org/project/lythonic/)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://walnutgeek.github.io/lythonic/)



**Lythonic**  
_Lightweight, reliable tools for database management and job scheduling in Python — built atop powerful industry standards: SQLite and Pydantic._

Lythonic helps you easily manage persistent data and scheduled tasks in your Python applications, no matter their size or complexity. It combines the trusted simplicity of SQLite (the world’s most used database) with Pydantic (the gold standard for data validation and modeling) to provide an effortless, modern development experience.

**Why use lythonic?**
- **Zero setup:** No need to run a database server or learn new dialects. SQLite is embedded and self-contained.
- **Reliable models:** Define your data models with clear type-checking, validation, and conversion using Pydantic.
- **Easy scheduling:** Integrates simple, robust job/task scheduling primitives (coming soon) for recurring or deferred operations.
- **Pythonic APIs:** Focus on productivity, safety, and clarity, following modern Python best practices throughout.

---

## Quick Start for Developers

```python
from pathlib import Path
from lythonic.state import DbModel, open_sqlite_db
from pydantic import Field

# Define your Pydantic-based model with automatic DDL generation
class Note(DbModel["Note"]):
    note_id: int = Field(default=-1, description="(PK) Unique identifier")
    content: str = Field(description="Note content")

# Create a new SQLite database or connect to existing:
db_path = Path("notes.db")
with open_sqlite_db(db_path) as conn:
    # Setup schema if it's new
    conn.execute(Note.create_ddl())
    conn.commit()

    # Insert a note
    n = Note(content="Hello world!")
    n.save(conn)   # Auto-assigns primary key
    conn.commit()

    # Query by id
    loaded = Note.load_by_id(conn, n.note_id)
    print(loaded.content)
```

You get:
- **Seamless storage and retrieval** of strongly-typed Python objects.
- **No boilerplate:** No need to hand-write SQL or adapters for most basic use.
- **Atomic, testable operations:** See `tabula_rasa_path` and test files for utilities on prepping file/db state for clean test runs.

---

## At a Glance

- Models are defined as Pydantic classes — strict types, clear defaults, easy serialization.
- Data is stored in local, portable `.db` files using SQLite.
- Utilities for file and directory lifecycle, safe test/cleanup (see `misc.py`).
- Test suite demonstrates end-to-end use: see `/tests`.
- No heavy dependencies or servers; the whole stack is pure Python.

---

Suitable for scripts, data apps, bot backends, experiments, quick prototypes, or serious projects where simplicity and reliability win.

See project docs and modules for advanced usage and extension patterns.


* * *

## Project Docs

For how to install uv and Python, see [installation.md](installation.md).

For development workflows, see [development.md](development.md).

For instructions on publishing to PyPI, see [publishing.md](publishing.md).

For documentation strategy and MkDocs setup, see [documenting.md](documenting.md).

* * *

*This project was built from
[simple-modern-uv](https://github.com/jlevy/simple-modern-uv).*
