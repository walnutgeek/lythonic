# Documentation Revamp Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reframe lythonic documentation with compose as the lead pillar, add a compose tutorial, and update all front-facing pages.

**Architecture:** Five independent deliverables — a new compose tutorial, rewritten index.md, rewritten README.md, updated pyproject.toml, and reordered mkdocs.yml. The tutorial is written first since the front pages excerpt from it.

**Tech Stack:** Markdown, mkdocs-material, mkdocstrings

---

### Task 1: Write the Compose Tutorial

**Files:**
- Create: `docs/tutorials/compose-pipeline.md`

This is the largest task. The tutorial walks through building a data-enrichment
pipeline using the compose stack. Each section builds on the previous with
runnable code.

- [ ] **Step 1: Create the tutorial file with the intro and namespace section**

```markdown
# Build a Pipeline

This tutorial walks through building a data-enrichment pipeline using
lythonic's compose stack. You'll register callables in a namespace, wire
them into a DAG, execute it, add provenance tracking, and layer on caching.

## Register Callables in a Namespace

A `Namespace` is a hierarchical registry for callables. Paths use `.` for
branches and `:` for the leaf name — like `"pipeline:fetch"`.

Start by defining two plain functions and registering them:

` ` `python
from lythonic.compose.namespace import Namespace

def fetch(url: str) -> dict:
    """Simulate fetching data from a URL."""
    return {"source": url, "raw": [1, 2, 3]}

def transform(data: dict) -> dict:
    """Double every value in the raw data."""
    return {"source": data["source"], "values": [v * 2 for v in data["raw"]]}

ns = Namespace()
ns.register(fetch, nsref="pipeline:fetch")
ns.register(transform, nsref="pipeline:transform")
` ` `

Once registered, callables are accessible by path or by attribute:

` ` `python
node = ns.get("pipeline:fetch")
result = node(url="https://example.com/data")
print(result)
# {'source': 'https://example.com/data', 'raw': [1, 2, 3]}

# Dot-access also works
result = ns.pipeline.fetch(url="https://example.com/data")
` ` `

Each registered callable is wrapped in a `NamespaceNode` that carries
metadata about the function's signature, its namespace path, and an
optional decorator.
```

Note: Replace the triple-backtick sequences above (shown with spaces for
escaping) with actual triple backticks in the file.

- [ ] **Step 2: Add the DAG definition section**

Append to the tutorial file:

```markdown
## Define a DAG

A `Dag` connects namespace nodes into a directed acyclic graph using the
`>>` operator. Nodes are created from registered `NamespaceNode`s, and
edges declare data flow.

` ` `python
from lythonic.compose.namespace import Dag

dag = Dag()

fetch_node = dag.node(ns.get("pipeline:fetch"))
transform_node = dag.node(ns.get("pipeline:transform"))

fetch_node >> transform_node
` ` `

The `>>` operator registers an edge from `fetch_node` to `transform_node`.
When executed, the output of `fetch` is wired into `transform` by matching
return types to parameter types.

You can also use a context manager, which validates the DAG on exit
(checking for cycles and type compatibility):

` ` `python
with Dag() as dag:
    f = dag.node(ns.get("pipeline:fetch"))
    t = dag.node(ns.get("pipeline:transform"))
    f >> t
# Validation runs automatically here
` ` `

For fan-out, one node can feed multiple downstream nodes:

` ` `python
def validate(data: dict) -> dict:
    """Check that values are positive."""
    assert all(v > 0 for v in data["values"]), "Negative value found"
    return data

ns.register(validate, nsref="pipeline:validate")

with Dag() as dag:
    f = dag.node(ns.get("pipeline:fetch"))
    t = dag.node(ns.get("pipeline:transform"))
    v = dag.node(ns.get("pipeline:validate"))
    f >> t >> v
` ` `
```

- [ ] **Step 3: Add the execution section**

Append to the tutorial file:

```markdown
## Execute the DAG

`DagRunner` executes the DAG asynchronously in topological order. Source
nodes (those with no upstream edges) receive their inputs via
`source_inputs`. Downstream nodes receive outputs wired from their
upstream nodes.

` ` `python
import asyncio
from lythonic.compose.dag_runner import DagRunner

runner = DagRunner(dag)

async def main():
    result = await runner.run(
        source_inputs={"fetch": {"url": "https://example.com/data"}}
    )
    print(result.status)    # "completed"
    print(result.outputs)   # {"validate": {"source": "https://example.com/data", "values": [2, 4, 6]}}

asyncio.run(main())
` ` `

The runner:

1. Executes nodes in topological order
2. Wires each node's return value to the next node's parameters by matching types
3. Collects sink node outputs (nodes with no downstream edges) into `result.outputs`

Without a `db_path`, the runner uses `NullProvenance` — no persistence,
just in-memory execution.
```

- [ ] **Step 4: Add the provenance section**

Append to the tutorial file:

```markdown
## Add Provenance

To track run history, pass a `db_path` to `DagRunner`. This creates a
SQLite database with two tables: `dag_runs` (run lifecycle) and
`node_executions` (per-node inputs, outputs, timing, errors).

` ` `python
from pathlib import Path
from lythonic.compose.dag_runner import DagRunner
from lythonic.compose.dag_provenance import DagProvenance

runner = DagRunner(dag, db_path=Path("pipeline.db"))

async def main():
    result = await runner.run(
        source_inputs={"fetch": {"url": "https://example.com/data"}},
        dag_nsref="pipeline:enrich",
    )
    print(result.run_id)    # UUID of this run
    print(result.status)    # "completed"

    # Query the provenance database
    prov = DagProvenance(Path("pipeline.db"))
    run = prov.get_run(result.run_id)
    print(run["status"])    # "completed"

    nodes = prov.get_node_executions(result.run_id)
    for n in nodes:
        print(f"{n['node_label']}: {n['status']}")
    # fetch: completed
    # transform: completed
    # validate: completed

asyncio.run(main())
` ` `

With provenance enabled, you can also restart paused or failed runs with
`runner.restart(run_id)` and selectively re-execute nodes with
`runner.replay(run_id, rerun_nodes={"transform"})`.
```

- [ ] **Step 5: Add the caching section**

Append to the tutorial file:

```markdown
## Add Caching

For expensive callables (API calls, slow computations), wrap them with
`register_cached_callable`. This adds SQLite-backed caching with
configurable TTL.

` ` `python
from lythonic.compose.cached import register_cached_callable

# Assume fetch_prices is defined in myapp.downloads
# register_cached_callable(
#     ns, "myapp.downloads:fetch_prices", "market:fetch_prices",
#     min_ttl=0.5, max_ttl=2.0, db_path=Path("cache.db"),
# )

# After registration, calls are served from cache when fresh:
# result = ns.market.fetch_prices(ticker="AAPL")
` ` `

TTL behavior:

- **age < `min_ttl`**: return cached value (fresh)
- **`min_ttl` <= age < `max_ttl`**: probabilistic refresh — the older the
  entry, the more likely it refreshes
- **age >= `max_ttl`** or cache miss: call the original function

TTL values are in days (e.g., `min_ttl=0.5` means 12 hours).

All cached method parameters must be "simple types" (primitives, `date`,
`datetime`, `Path`) so they can serve as cache key columns.

## Next Steps

- [API Reference: lythonic.compose.namespace](../reference/compose-namespace.md)
  — full Namespace, Dag, and DagNode API
- [API Reference: lythonic.compose.dag_runner](../reference/compose-dag-runner.md)
  — DagRunner, DagPause, restart, replay
- [API Reference: lythonic.compose.cached](../reference/compose-cached.md)
  — register_cached_callable, TTL, pushback
- [Your First Schema](first-schema.md) — learn the state pillar for
  structured data persistence
```

- [ ] **Step 6: Review the tutorial for consistency**

Read through the complete file. Verify:
- All import paths are correct (`lythonic.compose.namespace`, `lythonic.compose.dag_runner`, etc.)
- Code examples build on each other (the `ns` and `dag` objects carry forward)
- Triple backticks are actual backticks (not escaped)

- [ ] **Step 7: Commit**

```bash
git add docs/tutorials/compose-pipeline.md
git commit -m "docs: add compose pipeline tutorial"
```

---

### Task 2: Rewrite `docs/index.md`

**Files:**
- Modify: `docs/index.md`

- [ ] **Step 1: Replace the entire contents of `docs/index.md`**

```markdown
# Lythonic

**Lightweight pythonic toolkit for building complex workflows.**

Lythonic has two pillars: **compose** for building callable pipelines and
DAGs, and **state** for structured data persistence with SQLite and Pydantic.

## Compose: Callable Pipelines

Register functions in a namespace, wire them into a DAG, and execute:

` ` `python
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
` ` `

## State: Structured Persistence

Define tables as Pydantic models with automatic DDL, CRUD, and multi-tenant
support:

` ` `python
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
` ` `

## Next Steps

- [Build a Pipeline](tutorials/compose-pipeline.md) — end-to-end compose tutorial
- [Your First Schema](tutorials/first-schema.md) — define tables with Pydantic models
- [CRUD Operations](tutorials/crud-operations.md) — insert, query, update, delete
- [API Reference](reference/compose-namespace.md) — complete API documentation
```

Note: Replace the triple-backtick sequences (shown with spaces) with actual
triple backticks.

- [ ] **Step 2: Commit**

```bash
git add docs/index.md
git commit -m "docs: rewrite index.md with compose-forward framing"
```

---

### Task 3: Rewrite `README.md`

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Replace the entire contents of `README.md`**

```markdown
# lythonic

[![PyPI version](https://img.shields.io/pypi/v/lythonic.svg?label=lythonic&color=blue)](https://pypi.org/project/lythonic/)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://walnutgeek.github.io/lythonic/)

**Lightweight pythonic toolkit for building complex workflows.**

Lythonic combines composable callable pipelines with structured SQLite
persistence — two pillars that work together or independently.

## Installation

` ` `bash
uv add lythonic
` ` `

## Compose

Build callable pipelines and DAGs with automatic execution and provenance.

- **Namespace** — hierarchical registry for callables with dot-path access
- **DAG** — wire nodes with `>>`, validate types and cycles, fan-out/fan-in
- **DagRunner** — async execution with output wiring, pause/restart/replay
- **Caching** — SQLite-backed cache with probabilistic TTL refresh

## State

Structured data persistence powered by SQLite and Pydantic.

- **DbModel** — define tables as Pydantic models with automatic DDL generation
- **Schema** — manage multiple tables with referential integrity
- **CRUD** — insert, select, update, delete with typed filtering
- **Multi-tenant** — built-in user-scoped access patterns via `UserOwned`

## Documentation

Full documentation at [walnutgeek.github.io/lythonic](https://walnutgeek.github.io/lythonic/).
```

Note: Replace the triple-backtick sequences (shown with spaces) with actual
triple backticks.

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: rewrite README with compose-forward framing"
```

---

### Task 4: Update `pyproject.toml` and `mkdocs.yml`

**Files:**
- Modify: `pyproject.toml:10`
- Modify: `mkdocs.yml:2,59-83`

- [ ] **Step 1: Update the description in `pyproject.toml`**

Change line 10 from:

```toml
description = "Light-weight pythonic db integration - powered by the best of all time: sqlite and pydantic"
```

to:

```toml
description = "Lightweight pythonic toolkit for building complex workflows"
```

- [ ] **Step 2: Update `mkdocs.yml` site_description**

Change line 2 from:

```yaml
site_description: Light-weight pythonic SQLite integration with Pydantic
```

to:

```yaml
site_description: Lightweight pythonic toolkit for building complex workflows
```

- [ ] **Step 3: Replace the `nav` section in `mkdocs.yml`**

Replace lines 59-83 with:

```yaml
nav:
  - Home: index.md
  - Getting Started: getting-started.md
  - Tutorials:
      - Build a Pipeline: tutorials/compose-pipeline.md
      - Your First Schema: tutorials/first-schema.md
      - CRUD Operations: tutorials/crud-operations.md
      - Cashflow Tracking Example: tutorials/cashflow-example.md
  - How-To Guides:
      - Define a Schema: how-to/define-schema.md
      - Multi-Tenant Apps: how-to/multi-tenant.md
  - API Reference:
      - lythonic: reference/core.md
      - compose:
          - lythonic.compose: reference/compose.md
          - "...namespace": reference/compose-namespace.md
          - "...namespace_config": reference/compose-namespace-config.md
          - "...dag_runner": reference/compose-dag-runner.md
          - "...dag_provenance": reference/compose-dag-provenance.md
          - "...cached": reference/compose-cached.md
          - "...cli": reference/compose-cli.md
          - "...logic": reference/compose-logic.md
      - state:
          - lythonic.state: reference/state.md
          - "...user": reference/user.md
      - lythonic.types: reference/types.md
      - lythonic.periodic: reference/periodic.md
      - lythonic.misc: reference/misc.md
  - Release Notes:
      - v0.0.10: release_notes/v0.0.10.md
      - v0.0.9: release_notes/v0.0.9.md
```

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml mkdocs.yml
git commit -m "docs: update tagline and reorder nav for compose-forward framing"
```

---

### Task 5: Build and Verify

**Files:** None (verification only)

- [ ] **Step 1: Build the docs site**

```bash
uv run mkdocs build --strict 2>&1
```

Expected: Build succeeds with no errors. Warnings about missing pages would
indicate a nav entry pointing to a nonexistent file.

- [ ] **Step 2: Fix any build errors**

If the build fails, read the error output and fix the referenced files.
Common issues: broken internal links, missing files referenced in nav.

- [ ] **Step 3: Commit any fixes**

```bash
git add -A
git commit -m "docs: fix build errors from documentation revamp"
```

Only if there were fixes needed. Skip if the build passed cleanly.
