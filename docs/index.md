# Lythonic

**Compose Python logic into data-flow pipelines — sync or async, run anywhere.**

Write plain Python functions. Wire them with `>>`. Data flows visibly
between nodes — you can see what went in, what came out, what failed.
Unlike task schedulers where jobs are opaque units, lythonic tracks the
data itself.

## Data Flow, Not Task Flow

In Airflow or Prefect, you schedule *tasks* — opaque units that happen
to produce side effects. In lythonic, you compose *functions* — data
flows from one node to the next, with types checked at the edges.
Provenance records what data traversed each edge, not just "task X ran."

```python
from lythonic.compose.namespace import Dag

def fetch(url: str) -> dict:
    return {"source": url, "values": [1, 2, 3]}

def double(data: dict) -> dict:
    return {**data, "values": [v * 2 for v in data["values"]]}

dag = Dag()
dag.node(fetch) >> dag.node(double)

import asyncio
result = asyncio.run(dag(url="https://example.com"))
print(result.outputs["double"])
# {"source": "https://example.com", "values": [2, 4, 6]}
```

## Compose Freely

DAGs nest inside DAGs. Build small pipelines, reuse them as steps in
larger ones. Route data with `switch`, map over collections, or combine
both with `map(switch(...))`:

```python
from lythonic.compose.namespace import Dag, dag_factory

@dag_factory
def etl_pipeline() -> Dag:
    dag = Dag()
    dag.node(extract) >> dag.node(transform) >> dag.node(load)
    return dag

# Use as a step in a larger DAG
parent = Dag()
parent.node(setup) >> parent.node(etl_pipeline, label="etl") >> parent.node(report)

# Or map over a collection
parent.node(split) >> parent.map(etl_pipeline) >> parent.node(merge)

# Route by label
dag = Dag()
dag.node(classify) >> dag.switch([handle_text, handle_image], label="router")
```

## Run Transparently

Same DAG, different execution contexts:

```python
# Quick test — no provenance, no ceremony
result = await dag(url="https://example.com")

# Production — with provenance tracking
from lythonic.compose.dag_runner import DagRunner
from lythonic.compose.dag_provenance import DagProvenance
runner = DagRunner(dag, provenance=DagProvenance(Path("runs.db")))
result = await runner.run(source_inputs={"fetch": {"url": "..."}})

# Engine — cron-triggered, long-running
# lyth start  (reads lyth.yaml, activates triggers, runs poll loop)
```

## State: Structured Persistence

Define tables as Pydantic models with automatic DDL, CRUD, and multi-tenant
support:

```python
from lythonic.state import DbModel, Schema, open_sqlite_db
from pydantic import Field

class Author(DbModel["Author"]):
    author_id: int = Field(default=-1, description="(PK)")
    name: str

SCHEMA = Schema([Author])
SCHEMA.create_schema("library.db")

with open_sqlite_db("library.db") as conn:
    Author(name="Jane Austen").save(conn)
    conn.commit()
```

## Next Steps

- [Getting Started with lyth](tutorials/lyth-quickstart.md) — set up
  and run your first triggered pipeline
- [Build a Pipeline](tutorials/compose-pipeline.md) — end-to-end compose tutorial
- [Composable DAGs](how-to/composable-dags.md) — MapNode, CallNode,
  SwitchNode, MapSwitch, callable DAGs
- [Scheduled Triggers](tutorials/scheduled-triggers.md) — cron-based
  DAG execution
- [API Reference](reference/compose-namespace.md) — complete API docs
