# lythonic

[![PyPI version](https://img.shields.io/pypi/v/lythonic.svg?label=lythonic&color=blue)](https://pypi.org/project/lythonic/)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://walnutgeek.github.io/lythonic/)

**Compose Python logic into data-flow pipelines — sync or async, run anywhere.**

Write plain Python functions. Wire them with `>>`. Data flows visibly
between nodes — you can see what went in, what came out, what failed.
Unlike task schedulers where jobs are opaque units, lythonic tracks the
data itself.

## Quick Start

```bash
uv add lythonic
```

```python
from lythonic.compose.namespace import Dag

def fetch(url: str) -> dict:
    return {"source": url, "values": [1, 2, 3]}

def double(data: dict) -> dict:
    return {**data, "values": [v * 2 for v in data["values"]]}

dag = Dag()
dag.node(fetch) >> dag.node(double)

# Run it — sync or async, doesn't matter
import asyncio
result = asyncio.run(dag(url="https://example.com"))
print(result.outputs)  # {"double": {"source": "...", "values": [2, 4, 6]}}
```

## Why lythonic?

**Data flow, not task flow.** Each node receives typed data from upstream
and passes results downstream. The DAG runner wires inputs to outputs by
type — fan-out, fan-in, and map-reduce built in. Provenance tracking
records what data flowed through each edge.

**Compose freely.** DAGs nest inside DAGs. `dag.node(sub_dag)` runs a
sub-DAG as a single step. `dag.map(sub_dag)` runs it on each element of
a collection, concurrently. Build small, reuse everywhere.

**Run transparently.** `await dag()` for a quick test. `DagRunner` with
provenance for production. `lyth start` for a long-running engine with
cron-triggered pipelines. Same code, different execution context.

**Sync and async — mixed freely.** Write sync functions, async functions,
or both in the same DAG. Sync nodes run in a thread executor automatically.

## Features

- **DAG composition** — `>>` wiring, callable DAGs, MapNode, CallNode
- **`@dag_factory`** — define reusable DAG templates as decorated functions
- **Triggers** — cron-scheduled or push-triggered execution via `TriggerManager`
- **Provenance** — SQLite-backed tracking of runs, node executions, edge traversals
- **Caching** — per-callable SQLite cache with probabilistic TTL refresh
- **`lyth` CLI** — `start`, `stop`, `run`, `fire`, `status` commands
- **State** — Pydantic-based SQLite ORM with schema management and multi-tenant support

## Documentation

Full documentation at [walnutgeek.github.io/lythonic](https://walnutgeek.github.io/lythonic/).

![Star History](https://api.star-history.com/svg?repos=walnutgeek/lythonic&type=Date)   