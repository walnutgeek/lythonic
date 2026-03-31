# Build a Pipeline

This tutorial walks through building a data-enrichment pipeline using
lythonic's compose stack. You'll register callables in a namespace, wire
them into a DAG, execute it, add provenance tracking, and layer on caching.

## Register Callables in a Namespace

A `Namespace` is a hierarchical registry for callables. Paths use `.` for
branches and `:` for the leaf name — like `"pipeline:fetch"`.

Start by defining two plain functions and registering them:

```python
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
```

Once registered, callables are accessible by path or by attribute:

```python
node = ns.get("pipeline:fetch")
result = node(url="https://example.com/data")
print(result)
# {'source': 'https://example.com/data', 'raw': [1, 2, 3]}

# Dot-access also works
result = ns.pipeline.fetch(url="https://example.com/data")
```

Each registered callable is wrapped in a `NamespaceNode` that carries
metadata about the function's signature, its namespace path, and an
optional decorator.

## Define a DAG

A `Dag` connects namespace nodes into a directed acyclic graph using the
`>>` operator. Nodes are created from registered `NamespaceNode`s, and
edges declare data flow.

```python
from lythonic.compose.namespace import Dag

dag = Dag()

fetch_node = dag.node(ns.get("pipeline:fetch"))
transform_node = dag.node(ns.get("pipeline:transform"))

fetch_node >> transform_node
```

The `>>` operator registers an edge from `fetch_node` to `transform_node`.
When executed, the output of `fetch` is wired into `transform` by matching
return types to parameter types.

You can also use a context manager, which validates the DAG on exit
(checking for cycles and type compatibility):

```python
with Dag() as dag:
    f = dag.node(ns.get("pipeline:fetch"))
    t = dag.node(ns.get("pipeline:transform"))
    f >> t
# Validation runs automatically here
```

For fan-out, one node can feed multiple downstream nodes:

```python
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
```

## Execute the DAG

`DagRunner` executes the DAG asynchronously in topological order. Source
nodes (those with no upstream edges) receive their inputs via
`source_inputs`. Downstream nodes receive outputs wired from their
upstream nodes.

```python
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
```

The runner:

1. Executes nodes in topological order
2. Wires each node's return value to the next node's parameters by matching types
3. Collects sink node outputs (nodes with no downstream edges) into `result.outputs`

Without a `db_path`, the runner uses `NullProvenance` — no persistence,
just in-memory execution.

## Add Provenance

To track run history, pass a `db_path` to `DagRunner`. This creates a
SQLite database with two tables: `dag_runs` (run lifecycle) and
`node_executions` (per-node inputs, outputs, timing, errors).

```python
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
```

With provenance enabled, you can also restart paused or failed runs with
`runner.restart(run_id)` and selectively re-execute nodes with
`runner.replay(run_id, rerun_nodes={"transform"})`.

## Add Caching

For expensive callables (API calls, slow computations), wrap them with
`register_cached_callable`. This adds SQLite-backed caching with
configurable TTL.

```python
from lythonic.compose.cached import register_cached_callable

# Assume fetch_prices is defined in myapp.downloads
# register_cached_callable(
#     ns, "myapp.downloads:fetch_prices", "market:fetch_prices",
#     min_ttl=0.5, max_ttl=2.0, db_path=Path("cache.db"),
# )

# After registration, calls are served from cache when fresh:
# result = ns.market.fetch_prices(ticker="AAPL")
```

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
