# Build a Pipeline

This tutorial walks through building a data-enrichment pipeline using
lythonic's compose stack. You'll register callables in a DAG, wire
them into a pipeline, execute it, add provenance tracking, and layer on caching.

## Register Callables in a Namespace

A `Namespace` is a hierarchical registry for callables. Paths use `.` for
branches and `:` for the leaf name — like `"pipeline.data:fetch"`.

Each `Dag` owns its own `Namespace`. When you pass a plain callable to
`dag.node()`, it is auto-registered in the DAG's namespace using the
callable's module and name. You can also register callables in a standalone
`Namespace` for sharing across DAGs:

```python
from lythonic.compose.namespace import Namespace

def fetch(url: str) -> dict:
    """Simulate fetching data from a URL."""
    return {"source": url, "raw": [1, 2, 3]}

def transform(data: dict) -> dict:
    """Double every value in the raw data."""
    return {"source": data["source"], "values": [v * 2 for v in data["raw"]]}

ns = Namespace()
ns.register(fetch, nsref="pipeline.data:fetch")
ns.register(transform, nsref="pipeline.data:transform")
```

The dot in `"pipeline.data:fetch"` creates a nested branch. Once
registered, callables are accessible by path or by dot-access through
the branch hierarchy:

```python
node = ns.get("pipeline.data:fetch")
result = node(url="https://example.com/data")
print(result)
# {'source': 'https://example.com/data', 'raw': [1, 2, 3]}

# Dot-access walks the branches automatically
result = ns.pipeline.data.fetch(url="https://example.com/data")
```

Each registered callable is wrapped in a `NamespaceNode` that carries
metadata about the function's signature, its namespace path, and an
optional decorator.

## Define a DAG

A `Dag` connects callables into a directed acyclic graph using the
`>>` operator. The simplest way is to pass callables directly to
`dag.node()` — they are auto-registered in the DAG's own namespace:

```python
from lythonic.compose.namespace import Dag

dag = Dag()
dag.node(fetch) >> dag.node(transform)
```

You can also pass pre-registered `NamespaceNode`s:

```python
dag = Dag()
fetch_node = dag.node(ns.get("pipeline.data:fetch"))
transform_node = dag.node(ns.get("pipeline.data:transform"))
fetch_node >> transform_node
```

The `>>` operator registers an edge from `fetch_node` to `transform_node`.
When executed, the output of `fetch` is wired into `transform` by matching
return types to parameter types.

You can also use a context manager, which validates the DAG on exit
(checking for cycles and type compatibility):

```python
with Dag() as dag:
    dag.node(fetch) >> dag.node(transform)
# Validation runs automatically here
```

For fan-out, one node can feed multiple downstream nodes:

```python
def validate(data: dict) -> dict:
    """Check that values are positive."""
    assert all(v > 0 for v in data["values"]), "Negative value found"
    return data

with Dag() as dag:
    f = dag.node(fetch)
    t = dag.node(transform)
    v = dag.node(validate)
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

Without provenance, the runner uses `NullProvenance` — no persistence,
just in-memory execution. Sync node functions are dispatched to a thread
executor by default so they don't block the event loop; use the `@inline`
decorator to opt out for lightweight pure functions.

## Callable DAGs

A `Dag` is directly callable — `await dag(**kwargs)` creates a `DagRunner`
with `NullProvenance`, matches kwargs to source node parameters by name,
and returns a `DagRunResult`:

```python
async def main():
    result = await dag(url="https://example.com/data")
    print(result.status)    # "completed"
    print(result.outputs)   # {"validate": ...}

asyncio.run(main())
```

This is convenient for quick execution without explicit runner setup.

## Composable DAGs

### MapNode: Process Collections

`Dag.map(sub_dag, label)` creates a `MapNode` that runs a sub-DAG on each
element of an upstream `list` or `dict`, concurrently via `asyncio.gather`.
The sub-DAG must have exactly one source and one sink:

```python
sub_dag = Dag()
sub_dag.node(tokenize) >> sub_dag.node(count)

parent = Dag()
parent.node(split_text) >> parent.map(sub_dag, label="chunks") >> parent.node(reduce)
```

### CallNode: Inline Sub-DAGs

`dag.node(sub_dag, label="enrich")` creates a `CallNode` that runs a
sub-DAG once as a single step, passing the upstream output to the
sub-DAG's source:

```python
enrich_dag = Dag()
enrich_dag.node(lookup) >> enrich_dag.node(annotate)

parent = Dag()
parent.node(fetch) >> parent.node(enrich_dag, label="enrich") >> parent.node(save)
```

## Add Provenance

To track run history, pass a `DagProvenance` instance to `DagRunner`. This
creates a SQLite database with two tables: `dag_runs` (run lifecycle) and
`node_executions` (per-node inputs, outputs, timing, errors).

```python
from pathlib import Path
from lythonic.compose.dag_runner import DagRunner
from lythonic.compose.dag_provenance import DagProvenance

runner = DagRunner(dag, provenance=DagProvenance(Path("pipeline.db")))

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

`register_cached_callable` takes a `GlobalRef` string (module path +
callable name) and registers a cache-wrapped version in the namespace:

```python
from pathlib import Path
from lythonic.compose.cached import register_cached_callable

register_cached_callable(
    ns,
    gref="lythonic.compose.namespace:_parse_nsref",  # any importable callable
    nsref="cache:parse_nsref",
    min_ttl=0.5,   # days — fresh for 12 hours
    max_ttl=2.0,   # days — force refresh after 2 days
    db_path=Path("cache.db"),
)

# First call hits the original function and caches the result
result = ns.cache.parse_nsref(nsref="market.data:fetch_prices")
print(result)  # (['market', 'data'], 'fetch_prices')

# Subsequent calls within min_ttl are served from cache
result = ns.cache.parse_nsref(nsref="market.data:fetch_prices")
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
