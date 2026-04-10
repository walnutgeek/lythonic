# Getting Started with lyth

This tutorial walks through setting up a lythonic project from scratch:
writing a pipeline, configuring it in `lyth.yaml`, and running it with
the `lyth` CLI.

## Install

```bash
uv add lythonic
```

## Write Your Logic

Create a module with your pipeline functions. Sync or async — doesn't
matter:

```python
# myproject/pipeline.py
import logging

_log = logging.getLogger(__name__)

async def fetch_data(source: str = "default") -> dict:
    """Simulate fetching data from a source."""
    _log.info(f"Fetching from {source}")
    return {"source": source, "items": [1, 2, 3, 4, 5]}

def process(data: dict) -> dict:
    """Double every item. Sync function — runs in thread executor."""
    return {**data, "items": [x * 2 for x in data["items"]]}

async def summarize(data: dict) -> str:
    """Produce a summary string."""
    total = sum(data["items"])
    _log.info(f"Summary: {len(data['items'])} items, total={total}")
    return f"{data['source']}: {total}"
```

## Define the DAG

Use `@dag_factory` to define a reusable pipeline:

```python
# myproject/pipeline.py (continued)
from lythonic.compose.namespace import Dag, dag_factory

@dag_factory
def my_pipeline() -> Dag:
    dag = Dag()
    f = dag.node(fetch_data)
    p = dag.node(process)
    s = dag.node(summarize)
    f >> p >> s
    return dag
```

## Test It

Run the pipeline directly from Python — no config needed:

```python
import asyncio
from myproject.pipeline import my_pipeline

dag = my_pipeline()
result = asyncio.run(dag(source="test"))
print(result.status)   # "completed"
print(result.outputs)  # {"summarize": "test: 30"}
```

## Configure lyth.yaml

Create a `data/` directory and a `lyth.yaml` config file:

```yaml
# data/lyth.yaml
namespace:
  - nsref: "pipeline:main"
    gref: "myproject.pipeline:my_pipeline__"
    triggers:
      - name: "every_minute"
        type: "poll"
        schedule: "* * * * *"
```

This tells the engine:

- Register the DAG factory `my_pipeline` under `pipeline:main`
  (the `__` suffix invokes the factory via GlobalRef convention)
- Attach a trigger that fires every minute

## Run the Engine

```bash
# Start the engine (foreground, Ctrl+C to stop)
lyth start

# Output:
# Logging to data/lyth.log
# Starting lyth engine (pid=12345, data=data)
#   Activated trigger: every_minute (poll)
# Poll loop started. Press Ctrl+C to stop.
```

The engine:

1. Reads `data/lyth.yaml`
2. Creates SQLite databases in `data/` (dags.db, triggers.db, cache.db)
3. Resolves grefs and registers callables
4. Activates triggers and starts the poll loop
5. Logs to `data/lyth.log` with run context (run_id, node_label)

## Other Commands

```bash
# Run a specific DAG once
lyth run pipeline:main

# Fire a trigger manually
lyth fire every_minute

# Check status
lyth status

# Stop a running engine
lyth stop
```

## Check the Log

The log file includes DAG execution context:

```
2026-04-10 12:00:00,123 INFO     [myproject.pipeline] run=abc123 node=fetch_data Fetching from default
2026-04-10 12:00:00,456 INFO     [myproject.pipeline] run=abc123 node=summarize Summary: 5 items, total=30
```

## What's Next

- [Build a Pipeline](compose-pipeline.md) — deeper dive into DAG
  construction, fan-out/fan-in, provenance
- [Composable DAGs](../how-to/composable-dags.md) — nest DAGs with
  MapNode and CallNode
- [Scheduled Triggers](scheduled-triggers.md) — cron expressions,
  custom poll functions, push triggers
