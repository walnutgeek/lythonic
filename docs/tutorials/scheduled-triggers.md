# Scheduled Triggers

This tutorial shows how to set up a DAG that runs automatically on a
schedule using lythonic's trigger system.

## Define the DAG

Start with a simple DAG that fetches and processes data:

```python
from lythonic.compose.namespace import Dag, Namespace

async def fetch_prices() -> dict[str, float]:
    """Simulate fetching latest prices."""
    return {"AAPL": 185.0, "GOOG": 140.0}

async def summarize(prices: dict[str, float]) -> str:
    total = sum(prices.values())
    return f"Portfolio value: ${total:.2f}"

ns = Namespace()
ns.register(fetch_prices, nsref="market:fetch")
ns.register(summarize, nsref="market:summarize")

dag = Dag()
dag.node(ns.get("market:fetch")) >> dag.node(ns.get("market:summarize"))

ns.register(dag, nsref="pipelines:market_summary")
```

The DAG must be registered in the namespace so the trigger system can
look it up by `nsref`.

## Register a Trigger Definition

A trigger definition is declarative metadata — it describes *what* should
happen but doesn't start anything:

```python
from lythonic.periodic import Interval

ns.register_trigger(
    name="hourly_market",
    dag_nsref="pipelines:market_summary",
    trigger_type="poll",
    interval=Interval.from_string("1h"),
)
```

The `interval` uses lythonic's `Interval` format: `"1h"` (hourly),
`"30m"` (every 30 minutes), `"1D"` (daily), `"1W"` (weekly).

## Activate and Run

Activating a trigger creates a persistent record in the trigger database.
The `TriggerManager` coordinates everything:

```python
from pathlib import Path
from lythonic.compose.dag_provenance import DagProvenance
from lythonic.compose.trigger import TriggerManager, TriggerStore

store = TriggerStore(Path("triggers.db"))
provenance = DagProvenance(Path("provenance.db"))

manager = TriggerManager(
    namespace=ns,
    store=store,
    provenance=provenance,
)

manager.activate("hourly_market")
```

At this point the trigger definition is stored in the database with
status `active`, but nothing is running yet.

## Start the Poll Loop

Call `start()` to begin the background polling loop. It checks all active
poll triggers every second and fires them when their interval elapses:

```python
import asyncio

async def main():
    manager.start()

    # Your application runs here...
    # The trigger fires the DAG every hour in the background.
    await asyncio.sleep(3600)

    manager.stop()

asyncio.run(main())
```

Each time the trigger fires, the trigger system:

1. Runs the DAG via the namespace node's async wrapper
2. Records the event in `trigger_events` (trigger name, payload, run ID)
3. Updates `last_run_at` and `last_run_id` in the activation record
4. The DAG run itself is recorded in the provenance database

## Check Trigger History

Query the trigger store for past events:

```python
events = store.get_events("hourly_market")
for event in events:
    print(f"Fired at: {event['fired_at']}, status: {event['status']}, run: {event['run_id']}")
```

## Deactivate

To stop a trigger from firing without stopping the manager:

```python
manager.deactivate("hourly_market")
```

The trigger stays in the database with status `disabled`. Reactivate it
later with `manager.activate("hourly_market")`.

## Without Provenance

If you don't need DAG run tracking, skip the provenance parameter.
The manager uses `NullProvenance` by default:

```python
manager = TriggerManager(namespace=ns, store=store)
```

## Custom Poll Functions

For triggers that check an external source instead of running on a fixed
schedule, provide a `poll_fn`. It's called on each interval — return data
to fire the DAG, or `None` to skip:

```python
def check_queue() -> dict[str, str] | None:
    """Check for new messages. Returns payload or None."""
    # ... check external source ...
    return {"message": "new order"} if has_messages else None

ns.register_trigger(
    name="queue_check",
    dag_nsref="pipelines:process_order",
    trigger_type="poll",
    poll_fn=check_queue,
    interval=Interval.from_string("10s"),
)
```

When `poll_fn` returns data, it becomes the DAG's input payload — the
dict keys are matched to the DAG's source node parameters by name.
