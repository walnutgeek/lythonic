# Event-Driven DAG Triggers

Trigger DAG executions from scheduled intervals, polled external sources,
or push events from external code.

## Trigger Types

Two types, unified by the pattern "event produces data, data triggers DAG":

- **Poll** — a check function called periodically. Covers both schedule-based
  triggers (built-in time check using `Interval`/`Frequency`) and external
  polling (user-provided function that returns data or `None`).
- **Push** — external code calls `trigger_manager.fire(name, payload)`.

## Trigger Definitions (Namespace Layer)

`TriggerDef` is a Pydantic model registered in the namespace via
`ns.register_trigger()`. Purely declarative -- loading a namespace with
triggers does not start anything.

```python
# Schedule-based poll
ns.register_trigger(
    name="daily_etl",
    dag_nsref="pipelines:etl",
    trigger_type="poll",
    interval=Interval.from_string("1D"),
)

# Custom poll with user-provided check function
ns.register_trigger(
    name="new_orders",
    dag_nsref="pipelines:process_orders",
    trigger_type="poll",
    poll_fn=check_for_new_orders,
    interval=Interval.from_string("5m"),
)

# Push trigger
ns.register_trigger(
    name="webhook_received",
    dag_nsref="pipelines:handle_webhook",
    trigger_type="push",
)
```

`TriggerDef` fields: `name`, `dag_nsref`, `trigger_type` (poll/push),
`interval` (for poll, uses `lythonic.periodic.Interval`), `poll_fn` gref
(for custom poll, optional).

## Trigger Activations (Database Layer)

`TriggerStore` manages activation state in a dedicated SQLite database
(separate from `DagProvenance`).

Tables:

- **`trigger_activations`**: `name` (unique), `dag_nsref`, `trigger_type`,
  `status` (active/paused/disabled), `last_run_at`, `next_run_at`,
  `last_run_id`, `created_at`, `config_json`.
- **`trigger_events`**: `event_id`, `trigger_name`, `fired_at`, `run_id`
  (links to DagProvenance), `payload_json`, `status`
  (completed/failed/pending).

Activating a trigger creates/updates a row from the namespace definition:

```python
store = TriggerStore(Path("triggers.db"))
manager = TriggerManager(namespace=ns, store=store, provenance=provenance)
manager.activate("daily_etl")
```

## TriggerManager

Runtime coordinator:

- **Constructor**: `TriggerManager(namespace, store, provenance)`.
- **`activate(name)`** -- looks up `TriggerDef` from namespace, creates/updates
  activation in store.
- **`deactivate(name)`** -- sets activation status to disabled.
- **`fire(name, payload={})`** -- for push triggers: records event in store,
  looks up DAG by nsref, runs it with payload mapped to source inputs, records
  `run_id` back to event. Works whether manager is started or not.
- **`start()`** -- begins background asyncio task that polls all active poll
  triggers on their intervals. For each due trigger: calls poll_fn (or time
  check for schedule triggers), fires DAG if data returned.
- **`stop()`** -- cancels background task, graceful shutdown.

## Test Coverage

- TriggerDef registration and retrieval from namespace.
- TriggerStore CRUD for activations and events.
- Push trigger: `fire()` runs DAG, records event with run_id.
- Poll trigger (schedule): interval-based, mock time, verify DAG fires.
- Poll trigger (custom fn): poll function returns data, DAG fires.
- Start/stop lifecycle: background task processes due triggers, stops cleanly.
- Fire without start: push triggers work without `start()`.
- Deactivate: disabled trigger does not fire.

## Files

- `src/lythonic/compose/trigger.py` -- `TriggerDef`, `TriggerStore`,
  `TriggerManager`.
- `src/lythonic/compose/namespace.py` -- `register_trigger()`,
  `get_trigger()` on `Namespace`.
- `tests/test_trigger.py` -- all trigger tests.
- `docs/reference/compose-trigger.md` -- mkdocstrings reference page.
- `mkdocs.yml` -- add reference page to nav.
