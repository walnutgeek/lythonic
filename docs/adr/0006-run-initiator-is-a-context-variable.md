# Run initiator is a context variable

A run records who started it in `dag_runs.initiated_by`, and a trigger records
who last changed it in `trigger_activations.changed_by` and `changed_at`. The
value comes from the `run_initiator` context variable, set with the
`initiated_by` context manager, not from a parameter. woodglue needs this to
show and audit runs on a server that runs DAGs for many users (#14).

## Considered options

A `initiated_by=` parameter on `DagRunner.run` was the obvious alternative and
was declined because runs are mostly not started by the code that knows the
caller. A server request reaches `TriggerManager.fire`, a namespace node's
wrapper, or a DAG node that maps a sub-DAG; each would need the parameter
threaded through, and every sub-DAG run would need it copied from its parent.
A context variable crosses those layers unchanged, and child runs inherit it
because asyncio tasks copy the context and `DagRunner` runs sync nodes on
executor threads with a copy of it.

The one place that loses the context is `run_in_executor` without a copy, which
is exactly where `DagRunner.run` calls `create_run`. It therefore reads the
variable before the executor hop and passes it explicitly. `create_run` still
falls back to the variable when called directly, so callers on the event loop
thread need not pass it; requiring the argument was declined as it would make
every direct caller repeat what the context already says.

Initiators are opaque strings. Lythonic reserves only `SCHEDULER`
(`"scheduler"`), which the poll loop sets for scheduled fires; the host owns
the rest of the format (woodglue uses user ids, `cli:<os user>` and
`anonymous`). `initiated_by(None)` deliberately clears an outer initiator.

## Consequences

`restart` continues the same run, so the run keeps its original initiator and
nobody is recorded as the restarter. `replay` creates a new run, attributed to
its caller. A `restarted_by` column can be added if auditing restarts matters.

`changed_by` is written when an initiator is set or when the call changes the
trigger's status. `TriggerManager.activate_configured`, which hosts call on
every start, sets no initiator and skips triggers stored as `disabled`, so it
never changes a status a person set and leaves the last deliberate change in
place. It moved into lythonic from woodglue's `activate_triggers` so that every
host gets the rule that a deactivation survives restarts. An explicit
`activate()` of a disabled trigger with no initiator records
`changed_by = NULL` with the new `changed_at`, rather than leaving the name of
whoever disabled it on a state they did not set.

Existing databases gain the columns through `lythonic.state.add_missing_column`
when `DagProvenance` or `TriggerStore` is constructed. That costs one
`PRAGMA table_info` per table per construction, not per poll, so a schema
version (`PRAGMA user_version`) was not worth introducing yet.

Not done, because nothing needs them yet: an initiator on `trigger_events`
(reachable through `run_id` for every run that started), and filtering
`get_recent_runs` by initiator.
