# Callable DAG

Make `Dag` directly callable so DAGs can run without a `DagRunner` or
namespace registration.

## `Dag.__call__`

`Dag` gets an async `__call__(**kwargs) -> DagRunResult` method:

1. Creates `DagRunner(self, db_path=None)` (NullProvenance).
2. Builds `source_inputs` from kwargs -- for each source node, matches
   kwargs by parameter name (same logic as `dag_wrapper` in `_register_dag`).
3. Calls `await runner.run(source_inputs=source_inputs, dag_nsref=None)`.
4. Returns the `DagRunResult`.

Callers write `result = await dag(text="hello")` and access
`result.outputs["sink_label"]`.

## No `_register_dag` refactor

The `dag_wrapper` closure in `_register_dag` has overlapping logic but also
handles `dag_nsref` and a persistent `DagRunner` instance. Leave it as-is
for now -- the duplication is minor and can be DRYed up later.

## Test Coverage

- Basic callable: `await dag()` on a no-input DAG, verify `DagRunResult`.
- Callable with kwargs: `await dag(text="hello")` with source params.
- Uses NullProvenance: no DB file or provenance side effects.
- Word count end-to-end: `await wc.main_dag()` using the existing example.
- Error propagation: node failure produces `DagRunResult.status == "failed"`.

## Files Changed

- `src/lythonic/compose/namespace.py` -- add `Dag.__call__`.
- `tests/test_namespace.py` -- callable DAG tests.
- `tests/test_word_count.py` -- add callable-style companion test.
