# Callable DAG Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `Dag` directly callable via `await dag(**kwargs)` so DAGs can run without explicit `DagRunner` setup or namespace registration.

**Architecture:** `Dag.__call__(**kwargs)` builds `source_inputs` from kwargs (same pattern as `dag_wrapper` in `_register_dag`), creates a `DagRunner` with `NullProvenance`, runs the DAG, and returns `DagRunResult`.

**Tech Stack:** Python 3.11+, asyncio, pytest

---

## File Structure

| File | Responsibility |
|---|---|
| `src/lythonic/compose/namespace.py` | Add `Dag.__call__` method |
| `tests/test_namespace.py` | Callable DAG unit tests |
| `tests/test_word_count.py` | Add callable-style companion test |

---

### Task 1: Add `Dag.__call__` and tests

**Files:**
- Modify: `src/lythonic/compose/namespace.py:720-729` (before `__enter__`)
- Test: `tests/test_namespace.py`
- Test: `tests/test_word_count.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_namespace.py` after the `# Dag.map()` section, before `# Tags`:

```python
# Dag.__call__


async def test_dag_callable_no_inputs():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()

    def constant() -> str:
        return "hello"

    ns.register(constant, nsref="t:constant")

    dag = Dag()
    dag.node(ns.get("t:constant"))

    result = await dag()
    assert result.status == "completed"
    assert result.outputs["constant"] == "hello"


async def test_dag_callable_with_kwargs():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="t:fetch")  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="t:process")  # pyright: ignore[reportPrivateUsage]

    dag = Dag()
    f = dag.node(ns.get("t:fetch"))
    p = dag.node(ns.get("t:process"))
    f >> p  # pyright: ignore[reportUnusedExpression]

    result = await dag(ticker="AAPL")
    assert result.status == "completed"
    assert result.outputs["_another_fn"] == "AAPL"


async def test_dag_callable_error_propagation():
    from lythonic.compose.namespace import Dag, Namespace

    ns = Namespace()

    def boom() -> str:
        raise RuntimeError("intentional failure")

    ns.register(boom, nsref="t:boom")

    dag = Dag()
    dag.node(ns.get("t:boom"))

    result = await dag()
    assert result.status == "failed"
    assert "intentional failure" in (result.error or "")
```

Also add to `tests/test_word_count.py`:

```python
async def test_callable():
    import lythonic.examples.word_count as wc

    drr = await wc.main_dag()
    results = drr.outputs["reduce"]
    assert "conn" in results and "python" in results and "author" in results
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py::test_dag_callable_no_inputs tests/test_namespace.py::test_dag_callable_with_kwargs tests/test_namespace.py::test_dag_callable_error_propagation tests/test_word_count.py::test_callable -v`
Expected: FAIL — `Dag` has no `__call__` method.

- [ ] **Step 3: Implement `Dag.__call__`**

In `src/lythonic/compose/namespace.py`, add this method to the `Dag` class before `__enter__` (around line 722):

```python
    async def __call__(self, **kwargs: Any) -> DagRunResult:
        """
        Run the DAG directly with NullProvenance. Kwargs are matched
        to source node parameters by name.
        """
        from lythonic.compose.dag_runner import DagRunner, DagRunResult  # pyright: ignore[reportImportCycles]

        source_inputs: dict[str, dict[str, Any]] = {}
        for node in self.sources():
            if node.label in kwargs and isinstance(kwargs[node.label], dict):
                source_inputs[node.label] = kwargs[node.label]
                continue
            node_args = node.ns_node.method.args
            if node.ns_node.expects_dag_context():
                node_args = node_args[1:]
            node_kwargs = {a.name: kwargs[a.name] for a in node_args if a.name in kwargs}
            if node_kwargs:
                source_inputs[node.label] = node_kwargs

        runner = DagRunner(self, db_path=None)
        return await runner.run(source_inputs=source_inputs)
```

Note: `DagRunResult` needs to be imported for the return type annotation. Import it inside the method to avoid circular imports (same pattern as the existing `DagRunner` import in `_register_dag`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace.py -k "dag_callable" tests/test_word_count.py::test_callable -v`
Expected: All PASS.

- [ ] **Step 5: Run full test suite and lint**

Run: `make lint && make test`
Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace.py tests/test_word_count.py
git commit -m "feat: make Dag callable with async __call__"
```
