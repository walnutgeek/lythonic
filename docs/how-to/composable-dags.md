# Composable DAGs

This guide shows how to compose DAGs from sub-DAGs using `MapNode`,
`CallNode`, and callable DAGs.

## Map-Reduce with Dag.map()

`Dag.map(sub_dag, label)` runs a sub-DAG on each element of an upstream
`list` or `dict`, concurrently via `asyncio.gather`. The sub-DAG must
have exactly one source and one sink.

This example splits text into chunks, runs tokenize + count on each chunk
concurrently, then reduces the results:

```python
import re
from collections import Counter
from lythonic.compose.namespace import Dag

def split_text(text: str) -> list[str]:
    lines = text.splitlines(keepends=True)
    chunk_size = max(1, len(lines) // 3)
    return [
        "".join(ll)
        for ll in (lines[:chunk_size], lines[chunk_size:2*chunk_size], lines[2*chunk_size:])
    ]

def tokenize(text: str) -> list[str]:
    return re.split(r'[\s:(){}\[\]"\'\-\.,|#`=]+', text.lower())

def count(words: list[str]) -> dict[str, int]:
    return dict(Counter(words))

def reduce(counts_to_merge: list[dict[str, int]]) -> dict[str, int]:
    cc: Counter[str] = Counter()
    for c in counts_to_merge:
        for w, n in c.items():
            cc[w] += n
    return dict(cc.most_common(10))

# Sub-DAG: tokenize -> count (applied to each chunk)
tc_dag = Dag()
tc_dag.node(tokenize) >> tc_dag.node(count)

# Main DAG: split -> map(tc_dag) -> reduce
main_dag = Dag()
(
    main_dag.node(split_text)
    >> main_dag.map(tc_dag, label="chunks")
    >> main_dag.node(reduce)
)
```

## Inline Sub-DAGs with CallNode

`dag.node(sub_dag, label="enrich")` creates a `CallNode` that runs a
sub-DAG once as a single pipeline step. The upstream output is passed
to the sub-DAG's single source node:

```python
enrich_dag = Dag()
enrich_dag.node(lookup) >> enrich_dag.node(annotate)

parent = Dag()
parent.node(fetch) >> parent.node(enrich_dag, label="enrich") >> parent.node(save)
```

Like `MapNode`, the sub-DAG must have exactly one source and one sink.

## Running DAGs Directly

A `Dag` is callable — `await dag(**kwargs)` creates a `DagRunner` with
`NullProvenance` and returns a `DagRunResult`. Kwargs are matched to
source node parameters by name:

```python
import asyncio

result = asyncio.run(main_dag(text="hello world\nfoo bar\nbaz qux"))
print(result.status)   # "completed"
print(result.outputs)  # {"reduce": {"hello": 1, "world": 1, ...}}
```

For production use with provenance, create a `DagRunner` explicitly:

```python
from pathlib import Path
from lythonic.compose.dag_runner import DagRunner
from lythonic.compose.dag_provenance import DagProvenance

runner = DagRunner(main_dag, provenance=DagProvenance(Path("runs.db")))
result = asyncio.run(runner.run(source_inputs={"split_text": {"text": "hello world"}}))
```
