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

## FlatMap Behavior

When using `Dag.map()` with list inputs, sub-DAG results that are lists
are flattened into the parent result. Scalar results are appended as-is.
This makes it easy to have sub-DAGs that expand one input into multiple
outputs:

```python
def expand(item: str) -> list[str]:
    return [item, item.upper()]

expand_dag = Dag()
expand_dag.node(expand)

dag = Dag()
dag.node(split) >> dag.map(expand_dag, label="expand") >> dag.node(collect)
# If split returns ["a", "b"], the map output is ["a", "A", "b", "B"]
```

## Routing with SwitchNode

`dag.switch(branches, label)` routes data to one of several branch DAGs
based on a `LabelSwitch` value. The upstream node must return either:

- A dict with a `"__switch__"` key: `{"__switch__": "text", "content": "..."}`
- A tuple/list where the first element is the label: `("text", {"content": "..."})`

Branches can be DAGs, `@dag_factory` functions, or plain callables.
When passing a list, labels are auto-derived from names:

```python
def classify(doc: dict) -> tuple[str, dict]:
    kind = "text" if doc.get("type") == "text" else "image"
    return (kind, doc)

def handle_text(doc: dict) -> dict:
    return {**doc, "processed": True}

def handle_image(doc: dict) -> dict:
    return {**doc, "resized": True}

dag = Dag()
dag.node(classify) >> dag.switch([handle_text, handle_image], label="router")
```

With explicit labels via a dict:

```python
dag.node(classify) >> dag.switch(
    {"text": text_pipeline, "image": image_pipeline},
    label="router",
)
```

## MapSwitch: Map + Route

Combine `map` and `switch` to map over a collection and route each element
to a different branch based on its `LabelSwitch` value:

```python
def tag_items(items: list[dict]) -> list[tuple[str, dict]]:
    return [(item["type"], item) for item in items]

dag = Dag()
dag.node(tag_items) >> dag.map(
    dag.switch([handle_text, handle_image], label="route"),
    label="process_all",
) >> dag.node(merge)
```

Each element in the collection must provide a switch label (same protocol
as `SwitchNode`). FlatMap behavior applies to the results.
