# DAG example: word count with map-reduce
import re
from collections import Counter

import lythonic.state as ls
from lythonic.compose.namespace import Dag, Namespace


def get_text() -> str:
    return ls.__doc__ or ""


def split_text(text: str) -> list[str]:
    lines = text.splitlines(keepends=True)
    chunk_size = len(lines) // 3
    return [
        "".join(ll)
        for ll in (lines[:chunk_size], lines[chunk_size : 2 * chunk_size], lines[2 * chunk_size :])
    ]


def tokenize(text: str) -> list[str]:
    return re.split(r'[\s:(){}\[\]"\'\-\.,]+', text)


def count(words: list[str]) -> dict[str, int]:
    return dict(Counter(words))


def reduce(counts_to_merge: list[dict[str, int]]) -> dict[str, int]:
    cc: Counter[str] = Counter()
    for c in counts_to_merge:
        for w, n in c.items():
            cc[w] += n
    return dict(cc.most_common(10))


ns = Namespace()

# Sub-DAG: tokenize -> count (applied to each text chunk)
tc_dag = Dag()
tc_dag.node(tokenize) >> tc_dag.node(count)  # pyright: ignore[reportUnusedExpression]

# Main DAG: get_text -> split_text -> map(tc_dag) -> reduce
main_dag = Dag()
g = main_dag.node(get_text)
s = main_dag.node(split_text)
m = main_dag.map(tc_dag, label="chunks")
r = main_dag.node(reduce)
g >> s >> m >> r  # pyright: ignore[reportUnusedExpression]
