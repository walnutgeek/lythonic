# How to Convert To and From pandas, polars, numpy and pyarrow

Conversions live behind a **facade**: one attribute per library, named for that
library's conventional import alias. The rule that makes it work is the same
everywhere.

## The rule: class in, instance out

**Class access gives you constructors. Instance access gives you conversions.**

```python
import pandas as pd
from lythonic.frame import FrameData

fd = FrameData.pd.from_frame(pd.DataFrame({"a": [1, 2]}))   # class -> inbound
df = fd.pd.frame()                                          # instance -> outbound
```

`pd` is one attribute behaving two ways, so both halves of a round trip share a
spelling. Misuse is a type error, not a runtime surprise: `FrameData.pd.frame()`
and `fd.pd.from_frame(...)` are both unreachable.

## What each type carries

| Type | `np` | `pd` | `pl` | `pa` |
|---|---|---|---|---|
| `FrameData` | | ✔︎ | ✔︎ | ✔︎ |
| `ExposureMatrix` | ✔︎ | | | |
| `SymmetricMatrix` | ✔︎ | | | |
| `KeyedVector` | ✔︎ | ✔︎ | | |

`KeyedVector` has `pd` because a pandas `Series` is exactly an index plus
values. Polars and pyarrow have no index concept, so a keyed vector there would
have to become an invented two-column convention - route through `FrameData`
instead if you need those.

## Install what you use

All four libraries are optional extras:

```bash
uv add 'lythonic[numpy]'    # or [pandas], [polars], [pyarrow]
```

Nothing imports them until you touch a facade, so constructing, reading,
casting and JSON round-tripping all work with none of them installed. Reaching
for a missing one names the extra:

```text
ImportError: numpy is required for this conversion; install the `lythonic[numpy]` extra
```

## Tabular data

```python
import pandas as pd
from lythonic.frame import FrameData

fd = FrameData.pd.from_frame(pd.DataFrame({"a": [1, 2], "b": [3, 4]}))
fd.columns                       # ['a', 'b']
fd.pd.frame()                    # back to a DataFrame
```

Polars and pyarrow follow the same shape - `FrameData.pl.from_frame(df)` /
`fd.pl.frame()`, and `FrameData.pa.from_table(t)` / `fd.pa.table()` - which
makes `FrameData` the bridge between the three libraries.

## Vectors

```python
import numpy as np
from lythonic.vector import KeyedVector

v = KeyedVector.from_mapping({"USD": 0.4, "EUR": 0.6})

v.np.array()                     # array([0.4, 0.6])
v.pd.series()                    # Series indexed by ['USD', 'EUR']

KeyedVector.np.from_array(np.array([0.4, 0.6]), ["USD", "EUR"])
KeyedVector.pd.from_series(v.pd.series())
```

`from_series` validates what a pandas index does not guarantee: it rejects a
duplicated index, rejects a non-string index, and preserves index order as the
universe order.

## Exposure matrices

Arrays are dense `float64`, subjects as rows and targets as columns, with
absent cells materialized as the cell fill:

```python
from lythonic.exposure import ExposureMatrixBuilder

b = ExposureMatrixBuilder()
b.set_exposures("acct1", {"USD": 0.4, "EUR": 0.6})
b.set_exposures("acct2", {"EUR": 0.9})
m = b.build()

m.np.matrix()                    # 2x2, subjects x targets
m.np.row("acct1")                # aligned to the target universe
m.np.col("EUR")                  # aligned to the subject universe
```

Coming back the other way, values equal to the cell fill are dropped, which is
how a dense product becomes sparse again:

```python
from lythonic.exposure import ExposureMatrix

ExposureMatrix.np.from_matrix(m.np.matrix(), m.subjects, m.targets) == m   # True
```

## Symmetric matrices

Outbound, symmetry is materialized into a full square array:

```python
from lythonic.symmetric import SymmetricMatrix, SymmetricMatrixBuilder

b = SymmetricMatrixBuilder()
b.set_diagonal({"a": 1.0, "b": 1.0})
b.set_value("a", "b", 0.5)
m = b.build()

m.np.matrix()                    # [[1. , 0.5], [0.5, 1. ]]
m.np.diagonal()                  # array([1., 1.])
m.np.vector("a")                 # array([1. , 0.5])
```

Inbound, **only the lower triangle is read** and the upper half is discarded:

```python
import numpy as np

asymmetric = np.array([[1.0, 9.9], [0.5, 1.0]])
SymmetricMatrix.np.from_matrix(asymmetric, ["a", "b"]).value("a", "b")   # 0.5
```

!!! warning
    `from_matrix` cannot detect a transposed input, because every input is
    square. An upper-triangular array produces a silently wrong matrix rather
    than an error. When bringing a computed result home, prefer passing the
    universes from the matrix it came from.

Writing into a builder by position needs a frozen universe, since it addresses
keys by index:

```python
from lythonic.universe import Universe

b = SymmetricMatrixBuilder(universe=Universe(["a", "b"]))
b.np.set_matrix(np.array([[1.0, 0.0], [0.5, 1.0]]))
b.build().value("a", "b")        # 0.5
```

## Checking a matrix is usable

Positive semi-definiteness is a query, never enforced at construction:

```python
m.np.is_psd()                    # bool
m.np.min_eigenvalue()            # float
m.np.eigenvalues()               # ascending
```

The default tolerance is `n * eps * max(|lambda|)`, scaled to the matrix's own
magnitude. Some tolerance is mandatory - a mathematically valid sample
covariance routinely returns a smallest eigenvalue around `-1e-15` after a
floating-point round trip, so testing `>= 0` rejects good data. Pass `tol=` to
apply your own standard, or read `eigenvalues()` and decide yourself.

Nothing repairs a failing matrix: nearest-PSD projection, eigenvalue clipping
and shrinkage are estimation decisions that belong to you, and you have
`m.np.matrix()` and all of numpy.

## See also

- [Choose a Portable Data Type](choose-a-portable-type.md)
- [Compose Portable Data Types](compose-portable-types.md)
- Reference: [facade](../reference/facade.md)
