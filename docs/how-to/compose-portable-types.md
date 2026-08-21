# How to Compose Portable Data Types

Recipes for moving data between `FrameData`, `ExposureMatrix`,
`SymmetricMatrix` and `KeyedVector`. Each type is deliberately narrow; the
useful work happens where they meet.

## Turn a matrix row into a keyed vector

The matrix types return `dict[str, float]` from their row accessors. Those
dicts are ordered by universe, so building a vector from one is lossless:

```python
from lythonic.exposure import ExposureMatrixBuilder
from lythonic.vector import KeyedVector

b = ExposureMatrixBuilder()
b.set_exposures("acct1", {"USD": 0.4, "EUR": 0.6})
m = b.build()

v = KeyedVector.from_mapping(m.exposures_of("acct1"))
v.to_dict()                       # {'USD': 0.4, 'EUR': 0.6}
```

The same works for `SymmetricMatrix.diagonal()` and `values_of(key)`. Note the
difference: `ExposureMatrix.exposures_of` returns *stored* entries only, while
`SymmetricMatrix.values_of` returns an entry for every key in the universe.

## Align a vector to a matrix axis

`cast` onto the matrix's universe is the whole job, and returns the same object
when the vector is already aligned:

```python
weights = KeyedVector.from_mapping({"EUR": 0.7, "USD": 0.3, "JPY": 0.1})
aligned = weights.cast(list(m.targets))
list(aligned.universe) == list(m.targets)     # True
```

Keys outside the target universe are dropped; anything the matrix has that the
vector lacks arrives as NaN, so a gap propagates visibly instead of reading as
zero. Pass `fill=0.0` when zero is genuinely the right answer.

Once aligned, hand both to numpy:

```python
m.np.row("acct1") @ aligned.np.array()
```

## Build a covariance matrix from correlations and volatilities

A covariance matrix decomposes exactly into a correlation matrix and a vector
of standard deviations over the same universe, as `cov[i,j] = corr[i,j] * s_i *
s_j`. Lythonic has no dedicated covariance type - it is these two types
together:

```python
from lythonic.symmetric import SymmetricMatrixBuilder
from lythonic.universe import Universe
from lythonic.vector import KeyedVector

universe = Universe(["stocks", "bonds"])

b = SymmetricMatrixBuilder(universe=universe)
b.set_diagonal({"stocks": 1.0, "bonds": 1.0})
b.set_value("stocks", "bonds", -0.2)
corr = b.build()

vol = KeyedVector(universe=universe, values=[0.18, 0.05])


def to_covariance(corr, vol):
    b = SymmetricMatrixBuilder(universe=corr.universe)
    for a, other, value in corr.pairs():
        b.set_value(a, other, value * vol.value(a) * vol.value(other))
    return b.build()


cov = to_covariance(corr, vol)
cov.value("stocks", "stocks")     # 0.0324  == 0.18 ** 2
cov.value("stocks", "bonds")      # -0.0018
```

Going the other way, the volatilities are the square roots of the diagonal:

```python
from math import sqrt

vol_again = KeyedVector.from_mapping(
    {key: sqrt(value) for key, value in cov.diagonal().items()}
)
vol_again.to_dict()               # {'stocks': 0.18, 'bonds': 0.05}
```

`pairs()` yields every pair once with the earlier key first, including
self-pairs, so a loop over it covers the diagonal without a special case.

## Check the result is usable

Only after building a covariance matrix is definiteness worth asking about:

```python
cov.np.is_psd()                   # True
cov.np.min_eigenvalue()
```

A correlation matrix assembled from pairwise estimates is a common source of
matrices that are *not* positive semi-definite. The type will not stop you
building one - it makes no such promise - so check explicitly where it matters.

## Export a matrix as a table

`FrameData` is the route to polars and pyarrow, and the way to get a keyed
matrix into a spreadsheet:

```python
from lythonic.frame import FrameData

fd = FrameData(
    columns=["a", "b", "value"],
    data=[[x, y, value] for x, y, value in cov.pairs()],
)
fd.columns                        # ['a', 'b', 'value']
```

For an exposure matrix, iterate the subjects instead:

```python
rows = [
    [subject, target, value]
    for subject in m.subjects
    for target, value in m.exposures_of(subject).items()
]
FrameData(columns=["subject", "target", "exposure"], data=rows)
```

## Persist and reload

Everything round-trips through JSON with no optional dependency installed:

```python
from lythonic.symmetric import SymmetricMatrix

text = cov.model_dump_json()
SymmetricMatrix.model_validate_json(text) == cov      # True
```

Two properties worth relying on:

- **Byte equality tracks semantic equality.** The same content always
  serializes identically, whichever way it was built, so a hash over the
  serialized form is a sound change check.
- **Storage adapts silently.** A `SymmetricMatrix` picks a dense or sparse
  encoding by density. You cannot choose it and cannot observe it, which is
  what keeps the equality property true.

`KeyedVector` additionally writes non-finite values as `"NaN"`, `"Infinity"`
and `"-Infinity"` strings - valid JSON, lossless round trip, and equality
treats NaN as matching NaN so a reloaded vector equals the one you saved:

```python
from math import nan

v = KeyedVector.from_mapping({"a": 1.0, "b": nan})
KeyedVector.model_validate_json(v.model_dump_json()) == v   # True
```

## Amend a stored matrix

Built matrices are immutable. Round-trip through a builder, which arrives with
its universe frozen:

```python
b = cov.to_builder()
b.set_value("stocks", "bonds", -0.0015)
updated = b.build()
updated.value("stocks", "bonds")  # -0.0015
```

`build()` snapshots and leaves the builder usable, so you can checkpoint after
each merged source and the matrices you already handed out never change
underneath you.

## See also

- [Choose a Portable Data Type](choose-a-portable-type.md)
- [Convert To and From Libraries](library-conversions.md)
