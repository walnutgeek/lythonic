# How to Choose a Keyed Data Type

Lythonic has four types for data addressed by string keys. They look similar
from the outside; the difference is the *shape* of what you are holding.

| You have | Use | Keyed by |
|---|---|---|
| Rows and columns of mixed types | `FrameData` | Column names only |
| A value per (subject, target) pair, mostly empty | `ExposureMatrix` | Two different universes |
| A value per unordered pair over one key set | `SymmetricMatrix` | One universe, both axes |
| One value per key | `KeyedVector` | One universe |

Two questions settle almost every case:

- **Is it numeric and keyed on both sides?** If not, you want `FrameData` - it
  is the only one of the four that holds arbitrary column types and the only
  one with polars and pyarrow conversions.
- **Are the two axes the same key set?** If yes, `SymmetricMatrix`; if no,
  `ExposureMatrix`. This is not a style choice: `SymmetricMatrix` stores one
  triangle, so an asymmetric value has nowhere to go.

## Universes

The three numeric types name their axes with a `Universe`: an ordered,
duplicate-free sequence of string keys.

```python
from lythonic.universe import Universe

u = Universe(["USD", "EUR", "JPY"])
len(u), u[0], u.index("EUR"), "GBP" in u
```

Order is part of a universe's identity. Two universes with the same keys in a
different order are different universes, which is what makes "are these
aligned?" a cheap check rather than a set comparison.

```python
Universe(["USD", "EUR"]) == Universe(["EUR", "USD"])   # False
```

Universes are immutable and hashable. Duplicate keys raise at construction.

## Growing an axis

Every type that grows does so through a builder, never in place. Axes grow by
*first mention* and only ever append - nothing reorders an existing axis, so
positions already written stay valid.

```python
from lythonic.symmetric import SymmetricMatrixBuilder

b = SymmetricMatrixBuilder()          # open: grows as keys are mentioned
b.set_diagonal({"a": 1.0, "b": 1.0})
b.set_value("a", "b", 0.5)
list(b.build().universe)              # ['a', 'b']
```

Passing a universe to the builder declares it *and freezes it*, which is how a
curated key set catches typos:

```python
from lythonic.universe import Universe

b = SymmetricMatrixBuilder(universe=Universe(["a", "b"]))
b.set_diagonal({"a": 1.0, "b": 1.0})
try:
    b.set_value("a", "typo", 0.5)
except KeyError as e:
    print(e)                          # "'typo' not in frozen universe"
```

Use `freeze()` and `thaw()` to change that mid-build. `ExposureMatrixBuilder`
freezes each axis independently (`freeze_subjects`, `thaw_targets`, ...), since
the usual case is a curated target list against subjects discovered from data.

## Aligning to a different universe

All three numeric types have `cast`, and all three *drop* keys outside the new
universe silently. They differ in what happens to a key the cast introduces,
and the difference is not arbitrary:

```python
from lythonic.vector import KeyedVector

v = KeyedVector.from_mapping({"a": 1.0, "b": 2.0})

v.cast(["a"]).to_dict()                    # {'a': 1.0}      - narrowed
v.cast(["b", "a"]).to_dict()               # {'b': 2.0, 'a': 1.0} - reordered
v.cast(["a", "c"]).to_dict()               # {'a': 1.0, 'c': nan} - extended
v.cast(["a", "c"], fill=0.0).to_dict()     # {'a': 1.0, 'c': 0.0}
```

`KeyedVector` extends with NaN because NaN is the one fill that cannot pass for
a measurement - any arithmetic touching it yields NaN. `SymmetricMatrix`
refuses to extend at all, because it cannot invent a diagonal value:

```python
from lythonic.symmetric import SymmetricMatrixBuilder

b = SymmetricMatrixBuilder()
b.set_diagonal({"a": 1.0, "b": 1.0})
m = b.build()

try:
    m.cast(["a", "c"])
except KeyError as e:
    print(e)          # cast would introduce ['c'] with no diagonal value
```

To grow a symmetric matrix, go through `to_builder()`, which arrives frozen -
thaw it deliberately:

```python
b = m.to_builder()
b.thaw()
b.set_diagonal({"c": 1.0})
list(b.build().universe)              # ['a', 'b', 'c']
```

`cast` returns the same object when the universe is already equal, so a
defensive alignment call costs nothing.

## What each type refuses to do

Knowing the deliberate gaps saves reaching for the wrong thing:

- **No arithmetic on any of them.** These are storage, alignment and conversion
  types. Add, scale, multiply and decompose in numpy - see
  [Convert To and From Libraries](library-conversions.md).
- **`SymmetricMatrix` makes no positive semi-definiteness promise.** It is a
  query on the numpy facade, not an invariant, so loading a stored matrix needs
  no numpy and does no eigendecomposition.
- **Only `KeyedVector` accepts NaN and infinities.** The matrix types reject
  non-finite values outright.
- **Keys are `str` everywhere.** Composite keys encode as `"US:AAPL"`, and the
  encoding belongs to you.

## See also

- [Convert To and From Libraries](library-conversions.md) - the facade pattern
- [Compose Keyed Types](compose-keyed-types.md) - moving data between them
- Reference: [universe](../reference/universe.md), [frame](../reference/frame.md),
  [exposure](../reference/exposure.md), [symmetric](../reference/symmetric.md),
  [vector](../reference/vector.md)
