# Exposure Matrix

Status: design sketch, not implemented. Captures the decisions from a design
session so they can be reviewed before any code is written.

## Purpose

A sparse, keyed 2-D matrix of exposure values. Both axes are keyed by strings
rather than positions, and the storage is sparse because most cells are empty in
practice.

## Vocabulary

- **Subject** - the entity that *has* exposure (an account, a portfolio).
  The row axis.
- **Target** - the thing a subject is *exposed to* (a currency, a risk factor).
  The column axis.
- **Universe** - an ordered, duplicate-free sequence of keys for one axis.
- **Exposure** - the float value at a (subject, target) cell.
- **Cell fill** - the value a cell takes when no record exists for it.
- **Default row** - a target-to-value mapping applied to a subject that is
  otherwise unmodeled.

The two axes are semantically asymmetric, which is why they are not called
`x`/`y` or `row`/`col`. Homogeneous square matrices (correlation, covariance)
are explicitly out of scope; they are symmetric, square, and want PSD checks, so
they deserve their own type.

## Types

### `Universe`

Immutable, ordered, duplicate-free sequence of `str` keys, with an internal
key-to-index map. Accepts a plain `list[str]` anywhere a `Universe` is expected
and coerces on the way in, following the `GlobalRef` / `NsRef` convention.

### `ExposureMatrix`

Immutable Pydantic model:

```python
class ExposureMatrix(BaseModel):
    subjects: Universe
    targets: Universe
    cell_fill: float = 0.0
    records: list[Record]
```

`Record` holds a subject index, a target index, and a value. Indexes are an
internal storage detail and never appear in the interface.

Read API:

```python
m.exposure(subject, target)   # -> float, cell_fill when no record
m.exposures_of(subject)       # -> dict[str, float], stored entries only
m.exposures_to(target)        # -> dict[str, float], stored entries only
m.cast(subjects=..., targets=...)   # -> new ExposureMatrix
m.to_builder()                # -> ExposureMatrixBuilder
m.np.matrix()                 # -> ndarray, subjects x targets
m.np.row(subject)             # -> ndarray aligned to targets
m.np.col(target)              # -> ndarray aligned to subjects

ExposureMatrix.np.from_matrix(arr, subjects, targets, cell_fill=0.0)
```

The `np` attribute serves both directions: class access yields the inbound
facade, instance access the outbound one. There are no matrix or vector
operations on `ExposureMatrix` itself - transpose, matmul, and norms are done on
numpy arrays, and results come home through `from_matrix`.

Unknown keys raise `KeyError`. A known key with no record returns `cell_fill`,
so "not in the universe" and "no exposure" are distinguishable.

### `ExposureMatrixBuilder`

The only mutable object. Grows universes, accepts values, and produces an
`ExposureMatrix` via `build()`.

```python
b.set_exposure(subject, target, value)
b.set_exposures(subject, targets)      # dict, or None for the default row
b.np.set_exposures(subject, arr)       # requires a frozen target axis
b.np.set_matrix(arr)                   # requires both axes frozen
b.build()
```

`set_exposures` takes `targets` as a required argument with no default value.
`{}` sets a literally empty row; `None` applies the configured default row.

## Decisions

**Subject / target naming.** The asymmetry buys readable method names
(`exposures_of` vs `exposures_to`) that a symmetric pair like `xs`/`ys` cannot.
It also survives the type being used outside finance, which `holders`/`factors`
would not.

**`Universe` is a real type, not a `list[str]`.** Uniqueness and the key-to-index
map are invariants of the axis, not of the matrix, and `cast` needs cheap
"same universe?" and "same universe, different order?" answers.

**Keys are `str` only.** Keeps the model JSON-serializable with no custom
encoders, matching the `FrameData` bet. A generic `Universe[K]` would propagate
two type parameters through every signature for a case that has not come up.
Composite keys encode as `"US:AAPL"`, with the encoding owned by the caller.

**Immutable values, mutable builder.** Earlier drafts put a per-axis lock on the
matrix so universes could grow until frozen. That put a state machine in the
persisted type. Moving all growth into a transient builder removes it: nothing
that round-trips through JSON has a mode.

**Universes grow by first mention; order is first-mention order.** An axis with
no declared universe appends unknown keys as they arrive. Ordering, once
established, never permutes.

**Per-axis freezing on the builder.** The common case is asymmetric: targets are
a curated list where an unknown key is a typo, while subjects are discovered
from data. A single lock would force both into one policy. A builder seeded with
a declared universe for an axis rejects unknown keys on that axis by default;
pass an explicit flag to declare ordering without closing the axis.

**Freezing never enables reordering.** A frozen axis rejects new keys; an open
axis appends at the end. Neither permutes existing positions, so array-shaped
setters against a frozen axis are safe.

**Cell fill lives on the matrix and the builder; the default row lives only on
the builder.** The fill is a fact about how to read absent cells and must
survive serialization. The default row is a policy about how to treat unknown
subjects, belongs to the calling context, and dies at `build()`.

**Default rows are applied eagerly.** `set_exposures(subject, None)` writes the
default row's records immediately. The alternative - sweeping unmodeled subjects
at `build()` - cannot distinguish "explicitly modeled as empty" from "never
mentioned" without carrying a side-set of known-empty subjects to the end, and
it defers errors (a default row naming a key outside a frozen target universe)
from the offending call to `build()`.

`None` with no configured default row raises. A default row is validated against
the target axis when it is set, not when it is used.

**Fill-valued writes are normalized away.** Writing a value equal to `cell_fill`
removes any record for that cell, so storage is canonical and byte-level
equality tracks semantic equality. This is only safe because defaults are eager:
nothing downstream needs to recover "was this subject modeled?" from record
presence.

**Universe membership is tracked separately from records.** A subject enters the
universe by being mentioned, not by having a record, so
`set_exposures(subject, {})` and a row that normalizes away entirely both leave
the subject in the universe.

**`cast` is a silent projection.** Keys omitted from the new universe are
dropped along with their exposures; keys added get the default row, or
`cell_fill` when none is configured. Raising by default would make the primary
use case require a flag, which trains callers to pass it everywhere.

The cost is accepted knowingly: casting as a *validation* step looks identical
to casting as a *subset* step, so an unexpected exposure vanishes instead of
raising. If that flow appears, add a separate strict operation rather than a
flag on `cast`.

**numpy behind a facade.** `m.np.*` and `b.np.*` keep the optional-dependency
surface in one place, give one lazy-import site, and leave the core class
readable as the numpy surface grows. Arrays are dense `float64`, subjects as
rows, targets as columns, absent cells materialized as `cell_fill`. Inbound
arrays normalize fill values away, which is how a dense product becomes sparse
again.

`numpy` needs adding to `[project.optional-dependencies]` in `pyproject.toml`,
which currently lists pandas, polars, and pyarrow only.

**Dict and array accessors both exist.** Dict accessors work with no optional
dependencies installed and return stored entries only. Array accessors live on
the facade and raise `ImportError` when numpy is absent.

**The facade carries inbound constructors too.** A descriptor whose `__get__` is
overloaded on `obj: None` vs `obj: ExposureMatrix` binds a different facade for
class and instance access, so `ExposureMatrix.np.from_matrix(...)` and
`m.np.matrix()` share one attribute. This was spiked before being adopted, since
the concern was that it would cost more in type-checker friction than the
symmetry is worth. It does not - see below.

**Matrix and vector operations live on numpy, not on `ExposureMatrix`.** The
type's job is keyed sparse storage and conversion. Transpose is the interesting
omission: it is mechanically trivial but produces a matrix whose subjects are
semantically targets, which the type has no way to express.

## Spike results

`devtools/proto_np_facade.py` measured the descriptor's cost under the project's
basedpyright settings. Verdict: no friction worth avoiding.

- Types are exact, not `Any`. `ExposureMatrix.np` reveals as `NpIn`, `m.np` as
  `NpOut`, `m.np.matrix()` as `ndarray[tuple[Any, ...], dtype[float64]]`.
- Misuse is caught in both directions. `ExposureMatrix.np.matrix()` and
  `m.np.from_matrix(...)` are both attribute-access errors, and argument types
  inside the facades check normally.
- Pydantic needs only `np: ClassVar[NpAccess] = NpAccess()`. The `ClassVar`
  annotation keeps it out of `model_fields`; JSON round-trip is unaffected.
- The `np` attribute does **not** shadow an `import numpy as np` module alias
  inside the model's own class body. Annotations like `-> NDArray[np.float64]`
  on the model's methods resolve to the module under both basedpyright and
  runtime, since annotations are deferred and evaluated in module globals.
- Total cost was two `reportUnannotatedClassAttribute` warnings, both fixed by
  annotating the facades' back-reference attributes. `make lint` passes clean.

## Open questions

- **`from_matrix` mis-association.** Shape checking catches a length mismatch but
  nothing catches a transposed square array. A `with_values(arr)` variant that
  reuses an existing matrix's universes would avoid the hazard for the common
  case of bringing a product home.

- **`FrameData` facade retrofit.** The same `pd`/`pl`/`pa` facade pattern could
  replace `to_pandas` / `from_pandas` and friends. Deliberately out of scope
  here. Note that it would break the six public methods shipped in v0.0.22
  (`src/lythonic/frame.py`).

## Non-goals

- Correlation and covariance matrices. Square, symmetric, PSD-checked; a
  separate type.
- Non-string keys.
- Mutation of a built `ExposureMatrix`.
