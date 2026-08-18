# Exposure Matrix

Status: design sketch, not implemented. Captures the decisions from a design
session so they can be reviewed before any code is written.

Terms used here are defined in `CONTEXT.md`. Two decisions have their own
records: `docs/adr/0001-immutable-matrix-with-builder.md` and
`docs/adr/0002-exposure-records-as-index-triples.md`.

## Purpose

A sparse, keyed 2-D matrix of exposure values. Both axes are keyed by strings
rather than positions, and the storage is sparse because most cells are empty in
practice.

The two axes are semantically asymmetric, which is why they are not called
`x`/`y` or `row`/`col`. Homogeneous square matrices (correlation, covariance)
are explicitly out of scope; they are symmetric, square, and want PSD checks, so
they deserve their own type.

## Types

Two modules: `src/lythonic/universe.py` and `src/lythonic/exposure.py`, matching
`frame.py` in naming. `Universe` gets its own module because it is a reusable
type with its own test seam and may outlive `ExposureMatrix`.

### `Universe`

Immutable, ordered, duplicate-free sequence of `str` keys, with an internal
key-to-index map. Accepts a plain `list[str]` anywhere a `Universe` is expected
and coerces on the way in, following the `GlobalRef` / `NsRef` convention -
`no_info_plain_validator_function` with a plain serializer, as in
`src/lythonic/__init__.py`. Serializes transparently to a JSON array of strings.

Exposes `__len__`, `__iter__`, `__contains__`, `__getitem__(int) -> str`, and
`index(key) -> int`. Hashable, with equality over ordered keys, so "same
universe?" is a cached O(1) check. Construction with duplicate keys raises.

### `ExposureMatrix`

Immutable Pydantic model:

```python
class ExposureMatrix(BaseModel):
    subjects: Universe
    targets: Universe
    cell_fill: float = 0.0
    records: list[tuple[int, int, float]] = []
```

Read API:

```python
m.exposure(subject, target)   # -> float, cell_fill when no record
m.exposures_of(subject)       # -> dict[str, float], stored entries only
m.exposures_to(target)        # -> dict[str, float], stored entries only
m.cast(subjects=None, targets=None, default_row=None)   # -> ExposureMatrix
m.to_builder(default_row=None)                          # -> ExposureMatrixBuilder
m.np.matrix()                 # -> ndarray, subjects x targets
m.np.row(subject)             # -> ndarray aligned to targets
m.np.col(target)              # -> ndarray aligned to subjects

ExposureMatrix.np.from_matrix(arr, subjects, targets, cell_fill=0.0)
```

Unknown keys raise `KeyError`. A known key with no record returns `cell_fill`,
so "not in the universe" and "no exposure" are distinguishable. Equality
includes `cell_fill`, since two matrices with identical records but different
fills disagree about every absent cell.

### `ExposureMatrixBuilder`

The only mutable object.

```python
b = ExposureMatrixBuilder(
    subjects=None,              # open, grows by first mention
    targets=["USD", "EUR"],     # declared, therefore frozen
    cell_fill=0.0,
    default_row={"USD": 1.0},
)
b.thaw_targets()                # ordering pinned, axis now open
b.freeze_subjects()

b.set_exposure(subject, target, value)
b.set_exposures(subject, targets)      # dict replaces the row; None applies the default row
b.np.set_exposures(subject, arr)       # requires a frozen target axis
b.np.set_matrix(arr)                   # requires both axes frozen
b.exposure(subject, target)            # minimal reads
b.exposures_of(subject)
b.build()
```

## Decisions

**Subject / target naming.** The asymmetry buys readable method names
(`exposures_of` vs `exposures_to`) that a symmetric pair like `xs`/`ys` cannot.
It also survives the type being used outside finance, which `holders`/`factors`
would not. Exposure is a general statistics and risk-management idea, not a
finance-only one, so the type belongs in `lythonic`.

**`Universe` is a real type, not a `list[str]`.** Uniqueness and the key-to-index
map are invariants of the axis, not of the matrix, and `cast` needs cheap
"same universe?" and "same universe, different order?" answers.

**Keys are `str` only.** Keeps the model JSON-serializable with no custom
encoders, matching the `FrameData` bet. A generic `Universe[K]` would propagate
two type parameters through every signature for a case that has not come up.
Composite keys encode as `"US:AAPL"`, with the encoding owned by the caller.

**Domain-named, not generic.** No `SparseKeyedMatrix` base with `ExposureMatrix`
subclassing it. A generic base costs an inheritance layer and a permanent
name-translation table now, against a second use case that does not exist; if
one appears, extracting a base from a working concrete type is straightforward
and better-informed.

**Immutable values, mutable builder.** See ADR 0001.

**Universes grow by first mention; order is first-mention order.** An axis with
no declared universe appends unknown keys as they arrive. Ordering, once
established, never permutes.

**Per-axis freezing on the builder.** The common case is asymmetric: targets are
a curated list where an unknown key is a typo, while subjects are discovered
from data. A single flag would force both into one policy.

**Declaring a universe freezes that axis, with no opt-out parameter.** Pinning
an ordering while leaving an axis open is `ExposureMatrixBuilder(targets=[...])`
followed by `thaw_targets()`. The alternative, a `freeze_targets: bool | None`
whose `None` means "True if you passed a universe", is a tri-state nobody reads
correctly. `to_builder()` declares and freezes both axes, the safe default for
editing an existing matrix.

**Freezing never enables reordering.** A frozen axis rejects new keys; an open
axis appends at the end. Neither permutes existing positions, so array-shaped
setters against a frozen axis are safe. Thawing an axis after
`b.np.set_matrix(arr)` does not retroactively invalidate what was set.

**Cell fill lives on the matrix and the builder; the default row lives only on
the builder.** The fill is a fact about how to read absent cells and must
survive serialization. The default row is a policy about how to treat unknown
subjects, belongs to the calling context, and dies at `build()`.

**Cell fill may not be NaN.** NaN does not compare equal to itself, which breaks
both normalization (an explicit NaN record would never be recognized as
redundant) and equality (a matrix would compare unequal to itself). The
validator rejects non-finite fills, and `from_matrix` raises on an array
containing NaN rather than silently storing it. If "unknown" versus "zero" ever
needs modeling, it deserves an explicit mechanism rather than a float sentinel
the rest of the design must work around.

**Default rows are applied eagerly.** `set_exposures(subject, None)` writes the
default row's records immediately. The alternative - sweeping unmodeled subjects
at `build()` - cannot distinguish "explicitly modeled as empty" from "never
mentioned" without carrying a side-set of known-empty subjects to the end, and
it defers errors (a default row naming a key outside a frozen target universe)
from the offending call to `build()`.

`None` with no configured default row raises. A default row is validated against
the target axis when it is set, not when it is used.

**`set_exposures` replaces a row rather than merging into it.** Forced by the
`{}` decision: under merge semantics an empty dict would be a no-op, which would
destroy the "literally empty row" meaning. A merge would need a separate name;
none is provided, because the builder's read methods make
`b.set_exposures(s, {**b.exposures_of(s), **delta})` available.

**The builder exposes minimal reads.** `exposure`, `exposures_of`, and the
universes so far. Merging into a row by hand and conditional ingest ("only set
this if nothing is there yet") both need them, and neither is possible on a
write-only builder without building an intermediate matrix. Deliberately no
`exposures_to`: column reads mid-build are expensive against record storage and
no ingest pattern needs them.

**Fill-valued writes are normalized away.** Writing a value equal to `cell_fill`
removes any record for that cell, so storage is canonical and byte-level
equality tracks semantic equality. This is only safe because defaults are eager:
nothing downstream needs to recover "was this subject modeled?" from record
presence.

**Universe membership is tracked separately from records.** A subject enters the
universe by being mentioned, not by having a record, so
`set_exposures(subject, {})` and a row that normalizes away entirely both leave
the subject in the universe.

**`build()` snapshots; the builder stays usable.** Repeated builds produce
independent matrices, which supports checkpointing after each merged source. The
defensive copy this requires is a correctness requirement rather than an
optimization - see ADR 0001 - and is the thing to test hardest.

**`cast` is a silent projection.** Keys omitted from the new universe are
dropped along with their exposures; subjects added get the default row, or
`cell_fill` when none is configured. Raising by default would make the primary
use case require a flag, which trains callers to pass it everywhere. `None` for
an axis keeps that universe as-is, so one axis can be reordered without
restating the other. `cell_fill` is preserved.

The cost is accepted knowingly: casting as a *validation* step looks identical
to casting as a *subset* step, so an unexpected exposure vanishes instead of
raising. If that flow appears, add a separate strict operation rather than a
flag on `cast`.

**No default column.** A default row encodes "a subject I do not know about
looks typically like this" - a statement about the axis that arrives
unpredictably from data. Targets are the curated axis; they are declared, not
discovered. A default column would also collide with the default row at the
corner cell (new subject x new target), and the precedence rule needed to
resolve that is complexity bought for a case that does not appear to exist.

**Records serialize as index triples.** See ADR 0002.

**Validation is strict where ambiguity is real, lenient where it is not.** A
directly constructed or deserialized matrix silently canonicalizes what is
lossless to canonicalize - unsorted records, records whose value equals the cell
fill - and rejects out-of-range indexes and duplicate cells. Duplicate cells are
genuinely ambiguous, and last-wins would be a silent guess about producer
intent.

**numpy behind a facade, carrying both directions.** `m.np.*` and `b.np.*` keep
the optional-dependency surface in one place and give one lazy-import site. A
descriptor whose `__get__` is overloaded on `obj: None` vs `obj: ExposureMatrix`
binds a different facade for class and instance access, so
`ExposureMatrix.np.from_matrix(...)` and `m.np.matrix()` share one attribute.
Arrays are dense `float64`, subjects as rows, targets as columns, absent cells
materialized as `cell_fill`. Inbound arrays normalize fill values away, which is
how a dense product becomes sparse again.

`numpy` needs adding to `[project.optional-dependencies]` in `pyproject.toml`,
which currently lists pandas, polars, and pyarrow only.

**Dict and array accessors both exist.** Dict accessors work with no optional
dependencies installed and return stored entries only. Array accessors live on
the facade and raise `ImportError` when numpy is absent.

**Matrix and vector operations live on numpy, not on `ExposureMatrix`.** The
type's job is keyed sparse storage and conversion. Transpose is the interesting
omission: it is mechanically trivial but produces a matrix whose subjects are
semantically targets, which the type has no way to express.

## Spike results

`devtools/proto_np_facade.py` measured the descriptor's cost under the project's
basedpyright settings before the implementation existed. Verdict: no friction
worth avoiding. The spike has been deleted now that `lythonic.exposure` carries
the same shape; its findings are kept here.

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

## Testing

Per the repo's tiers: doctests on `Universe` and the simple accessors, where an
example doubles as documentation. A separate test file for builder semantics,
`cast`, JSON round-tripping, and validator behavior on non-canonical input. The
build-time defensive copy needs an explicit test - mutate the builder after
`build()` and assert the built matrix is unchanged.

## Open questions

- **`from_matrix` mis-association.** Shape checking catches a length mismatch but
  nothing catches a transposed square array. A `with_values(arr)` variant that
  reuses an existing matrix's universes would avoid the hazard for the common
  case of bringing a product home.

- ~~**`FrameData` facade retrofit.**~~ Resolved in v0.0.23: `FrameData` gained
  `pd`/`pl`/`pa` facades, the binding descriptor moved to `lythonic.facade`
  where both modules use it, and the six methods shipped in v0.0.22 were
  removed.

## Non-goals

- Correlation and covariance matrices. Square, symmetric, PSD-checked; a
  separate type.
- Non-string keys.
- Mutation of a built `ExposureMatrix`.
- Matrix arithmetic on the type itself.
