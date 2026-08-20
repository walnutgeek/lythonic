# Symmetric Matrix

Status: design sketch, not implemented. Captures the decisions from a design
session so they can be reviewed before any code is written.

Terms used here are defined in `CONTEXT.md`. Two decisions have their own
records: `docs/adr/0003-symmetric-storage-canonical-union.md` and
`docs/adr/0004-psd-is-a-query.md`.

## Purpose

A square, string-keyed matrix whose two axes are the same `Universe` and whose
value depends on an unordered pair of keys. It stores, persists, projects, and
converts to and from numpy. Arithmetic stays in numpy.

This is the type the `ExposureMatrix` sketch deferred as "correlation and
covariance matrices - square, symmetric, PSD-checked; a separate type". Two of
those three properties survived. The type is not correlation- or
covariance-specific, and it makes no claim about definiteness.

## Types

One module, `src/lythonic/symmetric.py`, matching `frame.py`, `universe.py`, and
`exposure.py` in naming. `Universe` is reused as-is.

### `SymmetricMatrix`

Immutable Pydantic model holding a `Universe` and one of two lower-triangle
storage variants, as a discriminated union:

```python
class DenseTriangle(BaseModel):
    values: list[float]          # length n(n+1)/2, cell (i, j) at i(i+1)/2 + j

class SparseTriangle(BaseModel):
    diagonal: list[float]        # length n, always dense
    pairs: list[tuple[int, int, float]]   # i > j, off-diagonal only

class SymmetricMatrix(BaseModel):
    universe: Universe
    storage: DenseTriangle | SparseTriangle
```

Read API:

```python
m.value(a, b)                 # -> float, argument order irrelevant
m.diagonal()                  # -> dict[str, float], one entry per key
m.values_of(key)              # -> dict[str, float], one entry per key
m.pairs()                     # -> every pair
m.cast(universe)              # -> SymmetricMatrix, subset and reorder only
m.to_builder()                # -> SymmetricMatrixBuilder, universe frozen
m.np.matrix()                 # -> ndarray, full n x n, symmetry materialized
m.np.diagonal()               # -> ndarray
m.np.vector(key)              # -> ndarray aligned to the universe
m.np.is_psd(tol=None)         # -> bool
m.np.min_eigenvalue()         # -> float
m.np.eigenvalues()            # -> ndarray, ascending

SymmetricMatrix.np.from_matrix(arr, universe)
```

Unknown keys raise `KeyError`. An absent off-diagonal cell reads as `0.0`.

### `SymmetricMatrixBuilder`

The only mutable object.

```python
b = SymmetricMatrixBuilder(universe=None)   # open, grows by first mention
b.freeze()
b.thaw()

b.set_value(a, b, value)      # one pair; (a, b) and (b, a) are the same cell
b.set_diagonal(mapping)       # bulk diagonals
b.np.set_matrix(arr)          # lower triangle only, requires a frozen universe
b.value(a, b)                 # minimal reads
b.diagonal()
b.build()                     # raises if any key has no diagonal
```

## Decisions

**A storage and interchange type, not a computational one.** The type's job is
keyed storage, persistence, projection, and conversion, exactly as
`ExposureMatrix`'s is. This is what excludes matrix arithmetic, factorizations,
and PSD repair, and it is why the numpy facade carries queries rather than
operations.

**One generic type, not correlation and covariance types.** Correlation and
covariance differ in their invariants - the diagonal is exactly 1.0 versus
arbitrary non-negative variances, and off-diagonals are bounded versus
unbounded - so a single type with a `kind` field would make every validator
conditional and reintroduce the mode-in-the-persisted-type problem ADR 0001
removed. Two types sharing a base would enforce those invariants properly, at
the cost of an inheritance layer and a union in every signature. Neither was
taken: the type carries no domain semantics at all, and correlation and
covariance are *usages* of it. The consequence accepted knowingly is that the
type cannot catch a correlation matrix with a diagonal that is not 1.0, or a
covariance matrix with a negative variance.

**Symmetry is structural, not validated.** Only a lower triangle is stored, so
there is no slot for an asymmetric value. This removes an entire question the
design would otherwise have to answer - reject on exact inequality, which fails
on floating-point round trips through an estimator, or accept within a
tolerance, and then silently pick a winner between the two halves.

**Storage is a canonical two-variant union.** See ADR 0003. The variant is
chosen by `k < n(n-1)/6`, the exact byte-count crossover, and the caller cannot
override it.

**Representation is not observable.** `values_of` returns an entry for every key
and `pairs()` yields every pair, materializing `0.0` for absent cells, so
nothing in the interface depends on which variant is stored. No `nnz`, no
`is_sparse`, no "stored entries only" accessor. Sparse storage therefore saves
bytes but not iteration cost.

**An absent off-diagonal cell means `0.0`, fixed.** There is no configurable
`cell_fill`. A knob would break the equality guarantee: the dense variant has no
absent cells, so two dense matrices with identical content and different fills
would compare unequal while denoting the same thing. Zero is also the genuine
neutral for every use this type has - zero correlation, zero covariance, no
edge. Stored zeros normalize away, and non-finite values are rejected, following
the same reasoning as the exposure matrix's NaN rule.

**PSD is a query.** See ADR 0004.

**The diagonal is always dense and always explicit.** Every key needs a
diagonal value, and `build()` raises naming any key that has none. A
`default_diagonal` policy on the builder was considered and rejected in favor of
the strict rule, because the failure it prevents is a silent one: a correlation
matrix built with a zero diagonal is not merely wrong but not PSD, so
`is_psd()` would report a data problem that is actually a defaulting problem.
`set_diagonal(mapping)` makes the strict rule a one-liner for the correlation
case.

**No row-wise writes.** Under symmetry the cells in one key's row *are* cells in
every other key's row, so a row-replace of the kind `ExposureMatrixBuilder`
offers would silently delete entries a caller thinks of as belonging elsewhere.
Merge semantics would avoid the deletes but leave two builders in one library
using similar names for opposite behavior, which is worse than either alone.
Bulk ingest goes through `set_diagonal` and `b.np.set_matrix`.

**`b.np.set_matrix` reads the lower triangle and ignores the upper.** Rejecting
on asymmetry would reintroduce the tolerance question the triangle layout was
chosen to avoid, and symmetrizing as `(A + A') / 2` silently absorbs a
transposed-block bug. Reading one triangle is at least predictable, and it
follows from the type's premise that the upper triangle does not exist.

**The universe grows by first mention; declaring it freezes it.** Mirrors
`ExposureMatrixBuilder`, down to `freeze()` / `thaw()` and the rule that
freezing never enables reordering. That rule matters more here: an open axis
appends, which only adds rows to the bottom of the triangle, while a permutation
would invalidate every stored offset rather than merely relabel it.

**`cast` is subset-and-reorder only.** Dropping keys is a silent projection, as
in the sibling. Introducing a key raises, because the type cannot invent a
diagonal for it - the same reasoning that made diagonals explicit. Growing a
matrix goes through `to_builder()`, which freezes the universe, so thawing is
deliberate.

`numpy` is already in `[project.optional-dependencies]`, added for the exposure
matrix, so no packaging change is needed.

## Testing

Per the repo's tiers: doctests on `value`, `diagonal`, and the simple reads,
where an example doubles as documentation. A separate test file for builder
semantics, the missing-diagonal error, `cast`, JSON round-tripping of both
variants, and validator behavior on non-canonical input.

The highest-value test is property-shaped and covers the storage union: build
the same content in ways that reach both variants, and assert the built matrices
are equal *and* serialize to identical bytes. Everything the union claims rests
on that one property. The build-time defensive copy needs the same explicit test
the exposure matrix has - mutate the builder after `build()` and assert the
built matrix is unchanged.

## Open questions

- **`from_matrix` transposition.** The mis-association hazard noted for
  `ExposureMatrix` is unavoidable here, because every input is square. Reading
  the lower triangle makes the outcome deterministic rather than safe: a caller
  who passes an upper-triangular array gets a silently wrong matrix rather than
  an error. A check that rejects an array whose upper triangle is non-zero and
  disagrees with its lower would catch it, at the cost of a comparison tolerance
  the design otherwise avoids entirely. See `docs/open-questions.md`.

## Non-goals

- Correlation- or covariance-specific invariants.
- PSD as a construction-time invariant, or any repair of a non-PSD matrix.
- Factorizations, eigenvectors, and matrix arithmetic on the type itself.
- Non-string keys.
- Mutation of a built `SymmetricMatrix`.
