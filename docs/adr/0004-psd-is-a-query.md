# Positive semi-definiteness is a query, not an invariant

`SymmetricMatrix` enforces squareness and symmetry but promises nothing about
definiteness. Whether a matrix is positive semi-definite is answered on demand
through the numpy facade - `m.np.is_psd()`, `m.np.min_eigenvalue()`,
`m.np.eigenvalues()` - and never at construction or deserialization.

A reader arriving from the type's origin will expect otherwise: this type was
scoped as the home for correlation and covariance matrices, described as
"square, symmetric, PSD-checked". Two of those three are invariants; the third
is not, and the name deliberately does not claim it.

## Considered options

Enforcing PSD at construction was rejected on three counts.

It would make numpy mandatory. The check is an eigendecomposition, numpy is an
optional extra alongside pandas, polars, and pyarrow, and every other part of
this type - construction, reads, `cast`, JSON round-trip - works with no
optional dependency installed. A validator that imports numpy would make the
extra a hard requirement for even reading a stored matrix.

It would put an `O(n^3)` computation on the deserialization path, paid on every
load, for a property most callers never ask about.

It would be wrong for the type as scoped. The type carries no correlation or
covariance semantics, and plenty of legitimately symmetric matrices are not PSD:
distance matrices, signed adjacency matrices, differences of two covariances.
Enforcing PSD would make the type narrower than its name.

An opt-in `require_psd` field was rejected separately. Persisted, it reintroduces
the mode-in-the-serialized-type problem ADR 0001 removed; unpersisted, a matrix
validated on the way in is silently unvalidated on the way back.

## Consequences

The tolerance becomes the caller's concern, so it has to be visible. A
mathematically PSD sample covariance routinely returns a minimum eigenvalue
around `-1e-15` after a floating-point round trip, so a bare `>= 0` test rejects
valid data. `is_psd()` defaults to `n * eps * max(|lambda|)`, scaled to the
matrix's own magnitude and dimension, following the convention
`numpy.linalg.matrix_rank` uses for its singular-value cutoff. Any fixed default
is wrong for someone, which is why `eigenvalues()` is exposed alongside it.

The check uses `eigvalsh`, not `cholesky`. Cholesky tests positive *definite*,
and rejects a rank-deficient covariance - more variables than observations,
which is the normal case in finance - that is legitimately semi-definite. It
also gives no measure of how far from PSD a matrix is.

Nothing repairs a failing matrix. Nearest-PSD projection, eigenvalue clipping,
and shrinkage are estimation decisions with several defensible variants each;
they belong to the caller, who has `m.np.matrix()` and all of numpy. The same
reasoning excludes factorizations.

Adding the invariant later would be a breaking change, since matrices already
persisted would fail to load.
