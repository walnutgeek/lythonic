# Symmetric matrix storage is a canonical two-variant union

`SymmetricMatrix` persists its values in one of two lower-triangle encodings - a
dense flat list, or a dense diagonal plus sparse off-diagonal triples - as a
tagged union in the serialized model. The variant is chosen by a fixed rule the
caller cannot override, so that two matrices with the same content always encode
identically.

Storing only a lower triangle is the part that needs no defense: it halves the
payload and makes an asymmetric value unrepresentable, so there is no symmetry
validator and no tolerance for "how symmetric is symmetric enough". Carrying
*two* triangle encodings is the decision worth recording, along with the price
paid to keep them from being observable.

## Considered options

A single dense triangle is the obvious design. It is simple, and it is wasteful
for the sparse cases this type is expected to see - a matrix over a few thousand
keys where almost every pair is unrelated stores millions of zeros.

Reusing `ExposureMatrix`'s sparse index triples for everything fails the other
way. The dense cases - correlation and covariance matrices, which are the
motivating ones - are close to fully populated, and triples cost three numbers
per value against the triangle's one.

Neither encoding dominates, so both exist. The crossover is exact rather than
tuned: with `n` keys and `k` stored off-diagonal cells, the dense triangle costs
`n(n+1)/2` numbers and the sparse form costs `n + 3k`, which are equal at
`k = n(n-1)/6`. Sparse is canonical below one third of the off-diagonal cells
populated, dense at or above it. Anyone can re-derive that constant, which is
the point of deriving it rather than picking a round number.

Letting the caller pick the encoding was rejected. The moment two matrices with
identical content can disagree about their form, byte equality stops tracking
semantic equality, and Pydantic's field-wise `__eq__` starts returning `False`
for matrices that denote the same thing. Recovering correct equality would mean
a hand-written `__eq__` and `__hash__` that normalize across variants - at which
case equal matrices would still serialize to different bytes, breaking any
content-addressed cache or "did this change?" check downstream.

## Consequences

The rule must be a pure function of `(n, k)`. Nothing about how a matrix was
built, what it is used for, or how it was previously stored may influence the
choice, or the equality guarantee is lost.

Canonicalization runs in the model validator as well as in `build()`, so a
hand-written JSON document in the non-canonical form is silently rewritten on
load. This is the same bargain ADR 0002 struck for record sorting: silent where
the transformation is lossless, loud where it is ambiguous. Losslessness holds
here only because an absent off-diagonal cell means exactly `0.0` - there is no
configurable fill, and `cell_fill` from the exposure glossary does not apply.

The representation is invisible in the interface. There is no `nnz`, no
`is_sparse`, and no "stored entries only" accessor; `values_of` returns an entry
for every key in the universe and `pairs()` yields every pair. A sparse matrix
therefore saves storage but not iteration cost. That is deliberate - an
observable density would promote the crossover constant into the public
contract, and changing it later would be a breaking change rather than a
re-encoding.

Bumping the constant does shift the stored bytes of matrices that straddle it.
Correctness is unaffected, but any hash taken over the serialized form will
move.
