# Exposure matrices are immutable; all growth happens in a builder

`ExposureMatrix` and `Universe` are immutable, and every mutation - growing a
universe, setting an exposure, applying a default row - happens on a separate
`ExposureMatrixBuilder` that produces a matrix via `build()`. A reader coming to
the code will notice the matrix has no setters at all, which is deliberate.

## Considered options

The design started somewhere else. Universes had to grow by first mention during
ingest but be closed afterwards, so the matrix carried a per-axis freeze flag:
mutable until declared, rejecting unknown keys after. That flag then had to be a
serialized field, because a persisted curated matrix must come back curated.

The alternative of keeping universes immutable but growing them copy-on-write
was rejected separately: populating n subjects through repeated `set_exposure`
calls would be O(n^2).

## Consequences

The persisted type has no modes. Nothing that round-trips through JSON has state
affecting what later calls are allowed to do, so reading a stored matrix tells
you everything about how it behaves. The freeze flags still exist, but on the
builder, where they are transient and never serialized.

`build()` returns a snapshot and leaves the builder usable, which requires a
defensive copy of records and universes. Without that copy the returned
"immutable" matrix aliases mutable builder state and a later `set_exposure`
silently mutates a value already handed off.

Editing a stored matrix requires a round trip through `to_builder()`, so the
common "load, add this quarter's exposures, save" flow costs one extra object.
