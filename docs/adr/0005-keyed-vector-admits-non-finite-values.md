# KeyedVector admits non-finite values

`KeyedVector` stores NaN and the infinities like any other float. It serializes
them as `"NaN"`, `"Infinity"`, and `"-Infinity"` via pydantic's
`ser_json_inf_nan="strings"`, defines equality so that NaN matches NaN
positionally, and uses NaN as the fill when `cast` extends a vector onto a wider
universe.

This directly contradicts the rule the two matrix types follow. `ExposureMatrix`
rejects a non-finite `cell_fill` and raises on an array containing NaN, and its
design notes say that if "unknown" versus "zero" ever needs modeling, "it
deserves an explicit mechanism rather than a float sentinel the rest of the
design must work around". A reader who finds one neighbouring type embracing
what another forbids will reasonably assume one of them is a mistake, so the
split is recorded here.

## Considered options

Rejecting non-finite values, as the matrices do, was the default and was
declined because `KeyedVector` is a more general object than either matrix. The
matrices exist to hold exposures and pairwise relationships, where a NaN is
upstream breakage worth failing on. A keyed vector is whatever a caller has one
value per key of - and computed results legitimately contain NaN and infinities.
Refusing to store them does not prevent them; it only moves the failure to
whichever boundary the caller crosses next, after the information about where
they came from is gone.

The serialization spelling was measured rather than assumed. Pydantic's default
turns NaN into `null`, and `null` then fails to validate back into `list[float]`
- so saving a vector and loading it would raise. Of the two working modes,
`constants` emits bare `NaN`, which Python reads and which `JSON.parse`, Go's
`encoding/json`, and `serde_json` all reject, making any stored file
Python-only. `strings` is valid JSON everywhere and round-trips losslessly. A
shorter bespoke alphabet - `.` for NaN, `Inf` and `-Inf` - was considered and
declined: it saves two to five bytes on values that should be rare, requires a
custom serializer and validator that every non-Python consumer must reimplement,
and borrows the glyph SAS and Stata use for *missing* to mean something this
type deliberately distinguishes from missing.

## Consequences

Equality needs a custom `__eq__`, because the default is not merely strict but
incoherent. Measured on a pydantic model holding `list[float]`:

```
v == v                   True     # list compares elements by identity first
v == V(same content)     False    # distinct float objects, NaN != NaN
```

Equality therefore depends on whether two floats happen to be the same object,
and a vector round-tripped through JSON compares unequal to the one that was
saved. `KeyedVector.__eq__` compares universes and then values positionally with
NaN matching NaN, the rule `numpy.array_equal(..., equal_nan=True)` uses. This
diverges from `SymmetricMatrix`, which relies on plain field-wise equality and
draws its guarantee from canonical storage - a divergence justified only by this
type admitting NaN at all.

No hashing question arises: a pydantic model with a `list[float]` field is
unhashable regardless of `frozen`.

Admitting NaN is what makes silent extension in `cast` acceptable. Elsewhere in
the library a fill is a hazard because a fabricated value is indistinguishable
from a measured one - a zero diagonal quietly makes a matrix non-PSD, a zero
exposure looks like a real zero. NaN cannot hide that way: any arithmetic
touching it yields NaN, so the fabrication propagates visibly to whoever
consumes it. It is the one fill value that announces itself, which is why
`cast(universe, fill=nan)` extends silently while `SymmetricMatrix.cast` refuses
to extend at all.

The JSON array becomes mixed-type on the wire, so a strict consumer schema must
describe entries as `number | string`.
