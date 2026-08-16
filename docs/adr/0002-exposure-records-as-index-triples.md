# Exposure records serialize as index triples

Exposures persist as `list[tuple[int, int, float]]` - a subject index, a target
index, and a value - rather than as keyed objects naming the subject and target
as strings. Indexes never appear in the interface; they are a wire and storage
detail. This is worth recording because it is the one place the design lets a
positional detail into a format that is otherwise fully keyed, and because a
persisted format is expensive to change once data exists.

## Considered options

Keyed objects (`{"subject": "acct1", "target": "USD", "value": 0.4}`) are
self-describing and survive a hand-edited universe, at the cost of repeating
both key strings in every record. A matrix with 100k records over 40 targets
would store `"USD"` tens of thousands of times and roughly triple the payload.

Parallel COO arrays (`{"si": [...], "ti": [...], "v": [...]}`) are the most
compact and load into numpy directly, but introduce a three-lists-must-be-equal
-length invariant and are the least legible of the three.

Index triples are safe here specifically because universes are immutable and
travel in the same document. Records are meaningless without them, and an index
cannot go stale, because no operation permutes an existing universe - open axes
only append.

## Consequences

The format is not self-describing. Anything reading it must read the universes
first, and a record list separated from its matrix is unrecoverable.

Records are stored sorted by (subject index, target index) and carry no cells
whose value equals the cell fill, so serialized bytes are canonical and byte
equality matches semantic equality.

Validation cannot be skipped on the untrusted path. A directly constructed or
deserialized matrix canonicalizes silently where doing so is lossless - sorting
records, dropping fill-valued ones - and rejects what is genuinely ambiguous or
corrupt: out-of-range indexes and duplicate cells.
