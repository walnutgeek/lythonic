"""
Square, string-keyed matrices whose two axes are one universe.

A `SymmetricMatrix` holds a value per unordered *pair* of keys drawn from a
single `Universe`: a correlation or covariance matrix, a distance matrix, a
similarity or adjacency matrix. `ExposureMatrix` cannot express these - its two
axes are different universes with different semantics, nothing keeps them
aligned, and nothing keeps the two halves of the matrix in agreement.

Symmetry here is structural rather than validated. Only a lower triangle is
stored, so there is no slot in which an asymmetric value could be put. That
removes the questions a validating design would have to answer: no symmetry
check, no tolerance for how symmetric is symmetric enough, and no rule for
which half wins when the two disagree.

>>> b = SymmetricMatrixBuilder()
>>> b.set_diagonal({"a": 1.0, "b": 1.0})
>>> b.set_value("a", "b", 0.5)
>>> m = b.build()
>>> m.value("b", "a")
0.5
>>> m.values_of("a")
{'a': 1.0, 'b': 0.5}

The type is generic, not correlation- or covariance-specific: it enforces
squareness and symmetry and claims nothing else. In particular it makes no
positive semi-definiteness promise - that is a question answered on demand
through the numpy facade, which keeps numpy optional and keeps an
eigendecomposition off the deserialization path. See
`docs/adr/0004-psd-is-a-query.md`. The cost is that the type cannot catch a
correlation matrix whose diagonal is not one, or a covariance matrix with a
negative variance; correlation and covariance are *usages* of this type, not
variants of it.

Every key needs an explicit diagonal value and `build()` raises naming any key
that has none. There is deliberately no default-diagonal policy: the failure it
would prevent is silent, since a correlation matrix built with a zero diagonal
is not merely wrong but not positive semi-definite, so the definiteness query
would report a data problem that is really a defaulting problem.

An absent off-diagonal pair reads as `0.0`. That is fixed, not configurable -
see `docs/adr/0003-symmetric-storage-canonical-union.md`, which also covers the
two storage encodings, why the caller cannot choose between them, and why the
choice is invisible in the interface.

Immutable, with all growth in `SymmetricMatrixBuilder`, following
`docs/adr/0001-immutable-matrix-with-builder.md`.
"""

from __future__ import annotations

from bisect import bisect_left
from collections.abc import Iterator, Mapping
from math import inf, isfinite
from types import ModuleType
from typing import TYPE_CHECKING, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lythonic.facade import LibAccess, require
from lythonic.universe import Universe

if TYPE_CHECKING:
    import numpy as np
    from numpy.typing import NDArray

Record = tuple[int, int, float]
"""
A row index, a column index with `row > column`, and a value.

Named `Record` rather than `Pair` because `CONTEXT.md` reserves *pair* for an
unordered pair of keys - what `SymmetricMatrix.pairs()` yields. These are the
storage triples ADR 0003 describes, and indexes never escape this module.
"""


def _offset(row: int, column: int) -> int:
    """Position of a lower-triangle cell in a flat row-major triangle."""
    return row * (row + 1) // 2 + column


def _ordered(i: int, j: int) -> tuple[int, int]:
    return (i, j) if i >= j else (j, i)


class DenseTriangle(BaseModel):
    """Every lower-triangle cell, flat and row-major, including the diagonal."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal["dense"] = "dense"
    values: list[float] = []


class SparseTriangle(BaseModel):
    """A dense diagonal plus only the off-diagonal cells that are non-zero."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    kind: Literal["sparse"] = "sparse"
    diagonal: list[float] = []
    records: list[Record] = []


Storage = DenseTriangle | SparseTriangle


def _pack(diagonal: list[float], off_diagonal: dict[tuple[int, int], float]) -> Storage:
    """
    Choose the canonical encoding for one matrix's content.

    The rule is the exact byte-count crossover, not a tuned constant: a dense
    triangle costs `n(n+1)/2` numbers and the sparse form costs `n + 3k`, which
    are equal at `k = n(n-1)/6`. It must stay a pure function of `n` and `k` -
    anything else and two matrices with the same content could encode
    differently, which is what equality here rests on. See ADR 0003.

    Compared as `6k < n(n-1)` rather than against `n(n-1)/6`: integer division
    would truncate the threshold and pick the larger encoding whenever it is
    non-integral, and float division would put a rounding question on the rule
    that decides the canonical form.
    """
    n = len(diagonal)
    if 6 * len(off_diagonal) < n * (n - 1):
        return SparseTriangle(
            diagonal=diagonal,
            records=sorted((row, column, v) for (row, column), v in off_diagonal.items()),
        )
    values = [0.0] * (n * (n + 1) // 2)
    for i, value in enumerate(diagonal):
        values[_offset(i, i)] = value
    for (row, column), value in off_diagonal.items():
        values[_offset(row, column)] = value
    return DenseTriangle(values=values)


class MatrixNpIn:
    """Class-access numpy facade: constructors that take dense arrays."""

    _numpy: ModuleType

    def __init__(self, owner: type[SymmetricMatrix]) -> None:  # pyright: ignore[reportUnusedParameter]
        # The owner is not kept: matrices are produced through the builder, so
        # there is nothing here to construct directly.
        self._numpy = require("numpy")

    def from_matrix(
        self, arr: NDArray[np.float64], universe: Universe | list[str]
    ) -> SymmetricMatrix:
        """
        Build from a dense square array, reading the lower triangle only.

        The upper triangle is ignored rather than checked. Rejecting on
        asymmetry would need the comparison tolerance the triangle layout
        exists to avoid, and averaging the halves would silently absorb a
        transposed-block bug; ignoring one half is at least predictable. The
        hazard that survives is recorded in `docs/open-questions.md`: nothing
        here can detect a transposed input, because every input is square.
        """
        b = SymmetricMatrixBuilder(universe=Universe(universe))
        b.np.set_matrix(arr)
        return b.build()


class MatrixNpOut:
    """Instance-access numpy facade: dense views and definiteness queries."""

    _m: SymmetricMatrix
    _numpy: ModuleType

    def __init__(self, owner: SymmetricMatrix) -> None:
        self._m = owner
        self._numpy = require("numpy")

    def matrix(self) -> NDArray[np.float64]:
        """Full square array with symmetry materialized."""
        numpy = self._numpy
        m = self._m
        n = len(m.universe)
        out = numpy.zeros((n, n), dtype=numpy.float64)
        for a, b, value in m.pairs():
            i, j = m.universe.index(a), m.universe.index(b)
            out[i, j] = out[j, i] = value
        return out

    def diagonal(self) -> NDArray[np.float64]:
        """The diagonal as an array, in universe order."""
        return self._numpy.array(list(self._m.diagonal().values()), dtype=self._numpy.float64)

    def vector(self, key: str) -> NDArray[np.float64]:
        """One key's values, aligned to the universe."""
        return self._numpy.array(list(self._m.values_of(key).values()), dtype=self._numpy.float64)

    def eigenvalues(self) -> NDArray[np.float64]:
        """Eigenvalues in ascending order, via the symmetric solver."""
        return self._numpy.linalg.eigvalsh(self.matrix())

    def min_eigenvalue(self) -> float:
        """Smallest eigenvalue, or `0.0` for an empty matrix."""
        eigenvalues = self.eigenvalues()
        return float(eigenvalues.min()) if eigenvalues.size else 0.0

    def is_psd(self, tol: float | None = None) -> bool:
        """
        Whether the matrix is positive semi-definite within `tol`.

        The default tolerance is `n * eps * max(|lambda|)`, scaled to the
        matrix's own magnitude and dimension, following the convention
        `numpy.linalg.matrix_rank` uses for its singular-value cutoff. Some
        tolerance is mandatory: a mathematically valid sample covariance
        routinely returns a smallest eigenvalue around `-1e-15` after a
        floating-point round trip, so a bare non-negativity test rejects good
        data.

        `eigvalsh` rather than `cholesky`, which tests positive *definite* and
        would reject a rank-deficient covariance - more variables than
        observations - that is legitimately semi-definite.
        """
        eigenvalues = self.eigenvalues()
        if not eigenvalues.size:
            return True
        if tol is None:
            eps = float(self._numpy.finfo(self._numpy.float64).eps)
            tol = len(self._m.universe) * eps * float(abs(eigenvalues).max())
        return bool(eigenvalues.min() >= -tol)


class BuilderNpAccess:
    """Array-shaped writers, valid only against a frozen universe."""

    _b: SymmetricMatrixBuilder
    _numpy: ModuleType

    def __init__(self, owner: SymmetricMatrixBuilder) -> None:
        self._b = owner
        self._numpy = require("numpy")

    def set_matrix(self, arr: NDArray[np.float64]) -> None:
        """Write every pair from a dense square array, lower triangle only."""
        b = self._b
        if not b.universe_frozen:
            raise ValueError("universe must be frozen to write a matrix by position")
        universe = b.universe
        n = len(universe)
        if arr.shape != (n, n):
            raise ValueError(f"array shape {arr.shape} does not match {n}x{n} universe")
        rows = arr.tolist()
        for i in range(n):
            for j in range(i + 1):
                b.set_value(universe[i], universe[j], float(rows[i][j]))


class SymmetricMatrix(BaseModel):
    """
    Immutable square matrix over one universe, keyed by unordered pairs.

    Reads by key raise `KeyError` for a key outside the universe. An
    off-diagonal pair with no stored value reads as `0.0`; the diagonal is
    always present, since a matrix cannot be built without it.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    universe: Universe
    storage: Storage = Field(default_factory=DenseTriangle, discriminator="kind")

    np: ClassVar[LibAccess[SymmetricMatrix, MatrixNpIn, MatrixNpOut]] = LibAccess(
        MatrixNpIn, MatrixNpOut
    )
    """Numpy facade. Class access gives constructors, instance access conversions."""

    @model_validator(mode="after")
    def _canonicalize(self) -> SymmetricMatrix:
        """
        Reject what is corrupt, then re-encode into the canonical variant.

        Rewriting the caller's encoding is the same bargain ADR 0002 struck for
        record sorting: silent where the transformation is lossless, loud where
        it is ambiguous. It is lossless here only because an absent off-diagonal
        pair means exactly zero.
        """
        diagonal, off_diagonal = self._decompose()
        canonical = _pack(diagonal, off_diagonal)
        if canonical != self.storage:
            # model_config is frozen, so bypass __setattr__ to normalize in place.
            object.__setattr__(self, "storage", canonical)
        return self

    def _decompose(self) -> tuple[list[float], dict[tuple[int, int], float]]:
        n = len(self.universe)
        storage = self.storage
        if isinstance(storage, DenseTriangle):
            expected = n * (n + 1) // 2
            if len(storage.values) != expected:
                raise ValueError(
                    f"{len(storage.values)} values for a {n}x{n} triangle, expected {expected}"
                )
            for value in storage.values:
                _require_finite(value)
            diagonal = [storage.values[_offset(i, i)] for i in range(n)]
            off_diagonal = {
                (row, column): storage.values[_offset(row, column)]
                for row in range(n)
                for column in range(row)
                if storage.values[_offset(row, column)] != 0.0
            }
            return diagonal, off_diagonal

        if len(storage.diagonal) != n:
            raise ValueError(f"diagonal of {len(storage.diagonal)} values for {n} keys")
        for value in storage.diagonal:
            _require_finite(value)
        off_diagonal: dict[tuple[int, int], float] = {}
        for row, column, value in storage.records:
            if not (0 <= column < row < n):
                raise ValueError(
                    f"pair index ({row}, {column}) out of range for a {n}x{n} lower triangle"
                )
            if (row, column) in off_diagonal:
                raise ValueError(f"duplicate pair ({row}, {column})")
            _require_finite(value)
            off_diagonal[(row, column)] = value
        return list(storage.diagonal), {k: v for k, v in off_diagonal.items() if v != 0.0}

    def _value_at(self, row: int, column: int) -> float:
        """
        Value at a lower-triangle position, with `row >= column`.

        Positional, so it stays private: the triangle layout is storage, and
        the glossary avoids row and column vocabulary for this type.
        """
        storage = self.storage
        if isinstance(storage, DenseTriangle):
            return storage.values[_offset(row, column)]
        if row == column:
            return storage.diagonal[row]
        position = bisect_left(storage.records, (row, column, -inf))
        if position < len(storage.records):
            found_row, found_column, value = storage.records[position]
            if (found_row, found_column) == (row, column):
                return value
        return 0.0

    def value(self, a: str, b: str) -> float:
        """Value at the pair `{a, b}`, regardless of the order they are named."""
        return self._value_at(*_ordered(self.universe.index(a), self.universe.index(b)))

    def diagonal(self) -> dict[str, float]:
        """The diagonal, keyed by key, in universe order."""
        return {key: self._value_at(i, i) for i, key in enumerate(self.universe)}

    def values_of(self, key: str) -> dict[str, float]:
        """
        Every value involving `key`, keyed by the other key.

        An entry per key in the universe, absent pairs materialized as `0.0`,
        so the answer never depends on how the matrix happens to be stored.
        """
        index = self.universe.index(key)
        return {other: self._value_at(*_ordered(index, j)) for j, other in enumerate(self.universe)}

    def pairs(self) -> Iterator[tuple[str, str, float]]:
        """
        Every pair once, in universe order, with the earlier key first.

        Includes self-pairs and pairs whose value is zero, so the sequence is
        the same for the same content however it is stored.
        """
        for i, later in enumerate(self.universe):
            for j in range(i + 1):
                yield self.universe[j], later, self._value_at(i, j)

    def cast(self, universe: Universe | list[str]) -> SymmetricMatrix:
        """
        A new matrix over `universe`, dropping keys outside it.

        A subset-and-reorder projection. Dropping is silent, as in
        `ExposureMatrix.cast`, but a key the cast would introduce raises: the
        type cannot invent a diagonal value for it, which is the same reason
        diagonals are explicit everywhere else. Growth goes through
        `to_builder`.
        """
        target = Universe(universe)
        if target == self.universe:
            return self
        introduced = [key for key in target if key not in self.universe]
        if introduced:
            raise KeyError(
                f"cast would introduce {introduced} with no diagonal value; use to_builder()"
            )
        b = SymmetricMatrixBuilder(universe=target)
        for i, later in enumerate(target):
            for j in range(i + 1):
                b.set_value(target[j], later, self.value(target[j], later))
        return b.build()

    def to_builder(self) -> SymmetricMatrixBuilder:
        """
        A builder seeded with this matrix, universe frozen.

        Freezing is the safe default for amending an existing matrix; thaw
        explicitly to grow it.
        """
        b = SymmetricMatrixBuilder(universe=self.universe)
        for a, other, value in self.pairs():
            b.set_value(a, other, value)
        return b


def _require_finite(value: float) -> None:
    if not isfinite(value):
        raise ValueError(f"values must be finite, got {value}")


class SymmetricMatrixBuilder:
    """
    Mutable accumulator that produces a `SymmetricMatrix`.

    The universe grows in first-mention order unless it was declared, which
    freezes it. Growth only ever appends - nothing permutes an existing
    position, which matters more here than in `ExposureMatrixBuilder`, since a
    permutation would invalidate every triangle offset rather than merely
    relabel a row.

    There is deliberately no row-wise write. Under symmetry the values in one
    key's row *are* values in every other key's row, so a row-replace would
    silently delete entries a caller thinks of as belonging elsewhere.
    """

    _keys: list[str]
    _positions: dict[str, int]
    _frozen: bool
    _diagonal: dict[int, float]
    _off_diagonal: dict[tuple[int, int], float]

    def __init__(self, universe: Universe | list[str] | None = None) -> None:
        self._keys = list(universe) if universe is not None else []
        self._positions = {key: i for i, key in enumerate(self._keys)}
        self._frozen = universe is not None
        self._diagonal = {}
        self._off_diagonal = {}

    @property
    def np(self) -> BuilderNpAccess:
        """Array-shaped writers, valid only against a frozen universe."""
        return BuilderNpAccess(self)

    @property
    def universe(self) -> Universe:
        """The universe accumulated so far."""
        return Universe(self._keys)

    @property
    def universe_frozen(self) -> bool:
        """Whether the universe rejects keys outside itself."""
        return self._frozen

    def freeze(self) -> None:
        """Reject keys outside the universe accumulated so far."""
        self._frozen = True

    def thaw(self) -> None:
        """Accept new keys again, appending them after the existing ones."""
        self._frozen = False

    def _index(self, key: str) -> int:
        position = self._positions.get(key)
        if position is not None:
            return position
        if self._frozen:
            raise KeyError(f"{key!r} not in frozen universe")
        self._keys.append(key)
        self._positions[key] = len(self._keys) - 1
        return self._positions[key]

    def _known(self, key: str) -> int:
        if key not in self._positions:
            raise KeyError(f"{key!r} not in universe")
        return self._positions[key]

    def set_value(self, a: str, b: str, value: float) -> None:
        """
        Set the value at the pair `{a, b}`, growing the universe if needed.

        Argument order is irrelevant. Writing zero to an off-diagonal pair
        removes it, keeping storage canonical; a zero on the diagonal is kept,
        since the diagonal is dense and every key needs one.
        """
        _require_finite(value)
        row, column = _ordered(self._index(a), self._index(b))
        if row == column:
            self._diagonal[row] = value
        elif value == 0.0:
            self._off_diagonal.pop((row, column), None)
        else:
            self._off_diagonal[(row, column)] = value

    def set_diagonal(self, values: Mapping[str, float]) -> None:
        """Set many diagonal values at once, growing the universe if needed."""
        for key, value in values.items():
            self.set_value(key, key, value)

    def value(self, a: str, b: str) -> float:
        """Value at the pair `{a, b}` so far, or `0.0` if none is set."""
        row, column = _ordered(self._known(a), self._known(b))
        if row == column:
            return self._diagonal.get(row, 0.0)
        return self._off_diagonal.get((row, column), 0.0)

    def diagonal(self) -> dict[str, float]:
        """The diagonal values set so far, keyed by key."""
        return {key: self._diagonal[i] for i, key in enumerate(self._keys) if i in self._diagonal}

    def build(self) -> SymmetricMatrix:
        """
        Snapshot the current state as an immutable matrix.

        Raises when any key has no diagonal value, naming them: with an open
        universe a key arriving through an off-diagonal write has no diagonal
        yet, so this is the normal end state of a naive ingest loop rather than
        an exotic one.

        The builder stays usable afterwards, which requires the copies made
        here - without them the returned matrix would alias mutable builder
        state. See ADR 0001.
        """
        missing = [key for i, key in enumerate(self._keys) if i not in self._diagonal]
        if missing:
            named = ", ".join(repr(key) for key in missing)
            raise ValueError(f"no diagonal value for {named}")
        diagonal = [self._diagonal[i] for i in range(len(self._keys))]
        return SymmetricMatrix(
            universe=Universe(self._keys),
            storage=_pack(diagonal, dict(self._off_diagonal)),
        )
