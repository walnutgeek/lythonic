"""
Sparse, string-keyed exposure matrices.

An exposure matrix relates *subjects* (the entities that hold exposure) to
*targets* (the things they are exposed to). Both axes are named by a
`Universe` of string keys, and storage is sparse because most cells are
empty in practice.

`ExposureMatrix` is immutable. All growth happens in `ExposureMatrixBuilder`,
which produces a matrix via `build()`. See
`docs/adr/0001-immutable-matrix-with-builder.md`.

>>> b = ExposureMatrixBuilder()
>>> b.set_exposure("acct1", "USD", 0.4)
>>> m = b.build()
>>> m.exposure("acct1", "USD")
0.4
>>> m.exposure("acct1", "EUR")
Traceback (most recent call last):
    ...
KeyError: "'EUR' not in universe"
"""

from __future__ import annotations

from bisect import bisect_left
from math import inf, isfinite
from types import ModuleType
from typing import TYPE_CHECKING, ClassVar

from pydantic import BaseModel, ConfigDict, model_validator

from lythonic.facade import LibAccess, require
from lythonic.universe import Universe

if TYPE_CHECKING:
    import numpy as np
    from numpy.typing import NDArray

Record = tuple[int, int, float]
"""A subject index, a target index, and a value. Indexes never escape this module."""


class MatrixNpIn:
    """Class-access numpy facade: constructors that take dense arrays."""

    _owner: type[ExposureMatrix]
    _numpy: ModuleType

    def __init__(self, owner: type[ExposureMatrix]) -> None:
        self._owner = owner
        self._numpy = require("numpy")

    def from_matrix(
        self,
        arr: NDArray[np.float64],
        subjects: Universe | list[str],
        targets: Universe | list[str],
        cell_fill: float = 0.0,
    ) -> ExposureMatrix:
        """
        Build a matrix from a dense array, dropping cells equal to `cell_fill`.

        This is how a numpy result comes home. NaN raises rather than being
        stored, since a NaN in the data is almost always upstream breakage.
        """
        subjects, targets = Universe(subjects), Universe(targets)
        if arr.shape != (len(subjects), len(targets)):
            raise ValueError(
                f"array shape {arr.shape} does not match {len(subjects)}x{len(targets)} universes"
            )
        if bool(self._numpy.isnan(arr).any()):
            raise ValueError("array contains NaN")
        records = [
            (si, ti, float(v))
            for si, row in enumerate(arr.tolist())
            for ti, v in enumerate(row)
            if v != cell_fill
        ]
        return self._owner(subjects=subjects, targets=targets, cell_fill=cell_fill, records=records)


class MatrixNpOut:
    """Instance-access numpy facade: dense views of a matrix."""

    _m: ExposureMatrix
    _numpy: ModuleType

    def __init__(self, owner: ExposureMatrix) -> None:
        self._m = owner
        self._numpy = require("numpy")

    def matrix(self) -> NDArray[np.float64]:
        """Dense `subjects` x `targets` array, absent cells set to `cell_fill`."""
        numpy = self._numpy
        m = self._m
        out = numpy.full((len(m.subjects), len(m.targets)), m.cell_fill, dtype=numpy.float64)
        for si, ti, value in m.records:
            out[si, ti] = value
        return out

    def _vector(self, size: int, stored: dict[str, float], keys: Universe) -> NDArray[np.float64]:
        numpy = self._numpy
        out = numpy.full(size, self._m.cell_fill, dtype=numpy.float64)
        for key, value in stored.items():
            out[keys.index(key)] = value
        return out

    def row(self, subject: str) -> NDArray[np.float64]:
        """One subject's exposures, aligned to the target universe."""
        m = self._m
        return self._vector(len(m.targets), m.exposures_of(subject), m.targets)

    def col(self, target: str) -> NDArray[np.float64]:
        """One target's exposures, aligned to the subject universe."""
        m = self._m
        return self._vector(len(m.subjects), m.exposures_to(target), m.subjects)


class BuilderNpAccess:
    """Array-shaped writers, valid only against frozen axes."""

    _b: ExposureMatrixBuilder
    _numpy: ModuleType

    def __init__(self, owner: ExposureMatrixBuilder) -> None:
        self._b = owner
        self._numpy = require("numpy")

    def _reject_nan(self, arr: NDArray[np.float64]) -> None:
        if bool(self._numpy.isnan(arr).any()):
            raise ValueError("array contains NaN")

    def set_exposures(self, subject: str, values: NDArray[np.float64]) -> None:
        """Replace a subject's row from an array aligned to the target universe."""
        b = self._b
        if not b.targets_frozen:
            raise ValueError("target axis must be frozen to write rows by position")
        targets = b.targets
        if values.shape != (len(targets),):
            raise ValueError(f"array shape {values.shape} does not match {len(targets)} targets")
        self._reject_nan(values)
        b.set_exposures(subject, {targets[i]: float(v) for i, v in enumerate(values)})

    def set_matrix(self, arr: NDArray[np.float64]) -> None:
        """Replace every row from a dense array aligned to both universes."""
        b = self._b
        if not (b.subjects_frozen and b.targets_frozen):
            raise ValueError("both axes must be frozen to write a matrix by position")
        subjects = b.subjects
        if arr.shape != (len(subjects), len(b.targets)):
            raise ValueError(
                f"array shape {arr.shape} does not match {len(subjects)}x{len(b.targets)} universes"
            )
        for i, subject in enumerate(subjects):
            self.set_exposures(subject, arr[i])


class ExposureMatrix(BaseModel):
    """
    Immutable sparse matrix of exposures, keyed by string on both axes.

    Reads by key raise `KeyError` for a key outside the relevant universe,
    and return `cell_fill` for a key that is in the universe but has no
    record, so "unknown key" and "no exposure" stay distinguishable.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    subjects: Universe
    targets: Universe
    cell_fill: float = 0.0
    records: list[Record] = []

    np: ClassVar[LibAccess[ExposureMatrix, MatrixNpIn, MatrixNpOut]] = LibAccess(
        MatrixNpIn, MatrixNpOut
    )
    """Numpy facade. Class access gives constructors, instance access conversions."""

    @model_validator(mode="after")
    def _canonicalize(self) -> ExposureMatrix:
        """
        Canonicalize what is lossless to canonicalize, reject what is ambiguous.

        Sorting and dropping fill-valued records preserve meaning, so producers
        are not forced to reimplement our ordering rules. Out-of-range indexes
        are corrupt, and duplicate cells are genuinely ambiguous - picking a
        winner would be a silent guess about intent.
        """
        if not isfinite(self.cell_fill):
            raise ValueError(f"cell_fill must be finite, got {self.cell_fill}")

        n_subjects, n_targets = len(self.subjects), len(self.targets)
        seen: set[tuple[int, int]] = set()
        for si, ti, _ in self.records:
            if not (0 <= si < n_subjects and 0 <= ti < n_targets):
                raise ValueError(
                    f"record index ({si}, {ti}) out of range for {n_subjects}x{n_targets} matrix"
                )
            if (si, ti) in seen:
                raise ValueError(f"duplicate record for cell ({si}, {ti})")
            seen.add((si, ti))

        canonical = sorted(r for r in self.records if r[2] != self.cell_fill)
        if canonical != self.records:
            # model_config is frozen, so bypass __setattr__ to normalize in place.
            object.__setattr__(self, "records", canonical)
        return self

    def exposure(self, subject: str, target: str) -> float:
        """Value at one cell, or `cell_fill` when no record exists for it."""
        si, ti = self.subjects.index(subject), self.targets.index(target)
        # Records are sorted by (subject, target) - ADR 0002 - so the run for one
        # subject is contiguous and binary search finds the cell directly.
        position = bisect_left(self.records, (si, ti, -inf))
        if position < len(self.records):
            rsi, rti, value = self.records[position]
            if (rsi, rti) == (si, ti):
                return value
        return self.cell_fill

    def exposures_of(self, subject: str) -> dict[str, float]:
        """Stored exposures of one subject, keyed by target."""
        si = self.subjects.index(subject)
        start = bisect_left(self.records, (si, 0, -inf))
        out: dict[str, float] = {}
        for rsi, ti, v in self.records[start:]:
            if rsi != si:
                break
            out[self.targets[ti]] = v
        return out

    def exposures_to(self, target: str) -> dict[str, float]:
        """Stored exposures to one target, keyed by subject."""
        ti = self.targets.index(target)
        return {self.subjects[si]: v for si, rti, v in self.records if rti == ti}

    def cast(
        self,
        subjects: Universe | list[str] | None = None,
        targets: Universe | list[str] | None = None,
        default_row: dict[str, float] | None = None,
    ) -> ExposureMatrix:
        """
        A new matrix over the given universes, keeping `cell_fill`.

        `None` keeps an axis as-is. This is a projection: keys outside the new
        universes are dropped along with their exposures. Subjects the cast
        introduces take `default_row`, restricted to the new target universe,
        or `cell_fill` when no default row is given. There is no default
        column - targets are the curated axis.
        """
        new_subjects = Universe(subjects) if subjects is not None else self.subjects
        new_targets = Universe(targets) if targets is not None else self.targets
        restricted = (
            {t: v for t, v in default_row.items() if t in new_targets}
            if default_row is not None
            else None
        )
        b = ExposureMatrixBuilder(
            subjects=new_subjects,
            targets=new_targets,
            cell_fill=self.cell_fill,
            default_row=restricted,
        )
        for subject in new_subjects:
            if subject in self.subjects:
                kept = {t: v for t, v in self.exposures_of(subject).items() if t in new_targets}
                b.set_exposures(subject, kept)
            elif restricted is not None:
                b.set_exposures(subject, None)
        return b.build()

    def to_builder(self, default_row: dict[str, float] | None = None) -> ExposureMatrixBuilder:
        """
        A builder seeded with this matrix, with both axes frozen.

        Freezing is the safe default for amending an existing matrix; thaw an
        axis explicitly to grow it.
        """
        b = ExposureMatrixBuilder(
            subjects=self.subjects,
            targets=self.targets,
            cell_fill=self.cell_fill,
            default_row=default_row,
        )
        for si, ti, value in self.records:
            b.set_exposure(self.subjects[si], self.targets[ti], value)
        return b


class ExposureMatrixBuilder:
    """
    Mutable accumulator that produces an `ExposureMatrix`.

    Universes grow in first-mention order.
    """

    _subjects: list[str]
    _targets: list[str]
    _subject_positions: dict[str, int]
    _target_positions: dict[str, int]
    _subjects_frozen: bool
    _targets_frozen: bool
    _cell_fill: float
    _cells: dict[tuple[int, int], float]
    _default_row: dict[str, float] | None

    def __init__(
        self,
        subjects: Universe | list[str] | None = None,
        targets: Universe | list[str] | None = None,
        cell_fill: float = 0.0,
        default_row: dict[str, float] | None = None,
    ) -> None:
        self._subjects = list(subjects) if subjects is not None else []
        self._targets = list(targets) if targets is not None else []
        self._subject_positions = {k: i for i, k in enumerate(self._subjects)}
        self._target_positions = {k: i for i, k in enumerate(self._targets)}
        self._subjects_frozen = subjects is not None
        self._targets_frozen = targets is not None
        self._cell_fill = cell_fill
        self._cells = {}
        if not isfinite(cell_fill):
            raise ValueError(f"cell_fill must be finite, got {cell_fill}")
        self._default_row = default_row
        if default_row is not None and self._targets_frozen:
            # Fail on the call that configures a bad default, not on the one that
            # uses it. Only a frozen axis can be violated; an open one would just
            # append, which would let the default row pre-seed the ordering.
            for target in default_row:
                if target not in self._targets:
                    raise KeyError(f"{target!r} not in frozen target universe")

    @property
    def np(self) -> BuilderNpAccess:
        """Array-shaped writers, valid only against frozen axes."""
        return BuilderNpAccess(self)

    @property
    def subjects(self) -> Universe:
        """The subject universe accumulated so far."""
        return Universe(self._subjects)

    @property
    def targets(self) -> Universe:
        """The target universe accumulated so far."""
        return Universe(self._targets)

    @property
    def subjects_frozen(self) -> bool:
        """Whether the subject axis rejects keys outside its universe."""
        return self._subjects_frozen

    @property
    def targets_frozen(self) -> bool:
        """Whether the target axis rejects keys outside its universe."""
        return self._targets_frozen

    def freeze_subjects(self) -> None:
        """Reject subjects outside the universe accumulated so far."""
        self._subjects_frozen = True

    def thaw_subjects(self) -> None:
        """Accept new subjects again, appending them after the existing ones."""
        self._subjects_frozen = False

    def freeze_targets(self) -> None:
        """Reject targets outside the universe accumulated so far."""
        self._targets_frozen = True

    def thaw_targets(self) -> None:
        """Accept new targets again, appending them after the existing ones."""
        self._targets_frozen = False

    def _index(
        self, key: str, keys: list[str], positions: dict[str, int], frozen: bool, axis: str
    ) -> int:
        # A position map, not list.index: ADR 0001 rejects an alternative design
        # for making ingest O(n^2), so ingest here must stay linear.
        position = positions.get(key)
        if position is not None:
            return position
        if frozen:
            raise KeyError(f"{key!r} not in frozen {axis} universe")
        keys.append(key)
        positions[key] = len(keys) - 1
        return positions[key]

    def _subject_index(self, subject: str) -> int:
        return self._index(
            subject, self._subjects, self._subject_positions, self._subjects_frozen, "subject"
        )

    def _target_index(self, target: str) -> int:
        return self._index(
            target, self._targets, self._target_positions, self._targets_frozen, "target"
        )

    def set_exposure(self, subject: str, target: str, value: float) -> None:
        """
        Set one cell, growing either universe if the key is new.

        Writing `cell_fill` removes the record, keeping storage canonical.
        """
        cell = (self._subject_index(subject), self._target_index(target))
        if value == self._cell_fill:
            self._cells.pop(cell, None)
        else:
            self._cells[cell] = value

    def set_exposures(self, subject: str, targets: dict[str, float] | None) -> None:
        """
        Replace a subject's whole row.

        An empty mapping models a subject with no exposures, which is not the
        same as a subject that was never mentioned. `None` applies the
        configured default row and raises if there is none.
        """
        if targets is None:
            if self._default_row is None:
                raise ValueError(f"no default row configured for subject {subject!r}")
            targets = self._default_row
        si = self._subject_index(subject)
        self._cells = {(rsi, ti): v for (rsi, ti), v in self._cells.items() if rsi != si}
        for target, value in targets.items():
            self.set_exposure(subject, target, value)

    def _known(self, key: str, positions: dict[str, int]) -> int:
        if key not in positions:
            raise KeyError(f"{key!r} not in universe")
        return positions[key]

    def exposure(self, subject: str, target: str) -> float:
        """Value at one cell so far, or `cell_fill` when no record exists for it."""
        si = self._known(subject, self._subject_positions)
        ti = self._known(target, self._target_positions)
        return self._cells.get((si, ti), self._cell_fill)

    def exposures_of(self, subject: str) -> dict[str, float]:
        """Stored exposures of one subject so far, keyed by target."""
        si = self._known(subject, self._subject_positions)
        return {self._targets[ti]: v for (rsi, ti), v in self._cells.items() if rsi == si}

    def build(self) -> ExposureMatrix:
        """Snapshot the current state as an immutable matrix."""
        records = [(si, ti, v) for (si, ti), v in sorted(self._cells.items())]
        return ExposureMatrix(
            subjects=Universe(self._subjects),
            targets=Universe(self._targets),
            cell_fill=self._cell_fill,
            records=records,
        )
