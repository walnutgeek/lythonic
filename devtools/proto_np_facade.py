"""
PROTOTYPE - throwaway. Delete before merging.

Answers one question: what does the `.np` facade cost under basedpyright and
pydantic, when the same attribute must serve inbound constructors on class
access (`ExposureMatrix.np.from_matrix(...)`) and outbound conversions on
instance access (`m.np.matrix()`).

Run: uv run python devtools/proto_np_facade.py
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, overload

from pydantic import BaseModel

if TYPE_CHECKING:
    import numpy as np
    from numpy.typing import NDArray


class NpIn:
    """Class-access facade: constructors that take arrays."""

    _owner: type[ExposureMatrix]

    def __init__(self, owner: type[ExposureMatrix]) -> None:
        self._owner = owner

    def from_matrix(
        self,
        arr: NDArray[np.float64],
        subjects: list[str],
        targets: list[str],
        cell_fill: float = 0.0,
    ) -> ExposureMatrix:
        if arr.shape != (len(subjects), len(targets)):
            raise ValueError(f"shape {arr.shape} != ({len(subjects)}, {len(targets)})")
        records = [
            (i, j, float(v))
            for i, row in enumerate(arr.tolist())
            for j, v in enumerate(row)
            if v != cell_fill
        ]
        return self._owner(subjects=subjects, targets=targets, cell_fill=cell_fill, records=records)


class NpOut:
    """Instance-access facade: dense views of a matrix."""

    _m: ExposureMatrix

    def __init__(self, owner: ExposureMatrix) -> None:
        self._m = owner

    def matrix(self) -> NDArray[np.float64]:
        import numpy

        m = self._m
        out = numpy.full((len(m.subjects), len(m.targets)), m.cell_fill, dtype=numpy.float64)
        for i, j, v in m.records:
            out[i, j] = v
        return out

    def row(self, subject: str) -> NDArray[np.float64]:
        return self.matrix()[self._m.subjects.index(subject)]

    def col(self, target: str) -> NDArray[np.float64]:
        return self.matrix()[:, self._m.targets.index(target)]


class NpAccess:
    """Binds `NpIn` on class access and `NpOut` on instance access."""

    @overload
    def __get__(self, obj: None, objtype: type[ExposureMatrix]) -> NpIn: ...
    @overload
    def __get__(
        self, obj: ExposureMatrix, objtype: type[ExposureMatrix] | None = None
    ) -> NpOut: ...

    def __get__(
        self, obj: ExposureMatrix | None, objtype: type[ExposureMatrix] | None = None
    ) -> NpIn | NpOut:
        if obj is None:
            if objtype is None:
                raise TypeError("no owner")
            return NpIn(objtype)
        return NpOut(obj)


class ExposureMatrix(BaseModel):
    subjects: list[str]
    targets: list[str]
    cell_fill: float = 0.0
    records: list[tuple[int, int, float]] = []

    np: ClassVar[NpAccess] = NpAccess()


def main() -> None:
    import numpy

    m = ExposureMatrix(
        subjects=["acct1", "acct2"],
        targets=["USD", "EUR", "JPY"],
        records=[(0, 0, 0.4), (1, 2, 0.9)],
    )
    print("model:", m.model_dump())
    print("round-trip:", ExposureMatrix.model_validate_json(m.model_dump_json()) == m)

    dense = m.np.matrix()
    print("instance access -> NpOut.matrix():\n", dense)
    print("row acct1:", m.np.row("acct1"))
    print("col JPY:", m.np.col("JPY"))

    back = ExposureMatrix.np.from_matrix(dense, m.subjects, m.targets)
    print("class access -> NpIn.from_matrix() round-trips:", back == m)

    product = dense @ numpy.eye(3)
    print("matmul result home:", ExposureMatrix.np.from_matrix(product, m.subjects, m.targets))

    print("'np' is a field?", "np" in ExposureMatrix.model_fields)


if __name__ == "__main__":
    main()
