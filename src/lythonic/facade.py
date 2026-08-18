"""
Class/instance-bound facades for optional-library conversions.

A facade groups everything one optional library can do with a type under a
single attribute named for that library's conventional import alias. Class
access yields inbound constructors, instance access yields outbound
conversions, so `FrameData.pd.from_frame(df)` and `fd.pd.frame()` are two
halves of one spelling rather than two unrelated methods.

`LibAccess` is the descriptor that binds those two halves. Annotate it as a
`ClassVar` so it stays out of a Pydantic model's fields, and give it the two
facade classes:

```python
class FrameData(BaseModel):
    pd: ClassVar[LibAccess[FrameData, PdIn, PdOut]] = LibAccess(PdIn, PdOut)
```

The overloads on `__get__` are what make misuse a type error: an outbound
conversion is unreachable from the class and an inbound constructor is
unreachable from an instance.
"""

from __future__ import annotations

from collections.abc import Callable
from types import ModuleType
from typing import Generic, TypeVar, overload

OwnerT = TypeVar("OwnerT")
InT = TypeVar("InT")
OutT = TypeVar("OutT")


class LibAccess(Generic[OwnerT, InT, OutT]):
    """
    Binds `InT` on class access and `OutT` on instance access.

    Both facades are constructed per access, so they may hold a back-reference
    to the owner without keeping any state of their own.
    """

    _inbound: Callable[[type[OwnerT]], InT]
    _outbound: Callable[[OwnerT], OutT]

    def __init__(
        self, inbound: Callable[[type[OwnerT]], InT], outbound: Callable[[OwnerT], OutT]
    ) -> None:
        self._inbound = inbound
        self._outbound = outbound

    @overload
    def __get__(self, obj: None, objtype: type[OwnerT]) -> InT: ...
    @overload
    def __get__(self, obj: OwnerT, objtype: type[OwnerT] | None = None) -> OutT: ...

    def __get__(self, obj: OwnerT | None, objtype: type[OwnerT] | None = None) -> InT | OutT:
        if obj is None:
            if objtype is None:
                raise TypeError("no owning class")
            return self._inbound(objtype)
        return self._outbound(obj)


def require(module_name: str, extra: str | None = None) -> ModuleType:
    """
    Import an optional dependency, naming the extra that installs it when missing.

    The default `ImportError` says only that a module is missing; a caller who
    reached here through a facade needs to be told what to install. `extra`
    defaults to the module name, which is how the extras are named.
    """
    from importlib import import_module

    try:
        return import_module(module_name)
    except ImportError as e:
        raise ImportError(
            f"{module_name} is required for this conversion; "
            f"install the `lythonic[{extra or module_name}]` extra"
        ) from e
