"""
Types: Type mapping system for converting between Python, JSON, and SQLite types.

This module provides a registry of known types and their conversion functions
for seamless data transformation between different representations.

## Core Concepts

- `KnownType`: A registered type with bidirectional mappings (string, JSON, DB)
- `KnownTypesMap`: Registry of all known types, accessed via `KNOWN_TYPES`
- `JsonBase`: Pydantic BaseModel with type discrimination via `type_gref`

## Type Mappings

Each `KnownType` provides three `MapPair`s for converting to/from:
- **string**: For CLI arguments, environment variables, config files
- **json**: For JSON serialization (API responses, storage)
- **db**: For SQLite storage (maps to INTEGER, REAL, TEXT, or BLOB)

## Built-in Types

Primitives: `int`, `float`, `str`, `bool`, `bytes`
Dates: `date`, `datetime` (ISO format strings)
Enums: `Enum` (string values), `IntEnum` (integer values)
Pydantic: `BaseModel`, `JsonBase` (JSON dicts)
Paths: `Path` (string representation)

## Usage

```python
from lythonic.types import KNOWN_TYPES, KnownType

# Resolve a type
kt = KNOWN_TYPES.resolve_type(datetime)

# Convert from string
dt = kt.string.map_from("2024-01-15T10:30:00")

# Convert to JSON
json_val = kt.json.map_to(dt)  # "2024-01-15T10:30:00"

# Convert to DB
db_val = kt.db.map_to(dt)  # "2024-01-15T10:30:00"
```

## Registering Custom Types

```python
from lythonic.types import KNOWN_TYPES, KnownTypeArgs

KNOWN_TYPES.register(
    KnownTypeArgs(
        concrete_type=MyType,
        map_from_string=MyType.parse,
        map_to_string=str,
    )
)
```

## JsonBase

`JsonBase` extends Pydantic's `BaseModel` with automatic type discrimination:

```python
from lythonic.types import JsonBase

class Dog(JsonBase):
    name: str

class Cat(JsonBase):
    name: str

# Serialize includes type reference
dog = Dog(name="Rex")
data = dog.to_json()  # {"type_gref": "mymodule:Dog", "name": "Rex"}

# Deserialize resolves correct type
animal = JsonBase.from_json(data)  # Returns Dog instance
```
"""

from __future__ import annotations

import base64
import inspect
import json as _json
from collections.abc import Callable
from datetime import date, datetime
from enum import Enum, IntEnum
from pathlib import Path
from typing import Any, Generic, Self, TypeVar, cast

from pydantic import (
    BaseModel,
    model_validator,
)
from typing_extensions import override

from lythonic import GlobalRef, GRef, str_or_none

# using this to allow for monkeypatching for NumPy
json_loads = _json.loads
json_dumps = _json.dumps


def base_model_to_json(bm: BaseModel) -> dict[str, Any]:
    return bm.model_dump(mode="json")


class JsonBase(BaseModel):
    type_gref: GRef | None = None

    @model_validator(mode="before")
    @classmethod
    def set_gref(cls, data: dict[str, Any]) -> dict[str, Any]:
        data["type_gref"] = str(GlobalRef(cls))
        return data

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> Self:
        if "type_gref" in json:
            ref = GlobalRef(json["type_gref"])
            cls_from_ref = ref.get_instance()
            if issubclass(cls_from_ref, cls):
                return cls_from_ref.model_validate(json)
        return cls.model_validate(json)

    def to_json(self) -> dict[str, Any]:
        return base_model_to_json(self)


def encode_base64(bb: bytes) -> str:
    return base64.b64encode(bb).decode()


def ensure_bytes(input: str | bytes) -> bytes:
    if isinstance(input, str):
        input = base64.b64decode(input)
    return input


def do_identity(x: Any) -> Any:
    return x


def passthru_none(
    *fns: Callable[[Any], Any], if_none: Callable[[Any], Any] | None = None
) -> Callable[[Any], Any]:
    if if_none is not None:
        fn = if_none
    else:
        if not fns:
            return do_identity
        if len(fns) == 1:
            fn = fns[0]
            if fn is str:
                return str_or_none
        else:

            def do_combined(x: Any) -> Any:
                r = x
                previous: Callable[[Any], Any] | None = None
                for f in fns:
                    if previous != f:
                        r = f(r)
                    previous = f
                return r

            fn = do_combined
    if (
        fn.__name__ == "do_someCaZieSoNo1come_up_with_this_name"
        or fn == do_identity
        or fn == str_or_none
    ):
        return fn

    def do_someCaZieSoNo1come_up_with_this_name(x: Any) -> Any:
        if x is None:
            return x
        else:
            return fn(x)

    return do_someCaZieSoNo1come_up_with_this_name


T = TypeVar("T")


class MapPair(Generic[T]):
    target_type: type
    map_from: Callable[[T], Any]
    map_to: Callable[[Any], T]

    def __init__(
        self, map_from: Callable[[T], Any], map_to: Callable[[Any], T], target_type: type
    ) -> None:
        self.map_from = map_from
        self.map_to = map_to
        self.target_type = target_type


def is_primitive(type_: type) -> bool:
    """could be used in json and db as it is"""
    return type_ in (int, float, bool, str)


class DbTypeInfo(Enum):
    def __init__(self, input_types: tuple[type, ...], target_type: type):
        assert isinstance(input_types, tuple), f"input_types must be tuple, not {type(input_types)}"
        cls = self.__class__
        if not hasattr(cls, "_value2member_map_"):
            cls._value2member_map_ = {}
        member_map = cls._value2member_map_
        for k in input_types:
            assert k not in member_map, f"Duplicate input type {k} in {self.name}"
            member_map[k] = self
        self.input_types = input_types
        self.target_type = target_type

    INTEGER = ((int, bool), int)
    REAL = ((float,), float)
    TEXT = ((str,), str)
    BLOB = ((bytes,), bytes)

    @classmethod
    def from_type(cls, t: type) -> DbTypeInfo:
        return cast(
            DbTypeInfo,
            cls._value2member_map_[t],
        )

    @classmethod
    def is_db_type(cls, type_: type) -> bool:
        """could be used in db as it is"""
        return type_ in cls._value2member_map_


class KnownTypeArgs(BaseModel):
    map_from_string: Callable[[str], Any] | None = None
    map_to_string: Callable[[Any], str] | None = None
    map_from_json: Callable[[Any], Any] | None = None
    map_to_json: Callable[[Any], Any] | None = None
    map_from_db: Callable[[Any], Any] | None = None
    map_to_db: Callable[[Any], Any] | None = None
    db_type: type | None = None
    json_type: type | None = None
    concrete_type: type | None = None
    abstract_type: type | None = None
    is_factory: bool = False
    name: str | None = None
    aliases: list[str] | None = None

    def get_type(self) -> type:
        assert self.abstract_type is not None or self.concrete_type is not None
        if self.concrete_type is not None:
            assert self.abstract_type is None and not self.is_factory, (
                "concrete types cannot be factories"
            )
            return self.concrete_type
        assert self.abstract_type is not None, " Redundant but no other way to shut up pyright"
        return self.abstract_type

    def is_abstract(self) -> bool:
        return self.abstract_type is self.get_type()

    def build_concrete_type(self, concrete_type: type) -> Self:
        assert self.is_factory, "only factory types can build concrete types"
        super_type = self.get_type()
        assert issubclass(concrete_type, super_type), (
            f"concrete type {concrete_type} is not a subclass of abstract type {super_type}"
        )
        clone = self.model_copy()
        clone.concrete_type = concrete_type
        clone.abstract_type = None
        clone.is_factory = False

        def remap_constructor_from_super_type_to_concrete_type(
            x: Callable[[Any], Any] | None,
        ) -> Callable[[Any], Any] | None:
            if x is None:
                return None
            if x is super_type:  # switch to constructor of concrete type
                return concrete_type
            if (
                inspect.ismethod(x) and x.__self__ is super_type
            ):  # switch to class  method of concrete type
                return getattr(concrete_type, x.__name__)
            return x  # leave as is

        clone.map_from_string = remap_constructor_from_super_type_to_concrete_type(
            clone.map_from_string
        )
        clone.map_from_json = remap_constructor_from_super_type_to_concrete_type(
            clone.map_from_json
        )
        clone.map_from_db = remap_constructor_from_super_type_to_concrete_type(clone.map_from_db)
        return clone

    def _set_string_defaults(self) -> None:
        if self.db_type is None:
            self.db_type = str
        if self.json_type is None:
            self.json_type = str
        self.map_from_string = passthru_none(self.get_type(), if_none=self.map_from_string)
        self.map_to_string = passthru_none(str, if_none=self.map_to_string)
        self.map_from_json = passthru_none(self.map_from_string, if_none=self.map_from_json)
        self.map_to_json = passthru_none(self.map_to_string, if_none=self.map_to_json)
        self.map_from_db = passthru_none(self.map_from_string, if_none=self.map_from_db)
        self.map_to_db = passthru_none(self.map_to_string, if_none=self.map_to_db)

    def _set_db_defaults(self) -> None:
        assert self.db_type is not None and self.map_from_db is not None
        self.map_to_db = passthru_none(self.db_type, if_none=self.map_to_db)
        if self.db_type is bytes:
            if self.json_type is None:
                self.json_type = str
            self.map_from_json = passthru_none(ensure_bytes, if_none=self.map_from_json)
            self.map_to_json = passthru_none(encode_base64, if_none=self.map_to_json)
            self.map_from_string = passthru_none(
                ensure_bytes, self.map_from_db, if_none=self.map_from_string
            )
            self.map_to_string = passthru_none(
                self.map_to_db, encode_base64, if_none=self.map_to_string
            )
        else:
            if self.json_type is None:
                self.json_type = self.db_type
            self.map_from_json = passthru_none(self.map_from_db, if_none=self.map_from_json)
            self.map_to_json = passthru_none(self.map_to_db, if_none=self.map_to_json)
            self.map_from_string = passthru_none(
                self.db_type, self.map_from_db, if_none=self.map_from_string
            )
            self.map_to_string = passthru_none(self.map_to_db, str, if_none=self.map_to_string)

    def _set_json_defaults(self) -> bool:
        if self.map_from_json is not None and self.map_to_json is not None:
            if self.db_type is None:
                self.db_type = str
            if self.json_type is None:
                self.json_type = dict
            self.map_from_json = passthru_none(self.map_from_json)
            self.map_to_json = passthru_none(self.map_to_json)
            self.map_from_string = passthru_none(
                json_loads, self.map_from_json, if_none=self.map_from_string
            )
            self.map_to_string = passthru_none(
                self.map_to_json, json_dumps, if_none=self.map_to_string
            )
            self.map_from_db = passthru_none(self.map_from_string, if_none=self.map_from_db)
            self.map_to_db = passthru_none(self.map_to_string, if_none=self.map_to_db)
            return True
        return False

    def resolve_the_rest(self) -> Self:
        assert not self.is_factory, "factory types cannot resolve the rest"
        if self.concrete_type is not None:
            assert self.abstract_type is None
            if self.map_from_string is not None or self.map_to_string is not None:
                self._set_string_defaults()
            else:
                if self.db_type is None and DbTypeInfo.is_db_type(self.concrete_type):  # pyright: ignore
                    self.db_type = self.concrete_type
                if self.db_type is not None:
                    self.map_from_db = passthru_none(
                        self.db_type, self.concrete_type, if_none=self.map_from_db
                    )
                    self._set_db_defaults()
                else:
                    self._set_json_defaults()

        else:
            assert self.abstract_type is not None
            assert (
                self.map_from_string is not None
                or self.map_from_json is not None
                or self.map_from_db is not None
            )
            if self.map_from_string is not None:
                self._set_string_defaults()
            elif self._set_json_defaults():
                pass
            elif self.db_type is not None:
                assert self.map_from_db is not None
                self.map_from_db = passthru_none(self.map_from_db)
                self._set_db_defaults()

        return self


class KnownType:
    name: str
    aliases: set[str]
    type_: type
    is_abstract: bool
    string: MapPair[str]
    json: MapPair[Any]
    db: MapPair[Any]

    @classmethod
    def ensure(cls, type_: type | str | KnownType) -> KnownType:
        if isinstance(type_, KnownType):
            return type_
        return KNOWN_TYPES.resolve_type(type_)

    def __init__(
        self,
        args: KnownTypeArgs,
    ) -> None:
        assert not args.is_factory, "factory types cannot be initialized directly"
        self.type_ = args.get_type()
        self.is_abstract = args.is_abstract()
        args.resolve_the_rest()
        assert (
            args.db_type is not None
            and args.map_from_db is not None
            and args.map_to_db is not None
            and args.json_type is not None
            and args.map_from_json is not None
            and args.map_to_json is not None
            and args.map_from_string is not None
            and args.map_to_string is not None
        ), f"{self.type_} has not been resolved"

        self.string = MapPair(args.map_from_string, args.map_to_string, str)  # pyright: ignore
        self.json = MapPair(args.map_from_json, args.map_to_json, args.json_type)
        self.db = MapPair(args.map_from_db, args.map_to_db, args.db_type)
        self.name = (args.name if args.name is not None else self.type_.__name__).lower()
        self.aliases = set()
        self.aliases.add(self.name)
        if args.aliases is not None:
            for alias in args.aliases:
                self.aliases.add(alias.lower())

    def get_type(self) -> type:
        return self.type_

    @property
    def db_type_info(self) -> DbTypeInfo:
        return DbTypeInfo.from_type(self.db.target_type)

    @override
    def __repr__(self) -> str:
        return f"KnownType({self.name!r}, {self.type_!r})"


class AbstractTypeHeap:
    type_: type
    ktype: KnownType | KnownTypeArgs | None
    parent: AbstractTypeHeap | None
    children: list[AbstractTypeHeap]

    def __init__(
        self,
        type_: type,
        ktype: KnownType | KnownTypeArgs | None = None,
        parent: AbstractTypeHeap | None = None,
    ) -> None:
        self.type_ = type_
        self.ktype = ktype
        self.parent = parent
        self.children = []
        if parent is not None:
            still_parents_children: list[AbstractTypeHeap] = []
            for child in parent.children:
                m = self.matches_exactly(child.type_)
                assert m is not True
                if m is None:
                    still_parents_children.append(child)
                else:
                    self.children.append(child)
            if len(self.children):
                parent.children = still_parents_children
            parent.children.append(self)

    def is_root(self) -> bool:
        return self.parent is None and self.type_ is object

    @staticmethod
    def root() -> AbstractTypeHeap:
        return AbstractTypeHeap(object)

    def matches_exactly(self, type_in_question: type) -> bool | None:
        if self.type_ is type_in_question:
            return True
        if issubclass(type_in_question, self.type_):
            return False
        return None

    def find(
        self, type_in_question: type
    ) -> tuple[AbstractTypeHeap | None, AbstractTypeHeap | None]:
        assert self.is_root(), "it has to be called on root of the heap"
        m = self.matches_exactly(type_in_question)
        if m is True:
            return self, self.parent
        assert m is not None, f"it has to match root() or not to get here: {self}"
        return self._find_in_children(type_in_question)

    def _find_in_children(
        self, type_in_question: type
    ) -> tuple[AbstractTypeHeap | None, AbstractTypeHeap | None]:
        for child in self.children:
            m = child.matches_exactly(type_in_question)
            if m:
                return child, self
            if m is False:
                return child._find_in_children(type_in_question)
        return None, self

    def add(self, t: KnownType | KnownTypeArgs) -> None:
        found, found_parent = self.find(t.get_type())
        assert found is None, (
            f"Duplicate abstract type {t.get_type} but generally should not happen found= {found}, found_parent={found_parent}"
        )
        AbstractTypeHeap(t.get_type(), t, found_parent)

    @override
    def __repr__(self) -> str:
        return f"AbstractTypeHeap: {self.type_}, {len(self.children)}, {self.parent}"


class KnownTypesMap:
    types: dict[str, KnownType]
    types_by_type: dict[type, KnownType]
    abstract_types: AbstractTypeHeap

    def __init__(self) -> None:
        self.types = {}
        self.types_by_type = {}
        self.abstract_types = AbstractTypeHeap.root()

    def register(self, *array_of_args: KnownTypeArgs) -> None:
        for args in array_of_args:
            if not args.is_factory:
                self.register_type(KnownType(args))
            else:
                self.abstract_types.add(args)

    def register_type(self, t: KnownType) -> None:
        for alias in t.aliases:
            if alias not in self.types:
                self.types[alias] = t
        assert t.get_type() not in self.types_by_type, f"Duplicate type {t.get_type()}"
        self.types_by_type[t.type_] = t
        if t.is_abstract:
            self.abstract_types.add(t)

    def resolve_type(self, type_: type | str) -> KnownType:
        """
        >>> KNOWN_TYPES.resolve_type(int)
        KnownType('int', <class 'int'>)
        >>> KNOWN_TYPES.resolve_type("int")
        KnownType('int', <class 'int'>)
        >>> KNOWN_TYPES.resolve_type("zzz")
        Traceback (most recent call last):
        ...
        ValueError: Unknown type zzz
        >>> KNOWN_TYPES.resolve_type(dict)
        Traceback (most recent call last):
        ...
        ValueError: Unknown type <class 'dict'>
        >>> KNOWN_TYPES.resolve_type(3)
        Traceback (most recent call last):
        ...
        TypeError: issubclass() arg 1 must be a class
        """
        if isinstance(type_, str):
            type_ = type_.lower()
            if type_ in self.types:
                return self.types[type_]
        else:
            if type_ in self.types_by_type:
                return self.types_by_type[type_]
            found, super_type = self.abstract_types.find(type_)
            assert found is None, (
                f"Why it is not in types_by_type found={found}, super_type={super_type}"
            )
            if super_type is not None and not super_type.is_root():
                assert super_type.ktype is not None
                if isinstance(super_type.ktype, KnownTypeArgs):
                    args: KnownTypeArgs = super_type.ktype
                    ktype: KnownType = KnownType(args.build_concrete_type(type_))
                    self.register_type(ktype)
                    return ktype
                assert isinstance(super_type.ktype, KnownType), (
                    f"super_type.ktype is not a KnownType: {super_type.ktype}"
                )
                return super_type.ktype
        raise ValueError(f"Unknown type {type_}")


KNOWN_TYPES = KnownTypesMap()


KNOWN_TYPES.register(
    KnownTypeArgs(concrete_type=int),
    KnownTypeArgs(concrete_type=float),
    KnownTypeArgs(concrete_type=str, aliases=["literal"]),
    KnownTypeArgs(concrete_type=bool),
    KnownTypeArgs(concrete_type=bytes),
    KnownTypeArgs(
        concrete_type=date,
        map_to_string=lambda x: x.isoformat(),
        map_from_string=date.fromisoformat,
    ),
    KnownTypeArgs(
        concrete_type=datetime,
        map_to_string=lambda x: x.isoformat(),
        map_from_string=datetime.fromisoformat,
    ),
    KnownTypeArgs(abstract_type=Path, map_from_string=Path),
    KnownTypeArgs(
        abstract_type=Enum, is_factory=True, map_from_string=Enum, map_to_string=lambda x: x.value
    ),
    KnownTypeArgs(abstract_type=IntEnum, db_type=int, is_factory=True, map_from_db=IntEnum),
    KnownTypeArgs(
        abstract_type=BaseModel,
        json_type=dict,
        is_factory=True,
        map_from_json=BaseModel.model_validate,
        map_to_json=base_model_to_json,
    ),
    KnownTypeArgs(
        abstract_type=JsonBase,
        json_type=dict,
        map_from_json=JsonBase.from_json,
        map_to_json=base_model_to_json,
    ),
    # TODO: when appropriate packages are available entries below should be initialized
    # KnownType("dataframe", pd.DataFrame, json_type=dict),
)
