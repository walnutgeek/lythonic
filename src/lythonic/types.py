import base64
import json as _json
from collections.abc import Callable
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Generic, Self, TypeVar, cast

from pydantic import (
    BaseModel,
    model_validator,
)
from typing_extensions import override

from lythonic import GlobalRef, GRef

# using this to allow for monkeypatching for NumPy
json_loads = _json.loads
json_dumps = _json.dumps


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
        return self.model_dump(mode="json")


def encode_base64(bb: bytes) -> str:
    return base64.b64encode(bb).decode()


def ensure_bytes(input: str | bytes) -> bytes:
    if isinstance(input, str):
        input = base64.b64decode(input)
    return input


def do_identity(x: Any) -> Any:
    return x


def str_or_none(s: Any) -> str | None:
    """
    >>> str_or_none(None)
    >>> str_or_none(5)
    '5'
    >>> str_or_none('')
    ''
    """
    return str(s) if s is not None else None


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


class SQLiteTypes(Enum):
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
    def from_type(cls, t: type) -> "SQLiteTypes":
        return cast(
            SQLiteTypes,
            cls._value2member_map_[t],
        )

    @classmethod
    def is_db_type(cls, type_: type) -> bool:
        """could be used in db as it is"""
        return type_ in cls._value2member_map_


def not_nones(*vals: Any) -> tuple[bool, ...]:
    return tuple(v is not None for v in vals)


class KnownType:
    name: str
    aliases: set[str]
    type_: type
    is_abstract: bool
    string: MapPair[str]
    json: MapPair[Any]
    db: MapPair[Any]

    @classmethod
    def ensure(cls, type_: Any) -> "KnownType":
        if isinstance(type_, KnownType):
            return type_
        return KNOWN_TYPES.resolve_type(type_)

    def __init__(
        self,
        name: str | None = None,
        aliases: list[str] | None = None,
        map_from_string: Callable[[str], Any] | None = None,
        map_to_string: Callable[[Any], str] | None = None,
        concrete_type: type | None = None,
        abstract_type: type | None = None,
        map_from_json: Callable[[Any], Any] | None = None,
        map_to_json: Callable[[Any], Any] | None = None,
        json_type: type | None = None,
        map_from_db: Callable[[Any], Any] | None = None,
        map_to_db: Callable[[Any], Any] | None = None,
        db_type: type | None = None,
    ) -> None:
        assert abstract_type is not None or concrete_type is not None
        if concrete_type is not None:
            assert abstract_type is None
            self.type_ = concrete_type
            self.is_abstract = False
            if map_from_string is not None or map_to_string is not None:
                if db_type is None:
                    db_type = str
                if json_type is None:
                    json_type = str
                map_from_string = passthru_none(concrete_type, if_none=map_from_string)
                map_to_string = passthru_none(str, if_none=map_to_string)
                map_from_json = passthru_none(map_from_string, if_none=map_from_json)
                map_to_json = passthru_none(map_to_string, if_none=map_to_json)
                map_from_db = passthru_none(map_from_string, if_none=map_from_db)
                map_to_db = passthru_none(map_to_string, if_none=map_to_db)
            else:
                if db_type is None and SQLiteTypes.is_db_type(concrete_type):
                    db_type = concrete_type
                if db_type is not None:
                    map_from_db = passthru_none(db_type, concrete_type, if_none=map_from_db)
                    map_to_db = passthru_none(db_type, if_none=map_to_db)
                    if db_type is bytes:
                        if json_type is None:
                            json_type = bytes
                        map_from_json = passthru_none(ensure_bytes, if_none=map_from_json)
                        map_to_json = passthru_none(encode_base64, if_none=map_to_json)
                        map_from_string = passthru_none(ensure_bytes, if_none=map_from_string)
                        map_to_string = passthru_none(encode_base64, if_none=map_to_string)
                    else:
                        if json_type is None:
                            json_type = db_type
                        map_from_json = passthru_none(map_from_db, if_none=map_from_json)
                        map_to_json = passthru_none(map_to_db, if_none=map_to_json)
                        map_from_string = passthru_none(concrete_type, if_none=map_from_string)
                        map_to_string = passthru_none(str, if_none=map_to_string)

        else:
            assert abstract_type is not None
            self.type_ = abstract_type
            self.is_abstract = True
            assert (
                map_from_string is not None or map_from_json is not None or map_from_db is not None
            )
            if map_from_string is not None:
                if db_type is None:
                    db_type = str
                if json_type is None:
                    json_type = str
                map_from_string = passthru_none(map_from_string)
                map_to_string = passthru_none(str, if_none=map_to_string)
                map_from_json = passthru_none(map_from_string, if_none=map_from_json)
                map_to_json = passthru_none(map_to_string, if_none=map_to_json)
                map_from_db = passthru_none(map_from_string, if_none=map_from_db)
                map_to_db = passthru_none(map_to_string, if_none=map_to_db)
            elif map_from_json is not None:
                if db_type is None:
                    db_type = str
                if json_type is None:
                    json_type = dict
                map_from_json = passthru_none(map_from_json)
                map_to_json = passthru_none(lambda j: j.to_json(), if_none=map_to_json)
                map_from_string = passthru_none(json_loads, map_from_json, if_none=map_from_string)
                map_to_string = passthru_none(map_to_json, json_dumps, if_none=map_to_string)
                map_from_db = passthru_none(map_from_string, if_none=map_from_db)
                map_to_db = passthru_none(map_to_string, if_none=map_to_db)
            elif db_type is not None:
                assert map_from_db is not None
                map_from_db = passthru_none(map_from_db)
                map_to_db = passthru_none(db_type, if_none=map_to_db)
                if db_type is bytes:
                    if json_type is None:
                        json_type = str
                    map_from_json = passthru_none(ensure_bytes, if_none=map_from_json)
                    map_to_json = passthru_none(encode_base64, if_none=map_to_json)
                    map_from_string = passthru_none(
                        ensure_bytes, map_from_db, if_none=map_from_string
                    )
                    map_to_string = passthru_none(map_to_db, encode_base64, if_none=map_to_string)
                else:
                    if json_type is None:
                        json_type = db_type
                    map_from_json = passthru_none(map_from_db, if_none=map_from_json)
                    map_to_json = passthru_none(map_to_db, if_none=map_to_json)
                    map_from_string = passthru_none(db_type, map_from_db, if_none=map_from_string)
                    map_to_string = passthru_none(map_to_db, str, if_none=map_to_string)

        assert (
            db_type is not None
            and map_from_db is not None
            and map_to_db is not None
            and json_type is not None
            and map_from_json is not None
            and map_to_json is not None
            and map_from_string is not None
            and map_to_string is not None
        )
        self.string = MapPair(map_from_string, map_to_string, str)
        self.json = MapPair(map_from_json, map_to_json, json_type)
        self.db = MapPair(map_from_db, map_to_db, db_type)

        self.name = (name if name is not None else self.type_.__name__).lower()
        self.aliases = set()
        self.aliases.add(self.name)
        if aliases is not None:
            for alias in aliases:
                self.aliases.add(alias.lower())

    @property
    def sqlite_type(self) -> SQLiteTypes:
        return SQLiteTypes.from_type(self.db.target_type)

    @override
    def __repr__(self) -> str:
        return f"KnownType({self.name!r}, {self.type_!r})"


class AbstractTypeHeap:
    type_: type
    ktype: KnownType | None
    parent: "AbstractTypeHeap|None"
    children: list["AbstractTypeHeap"]

    def __init__(
        self, type_: type, ktype: KnownType | None = None, parent: "AbstractTypeHeap|None" = None
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
    def root() -> "AbstractTypeHeap":
        return AbstractTypeHeap(object)

    def matches_exactly(self, type_in_question: type) -> bool | None:
        if self.type_ is type_in_question:
            return True
        if issubclass(type_in_question, self.type_):
            return False
        return None

    def find(self, type_in_question: type) -> "tuple[AbstractTypeHeap|None, AbstractTypeHeap|None]":
        assert self.is_root(), "it has to be called on root of the heap"
        m = self.matches_exactly(type_in_question)
        if m is True:
            return self, self.parent
        assert m is not None, f"it has to match root() or not to get here: {self}"
        return self._find_in_children(type_in_question)

    def _find_in_children(
        self, type_in_question: type
    ) -> "tuple[AbstractTypeHeap|None, AbstractTypeHeap|None]":
        for child in self.children:
            m = child.matches_exactly(type_in_question)
            if m:
                return child, self
            if m is False:
                return child._find_in_children(type_in_question)
        return None, self

    def add(self, t: KnownType) -> None:
        found, found_parent = self.find(t.type_)
        assert found is None, (
            f"Duplicate abstract type {t.type_} but generally should not happen found= {found}, found_parent={found_parent}"
        )
        AbstractTypeHeap(t.type_, t, found_parent)

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

    def register(self, *types: KnownType) -> None:
        for t in types:
            for alias in t.aliases:
                assert alias not in self.types, f"Duplicate alias {alias} for {t}"
                self.types[alias] = t
            assert t.type_ not in self.types_by_type, f"Duplicate type {t.type_} for {t}"
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
                return super_type.ktype
        raise ValueError(f"Unknown type {type_}")


KNOWN_TYPES = KnownTypesMap()


KNOWN_TYPES.register(
    KnownType(concrete_type=int),
    KnownType(concrete_type=float),
    KnownType(concrete_type=str),
    KnownType(concrete_type=bool),
    KnownType(concrete_type=bytes),
    KnownType(
        concrete_type=date,
        map_to_string=lambda x: x.isoformat(),
        map_from_string=date.fromisoformat,
    ),
    KnownType(
        concrete_type=datetime,
        map_to_string=lambda x: x.isoformat(),
        map_from_string=datetime.fromisoformat,
    ),
    KnownType(abstract_type=Path, map_from_string=Path),
    # TODO: when appropriate packages are available entries below should be initialized
    # KnownType("interval", Interval, json_type=str),
    # KnownType("dataframe", pd.DataFrame, json_type=dict),
)
