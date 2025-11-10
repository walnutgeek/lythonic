# Placeholder for an empty build.
from datetime import UTC, date, datetime
from enum import Enum, IntEnum
from typing import Any

import pytest
from pydantic import BaseModel, Field, ValidationError

from lythonic.types import KNOWN_TYPES, JsonBase, KnownType


class FruitEnum(str, Enum):
    pear = "pear"
    banana = "banana"


class ToolEnum(IntEnum):
    spanner = 1
    wrench = 2


class CookingModel(JsonBase):
    fruit: FruitEnum = FruitEnum.pear
    tool: ToolEnum = ToolEnum.spanner


class CookingBase(BaseModel):
    fruit: FruitEnum = FruitEnum.pear
    tool: ToolEnum = ToolEnum.spanner


def test_enum():
    assert (
        repr(CookingModel())
        == "CookingModel(type_gref=GlobalRef('tests.test_types:CookingModel'), fruit=<FruitEnum.pear: 'pear'>, tool=<ToolEnum.spanner: 1>)"
    )
    assert (
        repr(CookingModel(tool=2, fruit="banana"))  # pyright: ignore
        == "CookingModel(type_gref=GlobalRef('tests.test_types:CookingModel'), fruit=<FruitEnum.banana: 'banana'>, tool=<ToolEnum.wrench: 2>)"
    )
    with pytest.raises(ValidationError):
        CookingModel(fruit="other")  # pyright: ignore


class A(JsonBase):
    x: int
    y: str | None = Field(default=None)


class B(A):
    z: float = Field(default=0.0)


def test_json_base():
    a = A(x=1, y="hello")
    b = B(x=2, y="world", z=3.14)
    assert a.to_json() == {"type_gref": "tests.test_types:A", "x": 1, "y": "hello"}
    assert b.to_json() == {"type_gref": "tests.test_types:B", "x": 2, "y": "world", "z": 3.14}
    assert JsonBase.from_json(a.to_json()).to_json() == a.to_json()
    assert A.from_json(b.to_json()).to_json() == b.to_json()
    assert B.from_json({"x": 1, "y": "hello"}).to_json() == {
        "type_gref": "tests.test_types:B",
        "x": 1,
        "y": "hello",
        "z": 0.0,
    }
    assert A.from_json({"x": 1, "y": "hello"}).to_json() == {
        "type_gref": "tests.test_types:A",
        "x": 1,
        "y": "hello",
    }
    # jb = JsonBase(type_gref=GlobalRef(JsonBase))
    # assert jb.type_gref == GlobalRef(JsonBase)


@pytest.mark.debug
def test_known_types():
    def do_roundtrip_by_type(raw: Any, map_to: str | None = None, fail: bool = False) -> None:
        ktype: KnownType = KNOWN_TYPES.resolve_type(type(raw))  # pyright: ignore
        mapped_str = ktype.string.map_to(raw)
        mapped_json = ktype.json.map_to(raw)
        mapped_db = ktype.db.map_to(raw)
        actual = f"{mapped_str=} -> {mapped_json=} -> {mapped_db=}"
        if map_to is not None:
            assert map_to == actual, f"Expected {map_to} but got {actual}"
        else:
            print(f'do_roundtrip_by_type({raw!r}, "{actual}")')
        assert ktype.string.map_from(mapped_str) == raw, (
            f"Failed to roundtrip {raw} by string. ktype: {ktype}"
        )
        assert ktype.json.map_from(mapped_json) == raw, (
            f"Failed to roundtrip {raw} by json. ktype: {ktype}"
        )
        assert ktype.db.map_from(mapped_db) == raw, (
            f"Failed to roundtrip {raw} by db. ktype: {ktype}"
        )
        if fail:
            raise AssertionError

    do_roundtrip_by_type(
        date(2025, 11, 6),
        "mapped_str='2025-11-06' -> mapped_json='2025-11-06' -> mapped_db='2025-11-06'",
    )
    do_roundtrip_by_type(
        datetime(2025, 11, 7, 7, 58, 51, 107831, tzinfo=UTC),
        "mapped_str='2025-11-07T07:58:51.107831+00:00' -> mapped_json='2025-11-07T07:58:51.107831+00:00' -> mapped_db='2025-11-07T07:58:51.107831+00:00'",
    )
    do_roundtrip_by_type(1, "mapped_str='1' -> mapped_json=1 -> mapped_db=1")
    do_roundtrip_by_type(True, "mapped_str='True' -> mapped_json=True -> mapped_db=True")
    do_roundtrip_by_type(5.4, "mapped_str='5.4' -> mapped_json=5.4 -> mapped_db=5.4")
    do_roundtrip_by_type("hello", "mapped_str='hello' -> mapped_json='hello' -> mapped_db='hello'")
    do_roundtrip_by_type(
        b"hello", "mapped_str='aGVsbG8=' -> mapped_json='aGVsbG8=' -> mapped_db=b'hello'"
    )
    assert FruitEnum not in KNOWN_TYPES.types_by_type
    assert ToolEnum not in KNOWN_TYPES.types_by_type
    do_roundtrip_by_type(
        FruitEnum.pear, "mapped_str='pear' -> mapped_json='pear' -> mapped_db='pear'"
    )
    do_roundtrip_by_type(ToolEnum.spanner, "mapped_str='1' -> mapped_json=1 -> mapped_db=1")
    assert FruitEnum in KNOWN_TYPES.types_by_type
    assert ToolEnum in KNOWN_TYPES.types_by_type

    assert CookingBase not in KNOWN_TYPES.types_by_type
    assert CookingModel not in KNOWN_TYPES.types_by_type

    do_roundtrip_by_type(
        CookingModel(fruit="banana"),  # pyright: ignore
        """mapped_str='{"type_gref": "tests.test_types:CookingModel", "fruit": "banana", "tool": 1}' -> mapped_json={'type_gref': 'tests.test_types:CookingModel', 'fruit': 'banana', 'tool': 1} -> mapped_db='{"type_gref": "tests.test_types:CookingModel", "fruit": "banana", "tool": 1}'""",
    )
    do_roundtrip_by_type(
        CookingBase(fruit="banana"),  # pyright: ignore
        """mapped_str='{"fruit": "banana", "tool": 1}' -> mapped_json={'fruit': 'banana', 'tool': 1} -> mapped_db='{"fruit": "banana", "tool": 1}'""",
    )

    assert CookingBase in KNOWN_TYPES.types_by_type
    assert CookingModel not in KNOWN_TYPES.types_by_type
