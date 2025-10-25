# Placeholder for an empty build.
from pydantic import Field

from lythonic.types import JsonBase


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
