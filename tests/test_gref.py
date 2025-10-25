import asyncio
from typing import Any, final

from lythonic import GlobalRef, Result


def s2():
    return 2


async def a1():
    return 1


@final
class A2:
    def __init__(self, config: dict[str, Any]):
        config = dict(config)
        self.a = config.pop("a", 2)
        assert config == {}, f"not supported keys in config: {config}"

    async def __call__(self, *args: Any):
        return self.a + args[0]


@final
class M3:
    def __init__(self, config: dict[str, Any]):
        config = dict(config)
        self.i = config.pop("i", 3)
        assert config == {}, f"not supported keys in config: {config}"

    def __call__(self, *args: Any):
        return self.i * args[0]


def test_coros():
    for gref in [GlobalRef("asyncio"), GlobalRef(asyncio)]:
        assert not gref.is_async()
        assert not gref.is_function()
        assert not gref.is_class()
        assert gref.is_module()
        assert gref.get_module() == asyncio

    gref = GlobalRef(a1)
    assert gref.is_async()
    assert gref.is_function()
    assert not gref.is_class()
    assert not gref.is_module()
    assert gref.get_instance() == a1

    gref = GlobalRef(s2)
    assert not gref.is_async()
    assert gref.is_function()
    assert not gref.is_class()
    assert not gref.is_module()
    assert gref.get_instance() == s2

    gref = GlobalRef(A2)
    assert gref.is_async()
    assert not gref.is_function()
    assert gref.is_class()
    assert not gref.is_module()
    assert gref.get_instance() == A2

    gref = GlobalRef(M3)
    assert not gref.is_async()
    assert not gref.is_function()
    assert gref.is_class()
    assert not gref.is_module()
    assert gref.get_instance() == M3


def test_result_ok_err_unwrap():
    ok: Result[int, str] = Result[int, str].Ok(123)
    err: Result[int, str] = Result[int, str].Err("fail")
    assert ok.is_ok()
    assert not ok.is_err()
    assert ok.ok() == 123
    assert ok.err() is None
    assert ok.unwrap() == 123
    try:
        ok.unwrap_err()
    except ValueError as e:
        assert str(e) == "Called unwrap_err on Ok: 123"
    else:
        raise AssertionError("Expected ValueError on unwrap_err for Ok")

    assert err.is_err()
    assert not err.is_ok()
    assert err.err() == "fail"
    assert err.ok() is None
    assert err.unwrap_err() == "fail"
    try:
        err.unwrap()
    except ValueError as e:
        assert str(e) == "Called unwrap on Err: fail"
    else:
        raise AssertionError("Expected ValueError on unwrap for Err")

    assert repr(ok) == "Ok(123)"
    assert repr(err) == "Err('fail')"
