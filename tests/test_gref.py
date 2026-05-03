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


# Factory convention: __ suffix


def _test_factory() -> str:  # pyright: ignore[reportUnusedFunction]
    return "factory_result"


# Cached immutable result — takes priority over factory
_test_cached__ = "cached_value"  # pyright: ignore[reportUnusedVariable]


def _test_cached() -> str:  # pyright: ignore[reportUnusedFunction]
    return "should_not_be_called"


def test_factory_convention_calls_factory():
    gref = GlobalRef("tests.test_gref:_test_factory__")
    result = gref.get_instance()
    assert result == "factory_result"


def test_factory_convention_cached_takes_priority():
    gref = GlobalRef("tests.test_gref:_test_cached__")
    result = gref.get_instance()
    assert result == "cached_value"


def test_factory_convention_no_suffix_direct():
    gref = GlobalRef("tests.test_gref:_test_factory")
    result = gref.get_instance()
    assert callable(result)


def test_factory_convention_missing_raises():
    gref = GlobalRef("tests.test_gref:_nonexistent__")
    try:
        gref.get_instance()
        raise AssertionError("Expected AttributeError")
    except AttributeError:
        pass


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


# NsRef / NsRef tests

from lythonic import NsRef  # pyright: ignore[reportPrivateUsage]


def test_nsref_parse_scope_and_name():
    ref = NsRef("market.data:fetch_prices")
    assert ref.scope == ["market", "data"]
    assert ref.name == "fetch_prices"
    assert str(ref) == "market.data:fetch_prices"


def test_nsref_parse_simple_name():
    ref = NsRef("fetch_prices")
    assert ref.scope == []
    assert ref.name == "fetch_prices"
    assert str(ref) == "fetch_prices"


def test_nsref_parse_empty_scope():
    ref = NsRef(":fetch_prices")
    assert ref.scope == []
    assert ref.name == "fetch_prices"
    assert str(ref) == "fetch_prices"


def test_nsref_parse_empty_name():
    ref = NsRef("branch.path:")
    assert ref.scope == ["branch", "path"]
    assert ref.name == ""
    assert str(ref) == "branch.path:"


def test_nsref_equality_and_hash():
    a = NsRef("market:fetch")
    b = NsRef("market:fetch")
    c = NsRef("market:other")
    assert a == b
    assert a != c
    assert hash(a) == hash(b)
    d = {a: 1}
    assert d[b] == 1


def test_nsref_copy_constructor():
    original = NsRef("x.y:z")
    copy = NsRef(original)
    assert copy.scope == ["x", "y"]
    assert copy.name == "z"
    assert copy == original


def test_nsref_repr():
    ref = NsRef("market:fetch")
    assert repr(ref) == "NsRef('market:fetch')"


# GlobalRef / GlobalRef tests

from lythonic import GlobalRef  # pyright: ignore[reportPrivateUsage]


def test_globalref_is_nsref_subclass():
    gref = GlobalRef("lythonic.compose:Namespace")
    assert isinstance(gref, NsRef)
    assert gref.scope == ["lythonic", "compose"]
    assert gref.name == "Namespace"
    assert str(gref) == "lythonic.compose:Namespace"


def test_globalref_module_property():
    gref = GlobalRef("lythonic.compose:Namespace")
    assert gref.module == "lythonic.compose"


def test_globalref_from_module_uses_scope():
    import json

    gref = GlobalRef(json)
    assert isinstance(gref, NsRef)
    assert gref.scope == ["json"]
    assert gref.name == ""
    assert gref.is_module()
    assert gref.module == "json"


def test_globalref_from_class_uses_scope():
    gref = GlobalRef(GlobalRef)
    assert gref.scope == ["lythonic"]
    assert gref.name == "GlobalRef"
    assert gref.module == "lythonic"


# ModuleRef tests

from lythonic import ModuleRef


def test_moduleref_from_string():
    ref = ModuleRef("lythonic.compose.namespace")
    assert ref.path == "lythonic.compose.namespace"
    assert str(ref) == "lythonic.compose.namespace"
    mod = ref.import_module()
    assert mod.__name__ == "lythonic.compose.namespace"


def test_moduleref_from_module():
    import json

    ref = ModuleRef(json)
    assert ref.path == "json"
    mod = ref.import_module()
    assert mod is json


def test_moduleref_from_globalref():
    gref = GlobalRef("lythonic.compose:Namespace")
    ref = ModuleRef(gref)
    assert ref.path == "lythonic.compose"


def test_moduleref_repr():
    ref = ModuleRef("json")
    assert repr(ref) == "ModuleRef('json')"


def test_moduleref_equality():
    a = ModuleRef("json")
    b = ModuleRef("json")
    c = ModuleRef("os")
    assert a == b
    assert a != c
    assert hash(a) == hash(b)
