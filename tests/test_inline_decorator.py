from __future__ import annotations

from lythonic.compose.namespace import inline


def test_inline_sets_attribute():
    @inline
    def add(a: int, b: int) -> int:
        return a + b

    assert getattr(add, "_lythonic_inline", False) is True


def test_inline_preserves_callable():
    @inline
    def add(a: int, b: int) -> int:
        return a + b

    assert add(2, 3) == 5


def test_unmarked_function_has_no_attribute():
    def add(a: int, b: int) -> int:
        return a + b

    assert getattr(add, "_lythonic_inline", False) is False
