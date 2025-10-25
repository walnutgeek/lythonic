# import asyncio
# from collections.abc import Callable
# from typing import Any, final

# from lythonic import GlobalRef

# from lythonic.logic import Logic

# def assert_ae(
#     call: Callable[[], Any],
#     expected_to_start_with: str,
#     expected_exception: Any = AssertionError,
# ):
#     try:
#         call()
#     except Exception as e:
#         print(repr(str(e)))
#         assert isinstance(e, expected_exception)
#         assert str(e).startswith(expected_to_start_with)
#     else:
#         raise AssertionError(f"Expected AssertionError({expected_to_start_with!r}) not raised")


# def test_logic():
#     print(GlobalRef(test_logic))
#     assert Logic({"ref$": "test_gref:s2"}).call() == 2
#     assert_ae(lambda: Logic({"ref$": "test_gref:s2", "a": 3}), "Unexpected entries {'a': 3}")
#     assert asyncio.run(Logic({"ref$": "test_gref:A2"}).call(2)) == 4
#     assert asyncio.run(Logic({"ref$": "test_gref:A2", "a": 5}).call(2)) == 7
#     assert_ae(
#         lambda: Logic({"ref$": "test_gref:A2", "x": 5}),
#         "not supported keys in config: {'x': 5}",
#     )
#     assert Logic({"ref$": "test_gref:M3"}).call(2) == 6
#     assert Logic({"ref$": "test_gref:M3", "i": 5}).call(2) == 10
#     assert_ae(
#         lambda: Logic({"ref$": "test_gref:A2", "x": 4}),
#         "not supported keys in config: {'x': 4}",
#     )
#     assert Logic({"i": 5}, "test_gref:M3").call(2) == 10
#     assert Logic({"ref$": "test_gref:M3", "i": 5}, "test_gref:M4").call(2) == 10
#     assert_ae(
#         lambda: Logic({"ref$": "test_gref:M4", "i": 5}, "test_gref:M3").call(2),
#         "module 'test_gref' has no attribute 'M4'",
#         AttributeError,
#     )
