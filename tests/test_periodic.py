import asyncio
import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from lythonic.periodic import (
    EPOCH_ZERO,
    Interval,
    IntervalUnit,
    PeriodicTask,
    dt_from_bytes,
    dt_to_bytes,
    run_all,
)


# TODO: this test is slow and fails sometimes. See file: tests/test_periodic_ocasional_fail.txt
@pytest.mark.slow
def test_periodic():
    results: list[tuple[float, ...]] = []
    beginning = time.time()

    def dump_results(*t: Any):
        a = (time.time() - beginning, *t)
        print(a)
        results.append(a)

    async def run_all_tasks():
        def build_fn(
            return_value: bool,
            async_fn: bool,
            sleep_time: float,
        ):
            prefix: str = "a" if async_fn else ""
            rx = "r" if return_value else "x"
            n = f"{prefix}sync_fn_{rx}_{sleep_time}"

            def fn_end(start: float) -> float:
                t = time.time()
                dump_results("fn_end", n, t - start)
                if return_value:
                    return sleep_time
                else:
                    raise ValueError(sleep_time)

            if async_fn:

                async def fn() -> float:  # pyright: ignore [reportRedeclaration]
                    start = time.time()
                    await asyncio.sleep(sleep_time)
                    return fn_end(start)
            else:

                def fn() -> float:
                    start = time.time()
                    time.sleep(sleep_time)
                    return fn_end(start)

            fn.__name__ = n
            return fn

        shutdown_event = asyncio.Event()

        f_tasks = asyncio.create_task(
            run_all(
                *[
                    PeriodicTask(*v)
                    for v in [
                        (6, build_fn(True, True, 1)),
                        (8, build_fn(True, False, 2)),
                        (4, build_fn(False, True, 1)),
                        (7, build_fn(False, False, 1)),
                    ]
                ],
                shutdown_event=shutdown_event,
                collect_results=lambda n, r: dump_results("result", n, r),
            )
        )

        await asyncio.sleep(20)
        shutdown_event.set()
        await f_tasks
        return results

    asyncio.run(run_all_tasks())

    def r2s(t: tuple[Any, ...]) -> str:
        if t[1] == "fn_end":
            t = (int(t[0]), t[1], t[2], int(t[3]))
        elif t[1] == "result":
            v = t[3]
            if isinstance(v, tuple):
                v = cast(tuple[type, Any], v[:2])
            t = (int(t[0]), t[1], t[2], v)
        return str(t)

    str_results = tuple(map(r2s, results))
    for s in str_results:
        print(f"{s!r},")
    assert str_results == (
        "(1, 'fn_end', 'async_fn_r_1', 1)",
        "(1, 'result', 'async_fn_r_1', 1)",
        "(3, 'fn_end', 'sync_fn_r_2', 2)",
        "(3, 'result', 'sync_fn_r_2', 2)",
        "(4, 'fn_end', 'async_fn_x_1', 1)",
        "(4, 'result', 'async_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(5, 'fn_end', 'sync_fn_x_1', 1)",
        "(5, 'result', 'sync_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(6, 'fn_end', 'async_fn_x_1', 1)",
        "(6, 'result', 'async_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(7, 'fn_end', 'async_fn_r_1', 1)",
        "(7, 'result', 'async_fn_r_1', 1)",
        "(8, 'fn_end', 'sync_fn_x_1', 1)",
        "(8, 'result', 'sync_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(10, 'fn_end', 'sync_fn_r_2', 2)",
        "(10, 'result', 'sync_fn_r_2', 2)",
        "(11, 'fn_end', 'async_fn_x_1', 1)",
        "(11, 'result', 'async_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(13, 'fn_end', 'async_fn_r_1', 1)",
        "(13, 'result', 'async_fn_r_1', 1)",
        "(14, 'fn_end', 'async_fn_x_1', 1)",
        "(14, 'result', 'async_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(15, 'fn_end', 'sync_fn_x_1', 1)",
        "(15, 'result', 'sync_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(18, 'fn_end', 'sync_fn_r_2', 2)",
        "(18, 'result', 'sync_fn_r_2', 2)",
        "(19, 'fn_end', 'async_fn_x_1', 1)",
        "(19, 'result', 'async_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(20, 'fn_end', 'sync_fn_x_1', 1)",
        "(20, 'result', 'sync_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
    )
    asyncio.run(run_all())
    # assert False


def test_max_values_for_datetime_serialized():
    dt_max = datetime.max
    dt_min = datetime.min
    dt_max_utc = datetime.max.replace(tzinfo=UTC)
    dt_min_utc = datetime.min.replace(tzinfo=UTC)

    extremes = (
        (dt_max_utc - EPOCH_ZERO).total_seconds(),
        (EPOCH_ZERO - dt_min_utc).total_seconds(),
    )
    max_value_to_store = int(max(extremes) * 1000000)
    how_many_bytes = (max_value_to_store).bit_length() // 8 + 1
    print(f"{how_many_bytes=}")
    round_trip: Callable[[datetime], datetime] = lambda dt: dt_from_bytes(dt_to_bytes(dt))

    def round_trip_n(n: int, dt: datetime, dt_expected: datetime, step: int) -> tuple[bool, str]:
        s = ""
        all = True
        for _ in range(n):
            try:
                v = round_trip(dt)
                c = v == dt_expected
            except OverflowError:
                v, c = None, None
            s += f"{(c, v, dt, dt_expected)}\n"
            if not c:
                all = False
            dt = dt + timedelta(microseconds=step)
            dt_expected = dt_expected + timedelta(microseconds=step)
        return all, f" {n=} {all=}\n{s}"

    all, msg = round_trip_n(10, dt_max, dt_max_utc, -1)
    assert all, msg
    all, msg = round_trip_n(100, dt_max, dt_max_utc, -91)
    assert all, msg
    all, msg = round_trip_n(100, dt_max_utc, dt_max_utc, -791)
    assert all, msg
    all, msg = round_trip_n(10, dt_min, dt_min_utc, 1)
    assert all, msg
    all, msg = round_trip_n(100, dt_min, dt_min_utc, 91)
    assert all, msg
    all, msg = round_trip_n(100, dt_min_utc, dt_min_utc, 791)
    assert all, msg

    # print(f"{dt_max=} {round_trip_n(100, dt_max, dt_max_utc, -10)[1]}")
    # print(f"{dt_min=} {round_trip_n(100, dt_min, dt_min_utc, 10)[1]}")
    # assert False


def test_period():
    assert IntervalUnit.D == IntervalUnit.from_string("D")
    assert IntervalUnit.W == IntervalUnit.from_string("W")
    assert IntervalUnit.M == IntervalUnit.from_string("M")
    assert IntervalUnit.Y == IntervalUnit.from_string("years")
    assert IntervalUnit.s == IntervalUnit.from_string("seconds")

    dates = [
        date(1999, 1, 1),
        date(1999, 4, 2),
        date(1999, 7, 2),
        date(1999, 10, 1),
        date(1999, 12, 31),
    ]
    for d1, d2 in zip(dates[:-1], dates[1:], strict=False):
        assert d1 + IntervalUnit.Q.timedelta() == d2


def test_freq():
    dates = [date(1999, 4, 2), date(1999, 7, 2), date(1999, 10, 1), date(2000, 1, 1)]
    for n in range(1, 5):
        d = date(1999, 1, 1) + Interval.from_string(f"{n}Q").timedelta()
        assert d == dates[n - 1]
    try:
        Interval.from_string("1z")
        raise AssertionError
    except ValueError as e:
        assert str(e) == "('Invalid interval string', '1z')"


def test_find_file():
    d = Path("build") / "test_dir"
    d.mkdir(exist_ok=True, parents=True)
    file_dates = [date(1999, 1, 1), date(1999, 1, 8), date(1999, 1, 16), date(1999, 1, 23)]
    files = [d / f"{p}{fd:%Y%m%d}.csv" for p in ["", "a"] for fd in file_dates]
    for t in files:
        t.touch()
    f = Interval(1, IntervalUnit.W)
    test_dates = [date(1998, 12, 31) + timedelta(days=days) for days in range(35)]
    matches = tuple(f.find_file(d, as_of) for as_of in test_dates)
    assert matches[0] is None
    assert tuple(matches[1:8]) == (files[0],) * 7
    assert tuple(matches[8:15]) == (files[1],) * 7
    assert matches[15] is None
    assert tuple(matches[16:23]) == (files[2],) * 7
    assert tuple(matches[23:30]) == (files[3],) * 7
    assert len(matches[30:]) == 5
    assert sum(map(lambda x: x is None, matches[30:])) == 5
