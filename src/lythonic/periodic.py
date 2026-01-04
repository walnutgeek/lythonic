"""
Time and scheduling utilities.

This module provides tools for working with time intervals, frequencies, and periodic tasks:

## Time Simulation

`SimulatedTime` allows testing time-dependent code by offsetting the clock:

```python
from lythonic.periodic import stime
from datetime import timedelta

stime.set_offset(timedelta(days=1))  # Pretend it's tomorrow
print(stime.get_datetime())
stime.reset()  # Back to real time
```

## Frequencies and Intervals

- `Frequency`: Human-friendly periods (weekly, monthly, quarterly, annually)
- `FrequencyOffset`: Frequency with day offset (e.g., "20th of each month")
- `Interval`: Precise duration with multiplier (e.g., "3M" for 3 months, "2W" for 2 weeks)

```python
from lythonic.periodic import Frequency, Interval
from datetime import date

freq = Frequency("monthly")
print(freq.first_day(date(2025, 11, 15)))  # 2025-11-01
print(freq.last_day(date(2025, 11, 15)))   # 2025-11-30

interval = Interval.from_string("2W")
print(interval.timedelta())  # 14 days
```

## Periodic Tasks

Run async tasks at specified intervals:

```python
from lythonic.periodic import PeriodicTask, run_all

task = PeriodicTask(freq=60, logic=my_function)  # Run every 60 seconds
await run_all(task)
```

## Timing with Moment

Track elapsed time between checkpoints:

```python
from lythonic.periodic import Moment

m = Moment.start()
# ... do work ...
m = m.capture("step1")
# ... more work ...
m = m.capture("done")
print(m.chain())  # [start] 0.5s-> [step1] 1.2s-> [done]
```
"""

import asyncio
import calendar
import inspect
import logging
import sys
import time as tt
from collections.abc import Callable
from functools import total_ordering
from typing import Annotated, Any, Literal, final

from pydantic import BeforeValidator, PlainSerializer, WithJsonSchema
from typing_extensions import override

from lythonic import str_or_none, utc_now
from lythonic.types import KNOWN_TYPES, KnownTypeArgs

log = logging.getLogger(__name__)

import re
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from pathlib import Path

YEAR_IN_DAYS = 365.256
SECONDS_IN_DAY = 24 * 60 * 60


EPOCH_ZERO = datetime(1970, 1, 1, tzinfo=UTC)


def total_microseconds(d: timedelta) -> int:
    return (d.days * SECONDS_IN_DAY + d.seconds) * 1_000_000 + d.microseconds


def dt_to_bytes(dt: datetime) -> bytes:
    """
    Convert datetime to bytes

    >>> dt_to_bytes(datetime( 1900,1,1,0,0,0))
    b'\\xff\\xf8&\\xef\\xb7C`\\x00'
    >>> dt_to_bytes(datetime( 2000,1,1,0,0,0))
    b'\\x00\\x03]\\x01;7\\xe0\\x00'
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    mics = total_microseconds(dt - EPOCH_ZERO)
    return mics.to_bytes(8, "big", signed=True)


def dt_from_bytes(b: bytes) -> datetime:
    """
    Convert  bytes to datetime

    >>> dt_from_bytes(b'\\xff\\xf8&\\xef\\xb7C`\\x00')
    datetime.datetime(1900, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
    >>> dt_from_bytes(b'\\x00\\x03]\\x01;7\\xe0\\x00')
    datetime.datetime(2000, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
    """
    mics = int.from_bytes(b, "big", signed=True)
    return EPOCH_ZERO + timedelta(microseconds=mics)


DT_BYTES_LENGTH = len(dt_to_bytes(utc_now()))


class SimulatedTime:
    """
    >>> st = SimulatedTime()
    >>> st.get_datetime().tzinfo
    datetime.timezone.utc
    >>> cmp = lambda ss: abs((st.get_datetime()-utc_now()).total_seconds()-ss) < 1e-3
    >>> cmp(0)
    True
    >>> st.set_offset(timedelta(days=1))
    >>> cmp(86400)
    True
    >>> st.set_offset(timedelta(days=1).total_seconds())
    >>> cmp(86400)
    True
    >>> st.set_now(utc_now() + timedelta(days=1))
    >>> cmp(86400)
    True
    >>> st.set_now( (utc_now() - timedelta(days=1)).timestamp() )
    >>> cmp(-86400)
    True
    >>> st.is_real_time()
    False
    >>> st.reset()
    >>> st.is_real_time()
    True
    """

    offset: float

    def __init__(self, offset: float = 0.0) -> None:
        self.offset = offset

    def time(self):
        return tt.time() + self.offset

    def set_offset(self, offset: timedelta | float):
        if isinstance(offset, timedelta):
            self.offset = offset.total_seconds()
        else:
            self.offset = offset

    def set_now(self, dt: datetime | float):
        if isinstance(dt, datetime):
            epoch = dt.timestamp()
        else:
            epoch = dt
        self.offset = epoch - tt.time()

    def reset(self):
        self.offset = 0.0

    def is_real_time(self):
        return self.offset == 0.0

    def get_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.time(), tz=UTC)


stime: SimulatedTime = SimulatedTime()

FrequencyType = Literal["weekly", "monthly", "quarterly", "annually"]


class Frequency:
    """People friendly interval

    >>> Frequency("weekly").first_day(date(2025, 11, 21))
    datetime.date(2025, 11, 17)
    >>> Frequency("weekly").last_day(date(2025, 11, 21))
    datetime.date(2025, 11, 23)
    >>> Frequency("monthly").first_day(date(2025, 11, 21))
    datetime.date(2025, 11, 1)
    >>> Frequency("monthly").last_day(date(2025, 11, 21))
    datetime.date(2025, 11, 30)
    >>> Frequency("quarterly").first_day(date(2025, 11, 21))
    datetime.date(2025, 10, 1)
    >>> Frequency("quarterly").last_day(date(2025, 11, 21))
    datetime.date(2025, 12, 31)
    >>> Frequency("quarterly").first_day(date(2025, 2, 21))
    datetime.date(2025, 1, 1)
    >>> Frequency("quarterly").last_day(date(2025, 2, 21))
    datetime.date(2025, 3, 31)
    >>> Frequency("quarterly").last_day(date(2025, 5, 21))
    datetime.date(2025, 6, 30)
    >>> Frequency("annually").first_day(date(2025, 11, 21))
    datetime.date(2025, 1, 1)
    >>> Frequency("annually").last_day(date(2025, 11, 21))
    datetime.date(2025, 12, 31)
    """

    frequency: FrequencyType

    def __init__(self, frequency: FrequencyType) -> None:
        self.frequency = frequency

    @classmethod
    def ensure(cls, frequency: "Frequency|FrequencyType") -> "Frequency":
        if isinstance(frequency, Frequency):
            return frequency
        return cls(frequency)

    def first_day(self, as_of: date) -> date:
        if self.frequency == "weekly":
            return as_of - timedelta(days=as_of.weekday())
        elif self.frequency == "monthly":
            return as_of.replace(day=1)
        elif self.frequency == "quarterly":
            return as_of.replace(day=1, month=((as_of.month - 1) // 3) * 3 + 1)
        elif self.frequency == "annually":
            return as_of.replace(day=1, month=1)
        else:
            raise AssertionError(f"Invalid frequency: {self.frequency}")

    def last_day(self, as_of: date) -> date:
        if self.frequency == "weekly":
            return as_of + timedelta(days=6 - as_of.weekday())
        elif self.frequency == "monthly":
            return as_of.replace(day=calendar.monthrange(as_of.year, as_of.month)[1])
        elif self.frequency == "quarterly":
            month = ((as_of.month - 1) // 3) * 3 + 3
            return as_of.replace(day=calendar.monthrange(as_of.year, month)[1], month=month)
        elif self.frequency == "annually":
            return as_of.replace(day=31, month=12)
        else:
            raise AssertionError(f"Invalid frequency: {self.frequency}")

    def min_max_offset(
        self,
    ) -> tuple[int, int]:
        if self.frequency == "weekly":
            return (-7, 7)
        elif self.frequency == "monthly":
            return (-31, 31)
        elif self.frequency == "quarterly":
            return (-92, 92)
        elif self.frequency == "annually":
            return (-366, 366)
        else:
            raise AssertionError(f"Invalid frequency: {self.frequency}")

    def assert_offset(self, offset: int) -> None:
        min_offset, max_offset = self.min_max_offset()
        assert min_offset <= offset < max_offset, (
            f"Invalid offset: {offset} for frequency: {self.frequency} expected between {min_offset} and {max_offset}"
        )


class FrequencyOffset:
    """
    >>> FrequencyOffset(Frequency("weekly"), 0).boundaries(date(2025, 11, 21))
    (datetime.date(2025, 11, 17), datetime.date(2025, 11, 23))
    >>> FrequencyOffset("weekly", 1).boundaries(date(2025, 11, 21))
    (datetime.date(2025, 11, 18), datetime.date(2025, 11, 24))
    >>> FrequencyOffset(Frequency("weekly"), -1).boundaries(date(2025, 11, 21))
    (datetime.date(2025, 11, 16), datetime.date(2025, 11, 22))
    >>> on20thOfMonth = FrequencyOffset(Frequency("monthly"), 19)
    >>> on20thOfMonth.boundaries(date(2025, 11, 21))
    (datetime.date(2025, 11, 20), datetime.date(2025, 12, 19))
    >>> on20thOfMonth.boundaries(on20thOfMonth.boundaries(date(2025, 11, 21))[1]+timedelta(days=1))
    (datetime.date(2025, 12, 20), datetime.date(2026, 1, 19))
    >>> on3rdBeforeEndOfMonth = FrequencyOffset("monthly", -3)
    >>> b1 = on3rdBeforeEndOfMonth.boundaries(date(2025, 11, 21)); b1
    (datetime.date(2025, 10, 29), datetime.date(2025, 11, 27))
    >>> b2 = on3rdBeforeEndOfMonth.boundaries(b1[1]+timedelta(days=1)); b2
    (datetime.date(2025, 11, 28), datetime.date(2025, 12, 28))
    >>> b3 = on3rdBeforeEndOfMonth.boundaries(b2[1]+timedelta(days=1)); b3
    (datetime.date(2025, 12, 29), datetime.date(2026, 1, 28))
    >>> b4 = on3rdBeforeEndOfMonth.boundaries(b3[1]+timedelta(days=1)); b4
    (datetime.date(2026, 1, 29), datetime.date(2026, 2, 25))
    """

    frequency: Frequency
    offset: int

    def __init__(self, frequency: Frequency | FrequencyType, offset: int) -> None:
        self.frequency = Frequency.ensure(frequency)
        self.frequency.assert_offset(offset)
        self.offset = offset

    def boundaries(self, as_of: date) -> tuple[date, date]:
        """
        Return the boundaries around the given date (inclusive).
        """
        if self.offset >= 0:
            d = self.frequency.first_day(as_of)
            dd = (
                self.frequency.first_day(d - timedelta(days=1)),
                d,
                self.frequency.last_day(as_of) + timedelta(days=1),
            )
            with_delta = [t + timedelta(days=self.offset) for t in dd]
        else:
            d = self.frequency.last_day(as_of)
            dd = (
                self.frequency.first_day(as_of) - timedelta(days=1),
                d,
                self.frequency.last_day(d + timedelta(days=1)),
            )
            with_delta = [t + timedelta(days=1 + self.offset) for t in dd]

        assert with_delta[0] <= as_of
        if as_of < with_delta[1]:
            return with_delta[0], with_delta[1] - timedelta(days=1)
        else:
            assert with_delta[1] <= as_of < with_delta[2]
            return with_delta[1], with_delta[2] - timedelta(days=1)


class IntervalUnit(Enum):
    """
    Approximate time interval units expressed in days.

    >>> for n in "usmhDWMQY": print(f"{n} = {IntervalUnit.from_string(n).timedelta()}")
    u = 0:00:00.000001
    s = 0:00:01
    m = 0:01:00
    h = 1:00:00
    D = 1 day, 0:00:00
    W = 7 days, 0:00:00
    M = 30 days, 10:30:43.200000
    Q = 91 days, 7:32:09.600000
    Y = 365 days, 6:08:38.400000
    >>> [i.name for i in IntervalUnit]
    ['u', 's', 'm', 'h', 'D', 'W', 'M', 'Q', 'Y']
    >>> IntervalUnit.minutes
    IntervalUnit.m
    >>> list(IntervalUnit.aliases())
    ['microseconds', 'seconds', 'minutes', 'hours', 'days', 'weeks', 'months', 'quarters', 'years']

    """

    u = microseconds = 1 / (1_000_000 * SECONDS_IN_DAY)
    s = seconds = 1 / SECONDS_IN_DAY
    m = minutes = 1 / (60 * 24)
    h = hours = 1 / 24
    D = days = 1
    W = weeks = 7
    M = months = YEAR_IN_DAYS / 12
    Q = quarters = YEAR_IN_DAYS / 4
    Y = years = YEAR_IN_DAYS

    @classmethod
    def from_string(cls, n: str) -> "IntervalUnit":
        return cls[n]

    @classmethod
    def aliases(cls) -> dict[str, "IntervalUnit"]:
        return {n: e for n, e in cls.__members__.items() if len(n) != 1}

    def timedelta(self) -> timedelta:
        return timedelta(days=self.value)

    @override
    def __str__(self) -> str:
        return self.name

    @override
    def __repr__(self) -> str:
        return f"IntervalUnit.{str(self)}"


@final
@total_ordering
class Interval:
    """Mapping years, month, and quarter to real numbers of approximate days. Weeks and days mapped to integer. "

    >>> p = lambda i: f"{i} {i.timedelta()}"
    >>> p(Interval(2, IntervalUnit.s))
    '2s 0:00:02'
    >>> p(Interval.from_string("2s"))
    '2s 0:00:02'
    >>> p(Interval.from_string("2seconds"))
    '2s 0:00:02'
    >>> p(Interval.from_string("2 seconds"))
    '2s 0:00:02'
    >>> p(Interval.from_string("2 second"))
    Traceback (most recent call last):
    ...
    ValueError: ('Invalid interval string', '2 second')
    >>> p(Interval(1, IntervalUnit.D))
    '1D 1 day, 0:00:00'
    >>> float(Interval.from_string("1D"))
    1.0
    >>> float(Interval.from_string("3h"))
    0.125
    >>> Interval.from_string("1D") == Interval(1, IntervalUnit.D)
    True
    >>> Interval("1D") >= Interval.from_string("23 hours")
    True
    >>> Interval(Interval("1D")) >= Interval.from_string("24 hours")
    True
    >>> Interval.from_string("1D") >= Interval.from_string("25 hours")
    False
    """

    FREQ_RE = re.compile(f"(\\d+)([{''.join(p.name for p in IntervalUnit)}])")
    ALIAS_FREQ_RE = re.compile(f"(\\d+) ?({'|'.join(IntervalUnit.aliases())})")

    multiplier: int
    period: IntervalUnit

    def __init__(
        self, multiplier_or_value: "int|str|Interval", period: IntervalUnit | None = None
    ) -> None:
        if isinstance(multiplier_or_value, int):
            assert period is not None
            self.multiplier = multiplier_or_value
            self.period = period
        else:
            if isinstance(multiplier_or_value, str):
                multiplier_or_value = Interval.from_string(multiplier_or_value)
            self.multiplier = multiplier_or_value.multiplier
            self.period = multiplier_or_value.period

    @classmethod
    def from_string_safe(cls, s: "Interval|str|None") -> "Interval | None":
        if s is None:
            return None
        if isinstance(s, Interval):
            return s
        return cls.from_string(s)

    @classmethod
    def from_string(cls, s: str) -> "Interval":
        m = cls.matcher(s)
        if m:
            n, p = m.groups()
            return cls(int(n), IntervalUnit.from_string(p))
        else:
            raise ValueError("Invalid interval string", s)

    @classmethod
    def matcher(cls, s: str) -> re.Match[str] | None:
        return re.match(cls.FREQ_RE, s) or re.match(cls.ALIAS_FREQ_RE, s)

    def timedelta(self) -> timedelta:
        return self.multiplier * self.period.timedelta()

    def __float__(self) -> float:
        """Interval duration in days"""
        return float(self.multiplier * self.period.value)

    @override
    def __hash__(self):
        return hash((self.multiplier, self.period))

    @override
    def __eq__(self, other: object) -> bool:
        assert isinstance(other, Interval)
        return self.timedelta() == other.timedelta()

    def __lt__(self, other: object) -> bool:
        assert isinstance(other, Interval)
        return self.timedelta() < other.timedelta()

    def match(self, d: date | datetime, as_of: date | datetime) -> bool:
        return d <= as_of and d + self.timedelta() > as_of

    def find_file(
        self,
        path: Path,
        as_of: date | datetime,
        suffix: str = ".csv",
    ) -> Path | None:
        ff = list(
            reversed(
                sorted(
                    (date_from_name(f.name), f)
                    for f in path.glob(f"*{suffix}")
                    if re.match(r"^\d{8}", f.name[:8])
                )
            )
        )
        for d, f in ff:
            if d <= as_of:
                if self.match(d, as_of):
                    return f
                else:
                    break
        return None

    @override
    def __str__(self) -> str:
        return f"{self.multiplier}{self.period}"

    @override
    def __repr__(self) -> str:
        return f"Interval({self.multiplier}, {self.period!r})"


IntervalSafe = Annotated[
    Interval | str | None,
    BeforeValidator(Interval.from_string_safe),
    PlainSerializer(str_or_none, return_type=str),
    WithJsonSchema({"anyOf": [{"type": "string"}, {"type": "null"}]}),
]

KNOWN_TYPES.register(
    KnownTypeArgs(concrete_type=Interval, map_to_string=str),
)


def date_from_name(s: str) -> date:
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


class Moment:
    """
    >>> m = Moment.start()
    >>> m = m.capture("instant")
    >>> tt.sleep(1)
    >>> m = m.capture("a second")
    >>> s = m.chain()
    >>> s.startswith('[start] 0.0'), 's-> [instant] 1.' in s , s.endswith('s-> [a second]')
    (True, True, True)
    """

    time: float
    name: str
    prev: "Moment | None"

    def __init__(self, name: str, prev: "Moment | None" = None) -> None:
        self.time = tt.time()
        self.name = name
        self.prev = prev

    @staticmethod
    def start():
        """capture the starting moment"""
        return Moment("start")

    def capture(self, name: str):
        """capture the named moment relative to this one"""
        return Moment(name, self)

    def elapsed(self):
        """return time in seconds since previous moment"""
        if self.prev is None:
            return 0
        return self.time - self.prev.time

    @override
    def __str__(self):
        return (
            f" {self.elapsed():.3f}s-> [{self.name}]" if self.prev is not None else f"[{self.name}]"
        )

    def chain(self) -> str:
        return str(self) if self.prev is None else self.prev.chain() + str(self)


class PeriodicTask:
    """
    A task that runs at a specified frequency in seconds.

    Use with `run_all()` to execute multiple tasks in an async event loop.
    """

    freq: int
    logic: Callable[[], Any]
    last_run: float | None = None

    def __init__(self, freq: int, logic: Callable[[], Any]) -> None:
        self.freq = freq
        self.logic = logic

    def is_due(self):
        return self.last_run is None or stime.time() - self.last_run > self.freq


def gcd_pair(a: int, b: int) -> int:
    """
    >>> gcd_pair(4, 6)
    2
    >>> gcd_pair(6*15, 6*7)
    6
    >>> gcd_pair(6,35)
    1
    """
    return abs(a) if b == 0 else gcd_pair(b, a % b)


def gcd(*nn: int) -> int:
    """
    >>> gcd(4)
    4
    >>> gcd(4, 6)
    2
    >>> gcd(6*15, 6*7)
    6
    >>> gcd(6,35)
    1
    >>> gcd(6*15, 6*7, 6*5)
    6
    >>> gcd(6*15, 6*7, 10)
    2
    >>> gcd(6*15, 6*7, 35)
    1
    >>> gcd()
    Traceback (most recent call last):
    ...
    IndexError: tuple index out of range
    """
    r = nn[0]
    for i in range(1, len(nn)):
        r = gcd_pair(r, nn[i])
    return r


def _collect_nothing(n: str, x: Any):  # pyright: ignore [reportUnusedParameter]
    pass  # pragma: no cover


async def run_all(
    *tasks: PeriodicTask,
    shutdown_event: asyncio.Event | None = None,
    collect_results: Callable[[str, Any], None] = _collect_nothing,
):
    """
    Run multiple periodic tasks in an async loop.

    Tasks are scheduled based on their `freq` (in seconds). The loop tick interval
    is the GCD of all task frequencies for efficiency. Set `shutdown_event` to
    signal graceful termination.
    """
    if shutdown_event is None:
        shutdown_event = asyncio.Event()
    if len(tasks) == 0:
        log.warning("No tasks to run")
        return
    tick = gcd(*[t.freq for t in tasks])
    loop = asyncio.get_running_loop()

    while True:
        start = stime.time()
        for t in tasks:
            if t.is_due():
                t.last_run = start

                try:
                    if inspect.iscoroutinefunction(t.logic):
                        r = await t.logic()
                    else:
                        r = await loop.run_in_executor(None, t.logic)
                except (Exception, asyncio.CancelledError) as _:
                    r = sys.exc_info()
                collect_results(t.logic.__name__, r)
            if shutdown_event.is_set():
                return
        elapsed = stime.time() - start
        await asyncio.sleep(tick - elapsed if elapsed < tick else 0)


def adjust_as_of_date(as_of_date: date | None) -> date:
    """
    >>> adjust_as_of_date(None) == date.today()
    True
    >>> adjust_as_of_date(date(2021, 1, 1)) == date(2021, 1, 1)
    True
    """
    return date.today() if as_of_date is None else as_of_date
