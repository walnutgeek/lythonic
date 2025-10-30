import asyncio
import inspect
import logging
import sys
import time as tt
from collections.abc import Callable
from typing import Annotated, Any, final

from pydantic import BeforeValidator, PlainSerializer, WithJsonSchema
from typing_extensions import override

from lythonic.types import str_or_none

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


def utc_now() -> datetime:
    """return the current time in UTC
    >>> utc_now().tzinfo
    datetime.timezone.utc
    """
    return datetime.now(UTC)


def dt_to_bytes(dt: datetime) -> bytes:
    """Convert datetime to bytes
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
    """Convert  bytes to datetime
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


class IntervalUnit(Enum):
    """
    >>> IntervalUnit.D
    IntervalUnit.D
    """

    D = 1
    W = 7
    M = YEAR_IN_DAYS / 12
    Q = YEAR_IN_DAYS / 4
    Y = YEAR_IN_DAYS

    @classmethod
    def from_string(cls, n: str) -> "IntervalUnit":
        return cls[n.upper()]

    def timedelta(self) -> timedelta:
        return timedelta(days=self.value)

    @override
    def __str__(self) -> str:
        return self.name

    @override
    def __repr__(self) -> str:
        return f"IntervalUnit.{str(self)}"


@final
class Interval:
    _P = "".join(p.name for p in IntervalUnit)
    FREQ_RE = re.compile(r"(\d+)([" + _P + _P.lower() + "])")

    multiplier: int
    period: IntervalUnit

    def __init__(self, multiplier: int, period: IntervalUnit) -> None:
        self.multiplier = multiplier
        self.period = period

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
            raise ValueError("Invalid frequency string", s)

    @classmethod
    def matcher(cls, s: str) -> re.Match[str] | None:
        return re.match(cls.FREQ_RE, s)

    def timedelta(self) -> timedelta:
        return self.multiplier * self.period.timedelta()

    def match(self, d: date, as_of: date) -> bool:
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
