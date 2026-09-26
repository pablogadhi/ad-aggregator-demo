"""Time-bucket math for the analytics API (contract: design/contracts/openapi/analytics.yaml).

UTC buckets of one minute / hour / day. For a request [from, to):
- defaults: to = now, from = to - 1 h
- from >= to (as given) -> 400
- the effective start is `from` truncated DOWN to its bucket start
- the effective (exclusive) end is `to` rounded UP to the next bucket boundary unless already on
  one, so `to = now` includes the current, still-filling bucket
- more than `max_buckets` buckets in the effective range -> 400
- the series is zero-filled over every bucket of the effective range

Everything is exact integer arithmetic on timedeltas (microsecond resolution, no floats). UTC days
are exactly 86,400 s, so flooring on the Unix epoch matches Postgres `date_trunc(g, ts, 'UTC')`.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

Granularity = Literal["minute", "hour", "day"]

STEPS: dict[str, timedelta] = {
    "minute": timedelta(minutes=1),
    "hour": timedelta(hours=1),
    "day": timedelta(days=1),
}
DEFAULT_WINDOW = timedelta(hours=1)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class RangeError(ValueError):
    """The requested range is invalid (-> HTTP 400)."""


def floor_to(dt: datetime, step: timedelta) -> datetime:
    """Start of the UTC bucket containing `dt` (timedelta % timedelta is non-negative, so pre-1970 works)."""
    dt = dt.astimezone(UTC)
    return dt - (dt - EPOCH) % step


def ceil_to(dt: datetime, step: timedelta) -> datetime:
    """The first bucket boundary >= `dt`."""
    start = floor_to(dt, step)
    return start if start == dt else start + step


@dataclass(frozen=True)
class BucketRange:
    granularity: str
    start: datetime  # inclusive, bucket-aligned, UTC
    end: datetime  # exclusive, bucket-aligned, UTC

    @property
    def step(self) -> timedelta:
        return STEPS[self.granularity]

    @property
    def count(self) -> int:
        return (self.end - self.start) // self.step

    def starts(self) -> list[datetime]:
        return [self.start + i * self.step for i in range(self.count)]

    def zero_fill(self, counts: Mapping[datetime, int]) -> list[tuple[datetime, int]]:
        """One (bucket start, clicks) per bucket; buckets missing from `counts` are 0."""
        normalized = {k.astimezone(UTC): v for k, v in counts.items()}
        return [(s, int(normalized.get(s, 0))) for s in self.starts()]


def resolve_range(
    from_: datetime | None,
    to: datetime | None,
    granularity: str,
    *,
    now: datetime,
    max_buckets: int,
) -> BucketRange:
    if granularity not in STEPS:
        raise RangeError(f"unknown granularity {granularity!r}")
    for name, value in (("from", from_), ("to", to), ("now", now)):
        if value is not None and value.tzinfo is None:
            raise RangeError(f"{name} must carry a UTC offset")
    step = STEPS[granularity]
    try:
        to = (to or now).astimezone(UTC)
        from_ = (from_ or to - DEFAULT_WINDOW).astimezone(UTC)
        if from_ >= to:
            raise RangeError("from must be before to")
        start, end = floor_to(from_, step), ceil_to(to, step)
    except OverflowError as exc:  # e.g. to = 9999-12-31T23:59:59Z rounded up past datetime.max
        raise RangeError("timestamp out of range") from exc
    rng = BucketRange(granularity, start, end)
    if rng.count > max_buckets:
        raise RangeError(
            f"range spans {rng.count} {granularity} buckets; at most {max_buckets} allowed "
            "(narrow the range or use a coarser granularity)"
        )
    return rng
