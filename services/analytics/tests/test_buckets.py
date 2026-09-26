from datetime import UTC, datetime, timedelta, timezone

import pytest

from analytics.buckets import STEPS, BucketRange, RangeError, ceil_to, floor_to, resolve_range


def utc(*args, **kw):
    return datetime(*args, tzinfo=UTC, **kw)


NOW = utc(2026, 9, 26, 12, 34, 56, 789000)
MIN, HOUR, DAY = STEPS["minute"], STEPS["hour"], STEPS["day"]


# ---- floor / ceil ----


@pytest.mark.parametrize(
    ("dt", "step", "expected"),
    [
        (utc(2026, 9, 26, 12, 34, 56), MIN, utc(2026, 9, 26, 12, 34)),
        (utc(2026, 9, 26, 12, 34), MIN, utc(2026, 9, 26, 12, 34)),
        (utc(2026, 9, 26, 12, 34, 0, 1), MIN, utc(2026, 9, 26, 12, 34)),
        (utc(2026, 9, 26, 12, 59, 59, 999999), HOUR, utc(2026, 9, 26, 12)),
        (utc(2026, 9, 26, 23, 59), DAY, utc(2026, 9, 26)),
        (utc(2026, 9, 26), DAY, utc(2026, 9, 26)),
        (utc(1969, 12, 31, 23, 59, 30), MIN, utc(1969, 12, 31, 23, 59)),  # before the epoch
        (utc(1969, 12, 31, 12), DAY, utc(1969, 12, 31)),
    ],
)
def test_floor(dt, step, expected):
    assert floor_to(dt, step) == expected


@pytest.mark.parametrize(
    ("dt", "step", "expected"),
    [
        (utc(2026, 9, 26, 12, 34, 56), MIN, utc(2026, 9, 26, 12, 35)),
        (utc(2026, 9, 26, 12, 34), MIN, utc(2026, 9, 26, 12, 34)),  # already on a boundary
        (utc(2026, 9, 26, 12, 34, 0, 1), MIN, utc(2026, 9, 26, 12, 35)),  # 1 µs past -> next
        (utc(2026, 9, 26, 12, 0, 1), HOUR, utc(2026, 9, 26, 13)),
        (utc(2026, 9, 26, 0, 0, 1), DAY, utc(2026, 9, 27)),
        (utc(2026, 12, 31, 23, 30), DAY, utc(2027, 1, 1)),
    ],
)
def test_ceil(dt, step, expected):
    assert ceil_to(dt, step) == expected


def test_floor_uses_utc_not_the_offset_of_the_input():
    # 2026-09-26T01:30+05:00 == 2026-09-25T20:30Z -> the UTC day is the 25th
    local = datetime(2026, 9, 26, 1, 30, tzinfo=timezone(timedelta(hours=5)))
    assert floor_to(local, DAY) == utc(2026, 9, 25)
    assert floor_to(local, DAY).tzinfo == UTC
    # a half-hour offset: hour buckets are UTC hours, not local hours
    ist = datetime(2026, 9, 26, 10, 45, tzinfo=timezone(timedelta(hours=5, minutes=30)))  # 05:15Z
    assert floor_to(ist, HOUR) == utc(2026, 9, 26, 5)
    assert ceil_to(ist, HOUR) == utc(2026, 9, 26, 6)


# ---- resolve_range ----


def test_defaults_last_hour_including_current_minute():
    r = resolve_range(None, None, "minute", now=NOW, max_buckets=1440)
    # from = now - 1h truncated down, to = now rounded up -> 61 buckets, the last one still filling
    assert r.start == utc(2026, 9, 26, 11, 34)
    assert r.end == utc(2026, 9, 26, 12, 35)
    assert r.count == 61
    assert r.starts()[-1] == utc(2026, 9, 26, 12, 34)


def test_defaults_with_granularity_hour_and_day():
    h = resolve_range(None, None, "hour", now=NOW, max_buckets=1440)
    assert (h.start, h.end, h.count) == (utc(2026, 9, 26, 11), utc(2026, 9, 26, 13), 2)
    d = resolve_range(None, None, "day", now=NOW, max_buckets=1440)
    assert (d.start, d.end, d.count) == (utc(2026, 9, 26), utc(2026, 9, 27), 1)


def test_default_from_is_to_minus_one_hour():
    to = utc(2026, 1, 1, 10, 0)
    r = resolve_range(None, to, "minute", now=NOW, max_buckets=1440)
    assert (r.start, r.end, r.count) == (utc(2026, 1, 1, 9), to, 60)


def test_default_to_is_now():
    r = resolve_range(utc(2026, 9, 26, 12, 30, 10), None, "minute", now=NOW, max_buckets=1440)
    assert (r.start, r.end) == (utc(2026, 9, 26, 12, 30), utc(2026, 9, 26, 12, 35))


def test_aligned_range_is_unchanged():
    r = resolve_range(utc(2026, 1, 1, 10), utc(2026, 1, 1, 11), "minute", now=NOW, max_buckets=1440)
    assert (r.start, r.end, r.count) == (utc(2026, 1, 1, 10), utc(2026, 1, 1, 11), 60)


def test_sub_bucket_range_gives_one_bucket():
    r = resolve_range(
        utc(2026, 1, 1, 10, 0, 5), utc(2026, 1, 1, 10, 0, 6), "minute", now=NOW, max_buckets=1440
    )
    assert r.count == 1 and r.starts() == [utc(2026, 1, 1, 10)]


def test_range_straddling_a_boundary_gives_two_buckets():
    r = resolve_range(
        utc(2026, 1, 1, 10, 0, 59), utc(2026, 1, 1, 10, 1, 1), "minute", now=NOW, max_buckets=1440
    )
    assert r.starts() == [utc(2026, 1, 1, 10), utc(2026, 1, 1, 10, 1)]


@pytest.mark.parametrize(
    ("from_", "to"),
    [
        (utc(2026, 1, 1, 10), utc(2026, 1, 1, 10)),  # from == to
        (utc(2026, 1, 1, 11), utc(2026, 1, 1, 10)),  # from > to
        (utc(2026, 1, 1, 10, 0, 30), utc(2026, 1, 1, 10, 0, 30)),
    ],
)
def test_from_not_before_to_is_error(from_, to):
    with pytest.raises(RangeError, match="before"):
        resolve_range(from_, to, "minute", now=NOW, max_buckets=1440)


def test_from_in_the_future_with_default_to_is_error():
    with pytest.raises(RangeError):
        resolve_range(NOW + timedelta(minutes=5), None, "minute", now=NOW, max_buckets=1440)


def test_max_buckets_boundary():
    start = utc(2026, 1, 1)
    exactly = resolve_range(start, start + timedelta(minutes=1440), "minute", now=NOW, max_buckets=1440)
    assert exactly.count == 1440
    with pytest.raises(RangeError, match="1441"):
        resolve_range(start, start + timedelta(minutes=1441), "minute", now=NOW, max_buckets=1440)


def test_max_buckets_counts_effective_rounded_range():
    # 1439 min 2 s as given, but truncating/rounding makes it 1441 buckets -> 400
    start = utc(2026, 1, 1, 0, 0, 59)
    with pytest.raises(RangeError):
        resolve_range(start, start + timedelta(minutes=1439, seconds=2), "minute", now=NOW, max_buckets=1440)


def test_long_range_ok_with_coarser_granularity():
    start = utc(2026, 1, 1)
    end = start + timedelta(days=30)
    with pytest.raises(RangeError):
        resolve_range(start, end, "minute", now=NOW, max_buckets=1440)
    assert resolve_range(start, end, "hour", now=NOW, max_buckets=1440).count == 720
    assert resolve_range(start, end, "day", now=NOW, max_buckets=1440).count == 30


def test_offsets_are_normalized_to_utc():
    plus2 = timezone(timedelta(hours=2))
    r = resolve_range(
        datetime(2026, 1, 1, 12, 0, 30, tzinfo=plus2), datetime(2026, 1, 1, 12, 5, tzinfo=plus2),
        "minute", now=NOW, max_buckets=1440,
    )  # fmt: skip
    assert r.start == utc(2026, 1, 1, 10) and r.end == utc(2026, 1, 1, 10, 5)
    assert r.start.tzinfo == UTC and r.end.tzinfo == UTC


def test_naive_datetimes_rejected():
    with pytest.raises(RangeError, match="offset"):
        resolve_range(datetime(2026, 1, 1), None, "minute", now=NOW, max_buckets=1440)


def test_unknown_granularity():
    with pytest.raises(RangeError):
        resolve_range(None, None, "week", now=NOW, max_buckets=1440)


def test_overflow_is_range_error_not_crash():
    with pytest.raises(RangeError):
        resolve_range(None, datetime.max.replace(tzinfo=UTC), "day", now=NOW, max_buckets=1440)
    with pytest.raises(RangeError):
        resolve_range(None, datetime.min.replace(tzinfo=UTC) + timedelta(minutes=5), "minute", now=NOW,
                      max_buckets=1440)  # fmt: skip


# ---- zero fill ----


def test_zero_fill_all_buckets_in_order():
    r = BucketRange("minute", utc(2026, 1, 1, 10), utc(2026, 1, 1, 10, 4))
    filled = r.zero_fill({utc(2026, 1, 1, 10, 1): 5, utc(2026, 1, 1, 10, 3): 2})
    assert filled == [
        (utc(2026, 1, 1, 10), 0),
        (utc(2026, 1, 1, 10, 1), 5),
        (utc(2026, 1, 1, 10, 2), 0),
        (utc(2026, 1, 1, 10, 3), 2),
    ]


def test_zero_fill_matches_keys_in_other_offsets():
    # the DB driver may return timestamptz in the session time zone
    r = BucketRange("hour", utc(2026, 1, 1, 10), utc(2026, 1, 1, 12))
    key = datetime(2026, 1, 1, 12, tzinfo=timezone(timedelta(hours=1)))  # == 11:00Z
    assert r.zero_fill({key: 9}) == [(utc(2026, 1, 1, 10), 0), (utc(2026, 1, 1, 11), 9)]


def test_zero_fill_empty():
    r = BucketRange("day", utc(2026, 1, 1), utc(2026, 1, 3))
    assert r.zero_fill({}) == [(utc(2026, 1, 1), 0), (utc(2026, 1, 2), 0)]
