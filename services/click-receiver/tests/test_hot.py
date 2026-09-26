"""Hot-ad detection (spec §5.1.1) against the real RedisClickStore on fakeredis (Lua enabled)."""

from prometheus_client import REGISTRY

from click_receiver.store import HOT_ADS, HOT_PERM, bucket_key, flag_key, marks_key


def metric(name):
    return REGISTRY.get_sample_value(name) or 0.0


async def flush(h, ad_id, n):
    for _ in range(n):
        h.hot.record(ad_id)
    await h.hot.flush()


async def test_counts_are_batched_per_minute_bucket(h):
    await flush(h, 42, 3)
    await flush(h, 42, 2)
    minute = int(h.clock() // 60)
    assert await h.redis.get(bucket_key(42, minute)) == "5"
    assert 600 < await h.redis.ttl(bucket_key(42, minute)) <= 660
    assert not h.hot.is_hot(42)
    assert h.hot._pending == {}


async def test_sliding_10_minute_sum_marks_hot(h):
    # 1,199 clicks spread over the last 10 minutes: not hot yet
    for m in range(10):
        await flush(h, 42, 120 if m < 9 else 119)
        h.clock.t += 60
    h.clock.t -= 60  # stay in the 10th minute
    assert not h.hot.is_hot(42)
    before = metric("hot_ad_markings_total")
    await flush(h, 42, 1)  # 1,200 -> hot
    assert h.hot.is_hot(42)
    assert await h.redis.get(marks_key(42)) == "1"
    assert 0 < await h.redis.ttl(flag_key(42)) <= 600
    assert await h.redis.zscore(HOT_ADS, "42") == h.clock() + 600
    assert metric("hot_ad_markings_total") == before + 1


async def test_buckets_older_than_10_minutes_do_not_count(h):
    await flush(h, 42, 1000)
    h.clock.t += 10 * 60  # the first bucket is now 10 minutes old: out of the window
    await flush(h, 42, 300)
    assert not h.hot.is_hot(42)


async def test_still_hot_extends_without_new_marking(h):
    await flush(h, 42, 1200)
    await flush(h, 42, 1)
    await flush(h, 42, 1)
    assert await h.redis.get(marks_key(42)) == "1"


async def test_permanent_after_10_markings(h):
    for marking in range(1, 11):
        # the previous marking expired (flag TTL = the hysteresis) and the ad got hot again
        await h.redis.delete(flag_key(42))
        await flush(h, 42, 1200 if marking == 1 else 1)
        assert await h.redis.get(marks_key(42)) == str(marking)
        if marking < 10:
            assert not await h.redis.sismember(HOT_PERM, "42")
    assert await h.redis.sismember(HOT_PERM, "42")

    # temporary marking gone (flag + zset entry expired): still hot, forever
    await h.redis.delete(flag_key(42))
    h.clock.t += 601
    await h.hot.refresh()
    [item] = h.hot.items
    assert (item.ad_id, item.permanent, item.marks, item.hot_until) == (42, True, 10, None)
    assert h.hot.is_hot(42)
    assert await h.redis.zscore(HOT_ADS, "42") is None  # trimmed by the refresh
    assert REGISTRY.get_sample_value("hot_ads_permanent") == 1


async def test_refresh_learns_markings_of_other_receivers(h):
    # another receiver marked ad 43
    await h.redis.zadd(HOT_ADS, {"43": h.clock() + 300, "44": h.clock() - 1})
    await h.redis.set(marks_key(43), 2)
    await h.hot.refresh()
    assert [(i.ad_id, i.marks, i.permanent) for i in h.hot.items] == [(43, 2, False)]
    assert h.hot.items[0].hot_until.timestamp() == h.clock() + 300
    assert h.hot.refreshed_at is not None
    assert await h.redis.zscore(HOT_ADS, "44") is None


async def test_redis_down_keeps_last_known_set(h):
    await flush(h, 42, 1200)
    await h.hot.refresh()
    assert h.hot.is_hot(42)
    refreshed = h.hot.refreshed_at
    h.store.fail.update({"load_hot", "add_counts"})
    h.clock.t += 3600  # even long after hot_until
    await h.hot.refresh()
    assert h.hot.is_hot(42) and h.hot.refreshed_at == refreshed


async def test_failed_flush_is_merged_into_the_next(h):
    h.store.fail.add("add_counts")
    await flush(h, 42, 3)
    await flush(h, 42, 2)
    assert h.hot._pending == {42: 5}
    h.store.fail.clear()
    await h.hot.flush()
    assert await h.redis.get(bucket_key(42, int(h.clock() // 60))) == "5"


async def test_backlog_is_bounded(make_client):
    h, _ = make_client(hot_pending_max_ads=3)
    h.store.fail.add("add_counts")
    before = metric("hot_counter_dropped_total")
    for ad in range(1, 6):
        h.hot.record(ad)
    await h.hot.flush()
    assert len(h.hot._pending) == 3
    assert metric("hot_counter_dropped_total") == before + 2


async def test_stop_flushes_pending_counts(h):
    h.hot.start()
    h.hot.record(42)
    await h.hot.stop()
    assert await h.redis.get(bucket_key(42, int(h.clock() // 60))) == "1"
