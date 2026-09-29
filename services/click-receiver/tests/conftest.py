"""Fakes for click-receiver unit tests (no cluster).

- Redis: the real RedisClickStore on top of fakeredis (with Lua), so the dedup command, the
  pipelines and the mark script are exercised for real; `FlakyStore` injects failures/latency.
- Kafka: FakeProducer records messages or fails on demand.
- Postgres: AdCache over an in-memory AdSource.
"""

import asyncio
import signal

import fakeredis
import pytest
from fastapi.testclient import TestClient

from click_receiver.ads import Ad, AdCache
from click_receiver.hot import HotTracker
from click_receiver.main import app, get_service
from click_receiver.service import ClickService
from click_receiver.settings import Settings
from click_receiver.store import RedisClickStore

TEST_TIMEOUT_S = 60


@pytest.fixture(autouse=True)
def _test_timeout():
    """Every test fails after TEST_TIMEOUT_S instead of hanging the suite (the async tests that
    drive load also carry their own asyncio.timeout)."""

    def expired(signum, frame):
        raise TimeoutError(f"test exceeded {TEST_TIMEOUT_S} s")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.alarm(TEST_TIMEOUT_S)
    yield
    signal.alarm(0)
    signal.signal(signal.SIGALRM, previous)


class FakeProducer:
    def __init__(self):
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.fail: Exception | None = None
        self.delay = 0.0  # > ack_timeout: the wait expires like AsyncProducer's (bare TimeoutError)
        self.ack_timeouts: list[float] = []

    async def produce(self, topic, value, key=None, *, ack_timeout):
        self.ack_timeouts.append(ack_timeout)
        if self.delay:
            await asyncio.wait_for(asyncio.sleep(self.delay), ack_timeout)
        if self.fail is not None:
            raise self.fail
        self.messages.append((topic, key, value))
        return None


class FakeAdSource:
    def __init__(self, ads: dict[int, Ad]):
        self.ads = ads
        self.calls = 0
        self.error: Exception | None = None
        self.delay = 0.0

    async def get(self, ad_id):
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return self.ads.get(ad_id)


class FlakyStore:
    """Wraps a store; per-method failure injection."""

    def __init__(self, inner):
        self.inner = inner
        self.fail: set[str] = set()
        self.claim_delay = 0.0

    def __getattr__(self, name):
        attr = getattr(self.inner, name)

        async def wrapper(*args, **kwargs):
            if name == "claim" and self.claim_delay:
                await asyncio.sleep(self.claim_delay)
            if name in self.fail:
                raise ConnectionError(f"redis down ({name})")
            return await attr(*args, **kwargs)

        return wrapper


class Clock:
    def __init__(self, t: float = 1_790_000_000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t


class Harness:
    def __init__(self, **overrides):
        self.settings = Settings(pod_name="pod-1", node_name="node-a", **overrides)
        self.redis = fakeredis.FakeAsyncRedis(decode_responses=True)
        self.store = FlakyStore(RedisClickStore(self.redis, self.redis))
        self.producer = FakeProducer()
        self.source = FakeAdSource(
            {
                42: Ad(42, 7, "https://advertiser.example/landing", True),
                43: Ad(43, 7, "https://advertiser.example/other", True),
                44: Ad(44, 8, "https://gone.example", False),
            }
        )
        self.ads = AdCache(self.source, ttl=60, max_entries=100)
        self.clock = Clock()
        self.hot = HotTracker(self.store, self.settings, clock=self.clock)
        self.service = ClickService(self.settings, self.ads, self.store, self.producer, self.hot)


@pytest.fixture
def h():
    return Harness()


@pytest.fixture
def make_client():
    """make_client(**settings) -> (harness, TestClient) with the service swapped for fakes."""

    def make(**overrides):
        harness = Harness(**overrides)
        app.dependency_overrides[get_service] = lambda: harness.service
        return harness, TestClient(app, follow_redirects=False)

    yield make
    app.dependency_overrides.clear()


@pytest.fixture
def client(h):
    app.dependency_overrides[get_service] = lambda: h.service
    yield TestClient(app, follow_redirects=False)
    app.dependency_overrides.clear()
