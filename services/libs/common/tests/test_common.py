import asyncio

from fastapi.testclient import TestClient
from sdl_common import ServiceSettings, create_app
from sdl_common.contract import operations


def make_app(checks):
    settings = ServiceSettings(service_name="t", pod_name="p1", node_name="n1")
    return create_app(settings, readiness=checks, readiness_timeout=0.2)


def test_healthz_and_served_by():
    res = TestClient(make_app([])).get("/healthz")
    assert res.json() == {"status": "ok"}
    assert res.headers["X-Served-By"] == "p1@n1"


def test_readyz_reports_failing_and_slow_checks():
    async def ok():
        return None

    async def broken():
        raise ConnectionError("db down")

    async def slow():
        await asyncio.sleep(1)

    res = TestClient(make_app([("a", ok), ("b", broken), ("c", slow)])).get("/readyz")
    assert res.status_code == 503
    checks = res.json()["checks"]
    assert checks["a"] == "ok"
    assert "db down" in checks["b"]
    assert checks["c"].startswith("fail: TimeoutError")


def test_readyz_accepts_lazy_check_factory():
    res = TestClient(make_app(lambda: [])).get("/readyz")
    assert res.status_code == 200


def test_metrics_exposed():
    client = TestClient(make_app([]))
    client.get("/healthz")
    res = client.get("/metrics")
    assert res.status_code == 200
    assert "http_request" in res.text


def test_contract_operations_parsing():
    spec = {"paths": {"/x": {"get": {"responses": {"200": {}}}, "parameters": []}}}
    assert operations(spec) == {("GET", "/x"): {"200"}}


def test_access_log_sampling_keeps_errors(monkeypatch):
    import sdl_common.app as app_module
    from fastapi import HTTPException

    lines = []
    monkeypatch.setattr(app_module.log, "info", lambda msg, extra: lines.append(extra["extra_fields"]))
    settings = ServiceSettings(service_name="t", pod_name="p1", node_name="n1")
    app = create_app(settings, access_log_sample=0.0)

    @app.get("/ok")
    async def ok():
        return {}

    @app.get("/bad")
    async def bad():
        raise HTTPException(404)

    client = TestClient(app)
    for _ in range(5):
        assert client.get("/ok").headers["X-Served-By"] == "p1@n1"
    assert client.get("/bad").headers["X-Served-By"] == "p1@n1"
    client.get("/healthz")
    assert [(line["path"], line["status"]) for line in lines] == [("/bad", 404)]

    lines.clear()
    app_all = create_app(settings)  # default: every request logged, as before

    @app_all.get("/ok")
    async def ok2():
        return {}

    TestClient(app_all).get("/ok")
    assert [(line["method"], line["path"], line["status"]) for line in lines] == [("GET", "/ok", 200)]


def test_delivery_error_tells_ambiguous_from_definite():
    import asyncio

    from confluent_kafka import KafkaError
    from sdl_common.kafka import DeliveryError, _resolve

    async def outcome(code):
        fut = asyncio.get_running_loop().create_future()
        _resolve(fut, KafkaError(code), None)
        return fut.exception()

    async def run():
        timed_out = await outcome(KafkaError._MSG_TIMED_OUT)
        assert isinstance(timed_out, DeliveryError) and timed_out.possibly_persisted
        assert timed_out.error.code() == KafkaError._MSG_TIMED_OUT
        too_large = await outcome(KafkaError.MSG_SIZE_TOO_LARGE)
        assert not too_large.possibly_persisted
        assert not DeliveryError("local producer queue full").possibly_persisted

    asyncio.run(run())
