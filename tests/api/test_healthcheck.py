"""Health endpoint behavior used by availability monitors."""

import json
from types import SimpleNamespace

import pytest

from api.endpoint import healthcheck as healthcheck_endpoint


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("database", "expected_status"),
    [
        ({"status": "OK"}, 200),
        ({"status": "Fail", "details": "database unavailable"}, 503),
    ],
)
async def test_healthcheck_http_status_matches_database_state(
    monkeypatch, database, expected_status
):
    async def check_db():
        return database

    monkeypatch.setattr(healthcheck_endpoint, "_check_db", check_db)
    request = SimpleNamespace(
        app=SimpleNamespace(config={"RELEASE": "test", "ENVIRONMENT": "test"})
    )

    result = await healthcheck_endpoint.healthcheck(request)

    assert result.status == expected_status
    assert json.loads(result.body)["database"] == database


@pytest.mark.asyncio
async def test_liveness_does_not_probe_database(monkeypatch):
    async def fail_if_called():
        raise AssertionError("liveness must not probe the database")

    monkeypatch.setattr(healthcheck_endpoint, "_check_db", fail_if_called)
    request = SimpleNamespace(
        app=SimpleNamespace(config={"RELEASE": "test", "ENVIRONMENT": "test"})
    )

    result = await healthcheck_endpoint.liveness(request)

    assert result.status == 200
    assert json.loads(result.body) == {"status": "OK", "release": "test"}
