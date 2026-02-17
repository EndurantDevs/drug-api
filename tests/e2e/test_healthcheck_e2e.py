import pytest

pytestmark = pytest.mark.e2e


def test_healthcheck_contract(api_get):
    _, payload = api_get("/api/v1/healthcheck/")
    assert isinstance(payload, dict)
    assert {"date", "release", "environment", "database"}.issubset(payload.keys())
    assert isinstance(payload["database"], dict)
    assert payload["database"].get("status") in {"OK", "Fail"}
