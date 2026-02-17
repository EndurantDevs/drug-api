import pytest

pytestmark = pytest.mark.e2e


def test_drug_status_contract(api_get):
    _, payload = api_get("/api/v1/drug/")
    assert isinstance(payload, dict)
    assert {"date", "release", "environment", "product_count", "package_count"}.issubset(payload.keys())
    assert isinstance(payload["product_count"], int)
    assert isinstance(payload["package_count"], int)
    assert payload["product_count"] >= 0
    assert payload["package_count"] >= 0
