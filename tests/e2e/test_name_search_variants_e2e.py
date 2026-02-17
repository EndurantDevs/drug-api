from urllib.parse import quote

import pytest

pytestmark = pytest.mark.e2e


def _assert_group_response(payload):
    assert isinstance(payload, dict)
    assert set(payload.keys()) == {"generic", "brand"}
    assert isinstance(payload["generic"], list)
    assert isinstance(payload["brand"], list)


def test_name_products_combined(api_get, sample_catalog):
    _, payload = api_get(f"/api/v1/drug/name/{quote(sample_catalog['search_term'])}/products")
    _assert_group_response(payload)


def test_name_products_generic_only(api_get, sample_catalog):
    _, payload = api_get(f"/api/v1/drug/name/{quote(sample_catalog['search_term'])}/generic_products")
    _assert_group_response(payload)
    assert payload["brand"] == []


def test_name_products_brand_only(api_get, sample_catalog):
    _, payload = api_get(f"/api/v1/drug/name/{quote(sample_catalog['search_term'])}/brand_products")
    _assert_group_response(payload)
    assert payload["generic"] == []


def test_name_packages_combined(api_get, sample_catalog):
    _, payload = api_get(f"/api/v1/drug/name/{quote(sample_catalog['search_term'])}/packages")
    _assert_group_response(payload)


def test_name_packages_generic_only(api_get, sample_catalog):
    _, payload = api_get(f"/api/v1/drug/name/{quote(sample_catalog['search_term'])}/generic_packages")
    _assert_group_response(payload)
    assert payload["brand"] == []


def test_name_packages_brand_only(api_get, sample_catalog):
    _, payload = api_get(f"/api/v1/drug/name/{quote(sample_catalog['search_term'])}/brand_packages")
    _assert_group_response(payload)
    assert payload["generic"] == []


def test_name_products_not_found_returns_empty_groups(api_get, missing_values):
    _, payload = api_get(f"/api/v1/drug/name/{missing_values['product_name']}/products")
    _assert_group_response(payload)
    assert payload["generic"] == []
    assert payload["brand"] == []


def test_name_packages_not_found_returns_empty_groups(api_get, missing_values):
    _, payload = api_get(f"/api/v1/drug/name/{missing_values['product_name']}/packages")
    _assert_group_response(payload)
    assert payload["generic"] == []
    assert payload["brand"] == []
