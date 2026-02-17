from urllib.parse import quote

import pytest

pytestmark = pytest.mark.e2e


def test_ndc_lookup_by_product_ndc(api_get, sample_catalog):
    _, payload = api_get(f"/api/v1/drug/ndc/{quote(sample_catalog['product_ndc'])}")
    assert isinstance(payload, dict)
    assert payload.get("product_ndc") == sample_catalog["product_ndc"]
    assert "product_id" in payload


def test_ndc_lookup_not_found(api_get, missing_values):
    api_get(f"/api/v1/drug/ndc/{missing_values['product_ndc']}", expected_status=404)


def test_ndc_packages_by_product_ndc(api_get, sample_catalog):
    _, payload = api_get(f"/api/v1/drug/ndc/{quote(sample_catalog['product_ndc'])}/packages")
    assert isinstance(payload, list)
    for package in payload:
        assert package.get("product_ndc") == sample_catalog["product_ndc"]


def test_ndc_packages_unknown_product_returns_empty(api_get, missing_values):
    _, payload = api_get(f"/api/v1/drug/ndc/{missing_values['product_ndc']}/packages")
    assert payload == []


def test_ndc_package_lookup_includes_nested_product(api_get, sample_catalog):
    package_ndc = sample_catalog.get("package_ndc")
    if not package_ndc:
        pytest.skip("No package_ndc discovered in sample dataset.")

    _, payload = api_get(f"/api/v1/drug/ndc/package/{quote(package_ndc)}")
    assert isinstance(payload, dict)
    assert payload.get("package_ndc") == package_ndc
    assert isinstance(payload.get("product"), dict)
    assert payload["product"].get("product_ndc") == payload.get("product_ndc")


def test_ndc_package_lookup_not_found(api_get, missing_values):
    api_get(f"/api/v1/drug/ndc/package/{missing_values['package_ndc']}", expected_status=404)


def test_label_package_lookup_contract(api_get, sample_catalog):
    package_ndc = sample_catalog.get("package_ndc")
    if not package_ndc:
        pytest.skip("No package_ndc discovered in sample dataset.")

    _, payload = api_get(f"/api/v1/drug/label/package/{quote(package_ndc)}")
    assert isinstance(payload, dict)
    assert payload.get("package_ndc") == package_ndc
    assert isinstance(payload.get("product"), dict)
    if "label" in payload:
        assert isinstance(payload["label"], dict)


def test_label_package_lookup_not_found(api_get, missing_values):
    api_get(f"/api/v1/drug/label/package/{missing_values['package_ndc']}", expected_status=404)


def test_label_product_lookup_contract(api_get, sample_catalog):
    _, payload = api_get(f"/api/v1/drug/label/product/{quote(sample_catalog['product_ndc'])}")
    assert isinstance(payload, dict)
    assert payload.get("product_ndc") == sample_catalog["product_ndc"]
    if "label" in payload:
        assert isinstance(payload["label"], dict)


def test_label_product_lookup_not_found(api_get, missing_values):
    api_get(f"/api/v1/drug/label/product/{missing_values['product_ndc']}", expected_status=404)
