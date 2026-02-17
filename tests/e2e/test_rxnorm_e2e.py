from urllib.parse import quote

import pytest

pytestmark = pytest.mark.e2e


def test_rxnorm_products_known_id(api_get, sample_catalog):
    rxnorm_id = sample_catalog.get("rxnorm_id")
    if not rxnorm_id:
        pytest.skip("No rxnorm_id discovered in sample dataset.")

    _, payload = api_get(f"/api/v1/drug/rxnorm/{quote(rxnorm_id)}/products")
    assert isinstance(payload, list)
    assert payload
    for row in payload:
        assert isinstance(row, dict)
        assert "product_id" in row


def test_rxnorm_products_unknown_id_returns_404(api_get, missing_values):
    api_get(f"/api/v1/drug/rxnorm/{missing_values['rxnorm_id']}/products", expected_status=404)


def test_rxnorm_packages_known_id(api_get, sample_catalog):
    rxnorm_id = sample_catalog.get("rxnorm_id")
    if not rxnorm_id:
        pytest.skip("No rxnorm_id discovered in sample dataset.")

    _, payload = api_get(f"/api/v1/drug/rxnorm/{quote(rxnorm_id)}/packages")
    assert isinstance(payload, list)
    assert payload
    for row in payload:
        assert isinstance(row, dict)
        assert "package_ndc" in row


def test_rxnorm_packages_unknown_id_returns_404(api_get, missing_values):
    api_get(f"/api/v1/drug/rxnorm/{missing_values['rxnorm_id']}/packages", expected_status=404)
