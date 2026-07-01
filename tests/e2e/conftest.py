import os
import re
from urllib.parse import quote

import httpx
import pytest


@pytest.fixture(scope="session")
def e2e_base_url():
    base_url = os.environ.get("E2E_BASE_URL")
    if not base_url:
        pytest.skip("E2E_BASE_URL is not set; skipping e2e tests.")
    return base_url.rstrip("/")


@pytest.fixture(scope="session")
def e2e_client(e2e_base_url):
    with httpx.Client(
        base_url=e2e_base_url,
        timeout=30,
        headers={"Accept": "application/json"},
        follow_redirects=False,
    ) as client:
        yield client


@pytest.fixture(scope="session")
def api_get(e2e_client):
    def _get(path, expected_status=200):
        response = e2e_client.get(path)
        assert (
            response.status_code == expected_status
        ), f"GET {path} expected {expected_status}, got {response.status_code}: {response.text[:400]}"

        try:
            payload = response.json()
        except ValueError:
            payload = response.text
        return response, payload

    return _get


@pytest.fixture(scope="session")
def missing_values():
    return {
        "product_ndc": "0000-0000",
        "package_ndc": "0000-0000-0",
        "rxnorm_id": "999999999999",
        "product_name": "notarealdrugzz",
    }


def _pick_search_term(*values):
    for value in values:
        if not value:
            continue
        for token in re.split(r"[^A-Za-z0-9]+", str(value).lower()):
            if len(token) >= 3:
                return token
    return "aspirin"


@pytest.fixture(scope="session")
def sample_catalog(api_get):
    _, catalog_rows = api_get("/api/v1/drug/list-product/all/0/100")
    assert isinstance(catalog_rows, list) and catalog_rows, "Expected non-empty /list-product/all response."

    selected_product_dict = None
    for catalog_row in catalog_rows:
        product_ndc = catalog_row.get("product_ndc")
        if not product_ndc:
            continue

        _, product = api_get(f"/api/v1/drug/ndc/{quote(product_ndc)}")
        _, packages = api_get(f"/api/v1/drug/ndc/{quote(product_ndc)}/packages")

        package_ndc = None
        if isinstance(packages, list) and packages:
            package_ndc = packages[0].get("package_ndc")

        rxnorm_ids = product.get("rxnorm_ids") or []
        rxnorm_id = str(rxnorm_ids[0]).strip() if rxnorm_ids else None
        search_term = _pick_search_term(product.get("generic_name"), product.get("brand_name"), catalog_row.get("name"))

        selected_product_dict = {
            "product_ndc": product_ndc,
            "package_ndc": package_ndc,
            "rxnorm_id": rxnorm_id,
            "search_term": search_term,
            "raw_name": catalog_row.get("name") or search_term,
        }
        if package_ndc and rxnorm_id:
            break

    assert selected_product_dict is not None, "Could not discover a product from list endpoints."
    return selected_product_dict
