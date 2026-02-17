import pytest

pytestmark = pytest.mark.e2e


def test_list_product_all_default(api_get):
    _, payload = api_get("/api/v1/drug/list-product/all")
    assert isinstance(payload, list)
    assert payload, "Expected at least one product in list-product/all."
    assert {"product_ndc", "name"}.issubset(payload[0].keys())


def test_list_product_all_page_variant(api_get):
    _, payload = api_get("/api/v1/drug/list-product/all/0")
    assert isinstance(payload, list)


def test_list_product_all_page_size_variant(api_get):
    _, payload = api_get("/api/v1/drug/list-product/all/0/5")
    assert isinstance(payload, list)
    assert len(payload) <= 5


def test_list_product_all_query_filter_params(api_get):
    _, payload = api_get("/api/v1/drug/list-product/all/0/5?prefix=p&separator=%3A&suffix=s")
    assert isinstance(payload, list)
    assert len(payload) <= 5


def test_list_product_letter_default(api_get):
    _, payload = api_get("/api/v1/drug/list-product/a")
    assert isinstance(payload, list)


def test_list_product_letter_page_variant(api_get):
    _, payload = api_get("/api/v1/drug/list-product/a/0")
    assert isinstance(payload, list)


def test_list_product_letter_page_size_variant(api_get):
    _, payload = api_get("/api/v1/drug/list-product/a/0/5")
    assert isinstance(payload, list)
    assert len(payload) <= 5


def test_list_product_invalid_letter_returns_404(api_get):
    api_get("/api/v1/drug/list-product/ab", expected_status=404)


def test_list_product_negative_page_falls_back_to_zero(api_get):
    _, zero_page = api_get("/api/v1/drug/list-product/all/0/5")
    _, negative_page = api_get("/api/v1/drug/list-product/all/-1/5")
    assert negative_page == zero_page


def test_list_product_letter_negative_page_falls_back_to_zero(api_get):
    _, zero_page = api_get("/api/v1/drug/list-product/a/0/5")
    _, negative_page = api_get("/api/v1/drug/list-product/a/-1/5")
    assert negative_page == zero_page
