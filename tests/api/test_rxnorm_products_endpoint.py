import json

import pytest
import sanic.exceptions

from api.endpoint import drug


@pytest.mark.asyncio
async def test_products_by_rxnorm_returns_products(monkeypatch):
    expected_products = [{"product_ndc": "10019-929", "rxnorm_ids": ["1791588"]}]

    async def fake_get_products_by_rxnorm(rxnorm_id):
        assert rxnorm_id == "1791588"
        return expected_products

    monkeypatch.setattr(drug, "get_products_by_rxnorm", fake_get_products_by_rxnorm)

    response = await drug.products_by_rxnorm(None, " 1791588 ")

    assert response.status == 200
    assert json.loads(response.body) == expected_products


@pytest.mark.asyncio
async def test_products_by_rxnorm_returns_404_when_empty(monkeypatch):
    async def fake_get_products_by_rxnorm(_rxnorm_id):
        return []

    monkeypatch.setattr(drug, "get_products_by_rxnorm", fake_get_products_by_rxnorm)

    with pytest.raises(sanic.exceptions.NotFound):
        await drug.products_by_rxnorm(None, "9999999")


@pytest.mark.asyncio
async def test_packages_by_rxnorm_returns_packages(monkeypatch):
    expected_packages = [{"package_ndc": "10019-929-01"}]

    async def fake_get_packages_by_rxnorm(rxnorm_id):
        assert rxnorm_id == "1791588"
        return expected_packages

    monkeypatch.setattr(drug, "get_packages_by_rxnorm", fake_get_packages_by_rxnorm)

    response = await drug.packages_by_rxnorm(None, " 1791588 ")

    assert response.status == 200
    assert json.loads(response.body) == expected_packages


@pytest.mark.asyncio
async def test_packages_by_rxnorm_returns_404_when_empty(monkeypatch):
    async def fake_get_packages_by_rxnorm(_rxnorm_id):
        return []

    monkeypatch.setattr(drug, "get_packages_by_rxnorm", fake_get_packages_by_rxnorm)

    with pytest.raises(sanic.exceptions.NotFound):
        await drug.packages_by_rxnorm(None, "9999999")
