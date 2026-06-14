import pytest

from api import utils


class _FakeSelect:
    def __init__(self, rows):
        self.rows = rows

    def where(self, _clause):
        return self

    async def all(self):
        return self.rows


class _FakeColumn:
    def in_(self, values):
        return values


class _FakeQuery:
    def __init__(self, expected_product_ids, packages):
        self.expected_product_ids = expected_product_ids
        self.packages = packages

    def where(self, product_ids):
        self.product_ids = product_ids
        return self

    async def all(self):
        if self.product_ids == self.expected_product_ids:
            return self.packages
        return []


class _FakePackageRow:
    def __init__(self, package_ndc):
        self.package_ndc = package_ndc

    def to_json_dict(self):
        return {"package_ndc": self.package_ndc}


class _FakePackage:
    product_ndc = _FakeColumn()

    def __init__(self, query):
        self.query = query


@pytest.mark.asyncio
async def test_get_packages_by_rxnorm_uses_full_scalar_product_ndc_rows(monkeypatch):
    expected_product_ids = ["63323-476", "51662-1407"]
    packages = [_FakePackageRow("51662-1407-1")]

    monkeypatch.setattr(utils.db, "select", lambda _columns: _FakeSelect(expected_product_ids))
    monkeypatch.setattr(utils, "Package", _FakePackage(_FakeQuery(expected_product_ids, packages)))

    assert await utils.get_packages_by_rxnorm("992801") == [{"package_ndc": "51662-1407-1"}]
