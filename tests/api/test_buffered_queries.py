import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from api import utils
from api.endpoint import drug


class _FakeBufferedQuery:
    def __init__(self, rows_to_return):
        self.rows_to_return = rows_to_return
        self.all_called = False

    def where(self, _clause):
        return self

    def order_by(self, *_columns):
        return self

    def limit(self, _limit):
        return self

    def offset(self, _offset):
        return self

    async def all(self):
        self.all_called = True
        return self.rows_to_return

    def iterate(self):
        raise AssertionError("bounded endpoint should use all(), not iterate()")


class _FakeStreamingQuery:
    def __init__(self, rows_to_return):
        self.rows_to_return = rows_to_return
        self.iterate_calls = 0

    def where(self, _clause):
        return self

    def order_by(self, *_columns):
        return self

    def limit(self, _limit):
        return self

    def offset(self, _offset):
        return self

    async def all(self):
        raise AssertionError("unbounded endpoint should stream via iterate(), not all()")

    async def iterate(self):
        self.iterate_calls += 1
        for row in self.rows_to_return:
            yield row


class _FakeModelRow:
    def __init__(self, payload):
        self._payload = payload

    def to_json_dict(self):
        return dict(self._payload)


class _FakeColumn:
    def __eq__(self, _other):
        return ("eq", _other)

    def ilike(self, pattern):
        return ("ilike", pattern)

    def contains(self, values):
        return ("contains", values)


@asynccontextmanager
async def _noop_transaction():
    yield None


@pytest.mark.asyncio
async def test_unbounded_name_search_helpers_stream_via_iterate(monkeypatch):
    package_query = _FakeStreamingQuery(
        [("ibuprofen", "Advil", "TABLET", "TABLET", "0573-0169-40", "40 TABLET", 40, "", 1, "BOTTLE")]
    )
    product_query = _FakeStreamingQuery([("ibuprofen", "Advil", "TABLET", "0573-0169", "Haleon")])

    monkeypatch.setattr(utils.db, "transaction", _noop_transaction)
    monkeypatch.setattr(utils, "_package_rows_query", lambda _ndcs_query: package_query)
    monkeypatch.setattr(utils, "_product_rows_query", lambda _ndcs_query, _order: product_query)

    generic_packages = await utils.get_generic_packages("ibuprofen")
    brand_packages = await utils.get_brand_packages("ibuprofen")
    generic_products = await utils.get_generic_products("ibuprofen")
    brand_products = await utils.get_brand_products("ibuprofen")

    assert package_query.iterate_calls == 2
    assert product_query.iterate_calls == 2
    assert generic_packages[0]["package_ndc"] == "0573-0169-40"
    assert brand_packages[0]["brand_name"] == "Advil"
    assert generic_products[0]["product_ndc"] == "0573-0169"
    assert brand_products[0]["labeler_name"] == "Haleon"


@pytest.mark.asyncio
async def test_bounded_drug_endpoints_buffer_via_all_and_product_lists_stream(monkeypatch):
    package_query = _FakeBufferedQuery([_FakeModelRow({"package_ndc": "0573-0169-40"})])
    list_all_query = _FakeStreamingQuery(
        [{"product_ndc": "0573-0169", "generic_name": "ibuprofen", "brand_name": "Advil"}]
    )
    list_letter_query = _FakeStreamingQuery([{"product_ndc": "0573-0169", "generic_name": "ibuprofen"}])
    evidence_query = _FakeBufferedQuery([_FakeModelRow({"evidence_id": "ev-1"})])

    fake_package_model = SimpleNamespace(product_ndc=_FakeColumn(), query=package_query)
    fake_evidence_model = SimpleNamespace(
        rxnorm_ids=_FakeColumn(),
        product_ndc=_FakeColumn(),
        set_id=_FakeColumn(),
        query=evidence_query,
    )
    monkeypatch.setattr(drug.db, "transaction", _noop_transaction)
    monkeypatch.setattr(drug, "Package", fake_package_model)
    monkeypatch.setattr(drug, "DrugConditionEvidence", fake_evidence_model)

    package_response = await drug.product_packages_obj(None, "0573-0169")
    assert json.loads(package_response.body) == [{"package_ndc": "0573-0169-40"}]
    assert package_query.all_called is True

    monkeypatch.setattr(drug.db, "select", lambda _cols: list_all_query)
    list_all_response = await drug.list_product_all(SimpleNamespace(query_args=[]))
    assert json.loads(list_all_response.body) == [{"product_ndc": "0573-0169", "name": "Advil"}]
    assert list_all_query.iterate_calls == 1

    monkeypatch.setattr(drug.db, "select", lambda _cols: list_letter_query)
    list_letter_response = await drug.list_product_letter(None, "i")
    assert json.loads(list_letter_response.body) == [{"product_ndc": "0573-0169", "name": "ibuprofen"}]
    assert list_letter_query.iterate_calls == 1

    request_with_args = SimpleNamespace(args={})
    rxnorm_response = await drug.conditions_by_rxnorm(request_with_args, "5640")
    ndc_response = await drug.conditions_by_product_ndc(request_with_args, "0573-0169")
    label_response = await drug.condition_evidence_by_label(request_with_args, "set-1")
    assert json.loads(rxnorm_response.body) == [{"evidence_id": "ev-1"}]
    assert json.loads(ndc_response.body) == [{"evidence_id": "ev-1"}]
    assert json.loads(label_response.body) == [{"evidence_id": "ev-1"}]
    assert evidence_query.all_called is True


class _SequentialFirstQuery:
    def __init__(self, first_results):
        self._first_results = list(first_results)

    def where(self, _clause):
        return self

    async def first(self):
        return self._first_results.pop(0)


@pytest.mark.asyncio
async def test_package_and_label_lookup_endpoints_resolve_nested_records(monkeypatch):
    package_row = _FakeModelRow({"package_ndc": "0573-0169-40", "product_ndc": "0573-0169"})
    product_row = _FakeModelRow({"product_ndc": "0573-0169", "spl_id": "spl-1"})
    label_row = _FakeModelRow({"id": "spl-1", "set_id": "spl-1"})

    monkeypatch.setattr(
        drug,
        "Package",
        SimpleNamespace(package_ndc=_FakeColumn(), query=_SequentialFirstQuery([package_row, package_row])),
    )
    monkeypatch.setattr(
        drug,
        "Product",
        SimpleNamespace(
            product_ndc=_FakeColumn(), query=_SequentialFirstQuery([product_row, product_row, product_row])
        ),
    )
    monkeypatch.setattr(
        drug,
        "Label",
        SimpleNamespace(
            id=_FakeColumn(),
            set_id=_FakeColumn(),
            query=_SequentialFirstQuery([None, label_row, None, label_row]),
        ),
    )

    package_payload = json.loads((await drug.package_product_ndc_obj(None, "0573-0169-40")).body)
    assert package_payload["product"]["product_ndc"] == "0573-0169"

    package_label_payload = json.loads((await drug.package_ndc_obj(None, "0573-0169-40")).body)
    assert package_label_payload["label"]["set_id"] == "spl-1"

    product_label_payload = json.loads((await drug.label_product_ndc_obj(None, "0573-0169")).body)
    assert product_label_payload["label"]["set_id"] == "spl-1"


@pytest.mark.asyncio
async def test_name_routes_and_rxnorm_package_query_builders(monkeypatch):
    async def fake_generic_products(_name):
        return [{"product_ndc": "0573-0169"}]

    async def fake_brand_products(_name):
        return [{"product_ndc": "0573-0170"}]

    async def fake_generic_packages(_name):
        return [{"package_ndc": "0573-0169-40"}]

    async def fake_brand_packages(_name):
        return [{"package_ndc": "0573-0170-40"}]

    monkeypatch.setattr(drug, "get_generic_products", fake_generic_products)
    monkeypatch.setattr(drug, "get_brand_products", fake_brand_products)
    monkeypatch.setattr(drug, "get_generic_packages", fake_generic_packages)
    monkeypatch.setattr(drug, "get_brand_packages", fake_brand_packages)

    for suffix in ("/generic_products", "/brand_products", "/products"):
        response_body = json.loads(
            (
                await drug.product_data_by_name(
                    SimpleNamespace(url=f"http://example.test/v1/drug/name/advil{suffix}"), "advil"
                )
            ).body
        )
        assert "generic" in response_body and "brand" in response_body

    for suffix in ("/generic_packages", "/brand_packages", "/packages"):
        response_body = json.loads(
            (
                await drug.package_data_by_name(
                    SimpleNamespace(url=f"http://example.test/v1/drug/name/advil{suffix}"), "advil"
                )
            ).body
        )
        assert "generic" in response_body and "brand" in response_body

    package_stmt = utils._package_rows_query(utils._generic_product_ndcs_query("ibuprofen"))
    product_stmt = utils._product_rows_query(utils._brand_product_ndcs_query("advil"), utils.Product.brand_name)
    assert "package" in str(package_stmt.statement)
    assert "product" in str(product_stmt.statement)

    rxnorm_package_query = _FakeBufferedQuery([_FakeModelRow({"package_ndc": "0573-0169-40"})])
    monkeypatch.setattr(utils.db, "select", lambda _cols: _FakeBufferedQuery([{"product_ndc": "0573-0169"}]))
    monkeypatch.setattr(utils, "Package", SimpleNamespace(product_ndc=_FakeColumn(), query=rxnorm_package_query))
    rxnorm_packages = await utils.get_packages_by_rxnorm("5640")
    assert rxnorm_packages == [{"package_ndc": "0573-0169-40"}]
    assert rxnorm_package_query.all_called is True
