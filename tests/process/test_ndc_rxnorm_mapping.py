import pytest

from process import ndc_product


def _build_ndc_result(openfda):
    return {
        "product_ndc": "12345-678",
        "product_id": "12345-678_test",
        "generic_name": "Test Drug",
        "brand_name": "Test Drug",
        "labeler_name": "Test Lab",
        "dosage_form": "TABLET",
        "openfda": openfda,
        "packaging": [
            {
                "package_ndc": "12345-678-90",
                "description": "1 TABLET in 1 BLISTER PACK (12345-678-90)",
            }
        ],
    }


@pytest.mark.asyncio
async def test_process_results_maps_openfda_rxcui_to_rxnorm_ids(monkeypatch):
    captured = {}

    async def fake_push_objects(obj_list, cls):
        captured[cls.__tablename__] = obj_list

    monkeypatch.setattr(ndc_product, "push_objects", fake_push_objects)

    ctx = {"import_date": "20260213", "context": {"run": 0}}
    task = {"results": [_build_ndc_result({"rxcui": [1791588, "1791593"]})]}

    await ndc_product.process_results(ctx, task)

    rows = captured["product_20260213"]
    assert rows[0]["rxnorm_ids"] == ["1791588", "1791593"]


@pytest.mark.asyncio
async def test_process_results_sets_empty_rxnorm_ids_without_rxcui(monkeypatch):
    captured = {}

    async def fake_push_objects(obj_list, cls):
        captured[cls.__tablename__] = obj_list

    monkeypatch.setattr(ndc_product, "push_objects", fake_push_objects)

    ctx = {"import_date": "20260213", "context": {"run": 0}}
    task = {"results": [_build_ndc_result({})]}

    await ndc_product.process_results(ctx, task)

    rows = captured["product_20260213"]
    assert rows[0]["rxnorm_ids"] == []


@pytest.mark.asyncio
async def test_process_results_sets_is_otc_true_from_marketing_category(monkeypatch):
    captured = {}

    async def fake_push_objects(obj_list, cls):
        captured[cls.__tablename__] = obj_list

    monkeypatch.setattr(ndc_product, "push_objects", fake_push_objects)

    ctx = {"import_date": "20260213", "context": {"run": 0}}
    row = _build_ndc_result({"rxcui": [1791588]})
    row["marketing_category"] = "OTC Monograph Final"
    task = {"results": [row]}

    await ndc_product.process_results(ctx, task)

    rows = captured["product_20260213"]
    assert rows[0]["is_otc"] is True


@pytest.mark.asyncio
async def test_process_results_sets_is_otc_false_from_product_type(monkeypatch):
    captured = {}

    async def fake_push_objects(obj_list, cls):
        captured[cls.__tablename__] = obj_list

    monkeypatch.setattr(ndc_product, "push_objects", fake_push_objects)

    ctx = {"import_date": "20260213", "context": {"run": 0}}
    row = _build_ndc_result({"rxcui": [1791588]})
    row["product_type"] = "HUMAN PRESCRIPTION DRUG"
    task = {"results": [row]}

    await ndc_product.process_results(ctx, task)

    rows = captured["product_20260213"]
    assert rows[0]["is_otc"] is False
