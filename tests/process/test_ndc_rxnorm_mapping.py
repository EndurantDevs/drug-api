import pytest

from process import ndc_product
from process.ndc_stage import new_ndc_attempt


@pytest.fixture(autouse=True)
def inert_ndc_progress(monkeypatch):
    """Keep normalization tests independent of external telemetry services."""
    monkeypatch.setattr(ndc_product, "enqueue_live_progress", lambda **_kwargs: None)


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
    captured_rows_by_table_dict = {}

    async def fake_save_batch(_database, _attempt, products, packages):
        captured_rows_by_table_dict["product"] = products
        captured_rows_by_table_dict["package"] = packages

    monkeypatch.setattr(ndc_product, "save_ndc_batch", fake_save_batch)

    context_dict = {"ndc_attempt": new_ndc_attempt("synthetic-run", "rx_data")}
    task_dict = {"results": [_build_ndc_result({"rxcui": [1791588, "1791593"]})]}

    await ndc_product.process_results(context_dict, task_dict)

    product_rows = captured_rows_by_table_dict["product"]
    assert product_rows[0]["rxnorm_ids"] == ["1791588", "1791593"]


@pytest.mark.asyncio
async def test_process_results_sets_empty_rxnorm_ids_without_rxcui(monkeypatch):
    captured_rows_by_table_dict = {}

    async def fake_save_batch(_database, _attempt, products, packages):
        captured_rows_by_table_dict["product"] = products
        captured_rows_by_table_dict["package"] = packages

    monkeypatch.setattr(ndc_product, "save_ndc_batch", fake_save_batch)

    context_dict = {"ndc_attempt": new_ndc_attempt("synthetic-run", "rx_data")}
    task_dict = {"results": [_build_ndc_result({})]}

    await ndc_product.process_results(context_dict, task_dict)

    product_rows = captured_rows_by_table_dict["product"]
    assert product_rows[0]["rxnorm_ids"] == []


@pytest.mark.asyncio
async def test_process_results_sets_is_otc_true_from_marketing_category(monkeypatch):
    captured_rows_by_table_dict = {}

    async def fake_save_batch(_database, _attempt, products, packages):
        captured_rows_by_table_dict["product"] = products
        captured_rows_by_table_dict["package"] = packages

    monkeypatch.setattr(ndc_product, "save_ndc_batch", fake_save_batch)

    context_dict = {"ndc_attempt": new_ndc_attempt("synthetic-run", "rx_data")}
    ndc_result_dict = _build_ndc_result({"rxcui": [1791588]})
    ndc_result_dict["marketing_category"] = "OTC Monograph Final"
    task_dict = {"results": [ndc_result_dict]}

    await ndc_product.process_results(context_dict, task_dict)

    product_rows = captured_rows_by_table_dict["product"]
    assert product_rows[0]["is_otc"] is True


@pytest.mark.asyncio
async def test_process_results_sets_is_otc_false_from_product_type(monkeypatch):
    captured_rows_by_table_dict = {}

    async def fake_save_batch(_database, _attempt, products, packages):
        captured_rows_by_table_dict["product"] = products
        captured_rows_by_table_dict["package"] = packages

    monkeypatch.setattr(ndc_product, "save_ndc_batch", fake_save_batch)

    context_dict = {"ndc_attempt": new_ndc_attempt("synthetic-run", "rx_data")}
    ndc_result_dict = _build_ndc_result({"rxcui": [1791588]})
    ndc_result_dict["product_type"] = "HUMAN PRESCRIPTION DRUG"
    task_dict = {"results": [ndc_result_dict]}

    await ndc_product.process_results(context_dict, task_dict)

    product_rows = captured_rows_by_table_dict["product"]
    assert product_rows[0]["is_otc"] is False
