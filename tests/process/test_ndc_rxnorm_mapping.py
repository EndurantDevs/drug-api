import datetime
from copy import deepcopy
from itertools import permutations

import pytest

from process import ndc_product
from process.ndc_stage import _unique_ndc_rows, new_ndc_attempt


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


@pytest.mark.asyncio
@pytest.mark.parametrize("dates", [*permutations(("20230201", "20250715", "20240101")),
                                  (None, "20230201"), ("20230201", None)])
async def test_repeated_package_dates_are_unknown_without_losing_source_occurrences(monkeypatch, caplog, dates):
    result = _build_ndc_result({})
    package = result["packaging"][0]
    result["packaging"] = [{**package, "marketing_start_date": date} for date in dates]
    captured_rows = []

    async def save(_database, _attempt, products, packages):
        assert len(products) == 1
        assert len(_unique_ndc_rows(packages, "package_ndc")) == 1
        captured_rows.extend(packages)

    monkeypatch.setattr(ndc_product, "save_ndc_batch", save)
    await ndc_product.process_results({"ndc_attempt": new_ndc_attempt("synthetic-run", "rx_data")},
                                      {"results": [result]})
    assert len(captured_rows) == len(dates)
    assert all(row["marketing_start_date"] is None for row in captured_rows)
    assert [row["marketing_start_date"] for row in result["packaging"]] == list(dates)
    assert "using unknown dates for 1 identities" in caplog.text


@pytest.mark.parametrize("distinct_packages", [False, True])
def test_equal_or_distinct_package_dates_keep_their_values(caplog, distinct_packages):
    result = _build_ndc_result({})
    package = result["packaging"][0]
    result["packaging"] = [{**package, "marketing_start_date": "20230201"},
                           {**package, "package_ndc": "12345-678-91" if distinct_packages else package["package_ndc"],
                            "marketing_start_date": "20250715" if distinct_packages else "20230201"}]
    product = ndc_product._product_row_dict_from_record(result, [c.name for c in ndc_product.Product.__table__.columns])
    rows = ndc_product._package_rows_from_record(result, product, [c.name for c in ndc_product.Package.__table__.columns])
    assert [row["marketing_start_date"] for row in rows] == [datetime.date(2023, 2, 1),
        datetime.date(2025, 7, 15) if distinct_packages else datetime.date(2023, 2, 1)]
    assert not caplog.records


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", ["description", "sample", "different_product_record"])
async def test_package_date_normalization_does_not_hide_other_conflicts(monkeypatch, conflict):
    first = _build_ndc_result({})
    first["packaging"][0]["marketing_start_date"] = "20230201"
    second = deepcopy(first)
    second["packaging"][0]["marketing_start_date"] = "20250715"
    if conflict == "description":
        second["packaging"][0]["description"] = "different synthetic package"
    if conflict == "sample":
        second["packaging"][0]["sample"] = True
    if conflict == "different_product_record":
        second["product_id"] = "12345-678_second"
        results = [first, second]
    else:
        first["packaging"].extend(second["packaging"])
        results = [first]

    async def save(_database, _attempt, _products, packages):
        _unique_ndc_rows(packages, "package_ndc")

    monkeypatch.setattr(ndc_product, "save_ndc_batch", save)
    with pytest.raises(ValueError, match="conflicting NDC records"):
        await ndc_product.process_results({"ndc_attempt": new_ndc_attempt("synthetic-run", "rx_data")},
                                          {"results": results})
