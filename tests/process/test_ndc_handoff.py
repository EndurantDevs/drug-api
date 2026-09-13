"""Closed local handoff receipts, caller ownership and progress classification."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_imports
from process import control_lifecycle, ndc_handoff, ndc_product, ndc_publish


def handoff_receipt():
    """Return a complete synthetic receipt with its canonical candidate digest."""
    attempt_id = "n" + "a" * 32
    receipt_dict = {
        "format": ndc_handoff.HANDOFF_FORMAT, "run_id": "synthetic-run", "attempt_id": attempt_id,
        "schema": "synthetic_ndc", "database_oid": 10, "import_run_oid": 11,
        "complete": True, "published": False, "handed_off_at": "2026-01-01T00:00:00+00:00",
        "acquisition": {"complete": True},
        "counts": {"source_products": 1, "source_packages": 1, "product": 1, "package": 1, "batches": 1},
        "incumbent_oids": {"product": 21, "package": None},
        "tables": {name: {"name": f"{name}_{attempt_id}", "oid": identity, "row_count": 1,
                           "size_bytes": 4, "sha256": "b" * 64, "encoding": "postgres-copy-csv-v1",
                           "columns": [{"name": "key", "type": "text", "not_null": True}]}
                   for name, identity in (("product", 31), ("package", 32))},
    }
    receipt_dict["handoff_sha256"] = ndc_handoff._candidate_digest(receipt_dict)
    return receipt_dict


@pytest.mark.parametrize("corruption", ["none", "run", "attempt", "format", "published", "complete",
                                       "name", "oid", "pair", "count", "column", "timestamp", "digest", "extra"])
def test_receipt_classification_is_exact(corruption):
    receipt_dict = handoff_receipt()
    mutations_by_name = {
        "run": lambda: receipt_dict.update(run_id="another-run"),
        "attempt": lambda: receipt_dict.update(attempt_id="../candidate"),
        "format": lambda: receipt_dict.update(format="unknown"),
        "published": lambda: receipt_dict.update(published=True),
        "complete": lambda: receipt_dict.update(complete=False),
        "name": lambda: receipt_dict["tables"]["product"].update(name="product"),
        "oid": lambda: receipt_dict["tables"]["product"].update(oid=True),
        "pair": lambda: receipt_dict["tables"].pop("package"),
        "count": lambda: receipt_dict["tables"]["product"].update(row_count=2),
        "column": lambda: receipt_dict["tables"]["product"].update(columns=[{}]),
        "timestamp": lambda: receipt_dict.update(handed_off_at="2026-01-01"),
        "digest": lambda: receipt_dict.update(handoff_sha256="0" * 64),
        "extra": lambda: receipt_dict.update(extra=True),
    }
    if corruption != "none":
        mutations_by_name[corruption]()
        if corruption != "digest":
            receipt_dict["handoff_sha256"] = ndc_handoff._candidate_digest(receipt_dict)
    assert ndc_handoff.has_valid_ndc_handoff(receipt_dict, "synthetic-run") is (corruption == "none")


@pytest.mark.parametrize("receipt", [None, [], {"format": "ndc-stage-handoff-v1"}])
def test_partial_receipts_are_not_handoffs(receipt):
    assert not ndc_handoff.has_valid_ndc_handoff(receipt, "synthetic-run")


@pytest.mark.parametrize("mode,is_sample,refused", [("native", False, False), ("handoff", True, False),
                                                   ("handoff", False, True), ("other", False, True)])
@pytest.mark.asyncio
async def test_entry_refuses_borrowed_session_before_acquisition(monkeypatch, mode, is_sample, refused):
    monkeypatch.setenv("HLTHPRT_NDC_PUBLICATION_MODE", mode)
    monkeypatch.setattr(ndc_handoff, "current_session", lambda: object())
    if refused:
        monkeypatch.setattr(ndc_product, "ensure_import_run_table", AsyncMock())
        with pytest.raises((RuntimeError, ValueError)):
            await ndc_product.init_file({}, {"run_id": "synthetic-run", "test_mode": is_sample})
        ndc_product.ensure_import_run_table.assert_not_awaited()
    else:
        assert ndc_product._ndc_publication_mode({"test_mode": is_sample}) == mode


@pytest.mark.asyncio
async def test_publish_refuses_borrowed_session_before_ddl(monkeypatch):
    monkeypatch.setattr(ndc_handoff, "current_session", lambda: object())
    database = SimpleNamespace(status=AsyncMock())
    with pytest.raises(RuntimeError, match="unbound"):
        await ndc_publish.publish_ndc_tables(database, "synthetic_ndc", "attempt",
                                            attempt=object(), acquisition={"complete": True}, publication_mode="handoff")
    database.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["handoff", "other"])
async def test_unowned_or_unknown_publication_refuses_before_ddl(mode):
    database = SimpleNamespace(status=AsyncMock())
    with pytest.raises(ValueError):
        await ndc_publish.publish_ndc_tables(database, "synthetic_ndc", "attempt", publication_mode=mode)
    database.status.assert_not_awaited()


@pytest.mark.parametrize("kind", ["handoff", "wrong-run", "wrong-attempt", "running", "other-importer", "malformed"])
def test_durable_handoff_ignores_stale_progress(monkeypatch, kind):
    receipt_dict = handoff_receipt()
    run_dict = {"run_id": "synthetic-run", "importer": "ndc", "status": "finalizing",
                "phase_detail": "ndc stages awaiting publication", "progress": {"message": "awaiting publication"},
                "metrics": {"ndc_attempt_id": receipt_dict["attempt_id"], "ndc_handoff": receipt_dict}}
    if kind == "wrong-run":
        run_dict["run_id"] = "another-run"
    if kind == "wrong-attempt":
        run_dict["metrics"]["ndc_attempt_id"] = "n" + "c" * 32
    if kind == "running":
        run_dict["status"] = "running"
    if kind == "other-importer":
        run_dict["importer"] = "label"
    if kind == "malformed":
        receipt_dict["tables"].pop("package")
    reader = Mock(return_value={"phase": "stale acquisition", "status": "running"})
    monkeypatch.setattr(control_imports, "read_live_progress", reader)
    result_dict = control_imports._overlay_live_progress(run_dict)
    assert result_dict["status"] == run_dict["status"]
    assert result_dict["phase_detail"] == ("ndc stages awaiting publication" if kind == "handoff" else "stale acquisition")
    assert reader.call_count == int(kind != "handoff")


@pytest.mark.asyncio
async def test_handoff_result_never_claims_native_success(monkeypatch):
    receipt_dict = handoff_receipt()
    task = control_lifecycle.ControlTask("synthetic-run", "ndc", "process.ndc_product", "init_file", "ctx_task", {})
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    result_dict = await control_lifecycle._finish_native_result(task, receipt_dict)
    assert result_dict == {"status": "finalizing", "run_id": task.run_id, "result": receipt_dict}
    control_lifecycle._flush_terminal_status_events.assert_not_awaited()
    malformed_dict = deepcopy(receipt_dict)
    malformed_dict["tables"].pop("package")
    assert control_lifecycle._native_result_status(task, malformed_dict) == "running"


@pytest.mark.asyncio
async def test_malformed_handoff_never_announces_success(monkeypatch):
    monkeypatch.setenv("HLTHPRT_NDC_PUBLICATION_MODE", "handoff")
    monkeypatch.setenv("HLTHPRT_MAIN_RX_JSON_URL", "https://example.test/manifest")
    for name in ("ensure_import_run_table", "create_ndc_stages"):
        monkeypatch.setattr(ndc_product, name, AsyncMock())
    monkeypatch.setattr(ndc_product, "acquire_ndc_manifest", AsyncMock(return_value={}))
    monkeypatch.setattr(ndc_product, "consume_ndc_partitions", AsyncMock(return_value={"complete": True}))
    monkeypatch.setattr(ndc_product, "publish_ndc_tables", AsyncMock(return_value={"complete": True}))
    announcement = Mock()
    monkeypatch.setattr(ndc_product, "_announce_ndc_completion", announcement)
    with pytest.raises(RuntimeError, match="invalid receipt"):
        await ndc_product.init_file({}, {"run_id": "synthetic-run"})
    announcement.assert_not_called()
