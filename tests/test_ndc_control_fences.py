"""Late queue/cancellation results must not erase native NDC publication."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from arq import Retry
from arq.jobs import serialize_job
from arq.worker import create_worker

from api import control_imports, control_run_store
from process import NDC, ndc_product


@pytest.mark.asyncio
@pytest.mark.parametrize("current_status", ["running", "succeeded", "canceled"])
async def test_late_queued_cancel_returns_current_native_state_without_announcing_stale_progress(monkeypatch, current_status):
    queued_dict = {"run_id": "synthetic-run", "importer": "ndc", "status": "queued", "metrics": {}}
    current_dict = {**queued_dict, "status": current_status, "import_id": "native-attempt", "metrics": {"ndc_publication": "retained"}}
    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(side_effect=[queued_dict, current_dict]))
    monkeypatch.setattr(control_imports, "_remove_queued_job", AsyncMock(return_value={"removed": False}))
    monkeypatch.setattr(control_imports, "db", SimpleNamespace(status=AsyncMock(return_value=0)))
    monkeypatch.setattr(control_imports, "enqueue_status_event", lambda _record: pytest.fail("stale event"))
    monkeypatch.setattr(control_imports, "_write_run_live_progress", lambda *_args, **_kwargs: pytest.fail("stale progress"))
    assert await control_imports.request_cancel("synthetic-run") is current_dict
    statement = str(control_imports.db.status.call_args.args[0])
    assert "importer <> 'ndc' OR (status='queued'" in statement
    assert "NOT (COALESCE(metrics, '{}'::jsonb) ? 'ndc_attempt_id')" in statement


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [0, 1])
async def test_enqueue_metadata_uses_current_unclaimed_ndc_guard(monkeypatch, changed):
    monkeypatch.setattr(control_run_store, "db", SimpleNamespace(status=AsyncMock(return_value=changed)))
    result = await control_run_store.update_import_run_after_enqueue("rx_data", "synthetic-run", {
        "status": "queued", "phase_detail": "enqueued", "heartbeat_at": None,
        "progress": {}, "metrics": {}, "error": None,
    })
    assert result == changed
    statement = str(control_run_store.db.status.call_args.args[0])
    assert "importer <> 'ndc' OR (status='queued'" in statement
    assert "NOT (COALESCE(metrics, '{}'::jsonb) ? 'ndc_attempt_id')" in statement


@pytest.mark.asyncio
async def test_enqueue_cas_loss_returns_durable_state_without_stale_queued_event(monkeypatch):
    current_dict = {"run_id": "synthetic-run", "status": "succeeded", "metrics": {"ndc_publication": "retained"}}
    monkeypatch.setattr(control_imports, "db", SimpleNamespace(session=_admission_session))
    monkeypatch.setattr(control_imports, "ensure_import_run_table", AsyncMock())
    monkeypatch.setattr(control_imports, "insert_import_run", AsyncMock())
    monkeypatch.setattr(control_imports, "_enqueue", AsyncMock(return_value={"status": "queued"}))
    monkeypatch.setattr(control_imports, "update_import_run_after_enqueue", AsyncMock(return_value=0))
    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(return_value=current_dict))
    monkeypatch.setattr(control_imports, "enqueue_status_event", lambda _record: pytest.fail("stale event"))
    assert await control_imports.create_import_run({"run_id": "synthetic-run", "importer": "ndc"}) == (current_dict, True)


@asynccontextmanager
async def _admission_session():
    yield SimpleNamespace(commit=AsyncMock())


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["ndc", "label"])
async def test_ndc_admission_commits_before_queue_delivery_without_changing_label(monkeypatch, importer):
    events = []

    @asynccontextmanager
    async def session():
        async def commit():
            events.append("commit")
        yield SimpleNamespace(commit=commit)

    async def insert(_schema, record_dict):
        assert record_dict["import_id"] == "logical-source-id"
        events.append("insert")

    async def enqueue(_spec, _run):
        events.append("enqueue")
        return {"status": "queued"}

    monkeypatch.setattr(control_imports, "db", SimpleNamespace(session=session))
    monkeypatch.setattr(control_imports, "ensure_import_run_table", AsyncMock())
    monkeypatch.setattr(control_imports, "insert_import_run", insert)
    monkeypatch.setattr(control_imports, "_enqueue", enqueue)
    monkeypatch.setattr(control_imports, "update_import_run_after_enqueue", AsyncMock(return_value=1))
    monkeypatch.setattr(control_imports, "enqueue_status_event", lambda _record: None)
    monkeypatch.setattr(control_imports, "_write_run_live_progress", lambda *_args, **_kwargs: None)
    result, created = await control_imports.create_import_run({
        "run_id": "synthetic-run", "importer": importer, "import_id": "logical-source-id",
    })
    assert created and result["import_id"] == "logical-source-id"
    assert events == (["insert", "commit", "enqueue"] if importer == "ndc" else ["insert", "enqueue"])


@pytest.mark.parametrize("status", ["succeeded", "failed", "canceled"])
def test_terminal_native_row_is_not_overlaid_with_stale_running_progress(monkeypatch, status):
    monkeypatch.setattr(control_imports, "read_live_progress", lambda _run: pytest.fail("terminal overlay"))
    native_row_dict = {"run_id": "synthetic-run", "status": status, "metrics": {"ndc_attempt_id": "owned"}}
    assert control_imports._overlay_live_progress(native_row_dict) is native_row_dict


@pytest.mark.asyncio
async def test_explicit_retry_enqueues_a_fresh_run_without_reopening_failed_attempt(monkeypatch):
    failed_run_dict = {"run_id": "synthetic-failed", "importer": "ndc", "status": "failed",
                  "params": {"test_mode": True}, "metrics": {"ndc_attempt_id": "old-attempt"}}
    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(return_value=failed_run_dict))
    monkeypatch.setattr(control_imports, "db", SimpleNamespace(session=_admission_session))
    monkeypatch.setattr(control_imports, "ensure_import_run_table", AsyncMock())
    monkeypatch.setattr(control_imports, "insert_import_run", AsyncMock())
    monkeypatch.setattr(control_imports, "_enqueue", AsyncMock(return_value={"status": "queued"}))
    monkeypatch.setattr(control_imports, "update_import_run_after_enqueue", AsyncMock(return_value=1))
    monkeypatch.setattr(control_imports, "enqueue_status_event", lambda _record: None)
    monkeypatch.setattr(control_imports, "_write_run_live_progress", lambda *_args, **_kwargs: None)
    retried, created = await control_imports.retry_import_run(failed_run_dict["run_id"], {})
    assert created and retried["run_id"] != failed_run_dict["run_id"]
    assert retried["retry_of_run_id"] == failed_run_dict["run_id"] and retried["params"] == failed_run_dict["params"]
    assert "ndc_attempt_id" not in retried["metrics"]
    assert control_imports._enqueue.call_args.args[1]["run_id"] == retried["run_id"]
    assert control_imports.update_import_run_after_enqueue.call_args.args[1] == retried["run_id"]
    assert failed_run_dict["status"] == "failed" and failed_run_dict["metrics"] == {"ndc_attempt_id": "old-attempt"}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["retry", "cancel"])
async def test_ndc_worker_finishes_failed_acquisition_without_requeuing_owned_attempt(monkeypatch, failure):
    monkeypatch.setenv("HLTHPRT_MAIN_RX_JSON_URL", "https://example.test/ndc/manifest")
    for name in ("ensure_import_run_table", "create_ndc_stages", "discard_ndc_stages", "publish_ndc_tables"):
        monkeypatch.setattr(ndc_product, name, AsyncMock())
    monkeypatch.setattr(ndc_product, "fail_ndc_attempt", AsyncMock(return_value=1))
    monkeypatch.setattr(ndc_product, "_announce_ndc_failure", lambda _attempt: None)
    monkeypatch.setattr(ndc_product, "acquire_ndc_manifest", AsyncMock(
        side_effect=Retry() if failure == "retry" else asyncio.CancelledError()))
    pipeline = MagicMock()
    pipeline.__aenter__.return_value = pipeline
    pipeline.execute = AsyncMock(return_value=[serialize_job(
        "init_file", ({"run_id": "synthetic-run"},), {}, None, 0, serializer=NDC.job_serializer), 1, True])
    redis = MagicMock()
    redis.pipeline.return_value = pipeline
    worker = create_worker(NDC, redis_pool=redis, handle_signals=False, keep_result=0)
    monkeypatch.setattr(worker, "finish_job", AsyncMock())

    await worker.run_job("synthetic-job", 0)

    assert worker.jobs_failed == 1 and worker.jobs_retried == worker.jobs_complete == 0
    assert not worker.job_tasks
    worker.finish_job.assert_awaited_once()
    assert worker.finish_job.call_args.args[1] is True
    ndc_product.create_ndc_stages.assert_awaited_once()
    attempt = ndc_product.create_ndc_stages.call_args.args[1]
    ndc_product.fail_ndc_attempt.assert_awaited_once_with(ndc_product.db, attempt)
    ndc_product.discard_ndc_stages.assert_awaited_once_with(ndc_product.db, attempt)
    ndc_product.publish_ndc_tables.assert_not_called()
