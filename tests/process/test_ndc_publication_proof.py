"""Synthetic acquisition and coordinator failure boundaries; no external services."""

import asyncio
import hashlib
import io
import json
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import control_lifecycle, ndc_acquire, ndc_product, ndc_publish, ndc_stage
from process.ext import utils
from tests.process.ndc_publication_fixtures import install_coordinator_sources


def _zip_records(records):
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("records.json", json.dumps({"results": records}, ensure_ascii=False))
    return archive.getvalue()


def _install_downloads(monkeypatch, payloads_by_url):
    created_paths = []

    async def download(url, path, *, max_bytes):
        """Use only exact synthetic bytes and retain cleanup observations."""
        payload = payloads_by_url[url]
        assert len(payload) <= max_bytes
        Path(path).write_bytes(payload)
        created_paths.append(Path(path))
        return {"url": url, "size_bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}

    monkeypatch.setattr(ndc_acquire, "download_it_and_save", download)
    return created_paths


def _manifest(partitions):
    return json.dumps({"results": {"drug": {"ndc": {
        "export_date": "2026-01-01",
        "total_records": sum(part["records"] for part in partitions), "partitions": partitions,
    }}}}).encode()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_sample,expected_count", [(False, 3), (True, 1)])
async def test_acquisition_authenticates_actual_archives_and_acknowledges_every_batch(monkeypatch, is_sample, expected_count):
    partitions = [{"file": "https://synthetic.invalid/one.zip", "records": 2},
                  {"file": "https://synthetic.invalid/two.zip", "records": 1}]
    payloads_by_url = {partitions[0]["file"]: _zip_records([{"id": "é\n1"}, {"id": "2"}]),
                       partitions[1]["file"]: _zip_records([{"id": "3"}]),
                       "https://synthetic.invalid/manifest": _manifest(partitions)}
    created_paths = _install_downloads(monkeypatch, payloads_by_url)
    monkeypatch.setenv("SAVE_PER_PACK", "1")
    batches = []

    async def acknowledge(records):
        batches.append(records)

    manifest = await ndc_acquire.acquire_ndc_manifest("https://synthetic.invalid/manifest",
                                                    sample=is_sample, max_records=1)
    acquisition = await ndc_acquire.consume_ndc_partitions(manifest, acknowledge)
    assert acquisition["complete"] is not is_sample
    assert acquisition["export_date"] == "2026-01-01"
    section = json.loads(payloads_by_url["https://synthetic.invalid/manifest"])["results"]["drug"]["ndc"]
    assert acquisition["ndc_section_sha256"] == ndc_acquire._canonical_source_sha256(section)
    assert acquisition["selected_partitions_sha256"] == ndc_acquire._canonical_source_sha256(partitions[:1] if is_sample else partitions)
    assert sum(map(len, batches)) == expected_count
    assert sum(part["records"] for part in acquisition["partitions"]) == expected_count
    for receipt in acquisition["partitions"]:
        payload = payloads_by_url[receipt["url"]]
        assert receipt["sha256"] == hashlib.sha256(payload).hexdigest()
        assert receipt["size_bytes"] == len(payload)
    assert all(not path.parent.exists() for path in created_paths)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["truncated", "census", "batch"])
async def test_partition_failure_after_earlier_acknowledgment_never_returns_closure(monkeypatch, failure):
    partition_dict = {"file": "https://synthetic.invalid/one.zip", "records": 2}
    payload = _zip_records([{"id": "1"}, {"id": "2"}])
    if failure == "truncated":
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as output:
            output.writestr("records.json", '{"results":[{"id":"1"},{"id":"2"}')
        payload = archive.getvalue()
    if failure == "census":
        partition_dict["records"] = 3
    paths = _install_downloads(monkeypatch, {partition_dict["file"]: payload})
    monkeypatch.setenv("SAVE_PER_PACK", "1")
    acknowledged_records = []

    async def consume_batch(records):
        if failure == "batch" and acknowledged_records:
            raise RuntimeError("synthetic save failure")
        acknowledged_records.extend(records)

    with pytest.raises(Exception):
        await ndc_acquire._consume_declared_ndc_partition(partition_dict, 0, consume_batch)
    assert acknowledged_records
    assert all(not path.parent.exists() for path in paths)


@pytest.mark.asyncio
@pytest.mark.parametrize("should_cancel", [False, True])
async def test_parallel_failure_or_cancellation_drains_peer_work(monkeypatch, should_cancel):
    started = asyncio.Event()
    released = asyncio.Event()
    entered_partitions = []

    async def consume_partition(partition, _limit, _consumer):
        """Keep the second peer alive until cancellation must drain it."""
        entered_partitions.append(partition["file"])
        if partition["file"] == "one":
            await started.wait()
            if not should_cancel:
                raise RuntimeError("source failed")
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            released.set()

    monkeypatch.setattr(ndc_acquire, "_consume_declared_ndc_partition", consume_partition)
    monkeypatch.setenv("HLTHPRT_NDC_PARTITION_CONCURRENCY", "2")
    manifest_dict = {"selected": [{"file": "one"}, {"file": "two"}], "sample_limit": 0}
    task = asyncio.create_task(ndc_acquire.consume_ndc_partitions(manifest_dict, AsyncMock()))
    if should_cancel:
        await started.wait()
        task.cancel()
    with pytest.raises(asyncio.CancelledError if should_cancel else ExceptionGroup):
        await task
    assert released.is_set()
    assert entered_partitions == ["one", "two"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["total", "empty", "records", "url", "too_many"])
async def test_manifest_rejects_missing_or_inconsistent_census(monkeypatch, change):
    payload = json.loads(_manifest([{"file": "https://synthetic.invalid/one", "records": 1}]))
    ndc = payload["results"]["drug"]["ndc"]
    if change == "total":
        ndc["total_records"] = 2
    elif change == "empty":
        ndc["partitions"] = []
    elif change == "records":
        ndc["partitions"][0]["records"] = True
    elif change == "url":
        ndc["partitions"][0]["file"] = None
    else:
        ndc["partitions"] *= 65
    _install_downloads(monkeypatch, {"manifest": json.dumps(payload).encode()})
    with pytest.raises(ValueError):
        await ndc_acquire.acquire_ndc_manifest("manifest", sample=False, max_records=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("row_error", [False, True])
async def test_paired_save_counts_only_after_both_tables_commit(monkeypatch, row_error):
    attempt = ndc_stage.new_ndc_attempt("synthetic-run", "rx_data")
    events = []

    @asynccontextmanager
    async def transaction():
        try:
            yield "session"
        except Exception:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    async def insert_rows(_session, table, rows, _key):
        if table.name.startswith("package_") and row_error:
            raise RuntimeError("package failed")
        return len(rows)

    monkeypatch.setattr(ndc_stage, "lock_ndc_stages", AsyncMock())
    monkeypatch.setattr(ndc_stage, "_check_ndc_run_owner", AsyncMock())
    monkeypatch.setattr(ndc_stage, "_insert_verified_rows", insert_rows)
    database = SimpleNamespace(transaction=transaction)
    if row_error:
        with pytest.raises(RuntimeError, match="package failed"):
            await ndc_stage.save_ndc_batch(database, attempt, [{}], [{}])
        assert set(attempt.counts.values()) == {0}
    else:
        await ndc_stage.save_ndc_batch(database, attempt, [{}], [{}])
        assert set(attempt.counts.values()) == {1}
    assert events == ["rollback" if row_error else "commit"]


@pytest.mark.parametrize("rows,should_fail", [([], False), ([{"id": "1"}] * 2, False),
                                              ([{"id": ""}], True), ([{"id": None}], True),
                                              ([{"id": "1"}, {"id": "1", "value": 2}], True)])
def test_missing_or_conflicting_row_identity_is_never_dropped(rows, should_fail):
    if should_fail:
        with pytest.raises(ValueError):
            ndc_stage._unique_ndc_rows(rows, "id")
    else:
        assert len(ndc_stage._unique_ndc_rows(rows, "id")) == bool(rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("persisted", [[{"product_id": "one"}], [{"product_id": "other"}], []])
async def test_insert_verification_checks_the_actual_persisted_rows(persisted):
    attempt = ndc_stage.new_ndc_attempt("synthetic-run", "rx_data")
    session = SimpleNamespace(execute=AsyncMock(side_effect=[SimpleNamespace(all=lambda: ["one"]),
                                                          SimpleNamespace(mappings=lambda: persisted)]))
    rows = [{"product_id": "one"}]
    if persisted == rows:
        assert await ndc_stage._insert_verified_rows(session, attempt.tables["product"], rows, "product_id") == 1
    else:
        with pytest.raises(RuntimeError, match="differs"):
            await ndc_stage._insert_verified_rows(session, attempt.tables["product"], rows, "product_id")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "source", "save", "publish", "cancel"])
async def test_coordinator_never_publishes_before_complete_acquisition(monkeypatch, failure):
    events = []
    terminal_events = []
    monkeypatch.setattr(ndc_product, "enqueue_status_event", terminal_events.append)
    monkeypatch.setenv("HLTHPRT_MAIN_RX_JSON_URL", "https://synthetic.invalid/manifest")
    monkeypatch.setattr(ndc_product, "ensure_import_run_table", AsyncMock())
    monkeypatch.setattr(ndc_product, "create_ndc_stages", AsyncMock())
    monkeypatch.setattr(ndc_product, "fail_ndc_attempt", AsyncMock(return_value=1))
    monkeypatch.setattr(ndc_product, "discard_ndc_stages", AsyncMock())
    monkeypatch.setattr(ndc_product, "acquire_ndc_manifest", AsyncMock(return_value={"selected": []}))
    monkeypatch.setattr(ndc_product, "process_results", AsyncMock(side_effect=RuntimeError("save") if failure == "save" else None))

    async def consume(_manifest, consume_batch):
        events.append("acquire")
        if failure in {"source", "cancel"}:
            raise asyncio.CancelledError() if failure == "cancel" else RuntimeError("source")
        await consume_batch([{}])
        events.append("acknowledged")
        return {"complete": True}

    async def publish(*_args, **_kwargs):
        events.append("publish")
        if failure == "publish":
            raise RuntimeError("publish")
        return {"complete": True}

    monkeypatch.setattr(ndc_product, "consume_ndc_partitions", consume)
    monkeypatch.setattr(ndc_product, "publish_ndc_tables", publish)
    monkeypatch.setattr(ndc_product, "enqueue_live_progress", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("event")))
    if failure:
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
            await ndc_product.init_file({}, {"run_id": "synthetic-run"})
        ndc_product.fail_ndc_attempt.assert_awaited_once()
        ndc_product.discard_ndc_stages.assert_awaited_once()
        assert len(terminal_events) == 1 and terminal_events[0]["status"] == "failed"
        assert terminal_events[0]["error"] == {"code": "ndc_import_failed"}
        assert ("publish" in events) is (failure == "publish")
    else:
        assert await ndc_product.init_file({}, {"run_id": "synthetic-run"}) == {"complete": True}
        ndc_product.fail_ndc_attempt.assert_not_awaited()
        ndc_product.discard_ndc_stages.assert_not_awaited()
        assert events == ["acquire", "acknowledged", "publish"]
        assert len(terminal_events) == 1 and terminal_events[0]["status"] == "succeeded"
        assert terminal_events[0]["metrics"]["ndc_publication"] == {"complete": True}


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [0, 1])
async def test_generic_run_mark_never_announces_a_failed_row_transition(monkeypatch, changed):
    monkeypatch.setattr(control_lifecycle, "ensure_import_run_table", AsyncMock())
    monkeypatch.setattr(control_lifecycle, "db", SimpleNamespace(status=AsyncMock(return_value=changed)))
    events = []
    monkeypatch.setattr(control_lifecycle, "enqueue_live_progress", lambda **_kwargs: events.append("progress"))
    monkeypatch.setattr(control_lifecycle, "enqueue_status_event", lambda _record: events.append("status"))
    await control_lifecycle.mark_control_run("synthetic-run", status="failed", phase_detail="failure", progress_message="failed")
    statement = str(control_lifecycle.db.status.call_args.args[0])
    assert "importer <> 'ndc' OR NOT (COALESCE(metrics, '{}'::jsonb) ? 'ndc_attempt_id')" in statement
    assert events == (["progress", "status"] if changed else [])


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [2, 3, None])
async def test_download_digest_covers_exact_written_chunks_and_enforces_cap(limit):
    async def chunks(**_kwargs):
        yield b"a"
        yield b"bc"

    response = SimpleNamespace(status_code=200, aiter_bytes=chunks)
    destination = SimpleNamespace(write=AsyncMock())
    if limit == 2:
        with pytest.raises(ValueError, match="byte limit"):
            await utils._write_response_chunks(response, destination, max_bytes=limit)
        assert destination.write.call_count == 1
    else:
        assert await utils._write_response_chunks(response, destination, max_bytes=limit) == {
            "sha256": hashlib.sha256(b"abc").hexdigest(), "size_bytes": 3,
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", ["0", "17", "nan"])
async def test_partition_admission_rejects_unbounded_settings(monkeypatch, limit):
    if limit == "nan":
        monkeypatch.setenv("HLTHPRT_NDC_PARTITION_TIMEOUT_SECONDS", limit)
    else:
        monkeypatch.setenv("HLTHPRT_NDC_PARTITION_CONCURRENCY", limit)
    manifest_dict = {"selected": [{"file": "one"}], "sample_limit": 0}
    with pytest.raises(ExceptionGroup if limit == "nan" else ValueError):
        await ndc_acquire.consume_ndc_partitions(manifest_dict, AsyncMock())


@pytest.mark.asyncio
async def test_worker_shutdown_does_not_publish_or_mark_success(monkeypatch):
    monkeypatch.setattr(ndc_product, "db", SimpleNamespace(disconnect=AsyncMock()))
    monkeypatch.setattr(ndc_product, "publish_ndc_tables", AsyncMock())
    await ndc_product.shutdown({"import_date": "20200101", "context": {"product_count": 100}})
    ndc_product.db.disconnect.assert_awaited_once()
    ndc_product.publish_ndc_tables.assert_not_awaited()


def test_control_result_preserves_native_terminal_receipt_binding():
    task = control_lifecycle.ControlTask("synthetic-run", "ndc", "process.ndc_product", "init_file", "ctx_task", {})
    receipt_dict = {"format": "ndc-publication-v1", "run_id": "synthetic-run"}
    assert control_lifecycle._native_result_status(task, receipt_dict) == "succeeded"
    assert control_lifecycle._native_result_status(task, {**receipt_dict, "run_id": "other"}) == "running"
    assert control_lifecycle._native_result_status(task, None) == "running"


@pytest.mark.asyncio
async def test_insert_verification_bounds_sql_batches_without_omitting_late_rows():
    attempt = ndc_stage.new_ndc_attempt("synthetic-run", "rx_data")
    rows = [{"product_id": str(index)} for index in range(1201)]
    pending_rows = []
    batch_sizes = []

    async def execute(_statement, parameters=None):
        if parameters is not None:
            pending_rows[:] = parameters
            batch_sizes.append(len(parameters))
            return SimpleNamespace(all=lambda: parameters)
        return SimpleNamespace(mappings=lambda: list(pending_rows))

    session = SimpleNamespace(execute=execute)
    assert await ndc_stage._insert_verified_rows(session, attempt.tables["product"], rows, "product_id") == 1201
    assert batch_sizes == [500, 500, 201]


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_date", [None, "20260101", "2026-13-01", 20260101])
async def test_manifest_requires_unambiguous_native_export_date(monkeypatch, invalid_date):
    payload = json.loads(_manifest([{"file": "one", "records": 1}]))
    payload["results"]["drug"]["ndc"]["export_date"] = invalid_date
    _install_downloads(monkeypatch, {"manifest": json.dumps(payload).encode()})
    with pytest.raises(ValueError):
        await ndc_acquire.acquire_ndc_manifest("manifest", sample=False, max_records=1)


@pytest.mark.asyncio
async def test_manifest_rejects_repeated_partition_scope_even_when_totals_match(monkeypatch):
    payload = _manifest([{"file": "one", "records": 1}, {"file": "one", "records": 1}])
    _install_downloads(monkeypatch, {"manifest": payload})
    with pytest.raises(ValueError, match="repeats a partition"):
        await ndc_acquire.acquire_ndc_manifest("manifest", sample=False, max_records=1)


@pytest.mark.asyncio
async def test_scoped_source_identity_does_not_change_for_other_endpoint_updates(monkeypatch):
    payload = json.loads(_manifest([{"file": "one", "records": 1}]))
    first_bytes = json.dumps(payload).encode()
    payload["results"]["drug"]["label"] = {"export_date": "2026-02-01", "partitions": []}
    _install_downloads(monkeypatch, {"first": first_bytes, "second": json.dumps(payload).encode()})
    first = await ndc_acquire.acquire_ndc_manifest("first", sample=False, max_records=1)
    second = await ndc_acquire.acquire_ndc_manifest("second", sample=False, max_records=1)
    assert first["manifest"]["sha256"] != second["manifest"]["sha256"]
    for key in ("export_date", "ndc_section_sha256", "selected_partitions_sha256"):
        assert first[key] == second[key]


@pytest.mark.asyncio
@pytest.mark.parametrize("is_native_success", [False, True])
async def test_native_result_flushes_terminal_event_after_committed_result(monkeypatch, is_native_success):
    task = control_lifecycle.ControlTask("synthetic-run", "ndc", "process.ndc_product", "init_file", "ctx_task", {})
    receipt_dict = {"format": "ndc-publication-v1", "run_id": "synthetic-run"} if is_native_success else None
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    result = await control_lifecycle._finish_native_result(task, receipt_dict)
    assert result == {"status": "succeeded" if is_native_success else "running", "run_id": task.run_id, "result": receipt_dict}
    assert control_lifecycle._flush_terminal_status_events.await_count == int(is_native_success)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [0, 1])
async def test_coordinator_failure_event_requires_owned_transition(monkeypatch, changed):
    monkeypatch.setattr(ndc_product, "ensure_import_run_table", AsyncMock(side_effect=RuntimeError("original")))
    monkeypatch.setattr(ndc_product, "fail_ndc_attempt", AsyncMock(return_value=changed))
    calls = []

    def record_event(event_dict):
        calls.append(event_dict)
        raise ValueError("event unavailable")

    monkeypatch.setattr(ndc_product, "enqueue_status_event", record_event)
    with pytest.raises(RuntimeError, match="original"):
        await ndc_product.init_file({}, {"run_id": "synthetic-run"})
    assert len(calls) == changed


@pytest.mark.asyncio
async def test_cleanup_runs_after_status_failure_and_preserves_original_error(monkeypatch):
    monkeypatch.setattr(ndc_product, "ensure_import_run_table", AsyncMock(side_effect=RuntimeError("source failure")))
    monkeypatch.setattr(ndc_product, "fail_ndc_attempt", AsyncMock(side_effect=ValueError("status unavailable")))
    monkeypatch.setattr(ndc_product, "discard_ndc_stages", AsyncMock(side_effect=ValueError("cleanup unavailable")))
    with pytest.raises(RuntimeError, match="source failure"):
        await ndc_product.init_file({}, {"run_id": "synthetic-run"})
    ndc_product.discard_ndc_stages.assert_awaited_once()


@pytest.mark.asyncio
async def test_actual_download_receipts_and_archive_parser_share_http_transport_fixture(monkeypatch):
    manifest_url = "https://example.test/ndc/manifest.json"
    _section, payloads_by_url, temporary_paths = install_coordinator_sources(monkeypatch, manifest_url)
    batches = []

    async def acknowledge(records):
        batches.extend(records)

    manifest = await ndc_acquire.acquire_ndc_manifest(manifest_url, sample=False, max_records=1)
    acquisition = await ndc_acquire.consume_ndc_partitions(manifest, acknowledge)
    assert acquisition["complete"] and len(batches) == 2
    for receipt_dict in [acquisition["manifest"], *acquisition["partitions"]]:
        source_bytes = payloads_by_url[receipt_dict["url"]]
        assert receipt_dict["sha256"] == hashlib.sha256(source_bytes).hexdigest()
        assert receipt_dict["size_bytes"] == len(source_bytes)
    assert len(temporary_paths) == 3 and all(not path.exists() for path in temporary_paths)
